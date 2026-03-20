//! Database connection implementation

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;
use crate::parser::parse_sql;
use crate::storage::Storage;
use super::statement::{Statement as Stmt, Rows};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "thread-safe")]
use parking_lot::RwLock;

/// Database connection
pub struct Connection {
    /// Database path
    path: PathBuf,
    /// Connection flags
    flags: OpenFlags,
    /// Schema version
    schema_version: u32,
    /// Auto-commit mode
    autocommit: bool,
    /// In transaction
    in_transaction: bool,
    /// Storage snapshot used to restore state on rollback
    transaction_backup: Option<Storage>,
    /// Prepared statements cache
    #[cfg(feature = "thread-safe")]
    statements: RwLock<Vec<Arc<Stmt>>>,
    #[cfg(not(feature = "thread-safe"))]
    statements: Vec<Arc<Stmt>>,
    /// In-memory storage
    #[cfg(feature = "thread-safe")]
    storage: Arc<RwLock<Storage>>,
    #[cfg(not(feature = "thread-safe"))]
    storage: Arc<std::cell::RefCell<Storage>>,
    /// Whether this is an in-memory database
    is_memory: bool,
}

/// Connection open flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlags {
    /// Read-only mode
    pub read_only: bool,
    /// Create if not exists
    pub create: bool,
    /// URI filename
    pub uri: bool,
    /// Memory database
    pub memory: bool,
    /// No mutex
    pub no_mutex: bool,
    /// Full mutex
    pub full_mutex: bool,
    /// Shared cache
    pub shared_cache: bool,
    /// Private cache
    pub private_cache: bool,
}

impl Default for OpenFlags {
    fn default() -> Self {
        OpenFlags {
            read_only: false,
            create: true,
            uri: false,
            memory: false,
            no_mutex: false,
            full_mutex: cfg!(feature = "thread-safe"),
            shared_cache: false,
            private_cache: true,
        }
    }
}

impl OpenFlags {
    /// Create default read-write flags
    pub fn read_write() -> Self {
        OpenFlags {
            read_only: false,
            create: true,
            ..Default::default()
        }
    }

    /// Create read-only flags
    pub fn read_only() -> Self {
        OpenFlags {
            read_only: true,
            create: false,
            ..Default::default()
        }
    }

    /// Create memory database flags
    pub fn memory() -> Self {
        OpenFlags {
            memory: true,
            create: true,
            ..Default::default()
        }
    }
}

impl Connection {
    /// Open a database connection
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_flags(path, OpenFlags::read_write())
    }

    /// Open a database connection with flags
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Check if this is an in-memory database
        let is_memory = flags.memory || path.as_os_str() == ":memory:";

        // Load storage from file or create new
        let storage = if is_memory {
            Storage::new()
        } else if path.exists() && path.metadata().map(|m| m.len() > 0).unwrap_or(false) {
            // Load existing database
            Storage::load_from_file(&path)?
        } else {
            // Create new database
            if flags.create {
                if let Some(parent) = path.parent() {
                    if !parent.exists() {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| Error::io_err(&format!("cannot create directory: {}", e)))?;
                    }
                }
                // Create empty storage and save immediately to create the file
                let storage = Storage::new();
                storage.save_to_file(&path)?;
                storage
            } else {
                return Err(Error::sqlite(ErrorCode::CantOpen, "database file does not exist"));
            }
        };

        Ok(Connection {
            path,
            flags,
            schema_version: 0,
            autocommit: true,
            in_transaction: false,
            transaction_backup: None,
            #[cfg(feature = "thread-safe")]
            statements: RwLock::new(Vec::new()),
            #[cfg(not(feature = "thread-safe"))]
            statements: Vec::new(),
            #[cfg(feature = "thread-safe")]
            storage: Arc::new(RwLock::new(storage)),
            #[cfg(not(feature = "thread-safe"))]
            storage: Arc::new(std::cell::RefCell::new(storage)),
            is_memory,
        })
    }

    /// Open an in-memory database
    pub fn open_in_memory() -> Result<Self> {
        Self::open_with_flags(":memory:", OpenFlags::memory())
    }

    /// Execute a SQL statement
    pub fn execute(&self, sql: &str, params: impl IntoParams) -> Result<usize> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind(params)?;
        let mut count = 0;
        while stmt.step()? {
            count += 1;
        }
        Ok(count)
    }

    /// Execute a SQL statement with a callback
    pub fn execute_with_callback<F>(
        &self,
        sql: &str,
        params: impl IntoParams,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(&[Value]) -> Result<bool>,
    {
        let mut stmt = self.prepare(sql)?;
        stmt.bind(params)?;

        while stmt.step()? {
            let row = stmt.row()?;
            if !callback(&row)? {
                break;
            }
        }

        Ok(())
    }

    /// Prepare a SQL statement
    pub fn prepare(&self, sql: &str) -> Result<Stmt> {
        let ast = parse_sql(sql)?;

        // Create statement from AST
        let stmt = Stmt::new(self, ast, sql.to_string())?;

        Ok(stmt)
    }

    /// Execute a query and return rows
    pub fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind(params)?;
        Ok(Rows::new(stmt))
    }

    /// Execute a query returning a single row
    pub fn query_row(&self, sql: &str, params: impl IntoParams) -> Result<Option<Vec<Value>>> {
        let mut stmt = self.prepare(sql)?;
        stmt.bind(params)?;

        if stmt.step()? {
            let row = stmt.row()?;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    /// Execute a query returning a single scalar value
    pub fn query_scalar<T: FromValue>(&self, sql: &str, params: impl IntoParams) -> Result<T> {
        let row = self.query_row(sql, params)?;
        match row {
            Some(values) if !values.is_empty() => T::from_value(&values[0]),
            _ => Err(Error::sqlite(ErrorCode::Error, "No result returned")),
        }
    }

    /// Begin a transaction
    pub fn begin(&mut self) -> Result<()> {
        if self.in_transaction {
            return Err(Error::sqlite(ErrorCode::Error, "transaction already active"));
        }

        #[cfg(feature = "thread-safe")]
        {
            self.transaction_backup = Some(self.storage.read().clone());
        }
        #[cfg(not(feature = "thread-safe"))]
        {
            self.transaction_backup = Some(self.storage.borrow().clone());
        }

        self.autocommit = false;
        self.in_transaction = true;
        Ok(())
    }

    /// Commit the current transaction
    pub fn commit(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(Error::sqlite(ErrorCode::Error, "no active transaction"));
        }

        self.autocommit = true;
        self.in_transaction = false;
        self.transaction_backup = None;

        // Auto-save after commit for file databases
        if !self.is_memory {
            #[cfg(feature = "thread-safe")]
            {
                let storage = self.storage.read();
                storage.save_to_file(&self.path)?;
            }
            #[cfg(not(feature = "thread-safe"))]
            {
                let storage = self.storage.borrow();
                storage.save_to_file(&self.path)?;
            }
        }

        Ok(())
    }

    /// Rollback the current transaction
    pub fn rollback(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(Error::sqlite(ErrorCode::Error, "no active transaction"));
        }

        if let Some(snapshot) = self.transaction_backup.take() {
            #[cfg(feature = "thread-safe")]
            {
                *self.storage.write() = snapshot;
            }
            #[cfg(not(feature = "thread-safe"))]
            {
                *self.storage.borrow_mut() = snapshot;
            }
        }
        self.autocommit = true;
        self.in_transaction = false;
        Ok(())
    }

    /// Check if auto-commit is enabled
    pub fn is_autocommit(&self) -> bool {
        self.autocommit
    }

    /// Check if in a transaction
    pub fn is_in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Get the database path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the last insert rowid
    pub fn last_insert_rowid(&self) -> i64 {
        // TODO: Get actual last rowid from storage
        0
    }

    /// Get the number of changes from the last statement
    pub fn changes(&self) -> usize {
        // TODO: Get actual changes count
        0
    }

    /// Get the total changes since connection was opened
    pub fn total_changes(&self) -> usize {
        // TODO: Get actual total changes
        0
    }

    /// Get the last error message
    pub fn last_error_message(&self) -> &str {
        "not an error"
    }

    /// Get the last error code
    pub fn last_error_code(&self) -> ErrorCode {
        ErrorCode::Ok
    }

    /// Interrupt any running operation
    pub fn interrupt(&self) {
        // TODO: Implement interruption
    }

    /// Close the connection
    pub fn close(mut self) -> Result<()> {
        // Finalize all prepared statements
        #[cfg(feature = "thread-safe")]
        {
            self.statements.write().clear();
        }
        #[cfg(not(feature = "thread-safe"))]
        {
            self.statements.clear();
        }

        // If in transaction, rollback
        if self.in_transaction {
            if let Some(snapshot) = self.transaction_backup.take() {
                #[cfg(feature = "thread-safe")]
                {
                    *self.storage.write() = snapshot;
                }
                #[cfg(not(feature = "thread-safe"))]
                {
                    *self.storage.borrow_mut() = snapshot;
                }
            }
        }

        // Save to file if not in-memory database
        if !self.is_memory {
            #[cfg(feature = "thread-safe")]
            {
                let storage = self.storage.read();
                storage.save_to_file(&self.path)?;
            }
            #[cfg(not(feature = "thread-safe"))]
            {
                let storage = self.storage.borrow();
                storage.save_to_file(&self.path)?;
            }
        }

        Ok(())
    }

    /// Get schema version
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Set schema version
    pub fn set_schema_version(&mut self, version: u32) {
        self.schema_version = version;
    }

    /// Get storage (thread-safe version)
    #[cfg(feature = "thread-safe")]
    pub fn storage(&self) -> &Arc<RwLock<Storage>> {
        &self.storage
    }

    /// Get storage (single-threaded version)
    #[cfg(not(feature = "thread-safe"))]
    pub fn storage(&self) -> &Arc<std::cell::RefCell<Storage>> {
        &self.storage
    }

    /// Flush (save) the database to disk
    pub fn flush(&self) -> Result<()> {
        if self.is_memory {
            return Ok(());
        }

        #[cfg(feature = "thread-safe")]
        {
            let storage = self.storage.read();
            storage.save_to_file(&self.path)?;
        }
        #[cfg(not(feature = "thread-safe"))]
        {
            let storage = self.storage.borrow();
            storage.save_to_file(&self.path)?;
        }

        Ok(())
    }

    /// Check if this is an in-memory database
    pub fn is_memory(&self) -> bool {
        self.is_memory
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Save to file if not in-memory database and has changes
        if !self.is_memory {
            #[cfg(feature = "thread-safe")]
            {
                if let Some(storage) = self.storage.try_read() {
                    if storage.is_dirty() {
                        let _ = storage.save_to_file(&self.path);
                    }
                }
            }
            #[cfg(not(feature = "thread-safe"))]
            {
                let storage = self.storage.borrow();
                if storage.is_dirty() {
                    let _ = storage.save_to_file(&self.path);
                }
            }
        }
    }
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Connection {
            path: self.path.clone(),
            flags: self.flags,
            schema_version: self.schema_version,
            autocommit: self.autocommit,
            in_transaction: self.in_transaction,
            transaction_backup: self.transaction_backup.clone(),
            #[cfg(feature = "thread-safe")]
            statements: RwLock::new(Vec::new()), // Fresh statements list for clone
            #[cfg(not(feature = "thread-safe"))]
            statements: Vec::new(), // Fresh statements list for clone
            storage: self.storage.clone(), // Share storage
            is_memory: self.is_memory,
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("path", &self.path)
            .field("autocommit", &self.autocommit)
            .field("in_transaction", &self.in_transaction)
            .finish()
    }
}

/// Trait for types that can be converted to SQL parameters
pub trait IntoParams {
    /// Convert to parameter values
    fn into_params(self) -> Vec<Value>;
}

impl IntoParams for Vec<Value> {
    fn into_params(self) -> Vec<Value> {
        self
    }
}

impl IntoParams for &[Value] {
    fn into_params(self) -> Vec<Value> {
        self.to_vec()
    }
}

impl IntoParams for () {
    fn into_params(self) -> Vec<Value> {
        vec![]
    }
}

impl<const N: usize> IntoParams for [Value; N] {
    fn into_params(self) -> Vec<Value> {
        self.to_vec()
    }
}

impl IntoParams for &[&str] {
    fn into_params(self) -> Vec<Value> {
        self.iter().map(|s| Value::text(*s)).collect()
    }
}

impl IntoParams for &[i64] {
    fn into_params(self) -> Vec<Value> {
        self.iter().map(|i| Value::integer(*i)).collect()
    }
}

impl IntoParams for &[i32] {
    fn into_params(self) -> Vec<Value> {
        self.iter().map(|i| Value::integer(*i as i64)).collect()
    }
}

impl IntoParams for &[f64] {
    fn into_params(self) -> Vec<Value> {
        self.iter().map(|f| Value::float(*f)).collect()
    }
}

/// Trait for types that can be extracted from a SQL value
pub trait FromValue: Sized {
    /// Extract from a SQL value
    fn from_value(value: &Value) -> Result<Self>;
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(value.clone())
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_integer().ok_or_else(|| {
            Error::sqlite(ErrorCode::Mismatch, "expected integer")
        })
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_integer()
            .and_then(|i| i32::try_from(i).ok())
            .ok_or_else(|| Error::sqlite(ErrorCode::Mismatch, "expected 32-bit integer"))
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_float().ok_or_else(|| {
            Error::sqlite(ErrorCode::Mismatch, "expected float")
        })
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_text().map(|s| s.to_string()).ok_or_else(|| {
            Error::sqlite(ErrorCode::Mismatch, "expected text")
        })
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        value.as_blob().map(|b| b.to_vec()).ok_or_else(|| {
            Error::sqlite(ErrorCode::Mismatch, "expected blob")
        })
    }
}

impl FromValue for Option<i64> {
    fn from_value(value: &Value) -> Result<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            Some(FromValue::from_value(value)).transpose()
        }
    }
}

impl FromValue for Option<String> {
    fn from_value(value: &Value) -> Result<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            Some(FromValue::from_value(value)).transpose()
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i != 0),
            Value::Null => Ok(false),
            _ => Err(Error::sqlite(ErrorCode::Mismatch, "expected boolean")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_open_in_memory() {
        let conn = Connection::open_in_memory().unwrap();
        assert!(conn.is_autocommit());
        assert!(!conn.is_in_transaction());
    }

    #[test]
    fn test_connection_execute() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT)", []).unwrap();
    }

    #[test]
    fn test_connection_prepare() {
        let conn = Connection::open_in_memory().unwrap();
        let stmt = conn.prepare("SELECT 1").unwrap();
        // Statement should be valid
    }

    #[test]
    fn test_connection_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.begin().unwrap();
        assert!(conn.is_in_transaction());
        assert!(!conn.is_autocommit());
        conn.commit().unwrap();
        assert!(!conn.is_in_transaction());
        assert!(conn.is_autocommit());
    }

    #[test]
    fn test_connection_rollback() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.begin().unwrap();
        assert!(conn.is_in_transaction());
        conn.rollback().unwrap();
        assert!(!conn.is_in_transaction());
        assert!(conn.is_autocommit());
    }

    #[test]
    fn test_connection_begin_nested_fails() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.begin().unwrap();
        let err = conn.begin().unwrap_err();
        assert!(err.to_string().contains("transaction already active"));
    }

    #[test]
    fn test_connection_commit_without_transaction_fails() {
        let mut conn = Connection::open_in_memory().unwrap();
        let err = conn.commit().unwrap_err();
        assert!(err.to_string().contains("no active transaction"));
    }

    #[test]
    fn test_connection_rollback_without_transaction_fails() {
        let mut conn = Connection::open_in_memory().unwrap();
        let err = conn.rollback().unwrap_err();
        assert!(err.to_string().contains("no active transaction"));
    }

    #[test]
    fn test_into_params_vec() {
        let params: Vec<Value> = vec![Value::integer(1), Value::text("test")];
        let converted = params.into_params();
        assert_eq!(converted.len(), 2);
    }

    #[test]
    fn test_into_params_array() {
        let params = [Value::integer(1), Value::integer(2)];
        let converted = params.into_params();
        assert_eq!(converted.len(), 2);
    }

    #[test]
    fn test_into_params_str_slice() {
        let params = ["one", "two", "three"];
        let converted = (&params[..]).into_params();
        assert_eq!(converted.len(), 3);
        assert!(converted[0].is_text());
    }

    #[test]
    fn test_from_value_i64() {
        let v = Value::integer(42);
        let i: i64 = FromValue::from_value(&v).unwrap();
        assert_eq!(i, 42);
    }

    #[test]
    fn test_from_value_string() {
        let v = Value::text("hello");
        let s: String = FromValue::from_value(&v).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_from_value_option() {
        let v = Value::null();
        let opt: Option<i64> = FromValue::from_value(&v).unwrap();
        assert!(opt.is_none());

        let v = Value::integer(42);
        let opt: Option<i64> = FromValue::from_value(&v).unwrap();
        assert_eq!(opt, Some(42));
    }

    #[test]
    fn test_open_flags() {
        let flags = OpenFlags::read_write();
        assert!(!flags.read_only);
        assert!(flags.create);

        let flags = OpenFlags::read_only();
        assert!(flags.read_only);
        assert!(!flags.create);

        let flags = OpenFlags::memory();
        assert!(flags.memory);
        assert!(flags.create);
    }
}
