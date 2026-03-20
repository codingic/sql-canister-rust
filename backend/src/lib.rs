//! # SQLite-RS Canister
//!
//! This crate packages a pure Rust SQL engine as an Internet Computer canister.
//! The database runs in memory during execution and is snapshotted into stable
//! memory before upgrades so the data survives canister upgrades.

#![allow(dead_code)]
#![allow(unused_variables)]
#![warn(missing_docs)]

use candid::CandidType;
use ic_cdk::storage::{stable_restore, stable_save};
use serde::Deserialize;
use std::cell::RefCell;

// Core error types
pub mod error;

// Type definitions
pub mod types;

// Utility modules
pub mod util;

// Tokenizer module
pub mod tokenizer;

// Parser module
pub mod parser;

// Code generator
pub mod codegen;

// Virtual Database Engine
pub mod vdbe;

// Storage Engine modules
pub mod btree;
pub mod pager;
pub mod pcache;
pub mod wal;
pub mod vfs;
pub mod storage;

// Memory management
pub mod mem;

// Built-in functions
pub mod func;

// Public API
pub mod api;

// Re-exports for convenience
pub use api::connection::OpenFlags;
pub use api::{Connection, Row, Rows, Statement};
pub use error::{Error, ErrorCode, Result};
pub use types::{Affinity, Value, ValueType};

use parser::ast::Statement as SqlAstStatement;
use parser::parse_sql;
use storage::Storage;

/// Library version information
pub const VERSION: &str = "0.1.0";

/// SQLite format version number
pub const SQLITE_VERSION_NUMBER: i32 = 3052001;

/// Default page size
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

/// Maximum page size
pub const MAX_PAGE_SIZE: u32 = 65536;

/// Default cache size in pages
pub const DEFAULT_CACHE_SIZE: i32 = -2000;

/// Database header size in bytes
pub const DB_HEADER_SIZE: usize = 100;

/// Magic string for SQLite format
pub const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

const STABLE_STATE_VERSION: u32 = 1;

thread_local! {
    static DATABASE: RefCell<Connection> = RefCell::new(new_connection());
}

type ApiResult<T> = std::result::Result<T, String>;

#[derive(CandidType, Deserialize)]
struct StableState {
    version: u32,
    storage: Vec<u8>,
}

/// Candid-safe SQL value representation.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq)]
pub enum SqlValue {
    /// NULL value.
    Null,
    /// 64-bit signed integer.
    Integer(i64),
    /// 64-bit floating point number.
    Float(f64),
    /// UTF-8 text.
    Text(String),
    /// Binary blob.
    Blob(Vec<u8>),
}

/// Result returned by `execute`.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq)]
pub struct ExecuteResult {
    /// Human-readable execution status.
    pub message: String,
}

/// Result returned by `query`.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq)]
pub struct QueryResult {
    /// Result column names.
    pub columns: Vec<String>,
    /// Result rows.
    pub rows: Vec<Vec<SqlValue>>,
}

/// Lightweight database metadata.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq)]
pub struct DatabaseInfo {
    /// Existing table names.
    pub tables: Vec<String>,
}

/// Result returned by `execute_batch`.
#[derive(CandidType, Deserialize, Clone, Debug, PartialEq)]
pub struct BatchExecuteResult {
    /// Number of statements executed in this batch.
    pub statements_executed: u32,
    /// Whether the batch changed schema or row data.
    pub changed_schema_or_data: bool,
    /// Whether the batch produced a query result.
    pub has_query_result: bool,
    /// The last query result produced by the batch.
    pub last_query_result: QueryResult,
}

struct StatementExecution {
    changed_schema_or_data: bool,
    query_result: Option<QueryResult>,
}

impl From<Value> for SqlValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => SqlValue::Null,
            Value::Integer(i) => SqlValue::Integer(i),
            Value::Float(f) => SqlValue::Float(f),
            Value::Text(text) => SqlValue::Text(text),
            Value::Blob(bytes) => SqlValue::Blob(bytes),
        }
    }
}

fn new_connection() -> Connection {
    Connection::open_in_memory().expect("failed to initialize in-memory database")
}

fn error_message(err: Error) -> String {
    err.to_string()
}

fn encode_storage(storage: &Storage) -> ApiResult<Vec<u8>> {
    bincode::serialize(storage)
        .map_err(|err| format!("failed to serialize database snapshot: {err}"))
}

fn decode_storage(bytes: &[u8]) -> ApiResult<Storage> {
    bincode::deserialize(bytes)
        .map_err(|err| format!("failed to deserialize database snapshot: {err}"))
}

fn current_storage() -> Storage {
    DATABASE.with(|database| {
        let connection = database.borrow();
        #[cfg(feature = "thread-safe")]
        {
            connection.storage().read().clone()
        }
        #[cfg(not(feature = "thread-safe"))]
        {
            let snapshot = connection.storage().borrow().clone();
            snapshot
        }
    })
}

fn restore_storage(storage: Storage) {
    DATABASE.with(|database| {
        *database.borrow_mut() = new_connection();

        let connection = database.borrow();
        #[cfg(feature = "thread-safe")]
        {
            *connection.storage().write() = storage;
        }
        #[cfg(not(feature = "thread-safe"))]
        {
            *connection.storage().borrow_mut() = storage;
        }
    });
}

fn normalize_columns(columns: Vec<String>, row_width: usize) -> Vec<String> {
    if row_width == 0 {
        return columns;
    }

    if columns.len() == row_width && columns.iter().all(|name| !name.is_empty()) {
        return columns;
    }

    if columns.len() == 1 && (columns[0] == "*" || columns[0].ends_with(".*")) {
        return (0..row_width)
            .map(|index| format!("column_{index}"))
            .collect();
    }

    let mut normalized = columns;
    while normalized.len() < row_width {
        normalized.push(format!("column_{}", normalized.len()));
    }
    normalized
}

fn empty_query_result() -> QueryResult {
    QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
    }
}

fn run_query(database: &RefCell<Connection>, sql: &str) -> ApiResult<QueryResult> {
    ensure_query_statement(sql)?;
    let connection = database.borrow();
    let mut statement = connection.prepare(sql).map_err(error_message)?;
    let mut rows = Vec::new();

    while statement.step().map_err(error_message)? {
        let row = statement.row().map_err(error_message)?;
        rows.push(row.into_iter().map(SqlValue::from).collect::<Vec<_>>());
    }

    let columns = normalize_columns(
        statement.column_names.clone(),
        rows.first().map_or(0, Vec::len),
    );

    Ok(QueryResult { columns, rows })
}

fn run_batch_statement(database: &RefCell<Connection>, sql: &str) -> ApiResult<StatementExecution> {
    let ast = parse_sql(sql).map_err(error_message)?;

    match ast {
        SqlAstStatement::Select(_) | SqlAstStatement::CompoundSelect(_) => Ok(StatementExecution {
            changed_schema_or_data: false,
            query_result: Some(run_query(database, sql)?),
        }),
        SqlAstStatement::Begin(_) => {
            let mut connection = database.borrow_mut();
            connection.begin().map_err(error_message)?;
            Ok(StatementExecution {
                changed_schema_or_data: false,
                query_result: None,
            })
        }
        SqlAstStatement::Commit => {
            let mut connection = database.borrow_mut();
            connection.commit().map_err(error_message)?;
            Ok(StatementExecution {
                changed_schema_or_data: false,
                query_result: None,
            })
        }
        SqlAstStatement::Rollback(rollback) => {
            if rollback.savepoint.is_some() {
                return Err("ROLLBACK TO SAVEPOINT is not supported".to_string());
            }

            let mut connection = database.borrow_mut();
            connection.rollback().map_err(error_message)?;
            Ok(StatementExecution {
                changed_schema_or_data: false,
                query_result: None,
            })
        }
        SqlAstStatement::CreateTable(_)
        | SqlAstStatement::AlterTable(_)
        | SqlAstStatement::Insert(_)
        | SqlAstStatement::Update(_)
        | SqlAstStatement::Delete(_)
        | SqlAstStatement::Drop(_) => {
            let connection = database.borrow();
            connection.execute(sql, ()).map_err(error_message)?;
            Ok(StatementExecution {
                changed_schema_or_data: true,
                query_result: None,
            })
        }
        other => Err(format!("unsupported batch statement: {}", other)),
    }
}

fn ensure_query_statement(sql: &str) -> ApiResult<()> {
    match parse_sql(sql).map_err(error_message)? {
        SqlAstStatement::Select(_) | SqlAstStatement::CompoundSelect(_) => Ok(()),
        _ => Err("`query` only supports SELECT statements".to_string()),
    }
}

fn ensure_execute_statement(sql: &str) -> ApiResult<()> {
    match parse_sql(sql).map_err(error_message)? {
        SqlAstStatement::Select(_) | SqlAstStatement::CompoundSelect(_) => Err("`execute` does not support SELECT statements, use `query` instead".to_string()),
        SqlAstStatement::Begin(_)
        | SqlAstStatement::Commit
        | SqlAstStatement::Rollback(_)
        | SqlAstStatement::CreateTable(_)
        | SqlAstStatement::AlterTable(_)
        | SqlAstStatement::Insert(_)
        | SqlAstStatement::Update(_)
        | SqlAstStatement::Delete(_)
        | SqlAstStatement::Drop(_) => Ok(()),
        other => Err(format!("unsupported execute statement: {}", other)),
    }
}

/// Reset the canister to an empty in-memory database on first install.
#[ic_cdk::init]
fn init() {
    restore_storage(Storage::new());
}

/// Persist the database snapshot into stable memory before upgrade.
#[ic_cdk::pre_upgrade]
fn pre_upgrade() {
    let snapshot = StableState {
        version: STABLE_STATE_VERSION,
        storage: encode_storage(&current_storage()).unwrap_or_else(|message| ic_cdk::trap(&message)),
    };

    stable_save((snapshot,))
        .unwrap_or_else(|err| ic_cdk::trap(&format!("failed to save stable state: {err}")));
}

/// Restore the database snapshot from stable memory after upgrade.
#[ic_cdk::post_upgrade]
fn post_upgrade() {
    let (state,) = stable_restore::<(StableState,)>()
        .unwrap_or_else(|err| ic_cdk::trap(&format!("failed to restore stable state: {err}")));

    if state.version != STABLE_STATE_VERSION {
        ic_cdk::trap(&format!(
            "unsupported stable state version: expected {}, got {}",
            STABLE_STATE_VERSION, state.version
        ));
    }

    let storage = decode_storage(&state.storage)
        .unwrap_or_else(|message| ic_cdk::trap(&message));
    restore_storage(storage);
}

/// Execute a write-oriented SQL statement such as `CREATE`, `INSERT`, `UPDATE`, or `DELETE`.
#[ic_cdk::update]
fn execute(sql: String) -> ApiResult<ExecuteResult> {
    DATABASE.with(|database| {
        let ast = parse_sql(&sql).map_err(error_message)?;
        ensure_execute_statement(&sql)?;

        match ast {
            SqlAstStatement::Begin(_) => {
                let mut connection = database.borrow_mut();
                connection.begin().map_err(error_message)?;
            }
            SqlAstStatement::Commit => {
                let mut connection = database.borrow_mut();
                connection.commit().map_err(error_message)?;
            }
            SqlAstStatement::Rollback(rollback) => {
                if rollback.savepoint.is_some() {
                    return Err("ROLLBACK TO SAVEPOINT is not supported".to_string());
                }
                let mut connection = database.borrow_mut();
                connection.rollback().map_err(error_message)?;
            }
            _ => {
                let connection = database.borrow();
                connection.execute(&sql, ()).map_err(error_message)?;
            }
        }

        Ok(ExecuteResult {
            message: "SQL statement executed successfully".to_string(),
        })
    })
}

/// Execute a read-only SQL statement and return the result set.
#[ic_cdk::query]
fn query(sql: String) -> ApiResult<QueryResult> {
    DATABASE.with(|database| run_query(database, &sql))
}

/// Execute multiple SQL statements in a single canister call.
#[ic_cdk::update]
fn execute_batch(statements: Vec<String>) -> ApiResult<BatchExecuteResult> {
    DATABASE.with(|database| {
        let mut statements_executed = 0u32;
        let mut changed_schema_or_data = false;
        let mut has_query_result = false;
        let mut last_query_result = empty_query_result();

        for statement in statements {
            let sql = statement.trim();
            if sql.is_empty() {
                continue;
            }

            let execution = run_batch_statement(database, sql)?;
            statements_executed = statements_executed
                .checked_add(1)
                .ok_or_else(|| "too many statements in batch".to_string())?;
            changed_schema_or_data |= execution.changed_schema_or_data;

            if let Some(query_result) = execution.query_result {
                last_query_result = query_result;
                has_query_result = true;
            }
        }

        if statements_executed == 0 {
            return Err("`execute_batch` requires at least one non-empty statement".to_string());
        }

        Ok(BatchExecuteResult {
            statements_executed,
            changed_schema_or_data,
            has_query_result,
            last_query_result,
        })
    })
}

/// List the current table names stored in the canister database.
#[ic_cdk::query]
fn info() -> DatabaseInfo {
    let mut tables = current_storage()
        .tables
        .values()
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    tables.sort();
    DatabaseInfo { tables }
}

::candid::export_service!();

/// Return a raw C string pointer to the generated Candid interface.
#[no_mangle]
pub fn get_candid_pointer() -> *mut std::os::raw::c_char {
    let candid = std::ffi::CString::new(__export_service())
        .expect("generated Candid interface contained an interior NUL byte");
    candid.into_raw()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_PAGE_SIZE, 4096);
        assert_eq!(MAX_PAGE_SIZE, 65536);
        assert_eq!(DB_HEADER_SIZE, 100);
    }

    #[test]
    fn test_storage_roundtrip() {
        let storage = Storage::new();
        let bytes = encode_storage(&storage).unwrap();
        let restored = decode_storage(&bytes).unwrap();
        assert!(restored.tables.is_empty());
    }
}
