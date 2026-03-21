//! Simple in-memory storage layer

#![allow(missing_docs)]

use crate::error::{Error, ErrorCode, Result};
use crate::api::statement::evaluate_check_constraint;
use crate::parser::ast::Expr;
use crate::types::Value;
use serde::{Serialize, Deserialize};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

/// Magic number for database file format
const MAGIC_NUMBER: &[u8; 8] = b"SQLITERS";
/// Current file format version
const FILE_VERSION: u32 = 1;

/// A table column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub col_type: String,
    pub not_null: bool,
    pub primary_key: bool,
    pub unique: bool,
    #[serde(default)]
    pub check_constraints: Vec<Expr>,
    pub default_value: Option<Value>,
}

/// A table in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub next_rowid: i64,
    #[serde(skip, default)]
    column_lookup: HashMap<String, usize>,
    #[serde(skip, default)]
    unique_lookup: BTreeMap<usize, BTreeMap<Value, usize>>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        let column_lookup = Self::build_column_lookup(&columns);
        let unique_lookup = Self::build_unique_lookup(&name, &columns, &[])
            .expect("empty tables must satisfy uniqueness constraints");
        Table {
            name,
            columns,
            rows: Vec::new(),
            next_rowid: 1,
            column_lookup,
            unique_lookup,
        }
    }

    fn build_column_lookup(columns: &[Column]) -> HashMap<String, usize> {
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.to_lowercase(), index))
            .collect()
    }

    pub fn rebuild_column_lookup(&mut self) {
        self.column_lookup = Self::build_column_lookup(&self.columns);
    }

    pub fn rebuild_runtime_state(&mut self) -> Result<()> {
        self.column_lookup = Self::build_column_lookup(&self.columns);
        self.unique_lookup = Self::build_unique_lookup(&self.name, &self.columns, &self.rows)?;
        Ok(())
    }

    fn build_unique_lookup(
        table_name: &str,
        columns: &[Column],
        rows: &[Vec<Value>],
    ) -> Result<BTreeMap<usize, BTreeMap<Value, usize>>> {
        let mut unique_lookup = BTreeMap::new();

        for (row_index, row) in rows.iter().enumerate() {
            for (column_index, column) in columns.iter().enumerate() {
                let value = row.get(column_index).unwrap_or(&Value::Null);

                if (column.not_null || column.primary_key) && value.is_null() {
                    return Err(Error::sqlite(
                        ErrorCode::Constraint,
                        format!("NOT NULL constraint failed: {}.{}", table_name, column.name),
                    ));
                }

                for check_expr in &column.check_constraints {
                    if !evaluate_check_constraint(check_expr, table_name, columns, row)? {
                        return Err(Error::sqlite(
                            ErrorCode::Constraint,
                            format!("CHECK constraint failed: {}.{}", table_name, column.name),
                        ));
                    }
                }

                if !(column.primary_key || column.unique) || value.is_null() {
                    continue;
                }

                let existing = unique_lookup
                    .entry(column_index)
                    .or_insert_with(BTreeMap::new)
                    .insert(value.clone(), row_index);

                if existing.is_some_and(|existing_row_index| existing_row_index != row_index) {
                    let constraint_name = if column.primary_key {
                        "PRIMARY KEY"
                    } else {
                        "UNIQUE"
                    };
                    return Err(Error::sqlite(
                        ErrorCode::Constraint,
                        format!("{} constraint failed: {}.{}", constraint_name, table_name, column.name),
                    ));
                }
            }
        }

        Ok(unique_lookup)
    }

    fn record_unique_values(&mut self, row_index: usize, row: &[Value]) {
        for (column_index, column) in self.columns.iter().enumerate() {
            if !(column.primary_key || column.unique) {
                continue;
            }

            let value = row.get(column_index).unwrap_or(&Value::Null);
            if value.is_null() {
                continue;
            }

            self.unique_lookup
                .entry(column_index)
                .or_insert_with(BTreeMap::new)
                .insert(value.clone(), row_index);
        }
    }

    /// Find primary key column index
    pub fn primary_key_index(&self) -> Option<usize> {
        self.columns.iter().position(|c| c.primary_key)
    }

    pub fn primary_key_row_index(&self, value: &Value) -> Option<usize> {
        let primary_key_index = self.primary_key_index()?;
        self.unique_lookup
            .get(&primary_key_index)
            .and_then(|lookup| lookup.get(value))
            .copied()
    }

    pub fn unique_row_index(&self, column_index: usize, value: &Value) -> Option<usize> {
        self.unique_lookup
            .get(&column_index)
            .and_then(|lookup| lookup.get(value))
            .copied()
    }

    pub fn is_unique_column(&self, column_index: usize) -> bool {
        self.columns
            .get(column_index)
            .is_some_and(|column| column.primary_key || column.unique)
    }

    pub fn row_at(&self, row_index: usize) -> Option<&[Value]> {
        self.rows.get(row_index).map(Vec::as_slice)
    }

    /// Get column index by name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_lookup
            .get(&name.to_lowercase())
            .copied()
            .or_else(|| self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name)))
    }
}

/// In-memory database storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Storage {
    pub tables: HashMap<String, Table>,
    #[serde(default)]
    dirty: bool,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            tables: HashMap::new(),
            dirty: false,
        }
    }

    /// Load storage from a file
    pub fn load_from_file(path: &Path) -> Result<Self> {
        use byteorder::{LittleEndian, ReadBytesExt};

        let file = File::open(path)
            .map_err(|e| Error::sqlite(ErrorCode::CantOpen, &format!("cannot open file: {}", e)))?;

        let mut reader = BufReader::new(file);

        // Read and verify magic number
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)
            .map_err(|e| Error::sqlite(ErrorCode::Corrupt, &format!("cannot read magic number: {}", e)))?;

        if &magic != MAGIC_NUMBER {
            return Err(Error::sqlite(ErrorCode::Corrupt, "invalid database file format"));
        }

        // Read version
        let version = reader.read_u32::<LittleEndian>()
            .map_err(|e| Error::sqlite(ErrorCode::Corrupt, &format!("cannot read version: {}", e)))?;

        if version > FILE_VERSION {
            return Err(Error::sqlite(ErrorCode::Corrupt, &format!("unsupported file version: {}", version)));
        }

        // Read schema size
        let schema_size = reader.read_u64::<LittleEndian>()
            .map_err(|e| Error::sqlite(ErrorCode::Corrupt, &format!("cannot read schema size: {}", e)))?;

        // Read schema data
        let mut schema_bytes = vec![0u8; schema_size as usize];
        reader.read_exact(&mut schema_bytes)
            .map_err(|e| Error::sqlite(ErrorCode::Corrupt, &format!("cannot read schema: {}", e)))?;

        // Deserialize storage
        let mut storage: Storage = bincode::deserialize(&schema_bytes)
            .map_err(|e| Error::sqlite(ErrorCode::Corrupt, &format!("cannot deserialize: {}", e)))?;

        for table in storage.tables.values_mut() {
            table
                .rebuild_runtime_state()
                .map_err(|e| Error::sqlite(ErrorCode::Corrupt, &format!("invalid table state: {}", e)))?;
        }

        Ok(storage)
    }

    /// Save storage to a file
    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        use byteorder::{LittleEndian, WriteBytesExt};

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("cannot create directory: {}", e)))?;
        }

        let file = File::create(path)
            .map_err(|e| Error::sqlite(ErrorCode::CantOpen, &format!("cannot create file: {}", e)))?;

        let mut writer = BufWriter::new(file);

        // Write magic number
        writer.write_all(MAGIC_NUMBER)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("cannot write magic: {}", e)))?;

        // Write version
        writer.write_u32::<LittleEndian>(FILE_VERSION)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("cannot write version: {}", e)))?;

        // Serialize storage
        let schema_bytes = bincode::serialize(self)
            .map_err(|e| Error::sqlite(ErrorCode::Error, &format!("serialization error: {}", e)))?;

        // Write schema size
        writer.write_u64::<LittleEndian>(schema_bytes.len() as u64)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("cannot write size: {}", e)))?;

        // Write schema data
        writer.write_all(&schema_bytes)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("cannot write data: {}", e)))?;

        writer.flush()
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("cannot flush: {}", e)))?;

        Ok(())
    }

    /// Check if storage has unsaved changes
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark storage as having unsaved changes
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Mark storage as saved
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    /// Create a table
    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<()> {
        let table_name = name.to_lowercase();
        if self.tables.contains_key(&table_name) {
            return Err(Error::sqlite(ErrorCode::Error, &format!("table {} already exists", name)));
        }
        self.tables.insert(table_name, Table::new(name.to_string(), columns));
        self.dirty = true;
        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let table_name = name.to_lowercase();
        if self.tables.remove(&table_name).is_none() {
            return Err(Error::sqlite(ErrorCode::Error, &format!("no such table: {}", name)));
        }
        self.dirty = true;
        Ok(())
    }

    /// Rename a table.
    pub fn rename_table(&mut self, name: &str, new_name: &str) -> Result<()> {
        let table_name = name.to_lowercase();
        let new_table_name = new_name.to_lowercase();

        if table_name != new_table_name && self.tables.contains_key(&new_table_name) {
            return Err(Error::sqlite(
                ErrorCode::Error,
                &format!("table {} already exists", new_name),
            ));
        }

        let mut table = self.tables.remove(&table_name).ok_or_else(|| {
            Error::sqlite(ErrorCode::Error, &format!("no such table: {}", name))
        })?;
        table.name = new_name.to_string();
        self.tables.insert(new_table_name, table);
        self.dirty = true;
        Ok(())
    }

    /// Rename a column on a table.
    pub fn rename_column(&mut self, table_name: &str, old_name: &str, new_name: &str) -> Result<()> {
        let table = self.get_table_mut(table_name).ok_or_else(|| {
            Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name))
        })?;

        let column_index = table.column_index(old_name).ok_or_else(|| {
            Error::sqlite(ErrorCode::Error, &format!("no such column: {}", old_name))
        })?;

        if table
            .column_index(new_name)
            .is_some_and(|index| index != column_index)
        {
            return Err(Error::sqlite(
                ErrorCode::Error,
                &format!("duplicate column name: {}", new_name),
            ));
        }

        table.columns[column_index].name = new_name.to_string();
        table.rebuild_runtime_state()?;
        self.dirty = true;
        Ok(())
    }

    /// Add a column to an existing table.
    pub fn add_column(&mut self, table_name: &str, column: Column) -> Result<()> {
        let table = self.get_table_mut(table_name).ok_or_else(|| {
            Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name))
        })?;

        if table.column_index(&column.name).is_some() {
            return Err(Error::sqlite(
                ErrorCode::Error,
                &format!("duplicate column name: {}", column.name),
            ));
        }

        if column.primary_key {
            return Err(Error::sqlite(
                ErrorCode::Error,
                "ALTER TABLE ADD COLUMN does not support PRIMARY KEY",
            ));
        }

        if column.unique {
            return Err(Error::sqlite(
                ErrorCode::Error,
                "ALTER TABLE ADD COLUMN does not support UNIQUE",
            ));
        }

        let fill_value = column.default_value.clone().unwrap_or(Value::Null);
        if column.not_null && fill_value.is_null() {
            return Err(Error::sqlite(
                ErrorCode::Constraint,
                "Cannot add a NOT NULL column without a non-NULL default value",
            ));
        }

        for row in &mut table.rows {
            row.push(fill_value.clone());
        }

        table.columns.push(column);
        table.rebuild_runtime_state()?;
        self.dirty = true;
        Ok(())
    }

    /// Get a table by name
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(&name.to_lowercase())
    }

    /// Get a mutable table by name
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(&name.to_lowercase())
    }

    pub fn primary_key_row_index(&self, table_name: &str, value: &Value) -> Result<Option<usize>> {
        let table = self.get_table(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;
        Ok(table.primary_key_row_index(value))
    }

    pub fn row_at(&self, table_name: &str, row_index: usize) -> Result<Option<&[Value]>> {
        let table = self.get_table(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;
        Ok(table.row_at(row_index))
    }

    pub fn delete_row_index(&mut self, table_name: &str, row_index: usize) -> Result<usize> {
        let table = self.get_table_mut(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        if row_index >= table.rows.len() {
            return Ok(0);
        }

        table.rows.remove(row_index);
        table.rebuild_runtime_state()?;
        self.dirty = true;
        Ok(1)
    }

    pub fn update_row_index(&mut self, table_name: &str, updates: &[(String, Value)], row_index: usize) -> Result<usize> {
        let candidate_rows = {
            let table = self.get_table(table_name)
                .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

            if row_index >= table.rows.len() {
                return Ok(0);
            }

            let col_indices = updates
                .iter()
                .map(|(col_name, value)| {
                    table
                        .column_index(col_name)
                        .map(|idx| (idx, value.clone()))
                        .ok_or_else(|| {
                            Error::sqlite(ErrorCode::Error, &format!("no such column: {}", col_name))
                        })
                })
                .collect::<Result<Vec<_>>>()?;

            let mut candidate_rows = table.rows.clone();
            let candidate = &mut candidate_rows[row_index];
            for (column_index, value) in &col_indices {
                candidate[*column_index] = value.clone();
            }

            Table::build_unique_lookup(&table.name, &table.columns, &candidate_rows)?;
            candidate_rows
        };

        let table = self.get_table_mut(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;
        table.rows = candidate_rows;
        table.rebuild_runtime_state()?;
        self.dirty = true;
        Ok(1)
    }

    pub fn prepare_insert_row(&self, table_name: &str, values: Vec<Value>) -> Result<Vec<Value>> {
        let table = self.get_table(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        let mut row = vec![Value::Null; table.columns.len()];
        for (index, value) in values.into_iter().enumerate() {
            if index < row.len() {
                row[index] = value;
            }
        }

        if let Some(pk_idx) = table.primary_key_index() {
            if row[pk_idx].is_null() {
                row[pk_idx] = Value::integer(table.next_rowid);
            }
        }

        Ok(row)
    }

    pub fn validate_non_unique_row(&self, table_name: &str, row: &[Value]) -> Result<()> {
        let table = self.get_table(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;
        Self::validate_row_constraints(table, row, None, false)
    }

    /// Insert a row into a table
    pub fn insert(&mut self, table_name: &str, values: Vec<Value>) -> Result<i64> {
        let row = self.prepare_insert_row(table_name, values)?;

        {
            let table = self.get_table(table_name)
                .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;
            Self::validate_row(table, &row, None)?;
        }

        let table = self.get_table_mut(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        let rowid = table.next_rowid;
        table.next_rowid += 1;
        table.rows.push(row);
        let inserted_row_index = table.rows.len() - 1;
        let inserted_row = table.rows[inserted_row_index].clone();
        table.record_unique_values(inserted_row_index, &inserted_row);
        self.dirty = true;

        Ok(rowid)
    }

    /// Delete rows from a table matching a predicate
    pub fn delete<F>(&mut self, table_name: &str, predicate: F) -> Result<usize>
    where
        F: Fn(&[Value]) -> bool,
    {
        let table = self.get_table_mut(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        let original_len = table.rows.len();
        table.rows.retain(|row| !predicate(row));
        let deleted = original_len - table.rows.len();
        if deleted > 0 {
            table.rebuild_runtime_state()?;
            self.dirty = true;
        }
        Ok(deleted)
    }

    /// Update rows in a table matching a predicate
    pub fn update<F>(&mut self, table_name: &str, updates: &[(String, Value)], predicate: F) -> Result<usize>
    where
        F: Fn(&[Value]) -> bool,
    {
        // First, get the column indices
        let col_indices: Vec<(usize, Value)>;
        {
            let table = self.get_table(table_name)
                .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

            col_indices = updates
                .iter()
                .map(|(col_name, value)| {
                    table
                        .column_index(col_name)
                        .map(|idx| (idx, value.clone()))
                        .ok_or_else(|| {
                            Error::sqlite(ErrorCode::Error, &format!("no such column: {}", col_name))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
        }

        let (candidate_rows, count) = {
            let table = self.get_table(table_name)
                .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

            let mut candidate_rows = table.rows.clone();
            let mut count = 0;
            for (row_index, row) in table.rows.iter().enumerate() {
                if predicate(row) {
                    let candidate = &mut candidate_rows[row_index];
                    for (idx, value) in &col_indices {
                        candidate[*idx] = value.clone();
                    }
                    count += 1;
                }
            }

            if count > 0 {
                Table::build_unique_lookup(&table.name, &table.columns, &candidate_rows)?;
            }

            (candidate_rows, count)
        };

        // Now mutate the table
        let table = self.get_table_mut(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        if count > 0 {
            table.rows = candidate_rows;
            table.rebuild_runtime_state()?;
            self.dirty = true;
        }
        Ok(count)
    }

    /// Query rows from a table
    pub fn query<'a, F>(&'a self, table_name: &str, predicate: F) -> Result<Vec<&'a [Value]>>
    where
        F: Fn(&[Value]) -> bool,
    {
        let table = self.get_table(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        Ok(table.rows.iter()
            .filter(|row| predicate(row))
            .map(|row| row.as_slice())
            .collect())
    }

    fn validate_row(table: &Table, row: &[Value], current_row_index: Option<usize>) -> Result<()> {
        Self::validate_row_constraints(table, row, current_row_index, true)
    }

    fn validate_row_constraints(
        table: &Table,
        row: &[Value],
        current_row_index: Option<usize>,
        include_unique: bool,
    ) -> Result<()> {
        for (column_index, column) in table.columns.iter().enumerate() {
            let value = row.get(column_index).unwrap_or(&Value::Null);

            if (column.not_null || column.primary_key) && value.is_null() {
                return Err(Error::sqlite(
                    ErrorCode::Constraint,
                    format!("NOT NULL constraint failed: {}.{}", table.name, column.name),
                ));
            }

            for check_expr in &column.check_constraints {
                if !evaluate_check_constraint(check_expr, &table.name, &table.columns, row)? {
                    return Err(Error::sqlite(
                        ErrorCode::Constraint,
                        format!("CHECK constraint failed: {}.{}", table.name, column.name),
                    ));
                }
            }

            if include_unique && (column.primary_key || column.unique) && !value.is_null() {
                let duplicate_exists = table
                    .unique_lookup
                    .get(&column_index)
                    .and_then(|column_lookup| column_lookup.get(value))
                    .is_some_and(|row_index| current_row_index != Some(*row_index));

                if duplicate_exists {
                    let constraint_name = if column.primary_key {
                        "PRIMARY KEY"
                    } else {
                        "UNIQUE"
                    };
                    return Err(Error::sqlite(
                        ErrorCode::Constraint,
                        format!("{} constraint failed: {}.{}", constraint_name, table.name, column.name),
                    ));
                }
            }
        }

        Ok(())
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            col_type: "TEXT".to_string(),
            not_null: false,
            primary_key: false,
            unique: false,
            check_constraints: Vec::new(),
            default_value: None,
        }
    }

    #[test]
    fn insert_enforces_primary_key_uniqueness() {
        let mut storage = Storage::new();
        let mut id_column = test_column("id");
        id_column.primary_key = true;
        storage
            .create_table("users", vec![id_column, test_column("name")])
            .unwrap();

        storage
            .insert("users", vec![Value::integer(1), Value::text("alice")])
            .unwrap();

        let error = storage
            .insert("users", vec![Value::integer(1), Value::text("bob")])
            .unwrap_err();
        assert!(error.to_string().contains("PRIMARY KEY constraint failed"));
    }

    #[test]
    fn update_enforces_unique_constraint() {
        let mut storage = Storage::new();
        let id_column = {
            let mut column = test_column("id");
            column.primary_key = true;
            column
        };
        let email_column = {
            let mut column = test_column("email");
            column.unique = true;
            column
        };

        storage
            .create_table("users", vec![id_column, email_column])
            .unwrap();
        storage
            .insert("users", vec![Value::integer(1), Value::text("a@example.com")])
            .unwrap();
        storage
            .insert("users", vec![Value::integer(2), Value::text("b@example.com")])
            .unwrap();

        let error = storage
            .update(
                "users",
                &[("email".to_string(), Value::text("a@example.com"))],
                |row| row.first() == Some(&Value::integer(2)),
            )
            .unwrap_err();
        assert!(error.to_string().contains("UNIQUE constraint failed"));
    }

    #[test]
    fn rename_table_updates_lookup() {
        let mut storage = Storage::new();
        storage
            .create_table("users", vec![test_column("id")])
            .unwrap();

        storage.rename_table("users", "members").unwrap();

        assert!(storage.get_table("users").is_none());
        assert!(storage.get_table("members").is_some());
        assert_eq!(storage.get_table("members").unwrap().name, "members");
    }

    #[test]
    fn rename_column_updates_lookup() {
        let mut storage = Storage::new();
        storage
            .create_table("users", vec![test_column("name")])
            .unwrap();

        storage.rename_column("users", "name", "display_name").unwrap();

        let table = storage.get_table("users").unwrap();
        assert!(table.column_index("name").is_none());
        assert_eq!(table.column_index("display_name"), Some(0));
    }

    #[test]
    fn add_column_backfills_existing_rows_with_default() {
        let mut storage = Storage::new();
        storage
            .create_table("users", vec![test_column("id")])
            .unwrap();
        storage
            .insert("users", vec![Value::integer(1)])
            .unwrap();

        let mut status_column = test_column("status");
        status_column.not_null = true;
        status_column.default_value = Some(Value::text("active"));

        storage.add_column("users", status_column).unwrap();

        let table = storage.get_table("users").unwrap();
        assert_eq!(table.column_index("status"), Some(1));
        assert_eq!(table.rows, vec![vec![Value::integer(1), Value::text("active")]]);
    }

    #[test]
    fn add_column_rejects_not_null_without_default() {
        let mut storage = Storage::new();
        storage
            .create_table("users", vec![test_column("id")])
            .unwrap();
        storage
            .insert("users", vec![Value::integer(1)])
            .unwrap();

        let mut status_column = test_column("status");
        status_column.not_null = true;

        let error = storage.add_column("users", status_column).unwrap_err();
        assert!(error
            .to_string()
            .contains("Cannot add a NOT NULL column without a non-NULL default value"));
    }

    #[test]
    fn update_detects_unique_conflicts_across_multiple_rows() {
        let mut storage = Storage::new();
        let id_column = {
            let mut column = test_column("id");
            column.primary_key = true;
            column
        };
        let email_column = {
            let mut column = test_column("email");
            column.unique = true;
            column
        };

        storage
            .create_table("users", vec![id_column, email_column])
            .unwrap();
        storage
            .insert("users", vec![Value::integer(1), Value::text("a@example.com")])
            .unwrap();
        storage
            .insert("users", vec![Value::integer(2), Value::text("b@example.com")])
            .unwrap();

        let error = storage
            .update(
                "users",
                &[("email".to_string(), Value::text("shared@example.com"))],
                |_| true,
            )
            .unwrap_err();

        assert!(error.to_string().contains("UNIQUE constraint failed"));
    }

    #[test]
    fn load_rebuilds_unique_lookup_state() {
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sql_canister_storage_{unique_suffix}.bin"));

        let mut storage = Storage::new();
        let mut id_column = test_column("id");
        id_column.primary_key = true;
        storage
            .create_table("users", vec![id_column, test_column("name")])
            .unwrap();
        storage
            .insert("users", vec![Value::integer(1), Value::text("alice")])
            .unwrap();
        storage.save_to_file(&path).unwrap();

        let mut loaded = Storage::load_from_file(&path).unwrap();
        let error = loaded
            .insert("users", vec![Value::integer(1), Value::text("bob")])
            .unwrap_err();

        let _ = std::fs::remove_file(&path);

        assert!(error.to_string().contains("PRIMARY KEY constraint failed"));
    }

    #[test]
    fn insert_and_update_enforce_check_constraint() {
        let mut storage = Storage::new();
        let age_column = Column {
            name: "age".to_string(),
            col_type: "INTEGER".to_string(),
            not_null: false,
            primary_key: false,
            unique: false,
            check_constraints: vec![Expr::Binary(
                crate::parser::ast::BinaryOp::GreaterEqual,
                Box::new(Expr::Identifier("age".to_string())),
                Box::new(Expr::Literal(Value::integer(18))),
            )],
            default_value: None,
        };

        storage.create_table("users", vec![age_column]).unwrap();
        storage.insert("users", vec![Value::integer(20)]).unwrap();
        storage.insert("users", vec![Value::Null]).unwrap();

        let insert_error = storage.insert("users", vec![Value::integer(12)]).unwrap_err();
        assert!(insert_error.to_string().contains("CHECK constraint failed"));

        let update_error = storage
            .update("users", &[("age".to_string(), Value::integer(10))], |_| true)
            .unwrap_err();
        assert!(update_error.to_string().contains("CHECK constraint failed"));
    }

    #[test]
    fn prepare_insert_row_fills_primary_key_from_next_rowid() {
        let mut storage = Storage::new();
        let mut id_column = test_column("id");
        id_column.primary_key = true;
        storage.create_table("users", vec![id_column, test_column("name")]).unwrap();

        let row = storage
            .prepare_insert_row("users", vec![Value::Null, Value::text("Alice")])
            .unwrap();

        assert_eq!(row, vec![Value::integer(1), Value::text("Alice")]);
    }
}
