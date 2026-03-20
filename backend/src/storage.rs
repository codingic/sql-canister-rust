//! Simple in-memory storage layer

#![allow(missing_docs)]

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
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
}

/// A table in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    pub next_rowid: i64,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Table {
            name,
            columns,
            rows: Vec::new(),
            next_rowid: 1,
        }
    }

    /// Find primary key column index
    pub fn primary_key_index(&self) -> Option<usize> {
        self.columns.iter().position(|c| c.primary_key)
    }

    /// Get column index by name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
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
        let storage: Storage = bincode::deserialize(&schema_bytes)
            .map_err(|e| Error::sqlite(ErrorCode::Corrupt, &format!("cannot deserialize: {}", e)))?;

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

    /// Get a table by name
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(&name.to_lowercase())
    }

    /// Get a mutable table by name
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(&name.to_lowercase())
    }

    /// Insert a row into a table
    pub fn insert(&mut self, table_name: &str, values: Vec<Value>) -> Result<i64> {
        let table = self.get_table_mut(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        // Ensure we have the right number of values
        let mut row = vec![Value::Null; table.columns.len()];

        for (i, value) in values.into_iter().enumerate() {
            if i < row.len() {
                row[i] = value;
            }
        }

        // Handle auto-increment primary key
        if let Some(pk_idx) = table.primary_key_index() {
            if row[pk_idx].is_null() {
                row[pk_idx] = Value::integer(table.next_rowid);
            }
        }

        let rowid = table.next_rowid;
        table.next_rowid += 1;
        table.rows.push(row);
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

            col_indices = updates.iter()
                .filter_map(|(col_name, value)| {
                    table.column_index(col_name).map(|idx| (idx, value.clone()))
                })
                .collect();
        }

        // Now mutate the table
        let table = self.get_table_mut(table_name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

        let mut count = 0;
        for row in &mut table.rows {
            if predicate(row) {
                for (idx, value) in &col_indices {
                    row[*idx] = value.clone();
                }
                count += 1;
            }
        }
        if count > 0 {
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
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}
