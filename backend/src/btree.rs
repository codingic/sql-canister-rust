//! B-Tree storage engine
//!
//! Implements B-Tree data structures for table and index storage.

use crate::error::Result;
use crate::types::Pgno;

/// B-Tree cursor
#[derive(Debug, Clone)]
pub struct BtreeCursor {
    /// Root page number
    root_page: Pgno,
    /// Current page number
    current_page: Pgno,
    /// Cell index
    cell_idx: u32,
    /// Is at end
    at_end: bool,
    /// Is valid
    valid: bool,
}

impl BtreeCursor {
    /// Create a new cursor
    pub fn new(root_page: Pgno) -> Self {
        BtreeCursor {
            root_page,
            current_page: root_page,
            cell_idx: 0,
            at_end: false,
            valid: false,
        }
    }

    /// Move to first cell
    pub fn first(&mut self) -> Result<bool> {
        self.cell_idx = 0;
        self.valid = true;
        Ok(true)
    }

    /// Move to next cell
    pub fn next(&mut self) -> Result<bool> {
        self.cell_idx += 1;
        Ok(!self.at_end)
    }

    /// Move to previous cell
    pub fn prev(&mut self) -> Result<bool> {
        if self.cell_idx > 0 {
            self.cell_idx -= 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get current rowid
    pub fn rowid(&self) -> Option<i64> {
        if self.valid {
            Some(self.cell_idx as i64)
        } else {
            None
        }
    }

    /// Check if cursor is valid
    pub fn is_valid(&self) -> bool {
        self.valid
    }
}

/// B-Tree
#[derive(Debug)]
pub struct Btree {
    /// Root page number
    root_page: Pgno,
    /// Table name
    table_name: String,
    /// Cursors
    cursors: Vec<BtreeCursor>,
}

impl Btree {
    /// Create a new B-Tree
    pub fn new(root_page: Pgno, table_name: &str) -> Self {
        Btree {
            root_page,
            table_name: table_name.to_string(),
            cursors: Vec::new(),
        }
    }

    /// Open a cursor
    pub fn open_cursor(&mut self) -> usize {
        let cursor = BtreeCursor::new(self.root_page);
        self.cursors.push(cursor);
        self.cursors.len() - 1
    }

    /// Close a cursor
    pub fn close_cursor(&mut self, idx: usize) {
        if idx < self.cursors.len() {
            self.cursors[idx].valid = false;
        }
    }

    /// Get cursor
    pub fn get_cursor(&self, idx: usize) -> Option<&BtreeCursor> {
        self.cursors.get(idx)
    }

    /// Get mutable cursor
    pub fn get_cursor_mut(&mut self, idx: usize) -> Option<&mut BtreeCursor> {
        self.cursors.get_mut(idx)
    }

    /// Insert a row
    pub fn insert(&mut self, _payload: &[u8]) -> Result<i64> {
        Ok(1)
    }

    /// Delete a row
    pub fn delete(&mut self, _rowid: i64) -> Result<()> {
        Ok(())
    }

    /// Get table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

/// B-Tree payload
#[derive(Debug, Clone)]
pub struct BtreePayload {
    /// Key (for index) or rowid (for table)
    pub key: Option<i64>,
    /// Data
    pub data: Vec<u8>,
}

impl BtreePayload {
    /// Create a new payload
    pub fn new(data: Vec<u8>) -> Self {
        BtreePayload { key: None, data }
    }

    /// Create with key
    pub fn with_key(key: i64, data: Vec<u8>) -> Self {
        BtreePayload {
            key: Some(key),
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_new() {
        let btree = Btree::new(1, "test");
        assert_eq!(btree.table_name(), "test");
    }

    #[test]
    fn test_btree_cursor() {
        let mut btree = Btree::new(1, "test");
        let cursor_idx = btree.open_cursor();
        assert_eq!(cursor_idx, 0);
    }

    #[test]
    fn test_cursor_navigation() {
        let mut cursor = BtreeCursor::new(1);
        cursor.first().unwrap();
        assert!(cursor.is_valid());
    }
}
