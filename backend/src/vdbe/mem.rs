//! VDBE Mem module
//!
//! Memory cells for VDBE

#![allow(missing_docs)]

use crate::types::Value;

/// Memory cell flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemFlags {
    pub is_null: bool,
    pub is_integer: bool,
    pub is_float: bool,
    pub is_text: bool,
    pub is_blob: bool,
    pub is_static: bool,
    pub is_ephemeral: bool,
    pub is_dynamic: bool,
}

impl Default for MemFlags {
    fn default() -> Self {
        MemFlags {
            is_null: true,
            is_integer: false,
            is_float: false,
            is_text: false,
            is_blob: false,
            is_static: false,
            is_ephemeral: false,
            is_dynamic: false,
        }
    }
}

/// Memory cell for VDBE
#[derive(Debug, Clone)]
pub struct Mem {
    /// Cell value
    pub value: Value,
    /// Memory flags
    pub flags: MemFlags,
    /// Text encoding (1=UTF-8, 2=UTF-16LE, 3=UTF-16BE)
    pub encoding: u8,
}

impl Mem {
    /// Create a new NULL memory cell
    pub fn new() -> Self {
        Mem {
            value: Value::Null,
            flags: MemFlags::default(),
            encoding: 1,
        }
    }

    /// Create a memory cell with an integer value
    pub fn from_int(i: i64) -> Self {
        Mem {
            value: Value::integer(i),
            flags: MemFlags {
                is_null: false,
                is_integer: true,
                ..Default::default()
            },
            encoding: 1,
        }
    }

    /// Create a memory cell with a float value
    pub fn from_float(f: f64) -> Self {
        Mem {
            value: Value::float(f),
            flags: MemFlags {
                is_null: false,
                is_float: true,
                ..Default::default()
            },
            encoding: 1,
        }
    }

    /// Create a memory cell with a text value
    pub fn from_text(s: String) -> Self {
        Mem {
            value: Value::text(s),
            flags: MemFlags {
                is_null: false,
                is_text: true,
                ..Default::default()
            },
            encoding: 1,
        }
    }

    /// Create a memory cell with a blob value
    pub fn from_blob(b: Vec<u8>) -> Self {
        Mem {
            value: Value::blob(b),
            flags: MemFlags {
                is_null: false,
                is_blob: true,
                ..Default::default()
            },
            encoding: 1,
        }
    }

    /// Get the value type
    pub fn value_type(&self) -> crate::types::ValueType {
        self.value.value_type()
    }

    /// Check if NULL
    pub fn is_null(&self) -> bool {
        self.flags.is_null
    }

    /// Get as integer
    pub fn as_int(&self) -> Option<i64> {
        self.value.as_integer()
    }

    /// Get as float
    pub fn as_float(&self) -> Option<f64> {
        self.value.as_float()
    }

    /// Get as text
    pub fn as_text(&self) -> Option<&str> {
        self.value.as_text()
    }

    /// Get as blob
    pub fn as_blob(&self) -> Option<&[u8]> {
        self.value.as_blob()
    }

    /// Set to NULL
    pub fn set_null(&mut self) {
        self.value = Value::Null;
        self.flags = MemFlags::default();
    }

    /// Set to integer
    pub fn set_int(&mut self, i: i64) {
        self.value = Value::integer(i);
        self.flags = MemFlags {
            is_null: false,
            is_integer: true,
            ..Default::default()
        };
    }

    /// Set to float
    pub fn set_float(&mut self, f: f64) {
        self.value = Value::float(f);
        self.flags = MemFlags {
            is_null: false,
            is_float: true,
            ..Default::default()
        };
    }

    /// Set to text
    pub fn set_text(&mut self, s: String) {
        self.value = Value::text(s);
        self.flags = MemFlags {
            is_null: false,
            is_text: true,
            ..Default::default()
        };
    }

    /// Set to blob
    pub fn set_blob(&mut self, b: Vec<u8>) {
        self.value = Value::blob(b);
        self.flags = MemFlags {
            is_null: false,
            is_blob: true,
            ..Default::default()
        };
    }

    /// Copy from another memory cell
    pub fn copy_from(&mut self, other: &Mem) {
        self.value = other.value.clone();
        self.flags = other.flags;
        self.encoding = other.encoding;
    }

    /// Compare with another memory cell
    pub fn compare(&self, other: &Mem) -> std::cmp::Ordering {
        self.value.compare(&other.value)
    }

    /// Convert to serial type
    pub fn serial_type(&self) -> u64 {
        self.value.serial_type()
    }
}

impl Default for Mem {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Value> for Mem {
    fn from(value: Value) -> Self {
        let flags = match &value {
            Value::Null => MemFlags::default(),
            Value::Integer(_) => MemFlags {
                is_null: false,
                is_integer: true,
                ..Default::default()
            },
            Value::Float(_) => MemFlags {
                is_null: false,
                is_float: true,
                ..Default::default()
            },
            Value::Text(_) => MemFlags {
                is_null: false,
                is_text: true,
                ..Default::default()
            },
            Value::Blob(_) => MemFlags {
                is_null: false,
                is_blob: true,
                ..Default::default()
            },
        };

        Mem {
            value,
            flags,
            encoding: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_new() {
        let mem = Mem::new();
        assert!(mem.is_null());
    }

    #[test]
    fn test_mem_from_int() {
        let mem = Mem::from_int(42);
        assert_eq!(mem.as_int(), Some(42));
    }

    #[test]
    fn test_mem_from_float() {
        let mem = Mem::from_float(3.14);
        assert_eq!(mem.as_float(), Some(3.14));
    }

    #[test]
    fn test_mem_from_text() {
        let mem = Mem::from_text("hello".to_string());
        assert_eq!(mem.as_text(), Some("hello"));
    }

    #[test]
    fn test_mem_setters() {
        let mut mem = Mem::new();
        mem.set_int(42);
        assert_eq!(mem.as_int(), Some(42));
        mem.set_float(3.14);
        assert_eq!(mem.as_float(), Some(3.14));
    }
}
