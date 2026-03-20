//! Value types for SQLite-RS
//!
//! This module defines the core data types used throughout the library.

use std::borrow::Cow;
use std::fmt;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use serde::{Serialize, Deserialize};

/// SQLite value types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ValueType {
    /// NULL value
    Null = 0,
    /// 64-bit signed integer
    Integer = 1,
    /// 64-bit IEEE floating point
    Float = 2,
    /// UTF-8 string
    Text = 3,
    /// Binary blob
    Blob = 4,
}

impl ValueType {
    /// Get the type name as a string
    pub fn name(&self) -> &'static str {
        match self {
            ValueType::Null => "NULL",
            ValueType::Integer => "INTEGER",
            ValueType::Float => "REAL",
            ValueType::Text => "TEXT",
            ValueType::Blob => "BLOB",
        }
    }

    /// Convert from integer code
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(ValueType::Null),
            1 => Some(ValueType::Integer),
            2 => Some(ValueType::Float),
            3 => Some(ValueType::Text),
            4 => Some(ValueType::Blob),
            _ => None,
        }
    }

    /// Get the type affinity for this value type
    pub fn affinity(&self) -> Affinity {
        match self {
            ValueType::Null => Affinity::None,
            ValueType::Integer => Affinity::Integer,
            ValueType::Float => Affinity::Real,
            ValueType::Text => Affinity::Text,
            ValueType::Blob => Affinity::Blob,
        }
    }
}

impl Default for ValueType {
    fn default() -> Self {
        ValueType::Null
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Column type affinity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Affinity {
    /// No affinity
    None,
    /// INTEGER affinity
    Integer,
    /// REAL (floating point) affinity
    Real,
    /// TEXT affinity
    Text,
    /// BLOB affinity
    Blob,
    /// NUMERIC affinity
    Numeric,
}

impl Affinity {
    /// Determine affinity from declared column type
    pub fn from_declared_type(ty: &str) -> Self {
        let ty_upper = ty.to_uppercase();

        // Check for INT type
        if ty_upper.contains("INT") {
            return Affinity::Integer;
        }

        // Check for CHAR, CLOB, or TEXT
        if ty_upper.contains("CHAR")
            || ty_upper.contains("CLOB")
            || ty_upper.contains("TEXT")
        {
            return Affinity::Text;
        }

        // Check for BLOB (no type specified)
        if ty_upper.contains("BLOB") || ty.is_empty() {
            return Affinity::Blob;
        }

        // Check for REAL, FLOA, or DOUB
        if ty_upper.contains("REAL")
            || ty_upper.contains("FLOA")
            || ty_upper.contains("DOUB")
        {
            return Affinity::Real;
        }

        // Default to NUMERIC
        Affinity::Numeric
    }

    /// Apply affinity to a value type
    pub fn apply_to_type(&self, vt: ValueType) -> ValueType {
        match (self, vt) {
            // TEXT affinity converts to NUMERIC first
            (Affinity::Text, ValueType::Integer) => ValueType::Integer,
            (Affinity::Text, ValueType::Float) => ValueType::Float,
            (Affinity::Text, ValueType::Text) => ValueType::Text,
            (Affinity::Text, ValueType::Blob) => ValueType::Blob,
            (Affinity::Text, ValueType::Null) => ValueType::Null,

            // NUMERIC affinity tries to convert to number
            (Affinity::Numeric, ValueType::Text) => ValueType::Text, // Try to parse
            (Affinity::Numeric, _) => vt,

            // INTEGER affinity is like NUMERIC but prefers int
            (Affinity::Integer, ValueType::Float) => ValueType::Integer, // If no fractional part
            (Affinity::Integer, _) => vt,

            // REAL affinity converts to float
            (Affinity::Real, ValueType::Integer) => ValueType::Float,
            (Affinity::Real, _) => vt,

            // BLOB affinity doesn't convert
            (Affinity::Blob, _) => vt,

            // No affinity
            (Affinity::None, _) => vt,
        }
    }
}

impl Default for Affinity {
    fn default() -> Self {
        Affinity::None
    }
}

impl fmt::Display for Affinity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Affinity::None => write!(f, "NONE"),
            Affinity::Integer => write!(f, "INTEGER"),
            Affinity::Real => write!(f, "REAL"),
            Affinity::Text => write!(f, "TEXT"),
            Affinity::Blob => write!(f, "BLOB"),
            Affinity::Numeric => write!(f, "NUMERIC"),
        }
    }
}

/// SQLite value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// NULL value
    Null,
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit IEEE floating point
    Float(f64),
    /// UTF-8 string
    Text(String),
    /// Binary blob
    Blob(Vec<u8>),
}

impl Value {
    /// Create a NULL value
    pub fn null() -> Self {
        Value::Null
    }

    /// Create an integer value
    pub fn integer(i: i64) -> Self {
        Value::Integer(i)
    }

    /// Create a float value
    pub fn float(f: f64) -> Self {
        Value::Float(f)
    }

    /// Create a text value
    pub fn text(s: impl Into<String>) -> Self {
        Value::Text(s.into())
    }

    /// Create a blob value
    pub fn blob(b: Vec<u8>) -> Self {
        Value::Blob(b)
    }

    /// Get the type of this value
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Integer(_) => ValueType::Integer,
            Value::Float(_) => ValueType::Float,
            Value::Text(_) => ValueType::Text,
            Value::Blob(_) => ValueType::Blob,
        }
    }

    /// Check if this is a NULL value
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if this is an integer value
    pub fn is_integer(&self) -> bool {
        matches!(self, Value::Integer(_))
    }

    /// Check if this is a float value
    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    /// Check if this is a text value
    pub fn is_text(&self) -> bool {
        matches!(self, Value::Text(_))
    }

    /// Check if this is a blob value
    pub fn is_blob(&self) -> bool {
        matches!(self, Value::Blob(_))
    }

    /// Check if this is a numeric value (integer or float)
    pub fn is_numeric(&self) -> bool {
        matches!(self, Value::Integer(_) | Value::Float(_))
    }

    /// Get as integer (returns None for non-integer types)
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Get as float (returns None for non-numeric types)
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Get as text (returns None for non-text types)
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Get as blob (returns None for non-blob types)
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// Convert to a string representation
    pub fn to_string_value(&self) -> Cow<'_, str> {
        match self {
            Value::Null => Cow::Borrowed("NULL"),
            Value::Integer(i) => Cow::Owned(i.to_string()),
            Value::Float(f) => Cow::Owned(format!("{:.15}", f)),
            Value::Text(s) => Cow::Borrowed(s),
            Value::Blob(b) => Cow::Owned(hex::encode(b)),
        }
    }

    /// Get the size in bytes
    pub fn size(&self) -> usize {
        match self {
            Value::Null => 0,
            Value::Integer(_) => 8,
            Value::Float(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
        }
    }

    /// Apply type affinity to convert the value
    pub fn apply_affinity(&mut self, affinity: Affinity) -> bool {
        match (affinity, &*self) {
            (Affinity::Integer, Value::Float(f)) => {
                // Convert to integer if no fractional part
                if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                    *self = Value::Integer(*f as i64);
                    return true;
                }
            }
            (Affinity::Integer, Value::Text(s)) => {
                // Try to parse as integer
                if let Ok(i) = s.parse::<i64>() {
                    *self = Value::Integer(i);
                    return true;
                }
            }
            (Affinity::Real, Value::Integer(i)) => {
                *self = Value::Float(*i as f64);
                return true;
            }
            (Affinity::Real, Value::Text(s)) => {
                // Try to parse as float
                if let Ok(f) = s.parse::<f64>() {
                    *self = Value::Float(f);
                    return true;
                }
            }
            (Affinity::Numeric, Value::Text(s)) => {
                // Try to convert to number
                if let Ok(i) = s.parse::<i64>() {
                    *self = Value::Integer(i);
                    return true;
                }
                if let Ok(f) = s.parse::<f64>() {
                    // If integer representation fits, use integer
                    if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                        *self = Value::Integer(f as i64);
                    } else {
                        *self = Value::Float(f);
                    }
                    return true;
                }
            }
            (Affinity::Text, Value::Integer(i)) => {
                *self = Value::Text(i.to_string());
                return true;
            }
            (Affinity::Text, Value::Float(f)) => {
                *self = Value::Text(format!("{:.15}", f));
                return true;
            }
            _ => {}
        }
        false
    }

    /// Compare two values according to SQLite comparison rules
    pub fn compare(&self, other: &Value) -> Ordering {
        // SQLite comparison type precedence:
        // NULL < INTEGER/FLOAT < TEXT < BLOB

        match (self, other) {
            // Both NULL
            (Value::Null, Value::Null) => Ordering::Equal,

            // NULL is always less than anything else
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            // Numeric comparison
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Integer(a), Value::Float(b)) => compare_numeric(*a as f64, *b),
            (Value::Float(a), Value::Integer(b)) => compare_numeric(*a, *b as f64),
            (Value::Float(a), Value::Float(b)) => compare_numeric(*a, *b),

            // Numeric vs non-numeric
            (Value::Integer(_), Value::Text(_)) => Ordering::Less,
            (Value::Integer(_), Value::Blob(_)) => Ordering::Less,
            (Value::Float(_), Value::Text(_)) => Ordering::Less,
            (Value::Float(_), Value::Blob(_)) => Ordering::Less,
            (Value::Text(_), Value::Integer(_)) => Ordering::Greater,
            (Value::Text(_), Value::Float(_)) => Ordering::Greater,
            (Value::Blob(_), Value::Integer(_)) => Ordering::Greater,
            (Value::Blob(_), Value::Float(_)) => Ordering::Greater,

            // Text comparison
            (Value::Text(a), Value::Text(b)) => {
                // Use memcmp-style comparison
                let min_len = a.len().min(b.len());
                match a.as_bytes()[..min_len].cmp(&b.as_bytes()[..min_len]) {
                    Ordering::Equal => a.len().cmp(&b.len()),
                    other => other,
                }
            }

            // Blob comparison
            (Value::Blob(a), Value::Blob(b)) => {
                let min_len = a.len().min(b.len());
                match a[..min_len].cmp(&b[..min_len]) {
                    Ordering::Equal => a.len().cmp(&b.len()),
                    other => other,
                }
            }

            // Text vs Blob
            (Value::Text(a), Value::Blob(b)) => {
                let min_len = a.len().min(b.len());
                match a.as_bytes()[..min_len].cmp(&b[..min_len]) {
                    Ordering::Equal => a.len().cmp(&b.len()),
                    other => other,
                }
            }
            (Value::Blob(a), Value::Text(b)) => {
                let min_len = a.len().min(b.len());
                match a[..min_len].cmp(&b.as_bytes()[..min_len]) {
                    Ordering::Equal => a.len().cmp(&b.len()),
                    other => other,
                }
            }
        }
    }

    /// Calculate the serial type for storage
    pub fn serial_type(&self) -> u64 {
        match self {
            Value::Null => 0,
            Value::Integer(i) => {
                // Choose the smallest representation
                if *i == 0 {
                    8
                } else if *i == 1 {
                    9
                } else if *i >= i8::MIN as i64 && *i <= i8::MAX as i64 {
                    1
                } else if *i >= i16::MIN as i64 && *i <= i16::MAX as i64 {
                    2
                } else if *i >= -(2i64.pow(23)) && *i <= 2i64.pow(23) - 1 {
                    3
                } else if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    4
                } else if *i >= -(2i64.pow(47)) && *i <= 2i64.pow(47) - 1 {
                    5
                } else {
                    6
                }
            }
            Value::Float(_) => 7,
            Value::Text(s) => {
                let len = s.len() as u64;
                if len % 2 == 0 {
                    len * 2 + 12
                } else {
                    len * 2 + 13
                }
            }
            Value::Blob(b) => {
                let len = b.len() as u64;
                len * 2 + 12
            }
        }
    }

    /// Calculate the serialized size from serial type
    pub fn serial_type_size(serial_type: u64) -> usize {
        match serial_type {
            0 => 0,  // NULL
            1 => 1,  // 8-bit int
            2 => 2,  // 16-bit int
            3 => 3,  // 24-bit int
            4 => 4,  // 32-bit int
            5 => 6,  // 48-bit int
            6 => 8,  // 64-bit int
            7 => 8,  // float
            8 => 0,  // integer 0
            9 => 0,  // integer 1
            n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize, // BLOB
            n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize, // TEXT
            _ => 0,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::Integer(i) => {
                1u8.hash(state);
                i.hash(state);
            }
            Value::Float(f) => {
                2u8.hash(state);
                f.to_bits().hash(state);
            }
            Value::Text(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            Value::Blob(b) => {
                4u8.hash(state);
                b.hash(state);
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Value::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{:02x}", byte)?;
                }
                write!(f, "'")
            }
        }
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Blob(b)
    }
}

impl From<Option<i64>> for Value {
    fn from(opt: Option<i64>) -> Self {
        match opt {
            Some(i) => Value::Integer(i),
            None => Value::Null,
        }
    }
}

impl From<Option<String>> for Value {
    fn from(opt: Option<String>) -> Self {
        match opt {
            Some(s) => Value::Text(s),
            None => Value::Null,
        }
    }
}

/// Compare two floating point numbers with SQLite-style comparison
fn compare_numeric(a: f64, b: f64) -> Ordering {
    if a < b {
        Ordering::Less
    } else if a > b {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

/// Hex encoding/decoding module
mod hex {
    

    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, &'static str> {
        if s.len() % 2 != 0 {
            return Err("Invalid hex string length");
        }
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| "Invalid hex digit"))
            .collect()
    }
}

/// Row identifier type
pub type RowId = i64;

/// Page number type
pub type Pgno = u32;

/// Transaction ID type
pub type TxId = u64;

/// Serial number for varint encoding
pub type SerialNo = u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type_codes() {
        assert_eq!(ValueType::Null as i32, 0);
        assert_eq!(ValueType::Integer as i32, 1);
        assert_eq!(ValueType::Float as i32, 2);
        assert_eq!(ValueType::Text as i32, 3);
        assert_eq!(ValueType::Blob as i32, 4);
    }

    #[test]
    fn test_value_type_from_code() {
        assert_eq!(ValueType::from_code(0), Some(ValueType::Null));
        assert_eq!(ValueType::from_code(1), Some(ValueType::Integer));
        assert_eq!(ValueType::from_code(2), Some(ValueType::Float));
        assert_eq!(ValueType::from_code(3), Some(ValueType::Text));
        assert_eq!(ValueType::from_code(4), Some(ValueType::Blob));
        assert_eq!(ValueType::from_code(5), None);
    }

    #[test]
    fn test_affinity_from_declared_type() {
        assert_eq!(Affinity::from_declared_type("INTEGER"), Affinity::Integer);
        assert_eq!(Affinity::from_declared_type("INT"), Affinity::Integer);
        assert_eq!(Affinity::from_declared_type("BIGINT"), Affinity::Integer);
        assert_eq!(Affinity::from_declared_type("TEXT"), Affinity::Text);
        assert_eq!(Affinity::from_declared_type("VARCHAR"), Affinity::Text);
        assert_eq!(Affinity::from_declared_type("REAL"), Affinity::Real);
        assert_eq!(Affinity::from_declared_type("FLOAT"), Affinity::Real);
        assert_eq!(Affinity::from_declared_type("DOUBLE"), Affinity::Real);
        assert_eq!(Affinity::from_declared_type("BLOB"), Affinity::Blob);
        assert_eq!(Affinity::from_declared_type(""), Affinity::Blob);
        assert_eq!(Affinity::from_declared_type("NUMERIC"), Affinity::Numeric);
    }

    #[test]
    fn test_value_creation() {
        let v = Value::null();
        assert!(v.is_null());
        assert_eq!(v.value_type(), ValueType::Null);

        let v = Value::integer(42);
        assert!(v.is_integer());
        assert_eq!(v.as_integer(), Some(42));

        let v = Value::float(3.14);
        assert!(v.is_float());
        assert_eq!(v.as_float(), Some(3.14));

        let v = Value::text("hello");
        assert!(v.is_text());
        assert_eq!(v.as_text(), Some("hello"));

        let v = Value::blob(vec![1, 2, 3]);
        assert!(v.is_blob());
        assert_eq!(v.as_blob(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn test_value_numeric_check() {
        assert!(Value::integer(42).is_numeric());
        assert!(Value::float(3.14).is_numeric());
        assert!(!Value::text("42").is_numeric());
        assert!(!Value::null().is_numeric());
    }

    #[test]
    fn test_value_comparison_null() {
        assert_eq!(Value::Null.compare(&Value::Null), Ordering::Equal);
        assert_eq!(Value::Null.compare(&Value::integer(0)), Ordering::Less);
        assert_eq!(Value::integer(0).compare(&Value::Null), Ordering::Greater);
    }

    #[test]
    fn test_value_comparison_numeric() {
        assert_eq!(Value::integer(5).compare(&Value::integer(10)), Ordering::Less);
        assert_eq!(Value::integer(10).compare(&Value::integer(5)), Ordering::Greater);
        assert_eq!(Value::integer(5).compare(&Value::integer(5)), Ordering::Equal);

        assert_eq!(Value::float(5.0).compare(&Value::float(10.0)), Ordering::Less);
        assert_eq!(Value::integer(5).compare(&Value::float(5.0)), Ordering::Equal);
        assert_eq!(Value::float(5.0).compare(&Value::integer(5)), Ordering::Equal);
    }

    #[test]
    fn test_value_comparison_text() {
        assert_eq!(Value::text("a").compare(&Value::text("b")), Ordering::Less);
        assert_eq!(Value::text("b").compare(&Value::text("a")), Ordering::Greater);
        assert_eq!(Value::text("a").compare(&Value::text("a")), Ordering::Equal);

        // Numeric < Text
        assert_eq!(Value::integer(100).compare(&Value::text("a")), Ordering::Less);
    }

    #[test]
    fn test_value_comparison_blob() {
        let b1 = Value::blob(vec![1, 2, 3]);
        let b2 = Value::blob(vec![1, 2, 4]);
        let b3 = Value::blob(vec![1, 2, 3]);

        assert_eq!(b1.compare(&b2), Ordering::Less);
        assert_eq!(b2.compare(&b1), Ordering::Greater);
        assert_eq!(b1.compare(&b3), Ordering::Equal);
    }

    #[test]
    fn test_value_serial_type() {
        assert_eq!(Value::Null.serial_type(), 0);
        assert_eq!(Value::integer(0).serial_type(), 8);
        assert_eq!(Value::integer(1).serial_type(), 9);
        assert_eq!(Value::integer(127).serial_type(), 1);
        assert_eq!(Value::integer(32767).serial_type(), 2);
        assert_eq!(Value::float(3.14).serial_type(), 7);
    }

    #[test]
    fn test_value_serial_type_size() {
        assert_eq!(Value::serial_type_size(0), 0);   // NULL
        assert_eq!(Value::serial_type_size(1), 1);   // 8-bit int
        assert_eq!(Value::serial_type_size(2), 2);   // 16-bit int
        assert_eq!(Value::serial_type_size(4), 4);   // 32-bit int
        assert_eq!(Value::serial_type_size(6), 8);   // 64-bit int
        assert_eq!(Value::serial_type_size(7), 8);   // float
        assert_eq!(Value::serial_type_size(8), 0);   // integer 0
        assert_eq!(Value::serial_type_size(9), 0);   // integer 1
        assert_eq!(Value::serial_type_size(12), 0);  // empty blob
        assert_eq!(Value::serial_type_size(13), 0);  // empty text
        assert_eq!(Value::serial_type_size(14), 1);  // 1-byte blob
        assert_eq!(Value::serial_type_size(15), 1);  // 1-byte text
    }

    #[test]
    fn test_value_apply_affinity() {
        let mut v = Value::Float(42.0);
        assert!(v.apply_affinity(Affinity::Integer));
        assert!(v.is_integer());
        assert_eq!(v.as_integer(), Some(42));

        let mut v = Value::Text("42".to_string());
        assert!(v.apply_affinity(Affinity::Integer));
        assert!(v.is_integer());
        assert_eq!(v.as_integer(), Some(42));

        let mut v = Value::Text("3.14".to_string());
        assert!(v.apply_affinity(Affinity::Real));
        assert!(v.is_float());
    }

    #[test]
    fn test_value_from_conversions() {
        let v: Value = 42i32.into();
        assert!(v.is_integer());

        let v: Value = 42i64.into();
        assert!(v.is_integer());

        let v: Value = 3.14f64.into();
        assert!(v.is_float());

        let v: Value = "hello".into();
        assert!(v.is_text());

        let v: Value = String::from("hello").into();
        assert!(v.is_text());

        let v: Value = vec![1u8, 2, 3].into();
        assert!(v.is_blob());
    }

    #[test]
    fn test_value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::integer(42)), "42");
        assert_eq!(format!("{}", Value::text("hello")), "'hello'");
        assert_eq!(format!("{}", Value::text("it's")), "'it''s'");
        assert_eq!(format!("{}", Value::blob(vec![0x01, 0x02])), "X'0102'");
    }

    #[test]
    fn test_value_size() {
        assert_eq!(Value::Null.size(), 0);
        assert_eq!(Value::integer(42).size(), 8);
        assert_eq!(Value::float(3.14).size(), 8);
        assert_eq!(Value::text("hello").size(), 5);
        assert_eq!(Value::blob(vec![1, 2, 3]).size(), 3);
    }
}
