//! Row implementation

use crate::error::Result;
use crate::types::Value;
use super::connection::FromValue;
use super::statement::Statement;

/// A single row from a query result
#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<Value>,
    column_names: Vec<String>,
}

impl Row {
    /// Create a new row
    pub fn new(values: Vec<Value>, column_names: Vec<String>) -> Self {
        Row { values, column_names }
    }

    /// Get the number of columns
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value by column index (0-based)
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get a value by column name
    pub fn get_value_by_name(&self, name: &str) -> Option<&Value> {
        let index = self.column_names.iter().position(|n| n == name)?;
        self.values.get(index)
    }

    /// Get a value as a specific type
    pub fn get<T: FromValue>(&self, index: usize) -> Result<T> {
        let value = self.values.get(index).ok_or_else(|| {
            crate::error::Error::sqlite(
                crate::error::ErrorCode::Range,
                format!("column index {} out of bounds", index),
            )
        })?;
        T::from_value(value)
    }

    /// Get a value by column name as a specific type
    pub fn get_by_name<T: FromValue>(&self, name: &str) -> Result<T> {
        let index = self
            .column_names
            .iter()
            .position(|n| n == name)
            .ok_or_else(|| {
                crate::error::Error::sqlite(
                    crate::error::ErrorCode::Error,
                    format!("column '{}' not found", name),
                )
            })?;
        self.get(index)
    }

    /// Get column names
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Get all values
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Check if a column is NULL
    pub fn is_null(&self, index: usize) -> bool {
        self.values.get(index).map(|v| v.is_null()).unwrap_or(true)
    }

    /// Check if a column is NULL by name
    pub fn is_null_by_name(&self, name: &str) -> bool {
        self.get_value_by_name(name)
            .map(|v| v.is_null())
            .unwrap_or(true)
    }

    /// Get as integer
    pub fn get_int(&self, index: usize) -> Option<i32> {
        self.get_value(index)
            .and_then(|v| v.as_integer())
            .and_then(|i| i32::try_from(i).ok())
    }

    /// Get as 64-bit integer
    pub fn get_int64(&self, index: usize) -> Option<i64> {
        self.get_value(index).and_then(|v| v.as_integer())
    }

    /// Get as double
    pub fn get_double(&self, index: usize) -> Option<f64> {
        self.get_value(index).and_then(|v| v.as_float())
    }

    /// Get as text
    pub fn get_text(&self, index: usize) -> Option<&str> {
        self.get_value(index).and_then(|v| v.as_text())
    }

    /// Get as blob
    pub fn get_blob(&self, index: usize) -> Option<&[u8]> {
        self.get_value(index).and_then(|v| v.as_blob())
    }

    /// Convert to a tuple
    pub fn to_tuple<T1>(&self) -> Result<(T1,)>
    where
        T1: FromValue,
    {
        Ok((self.get(0)?,))
    }

    /// Convert to a 2-tuple
    pub fn to_tuple2<T1, T2>(&self) -> Result<(T1, T2)>
    where
        T1: FromValue,
        T2: FromValue,
    {
        Ok((self.get(0)?, self.get(1)?))
    }

    /// Convert to a 3-tuple
    pub fn to_tuple3<T1, T2, T3>(&self) -> Result<(T1, T2, T3)>
    where
        T1: FromValue,
        T2: FromValue,
        T3: FromValue,
    {
        Ok((self.get(0)?, self.get(1)?, self.get(2)?))
    }
}

impl std::ops::Index<usize> for Row {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

/// Row iterator
pub struct RowIterator {
    statement: Statement,
    current_row: Option<Row>,
}

impl RowIterator {
    /// Create a new row iterator from a statement
    pub fn new(mut statement: Statement) -> Self {
        // Execute the first step
        let has_row = statement.step().ok().unwrap_or(false);
        let current_row = if has_row {
            let values = statement.row().ok().unwrap_or_default();
            let names = statement.column_names.clone();
            Some(Row::new(values, names))
        } else {
            None
        };

        RowIterator {
            statement,
            current_row,
        }
    }
}

impl Iterator for RowIterator {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.current_row.take()?;
        let values = self.statement.row().ok().unwrap_or_default();
        let names = self.statement.column_names.clone();

        // Try to get next row
        match self.statement.step() {
            Ok(true) => {
                self.current_row = Some(Row::new(values, names));
            }
            Ok(false) => {
                self.current_row = None;
            }
            Err(e) => {
                return Some(Err(e));
            }
        }

        Some(Ok(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_new() {
        let values = vec![Value::integer(1), Value::text("hello")];
        let names = vec!["id".to_string(), "name".to_string()];
        let row = Row::new(values, names);

        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_row_get() {
        let values = vec![Value::integer(42), Value::text("test")];
        let names = vec!["id".to_string(), "name".to_string()];
        let row = Row::new(values, names);

        let id: i64 = row.get(0).unwrap();
        assert_eq!(id, 42);

        let name: String = row.get(1).unwrap();
        assert_eq!(name, "test");
    }

    #[test]
    fn test_row_get_by_name() {
        let values = vec![Value::integer(42), Value::text("test")];
        let names = vec!["id".to_string(), "name".to_string()];
        let row = Row::new(values, names);

        let id: i64 = row.get_by_name("id").unwrap();
        assert_eq!(id, 42);

        let name: String = row.get_by_name("name").unwrap();
        assert_eq!(name, "test");
    }

    #[test]
    fn test_row_get_value() {
        let values = vec![Value::integer(42)];
        let names = vec!["id".to_string()];
        let row = Row::new(values, names);

        assert!(row.get_value(0).is_some());
        assert!(row.get_value(1).is_none());
    }

    #[test]
    fn test_row_get_value_by_name() {
        let values = vec![Value::integer(42)];
        let names = vec!["id".to_string()];
        let row = Row::new(values, names);

        assert!(row.get_value_by_name("id").is_some());
        assert!(row.get_value_by_name("name").is_none());
    }

    #[test]
    fn test_row_is_null() {
        let values = vec![Value::null(), Value::integer(42)];
        let names = vec!["a".to_string(), "b".to_string()];
        let row = Row::new(values, names);

        assert!(row.is_null(0));
        assert!(!row.is_null(1));
    }

    #[test]
    fn test_row_index() {
        let values = vec![Value::integer(42)];
        let names = vec!["id".to_string()];
        let row = Row::new(values, names);

        assert_eq!(row[0], Value::integer(42));
    }

    #[test]
    fn test_row_getters() {
        let values = vec![
            Value::integer(42),
            Value::float(3.14),
            Value::text("hello"),
            Value::blob(vec![1, 2, 3]),
        ];
        let names = vec!["i".to_string(), "f".to_string(), "t".to_string(), "b".to_string()];
        let row = Row::new(values, names);

        assert_eq!(row.get_int(0), Some(42));
        assert_eq!(row.get_int64(0), Some(42));
        assert_eq!(row.get_double(1), Some(3.14));
        assert_eq!(row.get_text(2), Some("hello"));
        assert_eq!(row.get_blob(3), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn test_row_to_tuple() {
        let values = vec![Value::integer(42)];
        let names = vec!["id".to_string()];
        let row = Row::new(values, names);

        let tuple = row.to_tuple::<i64>().unwrap();
        assert_eq!(tuple, (42,));
    }

    #[test]
    fn test_row_to_tuple2() {
        let values = vec![Value::integer(42), Value::text("test")];
        let names = vec!["id".to_string(), "name".to_string()];
        let row = Row::new(values, names);

        let tuple = row.to_tuple2::<i64, String>().unwrap();
        assert_eq!(tuple, (42, "test".to_string()));
    }
}
