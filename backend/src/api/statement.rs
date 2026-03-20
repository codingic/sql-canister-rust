//! Prepared statement implementation

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;
use crate::parser::ast::{Statement as AstStatement, Expr, ResultColumn, SelectStmt, BinaryOp, UnaryOp};
use crate::storage::{Column, Storage, Table};
use super::connection::{Connection, IntoParams};
#[cfg(feature = "thread-safe")]
use parking_lot::RwLock;
use std::sync::Arc;

/// Prepared statement state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementState {
    /// Statement is prepared but not executed
    Prepared,
    /// Statement is currently executing
    Running,
    /// A row is available for reading
    RowReady,
    /// Execution is complete
    Done,
    /// An error occurred
    Error,
}

/// Prepared statement
pub struct Statement {
    /// Original SQL
    sql: String,
    /// Parsed AST
    ast: AstStatement,
    /// Current state
    state: StatementState,
    /// Bound parameters
    params: Vec<Value>,
    /// Current row values
    row_values: Vec<Value>,
    /// Column names
    pub column_names: Vec<String>,
    /// Column types
    column_types: Vec<Option<String>>,
    /// Has more rows
    has_more: bool,
    /// Result rows for SELECT
    result_rows: Vec<Vec<Value>>,
    /// Current row index
    current_row_idx: usize,
    /// Storage reference (thread-safe)
    #[cfg(feature = "thread-safe")]
    storage: Arc<RwLock<Storage>>,
    /// Storage reference (single-threaded)
    #[cfg(not(feature = "thread-safe"))]
    storage: Arc<std::cell::RefCell<Storage>>,
}

impl Statement {
    /// Create a new prepared statement
    pub fn new(conn: &Connection, ast: AstStatement, sql: String) -> Result<Self> {
        let (column_names, column_types) = Self::derive_column_info(&ast, conn)?;

        Ok(Statement {
            sql,
            ast,
            state: StatementState::Prepared,
            params: Vec::new(),
            row_values: Vec::new(),
            column_names,
            column_types,
            has_more: false,
            result_rows: Vec::new(),
            current_row_idx: 0,
            #[cfg(feature = "thread-safe")]
            storage: conn.storage().clone(),
            #[cfg(not(feature = "thread-safe"))]
            storage: conn.storage().clone(),
        })
    }

    /// Derive column info from AST
    fn derive_column_info(ast: &AstStatement, conn: &Connection) -> Result<(Vec<String>, Vec<Option<String>>)> {
        let mut names = Vec::new();
        let mut types = Vec::new();

        match ast {
            AstStatement::Select(select) => {
                #[cfg(feature = "thread-safe")]
                let storage = conn.storage().read();
                #[cfg(not(feature = "thread-safe"))]
                let storage = conn.storage().borrow();

                for col in &select.columns {
                    match col {
                        ResultColumn::Star => {
                            if let Some(from) = &select.from {
                                if let Some(source) = from.tables.first() {
                                    if let Some(table) = storage.get_table(&source.name) {
                                        for column in &table.columns {
                                            names.push(column.name.clone());
                                            types.push(Some(column.col_type.clone()));
                                        }
                                        continue;
                                    }
                                }
                            }

                            names.push("*".to_string());
                            types.push(None);
                        }
                        ResultColumn::TableStar(table_name) => {
                            if let Some(table) = storage.get_table(table_name) {
                                for column in &table.columns {
                                    names.push(column.name.clone());
                                    types.push(Some(column.col_type.clone()));
                                }
                                continue;
                            }

                            names.push(format!("{}.*", table_name));
                            types.push(None);
                        }
                        ResultColumn::Expr(expr, alias) => {
                            let name = alias.clone().unwrap_or_else(|| expr_to_name(expr));
                            names.push(name);
                            types.push(None);
                        }
                    }
                }
            }
            _ => {
                // Non-select statements don't have result columns
            }
        }

        Ok((names, types))
    }

    /// Get the SQL text
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the current state
    pub fn state(&self) -> StatementState {
        self.state
    }

    /// Get the number of parameters
    pub fn parameter_count(&self) -> usize {
        self.params.len()
    }

    /// Bind parameters
    pub fn bind(&mut self, params: impl IntoParams) -> Result<()> {
        self.params = params.into_params();
        Ok(())
    }

    /// Bind a parameter by index (1-based)
    pub fn bind_value(&mut self, index: usize, value: Value) -> Result<()> {
        if index == 0 {
            return Err(Error::sqlite(ErrorCode::Range, "parameter index must be >= 1"));
        }

        if index > self.params.len() {
            self.params.resize(index, Value::Null);
        }

        self.params[index - 1] = value;
        Ok(())
    }

    /// Bind a NULL
    pub fn bind_null(&mut self, index: usize) -> Result<()> {
        self.bind_value(index, Value::Null)
    }

    /// Bind an integer
    pub fn bind_int(&mut self, index: usize, value: i32) -> Result<()> {
        self.bind_value(index, Value::integer(value as i64))
    }

    /// Bind a 64-bit integer
    pub fn bind_int64(&mut self, index: usize, value: i64) -> Result<()> {
        self.bind_value(index, Value::integer(value))
    }

    /// Bind a double
    pub fn bind_double(&mut self, index: usize, value: f64) -> Result<()> {
        self.bind_value(index, Value::float(value))
    }

    /// Bind a text string
    pub fn bind_text(&mut self, index: usize, value: &str) -> Result<()> {
        self.bind_value(index, Value::text(value))
    }

    /// Bind a blob
    pub fn bind_blob(&mut self, index: usize, value: &[u8]) -> Result<()> {
        self.bind_value(index, Value::blob(value.to_vec()))
    }

    /// Execute one step of the statement
    pub fn step(&mut self) -> Result<bool> {
        self.step_impl()
    }

    fn step_impl(&mut self) -> Result<bool> {
        match self.state {
            StatementState::Prepared => {
                self.state = StatementState::Running;
            }
            StatementState::RowReady => {
                // Continue to next row
                self.current_row_idx += 1;
                if self.current_row_idx < self.result_rows.len() {
                    self.row_values = self.result_rows[self.current_row_idx].clone();
                    return Ok(true);
                } else {
                    self.state = StatementState::Done;
                    return Ok(false);
                }
            }
            StatementState::Done => {
                return Ok(false);
            }
            StatementState::Error => {
                return Err(Error::sqlite(ErrorCode::Misuse, "statement in error state"));
            }
            StatementState::Running => {
                // Already running, check if we have results
                if !self.result_rows.is_empty() && self.current_row_idx < self.result_rows.len() {
                    self.row_values = self.result_rows[self.current_row_idx].clone();
                    self.state = StatementState::RowReady;
                    return Ok(true);
                }
            }
        }

        // Clone the AST to avoid borrow conflicts
        let ast = self.ast.clone();

        // Execute the statement based on AST type
        match ast {
            AstStatement::Select(select) => {
                // Use read lock for SELECT
                #[cfg(feature = "thread-safe")]
                let storage = self.storage.read();
                #[cfg(not(feature = "thread-safe"))]
                let storage = self.storage.borrow();

                // Execute select and get results
                let result_rows = execute_select_query(&select, &storage)?;

                // Drop the lock before modifying self
                drop(storage);

                self.result_rows = result_rows;
                if !self.result_rows.is_empty() {
                    self.row_values = self.result_rows[0].clone();
                    self.state = StatementState::RowReady;
                    Ok(true)
                } else {
                    self.state = StatementState::Done;
                    Ok(false)
                }
            }

            AstStatement::CreateTable(create) => {
                #[cfg(feature = "thread-safe")]
                let mut storage = self.storage.write();
                #[cfg(not(feature = "thread-safe"))]
                let mut storage = self.storage.borrow_mut();

                let mut columns = Vec::new();
                for col_def in &create.columns {
                    columns.push(Column {
                        name: col_def.name.clone(),
                        col_type: col_def.type_name.clone().unwrap_or_else(|| "TEXT".to_string()),
                        not_null: col_def.constraints.iter().any(|c| {
                            matches!(c, crate::parser::ast::ColumnConstraint::NotNull)
                        }),
                        primary_key: col_def.constraints.iter().any(|c| {
                            matches!(c, crate::parser::ast::ColumnConstraint::PrimaryKey { .. })
                        }),
                    });
                }
                storage.create_table(&create.name, columns)?;
                self.state = StatementState::Done;
                Ok(false)
            }

            AstStatement::Insert(insert) => {
                #[cfg(feature = "thread-safe")]
                let mut storage = self.storage.write();
                #[cfg(not(feature = "thread-safe"))]
                let mut storage = self.storage.borrow_mut();

                // Get column mapping info first
                let (col_indices, num_cols) = {
                    let table = storage.get_table(&insert.table)
                        .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", insert.table)))?;

                    let col_indices: Vec<usize> = if insert.columns.is_empty() {
                        (0..table.columns.len()).collect()
                    } else {
                        insert.columns.iter()
                            .map(|col_name| {
                                table.column_index(col_name)
                                    .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such column: {}", col_name)))
                            })
                            .collect::<Result<Vec<_>>>()?
                    };

                    (col_indices, table.columns.len())
                };

                // Insert each row
                let table_name = insert.table.clone();
                for row_exprs in &insert.values {
                    let mut values = vec![Value::Null; num_cols];
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if i < col_indices.len() {
                            let val = eval_expr_to_value(&expr)?;
                            values[col_indices[i]] = val;
                        }
                    }
                    storage.insert(&table_name, values)?;
                }

                self.state = StatementState::Done;
                Ok(false)
            }

            AstStatement::Delete(delete) => {
                #[cfg(feature = "thread-safe")]
                let mut storage = self.storage.write();
                #[cfg(not(feature = "thread-safe"))]
                let mut storage = self.storage.borrow_mut();

                let predicate = if let Some(where_expr) = &delete.where_clause {
                    let table = storage.get_table(&delete.table)
                        .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", delete.table)))?;
                    Some(build_predicate(where_expr, table)?)
                } else {
                    None
                };

                let table_name = delete.table.clone();
                let count = if let Some(pred) = predicate {
                    storage.delete(&table_name, |row| pred(row))?
                } else {
                    storage.delete(&table_name, |_| true)?
                };

                let _ = count; // Suppress unused warning
                self.state = StatementState::Done;
                Ok(false)
            }

            AstStatement::Update(update) => {
                #[cfg(feature = "thread-safe")]
                let mut storage = self.storage.write();
                #[cfg(not(feature = "thread-safe"))]
                let mut storage = self.storage.borrow_mut();

                let updates: Vec<(String, Value)> = update.assignments.iter()
                    .map(|(col, expr)| {
                        let val = eval_expr_to_value(&expr)?;
                        Ok((col.clone(), val))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let predicate = if let Some(where_expr) = &update.where_clause {
                    let table = storage.get_table(&update.table)
                        .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", update.table)))?;
                    Some(build_predicate(where_expr, table)?)
                } else {
                    None
                };

                let table_name = update.table.clone();
                let count = if let Some(pred) = predicate {
                    storage.update(&table_name, &updates, |row| pred(row))?
                } else {
                    storage.update(&table_name, &updates, |_| true)?
                };

                let _ = count;
                self.state = StatementState::Done;
                Ok(false)
            }

            AstStatement::Drop(drop) => {
                #[cfg(feature = "thread-safe")]
                let mut storage = self.storage.write();
                #[cfg(not(feature = "thread-safe"))]
                let mut storage = self.storage.borrow_mut();

                if drop.object_type == crate::parser::ast::ObjectType::Table {
                    storage.drop_table(&drop.name)?;
                }
                self.state = StatementState::Done;
                Ok(false)
            }

            other => {
                self.state = StatementState::Error;
                Err(Error::sqlite(
                    ErrorCode::Error,
                    format!("unsupported statement: {}", other),
                ))
            }
        }
    }

    /// Reset the statement for re-execution

    /// Reset the statement for re-execution
    pub fn reset(&mut self) -> Result<()> {
        self.state = StatementState::Prepared;
        self.row_values.clear();
        self.has_more = false;
        self.result_rows.clear();
        self.current_row_idx = 0;
        Ok(())
    }

    /// Get the current row values
    pub fn row(&self) -> Result<Vec<Value>> {
        if self.state != StatementState::RowReady {
            return Err(Error::sqlite(
                ErrorCode::Misuse,
                "no row available - call step() first",
            ));
        }
        Ok(self.row_values.clone())
    }

    /// Get the number of columns in the result
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }

    /// Get a column name
    pub fn column_name(&self, index: usize) -> Option<&str> {
        self.column_names.get(index).map(|s| s.as_str())
    }

    /// Get a column type
    pub fn column_type(&self, index: usize) -> Option<&str> {
        self.column_types.get(index).and_then(|t| t.as_deref())
    }

    /// Get column value by index
    pub fn column_value(&self, index: usize) -> Option<&Value> {
        self.row_values.get(index)
    }

    /// Get column as integer
    pub fn column_int(&self, index: usize) -> Option<i32> {
        self.column_value(index)
            .and_then(|v| v.as_integer())
            .and_then(|i| i32::try_from(i).ok())
    }

    /// Get column as 64-bit integer
    pub fn column_int64(&self, index: usize) -> Option<i64> {
        self.column_value(index).and_then(|v| v.as_integer())
    }

    /// Get column as double
    pub fn column_double(&self, index: usize) -> Option<f64> {
        self.column_value(index).and_then(|v| v.as_float())
    }

    /// Get column as text
    pub fn column_text(&self, index: usize) -> Option<&str> {
        self.column_value(index).and_then(|v| v.as_text())
    }

    /// Get column as blob
    pub fn column_blob(&self, index: usize) -> Option<&[u8]> {
        self.column_value(index).and_then(|v| v.as_blob())
    }

    /// Check if column is NULL
    pub fn column_is_null(&self, index: usize) -> bool {
        self.column_value(index).map(|v| v.is_null()).unwrap_or(true)
    }

    /// Finalize the statement
    pub fn finalize(self) -> Result<()> {
        Ok(())
    }
}

impl std::fmt::Debug for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement")
            .field("sql", &self.sql)
            .field("state", &self.state)
            .field("param_count", &self.params.len())
            .finish()
    }
}

/// Convert expression to name
fn expr_to_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier(table, col) => {
            format!("{}.{}", table, col)
        }
        Expr::Literal(v) => v.to_string_value().into_owned(),
        Expr::Function(name, _) => name.clone(),
        _ => "?column?".to_string(),
    }
}

/// Convert expression to a value (for INSERT values)
fn eval_expr_to_value(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Null => Ok(Value::Null),
        Expr::Bool(b) => Ok(Value::integer(if *b { 1 } else { 0 })),
        Expr::Unary(UnaryOp::Negate, inner) => {
            match eval_expr_to_value(inner)? {
                Value::Integer(i) => Ok(Value::integer(-i)),
                Value::Float(f) => Ok(Value::float(-f)),
                _ => Ok(Value::Null),
            }
        }
        _ => Ok(Value::Null),
    }
}

/// Execute a SELECT query and return result rows
fn execute_select_query(select: &SelectStmt, storage: &Storage) -> Result<Vec<Vec<Value>>> {
    let mut result_rows = Vec::new();

    // Check if it's a simple SELECT with no FROM clause (like SELECT 1)
    if select.from.is_none() {
        let mut row = Vec::new();
        for col in &select.columns {
            match col {
                ResultColumn::Expr(expr, _) => {
                    if let Expr::Literal(v) = expr {
                        row.push(v.clone());
                    } else if let Expr::Function(name, _args) = expr {
                        // Handle aggregate functions without table
                        if name.eq_ignore_ascii_case("COUNT") {
                            row.push(Value::integer(1));
                        } else {
                            row.push(Value::Null);
                        }
                    } else {
                        row.push(Value::Null);
                    }
                }
                _ => {}
            }
        }
        if !row.is_empty() {
            result_rows.push(row);
        }
        return Ok(result_rows);
    }

    // Get the table
    let from = select.from.as_ref().unwrap();
    if from.tables.is_empty() {
        return Ok(result_rows);
    }

    let table_name = &from.tables[0].name;
    let table = storage.get_table(table_name)
        .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_name)))?;

    // Build predicate for WHERE clause
    let predicate = if let Some(where_expr) = &select.where_clause {
        Some(build_predicate(where_expr, table)?)
    } else {
        None
    };

    // Collect matching rows
    let matching_rows: Vec<&Vec<Value>> = table.rows.iter()
        .filter(|row| predicate.as_ref().map_or(true, |p| p(row)))
        .collect();

    // Check if query has aggregate functions
    let has_aggregates = select.columns.iter().any(|col| {
        matches!(col, ResultColumn::Expr(Expr::Function(name, _), _) if {
            let n = name.to_uppercase();
            n == "COUNT" || n == "SUM" || n == "AVG" || n == "MIN" || n == "MAX"
        })
    });

    if has_aggregates {
        // Aggregate query - return single row with aggregated values
        let mut result_row = Vec::new();

        for col in &select.columns {
            match col {
                ResultColumn::Expr(Expr::Function(name, args), _) => {
                    let func_name = name.to_uppercase();
                    let val = match func_name.as_str() {
                        "COUNT" => {
                            if args.is_empty() {
                                // COUNT(*)
                                Value::integer(matching_rows.len() as i64)
                            } else if let Some(Expr::Identifier(col_name)) = args.first() {
                                if let Some(idx) = table.column_index(col_name) {
                                    let count = matching_rows.iter()
                                        .filter(|row| !row.get(idx).map_or(true, |v| v.is_null()))
                                        .count();
                                    Value::integer(count as i64)
                                } else {
                                    Value::integer(matching_rows.len() as i64)
                                }
                            } else {
                                // COUNT(*)
                                Value::integer(matching_rows.len() as i64)
                            }
                        }
                        "SUM" => {
                            if let Some(Expr::Identifier(col_name)) = args.first() {
                                if let Some(idx) = table.column_index(col_name) {
                                    let sum: f64 = matching_rows.iter()
                                        .filter_map(|row| row.get(idx))
                                        .filter_map(|v| match v {
                                            Value::Integer(i) => Some(*i as f64),
                                            Value::Float(f) => Some(*f),
                                            _ => None,
                                        })
                                        .sum();
                                    Value::float(sum)
                                } else {
                                    Value::Null
                                }
                            } else {
                                Value::Null
                            }
                        }
                        "AVG" => {
                            if let Some(Expr::Identifier(col_name)) = args.first() {
                                if let Some(idx) = table.column_index(col_name) {
                                    let values: Vec<f64> = matching_rows.iter()
                                        .filter_map(|row| row.get(idx))
                                        .filter_map(|v| match v {
                                            Value::Integer(i) => Some(*i as f64),
                                            Value::Float(f) => Some(*f),
                                            _ => None,
                                        })
                                        .collect();
                                    if values.is_empty() {
                                        Value::Null
                                    } else {
                                        let avg = values.iter().sum::<f64>() / values.len() as f64;
                                        Value::float(avg)
                                    }
                                } else {
                                    Value::Null
                                }
                            } else {
                                Value::Null
                            }
                        }
                        "MIN" => {
                            if let Some(Expr::Identifier(col_name)) = args.first() {
                                if let Some(idx) = table.column_index(col_name) {
                                    matching_rows.iter()
                                        .filter_map(|row| row.get(idx))
                                        .filter_map(|v| match v {
                                            Value::Integer(i) => Some(*i as f64),
                                            Value::Float(f) => Some(*f),
                                            _ => None,
                                        })
                                        .fold(None, |min, v| {
                                            Some(min.map_or(v, |m: f64| m.min(v)))
                                        })
                                        .map(Value::float)
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                }
                            } else {
                                Value::Null
                            }
                        }
                        "MAX" => {
                            if let Some(Expr::Identifier(col_name)) = args.first() {
                                if let Some(idx) = table.column_index(col_name) {
                                    matching_rows.iter()
                                        .filter_map(|row| row.get(idx))
                                        .filter_map(|v| match v {
                                            Value::Integer(i) => Some(*i as f64),
                                            Value::Float(f) => Some(*f),
                                            _ => None,
                                        })
                                        .fold(None, |max, v| {
                                            Some(max.map_or(v, |m: f64| m.max(v)))
                                        })
                                        .map(Value::float)
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                }
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    };
                    result_row.push(val);
                }
                ResultColumn::Expr(expr, _) => {
                    // Non-aggregate expression in aggregate query - use first row
                    if let Some(first_row) = matching_rows.first() {
                        let val = evaluate_expr(expr, first_row.as_slice(), table, matching_rows.len() as i64)?;
                        result_row.push(val);
                    } else {
                        result_row.push(Value::Null);
                    }
                }
                _ => {}
            }
        }

        if !result_row.is_empty() {
            result_rows.push(result_row);
        }
    } else {
        // Non-aggregate query - return each row
        for row_data in &matching_rows {
            let mut result_row = Vec::new();

            for col in &select.columns {
                match col {
                    ResultColumn::Star => {
                        // Expand * to all columns
                        for val in row_data.iter() {
                            result_row.push(val.clone());
                        }
                    }
                    ResultColumn::TableStar(tbl_name) => {
                        // Expand table.* to all columns of that table
                        if tbl_name.eq_ignore_ascii_case(table_name) {
                            for val in row_data.iter() {
                                result_row.push(val.clone());
                            }
                        }
                    }
                    ResultColumn::Expr(expr, _) => {
                        let val = evaluate_expr(expr, row_data.as_slice(), table, matching_rows.len() as i64)?;
                        result_row.push(val);
                    }
                }
            }

            result_rows.push(result_row);
        }
    }

    Ok(result_rows)
}

/// Build a predicate function from a WHERE expression
fn build_predicate(expr: &Expr, table: &Table) -> Result<Box<dyn Fn(&[Value]) -> bool>> {
    let expr = expr.clone();
    let table = table.clone();
    Ok(Box::new(move |row: &[Value]| evaluate_condition(&expr, row, &table)))
}

fn evaluate_condition(expr: &Expr, row: &[Value], table: &Table) -> bool {
    match expr {
        Expr::Unary(UnaryOp::Not, inner) => !evaluate_condition(inner, row, table),
        Expr::IsNull { expr: inner, not } => {
            let value = evaluate_expr(inner, row, table, 1).unwrap_or(Value::Null);
            if *not { !value.is_null() } else { value.is_null() }
        }
        Expr::Like { expr: inner, not, pattern } => {
            let value = evaluate_expr(inner, row, table, 1).unwrap_or(Value::Null);
            let pattern = evaluate_expr(pattern, row, table, 1).unwrap_or(Value::Null);
            let matches = match (value.as_text(), pattern.as_text()) {
                (Some(text), Some(pattern)) => like_match(text, pattern),
                _ => false,
            };
            if *not { !matches } else { matches }
        }
        Expr::Binary(op, left, right) => {
            match op {
                BinaryOp::And => {
                    evaluate_condition(left, row, table) && evaluate_condition(right, row, table)
                }
                BinaryOp::Or => {
                    evaluate_condition(left, row, table) || evaluate_condition(right, row, table)
                }
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::Less
                | BinaryOp::LessEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterEqual => {
                    let left_val = evaluate_expr(left, row, table, 1).unwrap_or(Value::Null);
                    let right_val = evaluate_expr(right, row, table, 1).unwrap_or(Value::Null);
                    compare_values(&left_val, &right_val, *op)
                }
                _ => {
                    let value = evaluate_expr(expr, row, table, 1).unwrap_or(Value::Null);
                    value_to_bool(&value)
                }
            }
        }
        _ => {
            let value = evaluate_expr(expr, row, table, 1).unwrap_or(Value::Null);
            value_to_bool(&value)
        }
    }
}

/// Simple LIKE pattern matching
fn like_match(s: &str, pattern: &str) -> bool {
    let s_lower = s.to_lowercase();
    let p_lower = pattern.to_lowercase();

    // Convert SQL LIKE pattern to regex-like matching
    let mut pattern_chars = p_lower.chars().peekable();
    let mut s_chars = s_lower.chars().peekable();

    fn match_helper(pat: &mut std::iter::Peekable<std::str::Chars>, s: &mut std::iter::Peekable<std::str::Chars>) -> bool {
        loop {
            match (pat.peek(), s.peek()) {
                (None, None) => return true,
                (None, Some(_)) => return false,
                (Some('%'), _) => {
                    pat.next();
                    // Skip consecutive %
                    while pat.peek() == Some(&'%') {
                        pat.next();
                    }
                    if pat.peek().is_none() {
                        return true; // % at end matches everything
                    }
                    // Try matching rest of pattern
                    while s.peek().is_some() {
                        let mut pat_clone = pat.clone();
                        let mut s_clone = s.clone();
                        if match_helper(&mut pat_clone, &mut s_clone) {
                            return true;
                        }
                        s.next();
                    }
                    return false;
                }
                (Some('_'), _) => {
                    pat.next();
                    s.next();
                }
                (Some(p), Some(c)) if *p == *c => {
                    pat.next();
                    s.next();
                }
                (Some(_), None) => return false,
                (Some(_), Some(_)) => return false,
            }
        }
    }

    match_helper(&mut pattern_chars, &mut s_chars)
}

fn value_to_bool(val: &Value) -> bool {
    match val {
        Value::Integer(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::Text(s) => !s.is_empty() && s != "0",
        _ => false,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(i1), Value::Integer(i2)) => i1 == i2,
        (Value::Float(f1), Value::Float(f2)) => f1 == f2,
        (Value::Text(t1), Value::Text(t2)) => t1 == t2,
        (Value::Integer(i), Value::Float(f)) => (*i as f64) == *f,
        (Value::Float(f), Value::Integer(i)) => *f == (*i as f64),
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

fn compare_values(left: &Value, right: &Value, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Equal => values_equal(left, right),
        BinaryOp::NotEqual => !values_equal(left, right),
        BinaryOp::Less => compare_values_cmp(left, right) == Some(std::cmp::Ordering::Less),
        BinaryOp::LessEqual => matches!(compare_values_cmp(left, right), Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)),
        BinaryOp::Greater => compare_values_cmp(left, right) == Some(std::cmp::Ordering::Greater),
        BinaryOp::GreaterEqual => matches!(compare_values_cmp(left, right), Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)),
        BinaryOp::And => value_to_bool(left) && value_to_bool(right),
        BinaryOp::Or => value_to_bool(left) || value_to_bool(right),
        _ => false,
    }
}

fn compare_values_cmp(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(i1), Value::Integer(i2)) => Some(i1.cmp(i2)),
        (Value::Float(f1), Value::Float(f2)) => Some(f1.partial_cmp(f2)?),
        (Value::Text(t1), Value::Text(t2)) => Some(t1.cmp(t2)),
        (Value::Integer(i), Value::Float(f)) => Some((*i as f64).partial_cmp(f)?),
        (Value::Float(f), Value::Integer(i)) => Some(f.partial_cmp(&(*i as f64))?),
        _ => None,
    }
}

/// Evaluate an expression against a row
fn evaluate_expr(
    expr: &Expr,
    row: &[Value],
    table: &crate::storage::Table,
    _row_count: i64,
) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Null => Ok(Value::Null),
        Expr::Bool(b) => Ok(Value::integer(if *b { 1 } else { 0 })),
        Expr::Identifier(name) => {
            // Find column by name
            if let Some(idx) = table.column_index(name) {
                Ok(row.get(idx).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        Expr::QualifiedIdentifier(_table_name, col_name) => {
            if let Some(idx) = table.column_index(col_name) {
                Ok(row.get(idx).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Binary(op, left, right) => {
            let left_val = evaluate_expr(left, row, table, _row_count)?;
            let right_val = evaluate_expr(right, row, table, _row_count)?;

            match op {
                BinaryOp::Add => {
                    match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a + b)),
                        (Value::Float(a), Value::Float(b)) => Ok(Value::float(a + b)),
                        (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 + b)),
                        (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a + *b as f64)),
                        _ => Ok(Value::Null),
                    }
                }
                BinaryOp::Subtract => {
                    match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a - b)),
                        (Value::Float(a), Value::Float(b)) => Ok(Value::float(a - b)),
                        (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 - b)),
                        (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a - *b as f64)),
                        _ => Ok(Value::Null),
                    }
                }
                BinaryOp::Multiply => {
                    match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a * b)),
                        (Value::Float(a), Value::Float(b)) => Ok(Value::float(a * b)),
                        (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 * b)),
                        (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a * *b as f64)),
                        _ => Ok(Value::Null),
                    }
                }
                BinaryOp::Divide => {
                    match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) if *b != 0 => Ok(Value::integer(a / b)),
                        (Value::Float(a), Value::Float(b)) if *b != 0.0 => Ok(Value::float(a / b)),
                        (Value::Integer(a), Value::Float(b)) if *b != 0.0 => Ok(Value::float(*a as f64 / b)),
                        (Value::Float(a), Value::Integer(b)) if *b != 0 => Ok(Value::float(a / *b as f64)),
                        _ => Ok(Value::Null),
                    }
                }
                BinaryOp::Concat => {
                    let left_str = match &left_val {
                        Value::Text(s) => s.clone(),
                        Value::Integer(i) => i.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Null => String::new(),
                        Value::Blob(b) => String::from_utf8_lossy(b).to_string(),
                    };
                    let right_str = match &right_val {
                        Value::Text(s) => s.clone(),
                        Value::Integer(i) => i.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Null => String::new(),
                        Value::Blob(b) => String::from_utf8_lossy(b).to_string(),
                    };
                    Ok(Value::text(format!("{}{}", left_str, right_str)))
                }
                _ => Ok(Value::Null),
            }
        }
        Expr::Function(name, args) => {
            let func_name = name.to_uppercase();
            match func_name.as_str() {
                "COUNT" => {
                    // COUNT(*) returns row count, COUNT(col) counts non-null values
                    if args.is_empty() {
                        Ok(Value::integer(1))
                    } else if let Expr::Identifier(col_name) = &args[0] {
                        if let Some(idx) = table.column_index(col_name) {
                            let count = row.get(idx)
                                .map(|v| if v.is_null() { 0 } else { 1 })
                                .unwrap_or(0);
                            Ok(Value::integer(count))
                        } else {
                            Ok(Value::integer(0))
                        }
                    } else {
                        // COUNT(*) - handled by aggregating at higher level
                        Ok(Value::integer(1))
                    }
                }
                "SUM" => {
                    if let Some(Expr::Identifier(col_name)) = args.first() {
                        if let Some(idx) = table.column_index(col_name) {
                            Ok(row.get(idx).cloned().unwrap_or(Value::Null))
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "AVG" => {
                    if let Some(Expr::Identifier(col_name)) = args.first() {
                        if let Some(idx) = table.column_index(col_name) {
                            let val = row.get(idx);
                            match val {
                                Some(Value::Integer(i)) => Ok(Value::float(*i as f64)),
                                Some(Value::Float(f)) => Ok(Value::float(*f)),
                                _ => Ok(Value::Null),
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "UPPER" => {
                    if let Some(Expr::Identifier(col_name)) = args.first() {
                        if let Some(idx) = table.column_index(col_name) {
                            if let Some(Value::Text(s)) = row.get(idx) {
                                Ok(Value::text(s.to_uppercase()))
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "LOWER" => {
                    if let Some(Expr::Identifier(col_name)) = args.first() {
                        if let Some(idx) = table.column_index(col_name) {
                            if let Some(Value::Text(s)) = row.get(idx) {
                                Ok(Value::text(s.to_lowercase()))
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "LENGTH" => {
                    if let Some(Expr::Identifier(col_name)) = args.first() {
                        if let Some(idx) = table.column_index(col_name) {
                            if let Some(val) = row.get(idx) {
                                match val {
                                    Value::Text(s) => Ok(Value::integer(s.len() as i64)),
                                    Value::Blob(b) => Ok(Value::integer(b.len() as i64)),
                                    _ => Ok(Value::Null),
                                }
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "ABS" => {
                    if let Some(Expr::Identifier(col_name)) = args.first() {
                        if let Some(idx) = table.column_index(col_name) {
                            if let Some(val) = row.get(idx) {
                                match val {
                                    Value::Integer(i) => Ok(Value::integer(i.abs())),
                                    Value::Float(f) => Ok(Value::float(f.abs())),
                                    _ => Ok(Value::Null),
                                }
                            } else {
                                Ok(Value::Null)
                            }
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "COALESCE" | "IFNULL" => {
                    for arg in args {
                        if let Expr::Identifier(col_name) = arg {
                            if let Some(idx) = table.column_index(col_name) {
                                if let Some(val) = row.get(idx) {
                                    if !val.is_null() {
                                        return Ok(val.clone());
                                    }
                                }
                            }
                        } else if let Expr::Literal(v) = arg {
                            if !v.is_null() {
                                return Ok(v.clone());
                            }
                        }
                    }
                    Ok(Value::Null)
                }
                "TYPEOF" => {
                    if let Some(Expr::Identifier(col_name)) = args.first() {
                        if let Some(idx) = table.column_index(col_name) {
                            if let Some(val) = row.get(idx) {
                                let type_name = match val {
                                    Value::Null => "null",
                                    Value::Integer(_) => "integer",
                                    Value::Float(_) => "real",
                                    Value::Text(_) => "text",
                                    Value::Blob(_) => "blob",
                                };
                                return Ok(Value::text(type_name.to_string()));
                            }
                        }
                    }
                    Ok(Value::text("null".to_string()))
                }
                _ => Ok(Value::Null),
            }
        }
        Expr::Parenthesized(inner) => {
            evaluate_expr(inner, row, table, _row_count)
        }
        _ => Ok(Value::Null),
    }
}

/// Iterator over result rows
pub struct Rows {
    statement: Statement,
    exhausted: bool,
}

impl Rows {
    /// Create a new rows iterator
    pub fn new(statement: Statement) -> Self {
        Rows {
            statement,
            exhausted: false,
        }
    }

    /// Get the next row
    pub fn next(&mut self) -> Result<Option<Vec<Value>>> {
        if self.exhausted {
            return Ok(None);
        }

        if self.statement.step()? {
            Ok(Some(self.statement.row()?))
        } else {
            self.exhausted = true;
            Ok(None)
        }
    }

    /// Get the underlying statement
    pub fn statement(&self) -> &Statement {
        &self.statement
    }

    /// Get the underlying statement mutably
    pub fn statement_mut(&mut self) -> &mut Statement {
        &mut self.statement
    }
}

impl Iterator for Rows {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement_new() {
        let conn = Connection::open_in_memory().unwrap();
        let stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();
        assert_eq!(stmt.sql(), "COMMIT");
        assert_eq!(stmt.state(), StatementState::Prepared);
    }

    #[test]
    fn test_statement_bind() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();

        stmt.bind_int(1, 42).unwrap();
        assert_eq!(stmt.parameter_count(), 1);

        stmt.bind_text(2, "hello").unwrap();
        assert_eq!(stmt.parameter_count(), 2);
    }

    #[test]
    fn test_statement_bind_null() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();

        stmt.bind_null(1).unwrap();
        assert!(stmt.params[0].is_null());
    }

    #[test]
    fn test_statement_bind_invalid_index() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();

        let result = stmt.bind_int(0, 42);
        assert!(result.is_err());
    }

    #[test]
    fn test_statement_reset() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();

        stmt.bind_int(1, 42).unwrap();
        stmt.reset().unwrap();
        assert_eq!(stmt.state(), StatementState::Prepared);
    }

    #[test]
    fn test_statement_step_commit() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();

        let has_row = stmt.step().unwrap();
        assert!(!has_row);
        assert_eq!(stmt.state(), StatementState::Done);
    }

    #[test]
    fn test_statement_row_without_step() {
        let conn = Connection::open_in_memory().unwrap();
        let stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();

        let result = stmt.row();
        assert!(result.is_err());
    }

    #[test]
    fn test_rows_iterator() {
        let conn = Connection::open_in_memory().unwrap();
        let stmt = Statement::new(&conn, AstStatement::Commit, "COMMIT".to_string()).unwrap();
        let rows = Rows::new(stmt);

        let count = rows.count();
        assert_eq!(count, 0);
    }
}
