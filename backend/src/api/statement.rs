//! Prepared statement implementation

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;
use crate::parser::ast::{Statement as AstStatement, AlterAction, ColumnConstraint, ColumnDef, CompoundOperator, CompoundSelectStmt, Expr, FromClause, InSource, JoinClause, JoinConstraint, JoinKind, OrderByItem, ResultColumn, SelectStmt, BinaryOp, UnaryOp};
use crate::storage::{Column, Storage, Table};
use super::connection::{Connection, IntoParams};
#[cfg(feature = "thread-safe")]
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

struct QuerySource<'a> {
    name: String,
    alias: Option<String>,
    start: usize,
    table: &'a Table,
}

impl QuerySource<'_> {
    fn width(&self) -> usize {
        self.table.columns.len()
    }

    fn end(&self) -> usize {
        self.start + self.width()
    }

    fn matches_name(&self, candidate: &str) -> bool {
        self.name.eq_ignore_ascii_case(candidate)
            || self
                .alias
                .as_ref()
                .is_some_and(|alias| alias.eq_ignore_ascii_case(candidate))
    }
}

struct QueryContext<'a> {
    sources: Vec<QuerySource<'a>>,
    unqualified_lookup: BTreeMap<String, usize>,
    qualified_lookup: BTreeMap<String, usize>,
}

impl<'a> QueryContext<'a> {
    fn resolve_identifier(&self, name: &str) -> Option<usize> {
        self.unqualified_lookup.get(&name.to_lowercase()).copied()
    }

    fn resolve_qualified_identifier(&self, table: &str, column: &str) -> Option<usize> {
        self.qualified_lookup
            .get(&format!("{}:{}", table.to_lowercase(), column.to_lowercase()))
            .copied()
    }

    fn slice_for_source<'b>(&self, row: &'b [Value], source_name: &str) -> Option<&'b [Value]> {
        self.sources
            .iter()
            .find(|source| source.matches_name(source_name))
            .map(|source| &row[source.start..source.end()])
    }
}

fn build_query_context<'a>(select: &SelectStmt, storage: &'a Storage) -> Result<QueryContext<'a>> {
    let Some(from) = &select.from else {
        return Ok(QueryContext {
            sources: Vec::new(),
            unqualified_lookup: BTreeMap::new(),
            qualified_lookup: BTreeMap::new(),
        });
    };

    let mut sources = Vec::new();
    let mut next_start = 0;

    for table_ref in &from.tables {
        let table = storage
            .get_table(&table_ref.name)
            .ok_or_else(|| Error::sqlite(ErrorCode::Error, &format!("no such table: {}", table_ref.name)))?;
        sources.push(QuerySource {
            name: table_ref.name.clone(),
            alias: table_ref.alias.clone(),
            start: next_start,
            table,
        });
        next_start += table.columns.len();
    }

    let mut occurrences: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let mut qualified_lookup = BTreeMap::new();

    for source in &sources {
        for (offset, column) in source.table.columns.iter().enumerate() {
            let absolute_index = source.start + offset;
            let column_key = column.name.to_lowercase();
            let entry = occurrences.entry(column_key.clone()).or_insert((0, absolute_index));
            entry.0 += 1;
            let qualified_key = format!("{}:{}", source.name.to_lowercase(), column_key);
            qualified_lookup.insert(qualified_key, absolute_index);
            if let Some(alias) = &source.alias {
                let alias_key = format!("{}:{}", alias.to_lowercase(), column.name.to_lowercase());
                qualified_lookup.insert(alias_key, absolute_index);
            }
        }
    }

    let unqualified_lookup = occurrences
        .into_iter()
        .filter_map(|(name, (count, index))| (count == 1).then_some((name, index)))
        .collect();

    Ok(QueryContext {
        sources,
        unqualified_lookup,
        qualified_lookup,
    })
}

fn build_joined_rows(from: &FromClause, context: &QueryContext<'_>) -> Result<Vec<Vec<Value>>> {
    let Some(first_source) = context.sources.first() else {
        return Ok(Vec::new());
    };

    if first_source.table.rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows: Vec<Vec<Value>> = first_source.table.rows.iter().cloned().collect();

    for (join_index, join) in from.joins.iter().enumerate() {
        let right_source_index = join_index + 1;
        let right_source = &context.sources[right_source_index];
        let mut next_rows = Vec::new();

        for left_row in &rows {
            let mut matched = false;

            for right_row in &right_source.table.rows {
                let mut combined = left_row.clone();
                combined.extend(right_row.iter().cloned());

                if join_matches(join, &combined, context, right_source_index)? {
                    matched = true;
                    next_rows.push(combined);
                }
            }

            if !matched && matches!(join.kind, JoinKind::Left) {
                let mut combined = left_row.clone();
                combined.extend(std::iter::repeat(Value::Null).take(right_source.width()));
                next_rows.push(combined);
            }
        }

        rows = next_rows;
    }

    Ok(rows)
}

fn join_matches(
    join: &JoinClause,
    row: &[Value],
    context: &QueryContext<'_>,
    right_source_index: usize,
) -> Result<bool> {
    match &join.constraint {
        None => Ok(true),
        Some(JoinConstraint::On(expr)) => Ok(evaluate_condition(expr, row, context)),
        Some(JoinConstraint::Using(columns)) => using_columns_match(columns, row, context, right_source_index),
    }
}

fn using_columns_match(
    columns: &[String],
    row: &[Value],
    context: &QueryContext<'_>,
    right_source_index: usize,
) -> Result<bool> {
    for column in columns {
        let (left_index, right_index) = resolve_using_column_indices(context, right_source_index, column)?;
        if !values_equal(
            row.get(left_index).unwrap_or(&Value::Null),
            row.get(right_index).unwrap_or(&Value::Null),
        ) {
            return Ok(false);
        }
    }

    Ok(true)
}

fn resolve_using_column_indices(
    context: &QueryContext<'_>,
    right_source_index: usize,
    column: &str,
) -> Result<(usize, usize)> {
    let right_source = context
        .sources
        .get(right_source_index)
        .ok_or_else(|| Error::sqlite(ErrorCode::Error, "invalid join source index"))?;
    let right_offset = right_source.table.column_index(column).ok_or_else(|| {
        Error::sqlite(
            ErrorCode::Error,
            &format!("USING column not found on right side: {}", column),
        )
    })?;

    let left_matches: Vec<usize> = context.sources[..right_source_index]
        .iter()
        .flat_map(|source| {
            source
                .table
                .column_index(column)
                .map(|offset| source.start + offset)
        })
        .collect();

    match left_matches.as_slice() {
        [left_index] => Ok((*left_index, right_source.start + right_offset)),
        [] => Err(Error::sqlite(
            ErrorCode::Error,
            &format!("USING column not found on left side: {}", column),
        )),
        _ => Err(Error::sqlite(
            ErrorCode::Error,
            &format!("ambiguous USING column on left side: {}", column),
        )),
    }
}

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
        match ast {
            AstStatement::Select(select) => {
                #[cfg(feature = "thread-safe")]
                let storage = conn.storage().read();
                #[cfg(not(feature = "thread-safe"))]
                let storage = conn.storage().borrow();
                return derive_select_column_info(select, &storage);
            }
            AstStatement::CompoundSelect(compound) => {
                #[cfg(feature = "thread-safe")]
                let storage = conn.storage().read();
                #[cfg(not(feature = "thread-safe"))]
                let storage = conn.storage().borrow();
                return derive_select_column_info(&compound.left, &storage);
            }
            _ => {
                // Non-select statements don't have result columns
            }
        }

        Ok((Vec::new(), Vec::new()))
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

            AstStatement::CompoundSelect(compound) => {
                #[cfg(feature = "thread-safe")]
                let storage = self.storage.read();
                #[cfg(not(feature = "thread-safe"))]
                let storage = self.storage.borrow();

                let result_rows = execute_compound_select_query(&compound, &storage)?;

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
                    columns.push(build_storage_column(col_def));
                }
                storage.create_table(&create.name, columns)?;
                self.state = StatementState::Done;
                Ok(false)
            }

            AstStatement::AlterTable(alter) => {
                #[cfg(feature = "thread-safe")]
                let mut storage = self.storage.write();
                #[cfg(not(feature = "thread-safe"))]
                let mut storage = self.storage.borrow_mut();

                match &alter.action {
                    AlterAction::RenameTo(new_name) => {
                        storage.rename_table(&alter.table, new_name)?;
                    }
                    AlterAction::RenameColumn { old, new } => {
                        storage.rename_column(&alter.table, old, new)?;
                    }
                    AlterAction::AddColumn(column) => {
                        storage.add_column(&alter.table, build_storage_column(column))?;
                    }
                    other => {
                        return Err(Error::sqlite(
                            ErrorCode::Error,
                            format!("unsupported alter table action: {:?}", other),
                        ));
                    }
                }

                self.state = StatementState::Done;
                Ok(false)
            }

            AstStatement::Insert(insert) => {
                #[cfg(feature = "thread-safe")]
                let mut storage = self.storage.write();
                #[cfg(not(feature = "thread-safe"))]
                let mut storage = self.storage.borrow_mut();

                // Get column mapping info first
                let (col_indices, num_cols, defaults) = {
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

                    let defaults = table
                        .columns
                        .iter()
                        .map(|column| column.default_value.clone())
                        .collect::<Vec<_>>();

                    (col_indices, table.columns.len(), defaults)
                };

                // Insert each row
                let table_name = insert.table.clone();
                for row_exprs in &insert.values {
                    let mut values = vec![Value::Null; num_cols];

                    for (column_index, default_value) in defaults.iter().enumerate() {
                        if let Some(default_value) = default_value {
                            values[column_index] = default_value.clone();
                        }
                    }

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

fn build_storage_column(col_def: &ColumnDef) -> Column {
    Column {
        name: col_def.name.clone(),
        col_type: col_def
            .type_name
            .clone()
            .unwrap_or_else(|| "TEXT".to_string()),
        not_null: col_def
            .constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::NotNull)),
        primary_key: col_def
            .constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::PrimaryKey { .. })),
        unique: col_def
            .constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::Unique)),
        default_value: col_def.constraints.iter().find_map(|constraint| match constraint {
            ColumnConstraint::Default(expr) => eval_expr_to_value(expr).ok(),
            _ => None,
        }),
    }
}

fn derive_select_column_info(select: &SelectStmt, storage: &Storage) -> Result<(Vec<String>, Vec<Option<String>>)> {
    let mut names = Vec::new();
    let mut types = Vec::new();

    for col in &select.columns {
        match col {
            ResultColumn::Star => {
                if let Some(from) = &select.from {
                    for source in &from.tables {
                        if let Some(table) = storage.get_table(&source.name) {
                            for column in &table.columns {
                                names.push(column.name.clone());
                                types.push(Some(column.col_type.clone()));
                            }
                        }
                    }

                    if !names.is_empty() {
                        continue;
                    }
                }

                names.push("*".to_string());
                types.push(None);
            }
            ResultColumn::TableStar(table_name) => {
                if let Some(from) = &select.from {
                    if let Some(source) = from.tables.iter().find(|source| {
                        source.name.eq_ignore_ascii_case(table_name)
                            || source
                                .alias
                                .as_ref()
                                .is_some_and(|alias| alias.eq_ignore_ascii_case(table_name))
                    }) {
                        if let Some(table) = storage.get_table(&source.name) {
                            for column in &table.columns {
                                names.push(column.name.clone());
                                types.push(Some(column.col_type.clone()));
                            }
                            continue;
                        }
                    }
                }

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

    Ok((names, types))
}

fn execute_compound_select_query(compound: &CompoundSelectStmt, storage: &Storage) -> Result<Vec<Vec<Value>>> {
    let (expected_columns, _) = derive_select_column_info(&compound.left, storage)?;
    let expected_width = expected_columns.len();
    let mut rows = execute_select_query(&compound.left, storage)?;
    validate_compound_width(&rows, expected_width)?;

    for part in &compound.parts {
        let (part_columns, _) = derive_select_column_info(&part.select, storage)?;
        if part_columns.len() != expected_width {
            return Err(Error::sqlite(
                ErrorCode::Error,
                "SELECTs in a compound query do not have the same number of result columns",
            ));
        }

        let part_rows = execute_select_query(&part.select, storage)?;
        validate_compound_width(&part_rows, expected_width)?;
        apply_compound_operator(&mut rows, part.operator, part_rows);
    }

    if !compound.order_by.is_empty() {
        let output_context = build_output_query_context(&expected_columns);
        rows.sort_by(|left, right| compare_compound_order_by_rows(left, right, &output_context, &compound.order_by));
    }

    apply_limit_offset(rows, compound.limit.as_ref(), compound.offset.as_ref())
}

fn execute_query_ast(statement: &AstStatement, storage: &Storage) -> Result<Vec<Vec<Value>>> {
    match statement {
        AstStatement::Select(select) => execute_select_query(select, storage),
        AstStatement::CompoundSelect(compound) => execute_compound_select_query(compound, storage),
        _ => Err(Error::sqlite(ErrorCode::Error, "subquery must be a SELECT statement")),
    }
}

fn query_ast_column_count(statement: &AstStatement, storage: &Storage) -> Result<usize> {
    match statement {
        AstStatement::Select(select) => Ok(derive_select_column_info(select, storage)?.0.len()),
        AstStatement::CompoundSelect(compound) => Ok(derive_select_column_info(&compound.left, storage)?.0.len()),
        _ => Err(Error::sqlite(ErrorCode::Error, "subquery must be a SELECT statement")),
    }
}

fn materialize_scalar_subquery(statement: &AstStatement, storage: &Storage) -> Result<Value> {
    let column_count = query_ast_column_count(statement, storage)?;
    if column_count != 1 {
        return Err(Error::sqlite(
            ErrorCode::Error,
            "scalar subquery must return exactly one column",
        ));
    }

    let rows = execute_query_ast(statement, storage)?;
    if rows.len() > 1 {
        return Err(Error::sqlite(
            ErrorCode::Error,
            "scalar subquery must return at most one row",
        ));
    }

    Ok(rows
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .unwrap_or(Value::Null))
}

fn materialize_select_in_subqueries(select: &SelectStmt, storage: &Storage) -> Result<SelectStmt> {
    Ok(SelectStmt {
        distinct: select.distinct,
        columns: select
            .columns
            .iter()
            .map(|column| match column {
                ResultColumn::Star => Ok(ResultColumn::Star),
                ResultColumn::TableStar(name) => Ok(ResultColumn::TableStar(name.clone())),
                ResultColumn::Expr(expr, alias) => Ok(ResultColumn::Expr(
                    materialize_expr_in_subqueries(expr, storage)?,
                    alias.clone(),
                )),
            })
            .collect::<Result<Vec<_>>>()?,
        from: select.from.clone(),
        where_clause: select
            .where_clause
            .as_ref()
            .map(|expr| materialize_expr_in_subqueries(expr, storage))
            .transpose()?,
        group_by: select
            .group_by
            .iter()
            .map(|expr| materialize_expr_in_subqueries(expr, storage))
            .collect::<Result<Vec<_>>>()?,
        having: select
            .having
            .as_ref()
            .map(|expr| materialize_expr_in_subqueries(expr, storage))
            .transpose()?,
        order_by: select
            .order_by
            .iter()
            .map(|item| {
                Ok(OrderByItem {
                    expr: materialize_expr_in_subqueries(&item.expr, storage)?,
                    ascending: item.ascending,
                    nulls_first: item.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        limit: select
            .limit
            .as_ref()
            .map(|expr| materialize_expr_in_subqueries(expr, storage))
            .transpose()?,
        offset: select
            .offset
            .as_ref()
            .map(|expr| materialize_expr_in_subqueries(expr, storage))
            .transpose()?,
    })
}

fn materialize_expr_in_subqueries(expr: &Expr, storage: &Storage) -> Result<Expr> {
    match expr {
        Expr::Unary(op, inner) => Ok(Expr::Unary(*op, Box::new(materialize_expr_in_subqueries(inner, storage)?))),
        Expr::Binary(op, left, right) => Ok(Expr::Binary(
            *op,
            Box::new(materialize_expr_in_subqueries(left, storage)?),
            Box::new(materialize_expr_in_subqueries(right, storage)?),
        )),
        Expr::Function(name, args) => Ok(Expr::Function(
            name.clone(),
            args.iter()
                .map(|arg| materialize_expr_in_subqueries(arg, storage))
                .collect::<Result<Vec<_>>>()?,
        )),
        Expr::Parenthesized(inner) => Ok(Expr::Parenthesized(Box::new(materialize_expr_in_subqueries(inner, storage)?))),
        Expr::Subquery(statement) => Ok(Expr::Literal(materialize_scalar_subquery(statement, storage)?)),
        Expr::Exists(statement) => {
            let rows = execute_query_ast(statement, storage)?;
            Ok(Expr::Bool(!rows.is_empty()))
        }
        Expr::In { expr, not, source } => {
            let inner = Box::new(materialize_expr_in_subqueries(expr, storage)?);
            let source = match source {
                InSource::List(list) => InSource::List(
                    list.iter()
                        .map(|item| materialize_expr_in_subqueries(item, storage))
                        .collect::<Result<Vec<_>>>()?,
                ),
                InSource::Subquery(statement) => {
                    let column_count = query_ast_column_count(statement, storage)?;
                    if column_count != 1 {
                        return Err(Error::sqlite(
                            ErrorCode::Error,
                            "subquery for IN must return exactly one column",
                        ));
                    }

                    let rows = execute_query_ast(statement, storage)?;
                    InSource::List(
                        rows.into_iter()
                            .map(|row| Expr::Literal(row.into_iter().next().unwrap_or(Value::Null)))
                            .collect(),
                    )
                }
            };

            Ok(Expr::In {
                expr: inner,
                not: *not,
                source,
            })
        }
        Expr::Between { expr, not, low, high } => Ok(Expr::Between {
            expr: Box::new(materialize_expr_in_subqueries(expr, storage)?),
            not: *not,
            low: Box::new(materialize_expr_in_subqueries(low, storage)?),
            high: Box::new(materialize_expr_in_subqueries(high, storage)?),
        }),
        Expr::IsNull { expr, not } => Ok(Expr::IsNull {
            expr: Box::new(materialize_expr_in_subqueries(expr, storage)?),
            not: *not,
        }),
        Expr::Like { expr, not, pattern } => Ok(Expr::Like {
            expr: Box::new(materialize_expr_in_subqueries(expr, storage)?),
            not: *not,
            pattern: Box::new(materialize_expr_in_subqueries(pattern, storage)?),
        }),
        _ => Ok(expr.clone()),
    }
}

fn build_output_query_context(columns: &[String]) -> QueryContext<'static> {
    let mut occurrences: BTreeMap<String, (usize, usize)> = BTreeMap::new();

    for (index, name) in columns.iter().enumerate() {
        let key = name.to_lowercase();
        let entry = occurrences.entry(key).or_insert((0, index));
        entry.0 += 1;
    }

    let unqualified_lookup = occurrences
        .into_iter()
        .filter_map(|(name, (count, index))| (count == 1).then_some((name, index)))
        .collect();

    QueryContext {
        sources: Vec::new(),
        unqualified_lookup,
        qualified_lookup: BTreeMap::new(),
    }
}

fn compare_compound_order_by_rows(
    left: &[Value],
    right: &[Value],
    context: &QueryContext<'_>,
    order_by: &[OrderByItem],
) -> Ordering {
    for item in order_by {
        let left_value = evaluate_compound_order_expr(&item.expr, left, context).unwrap_or(Value::Null);
        let right_value = evaluate_compound_order_expr(&item.expr, right, context).unwrap_or(Value::Null);
        let ordering = left_value.cmp(&right_value);

        if ordering != Ordering::Equal {
            return if item.ascending { ordering } else { ordering.reverse() };
        }
    }

    Ordering::Equal
}

fn evaluate_compound_order_expr(expr: &Expr, row: &[Value], context: &QueryContext<'_>) -> Result<Value> {
    if let Expr::Literal(Value::Integer(position)) = expr {
        if *position > 0 {
            return Ok(row.get((*position - 1) as usize).cloned().unwrap_or(Value::Null));
        }
    }

    evaluate_expr(expr, row, context, 1)
}

fn apply_compound_operator(rows: &mut Vec<Vec<Value>>, operator: CompoundOperator, mut part_rows: Vec<Vec<Value>>) {
    match operator {
        CompoundOperator::UnionAll => rows.append(&mut part_rows),
        CompoundOperator::Union => {
            rows.append(&mut part_rows);
            deduplicate_rows(rows);
        }
        CompoundOperator::Intersect => {
            deduplicate_rows(rows);
            deduplicate_rows(&mut part_rows);
            let right_rows: BTreeSet<Vec<Value>> = part_rows.into_iter().collect();
            rows.retain(|row| right_rows.contains(row));
        }
        CompoundOperator::Except => {
            deduplicate_rows(rows);
            deduplicate_rows(&mut part_rows);
            let right_rows: BTreeSet<Vec<Value>> = part_rows.into_iter().collect();
            rows.retain(|row| !right_rows.contains(row));
        }
    }
}

fn validate_compound_width(rows: &[Vec<Value>], expected_width: usize) -> Result<()> {
    if rows.iter().all(|row| row.len() == expected_width) {
        Ok(())
    } else {
        Err(Error::sqlite(
            ErrorCode::Error,
            "SELECTs in a compound query do not have the same number of result columns",
        ))
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
        Expr::QualifiedIdentifier(_, col) => col.clone(),
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
        Expr::Parenthesized(inner) => eval_expr_to_value(inner),
        Expr::Unary(UnaryOp::Negate, inner) => {
            match eval_expr_to_value(inner)? {
                Value::Integer(i) => Ok(Value::integer(-i)),
                Value::Float(f) => Ok(Value::float(-f)),
                _ => Ok(Value::Null),
            }
        }
        Expr::Unary(UnaryOp::Not, inner) => {
            let value = eval_expr_to_value(inner)?;
            Ok(Value::integer((!value_to_bool(&value)) as i64))
        }
        Expr::Unary(UnaryOp::BitNot, inner) => {
            match eval_expr_to_value(inner)? {
                Value::Integer(i) => Ok(Value::integer(!i)),
                _ => Ok(Value::Null),
            }
        }
        Expr::Binary(op, left, right) => {
            let left_val = eval_expr_to_value(left)?;
            let right_val = eval_expr_to_value(right)?;

            match op {
                BinaryOp::Add => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a + b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::float(a + b)),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 + b)),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a + *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Subtract => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a - b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::float(a - b)),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 - b)),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a - *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Multiply => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a * b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::float(a * b)),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 * b)),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a * *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Divide => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) if *b != 0 => Ok(Value::float(*a as f64 / *b as f64)),
                    (Value::Float(a), Value::Float(b)) if *b != 0.0 => Ok(Value::float(a / b)),
                    (Value::Integer(a), Value::Float(b)) if *b != 0.0 => Ok(Value::float(*a as f64 / b)),
                    (Value::Float(a), Value::Integer(b)) if *b != 0 => Ok(Value::float(a / *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Modulo => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) if *b != 0 => Ok(Value::integer(a % b)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Concat => Ok(Value::text(format!(
                    "{}{}",
                    left_val.to_string_value(),
                    right_val.to_string_value()
                ))),
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::Less
                | BinaryOp::LessEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterEqual => Ok(Value::integer(compare_values(&left_val, &right_val, *op) as i64)),
                BinaryOp::And => Ok(Value::integer((value_to_bool(&left_val) && value_to_bool(&right_val)) as i64)),
                BinaryOp::Or => Ok(Value::integer((value_to_bool(&left_val) || value_to_bool(&right_val)) as i64)),
            }
        }
        Expr::In { expr, not, source } => {
            let target = eval_expr_to_value(expr)?;
            let found = match source {
                InSource::List(list) => list.iter().any(|item| {
                    eval_expr_to_value(item)
                        .map(|value| values_equal(&target, &value))
                        .unwrap_or(false)
                }),
                InSource::Subquery(_) => false,
            };
            Ok(Value::integer((if *not { !found } else { found }) as i64))
        }
        Expr::Between { expr, not, low, high } => {
            let value = eval_expr_to_value(expr)?;
            let low = eval_expr_to_value(low)?;
            let high = eval_expr_to_value(high)?;
            let matches = matches!(compare_values_cmp(&value, &low), Some(Ordering::Greater | Ordering::Equal))
                && matches!(compare_values_cmp(&value, &high), Some(Ordering::Less | Ordering::Equal));
            Ok(Value::integer((if *not { !matches } else { matches }) as i64))
        }
        Expr::IsNull { expr, not } => {
            let value = eval_expr_to_value(expr)?;
            Ok(Value::integer((if *not { !value.is_null() } else { value.is_null() }) as i64))
        }
        Expr::Like { expr, not, pattern } => {
            let value = eval_expr_to_value(expr)?;
            let pattern = eval_expr_to_value(pattern)?;
            let matches = match (value.as_text(), pattern.as_text()) {
                (Some(text), Some(pattern)) => like_match(text, pattern),
                _ => false,
            };
            Ok(Value::integer((if *not { !matches } else { matches }) as i64))
        }
        _ => Ok(Value::Null),
    }
}

fn evaluate_offset_expr(expr: &Expr) -> Result<usize> {
    match eval_expr_to_value(expr)? {
        Value::Integer(value) => Ok(value.max(0) as usize),
        Value::Float(value) => Ok(value.max(0.0) as usize),
        Value::Null => Ok(0),
        other => Err(Error::sqlite(
            ErrorCode::Error,
            format!("OFFSET requires a numeric expression, got {}", other),
        )),
    }
}

fn evaluate_limit_expr(expr: &Expr) -> Result<Option<usize>> {
    match eval_expr_to_value(expr)? {
        Value::Integer(value) if value < 0 => Ok(None),
        Value::Integer(value) => Ok(Some(value as usize)),
        Value::Float(value) if value < 0.0 => Ok(None),
        Value::Float(value) => Ok(Some(value as usize)),
        Value::Null => Ok(Some(0)),
        other => Err(Error::sqlite(
            ErrorCode::Error,
            format!("LIMIT requires a numeric expression, got {}", other),
        )),
    }
}

fn compare_order_by_rows(left: &[Value], right: &[Value], context: &QueryContext<'_>, order_by: &[OrderByItem]) -> Ordering {
    for item in order_by {
        let left_value = evaluate_expr(&item.expr, left, context, 1).unwrap_or(Value::Null);
        let right_value = evaluate_expr(&item.expr, right, context, 1).unwrap_or(Value::Null);
        let ordering = left_value.cmp(&right_value);

        if ordering != Ordering::Equal {
            return if item.ascending {
                ordering
            } else {
                ordering.reverse()
            };
        }
    }

    Ordering::Equal
}

fn deduplicate_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen = BTreeSet::new();
    rows.retain(|row| seen.insert(row.clone()));
}

fn apply_limit_offset(mut rows: Vec<Vec<Value>>, limit: Option<&Expr>, offset: Option<&Expr>) -> Result<Vec<Vec<Value>>> {
    let offset_value = match offset {
        Some(expr) => evaluate_offset_expr(expr)?,
        None => 0,
    };

    let limit_value = match limit {
        Some(expr) => evaluate_limit_expr(expr)?,
        None => None,
    };

    let iter = rows.drain(..).skip(offset_value);
    let limited = match limit_value {
        Some(limit_value) => iter.take(limit_value).collect(),
        None => iter.collect(),
    };

    Ok(limited)
}

/// Execute a SELECT query and return result rows
fn execute_select_query(select: &SelectStmt, storage: &Storage) -> Result<Vec<Vec<Value>>> {
    let select = materialize_select_in_subqueries(select, storage)?;
    let mut result_rows = Vec::new();

    // Check if it's a simple SELECT with no FROM clause (like SELECT 1)
    if select.from.is_none() {
        let mut row = Vec::new();
        for col in &select.columns {
            match col {
                ResultColumn::Expr(expr, _) => {
                    row.push(eval_expr_to_value(expr)?);
                }
                _ => {}
            }
        }
        if !row.is_empty() {
            result_rows.push(row);
        }

        if select.distinct {
            deduplicate_rows(&mut result_rows);
        }

        return apply_limit_offset(result_rows, select.limit.as_ref(), select.offset.as_ref());
    }

    let context = build_query_context(&select, storage)?;
    if context.sources.is_empty() {
        return Ok(result_rows);
    }

    let Some(from) = &select.from else {
        return Ok(result_rows);
    };

    let combined_rows = build_joined_rows(from, &context)?;
    let mut matching_rows: Vec<&[Value]> = combined_rows.iter()
        .map(Vec::as_slice)
        .filter(|row| {
            select
                .where_clause
                .as_ref()
                .map_or(true, |expr| evaluate_condition(expr, row, &context))
        })
        .collect();

    let has_aggregates = select.columns.iter().any(result_column_contains_aggregate)
        || select.having.as_ref().map_or(false, expr_contains_aggregate)
        || select.order_by.iter().any(|item| expr_contains_aggregate(&item.expr));

    if has_aggregates || !select.group_by.is_empty() {
        let mut grouped_rows = build_row_groups(&select, &matching_rows, &context)?;
        let mut grouped_results = Vec::new();

        for group_rows in grouped_rows.drain(..) {
            let representative = group_rows.first().copied().unwrap_or(&[]);

            if let Some(having) = &select.having {
                let passes = evaluate_expr_in_group(having, representative, &group_rows, &context)
                    .map(|value| value_to_bool(&value))
                    .unwrap_or(false);
                if !passes {
                    continue;
                }
            }

            let mut result_row = Vec::new();
            for col in &select.columns {
                match col {
                    ResultColumn::Star => {
                        result_row.extend(representative.iter().cloned());
                    }
                    ResultColumn::TableStar(tbl_name) => {
                        if let Some(source_slice) = context.slice_for_source(representative, tbl_name) {
                            result_row.extend(source_slice.iter().cloned());
                        }
                    }
                    ResultColumn::Expr(expr, _) => {
                        result_row.push(evaluate_expr_in_group(expr, representative, &group_rows, &context)?);
                    }
                }
            }

            grouped_results.push(GroupedResultRow {
                row: result_row,
                representative: representative.to_vec(),
                group_rows,
            });
        }

        if select.distinct {
            deduplicate_grouped_rows(&mut grouped_results);
        }

        if !select.order_by.is_empty() {
            grouped_results.sort_by(|left, right| {
                compare_grouped_order_by_rows(left, right, &context, &select.order_by)
            });
        }

        let rows = grouped_results.into_iter().map(|group| group.row).collect();
        return apply_limit_offset(rows, select.limit.as_ref(), select.offset.as_ref());
    }

    if !select.order_by.is_empty() {
        matching_rows.sort_by(|left, right| compare_order_by_rows(left, right, &context, &select.order_by));
    }
    for row_data in &matching_rows {
        let mut result_row = Vec::new();

        for col in &select.columns {
            match col {
                ResultColumn::Star => {
                    for val in row_data.iter() {
                        result_row.push(val.clone());
                    }
                }
                ResultColumn::TableStar(tbl_name) => {
                    if let Some(source_slice) = context.slice_for_source(row_data, tbl_name) {
                        for val in source_slice.iter() {
                            result_row.push(val.clone());
                        }
                    }
                }
                ResultColumn::Expr(expr, _) => {
                    let val = evaluate_expr(expr, row_data, &context, matching_rows.len() as i64)?;
                    result_row.push(val);
                }
            }
        }

        result_rows.push(result_row);
    }

    if select.distinct {
        deduplicate_rows(&mut result_rows);
    }

    apply_limit_offset(result_rows, select.limit.as_ref(), select.offset.as_ref())
}

struct GroupedResultRow<'a> {
    row: Vec<Value>,
    representative: Vec<Value>,
    group_rows: Vec<&'a [Value]>,
}

fn build_row_groups<'a>(select: &SelectStmt, matching_rows: &[&'a [Value]], context: &QueryContext<'_>) -> Result<Vec<Vec<&'a [Value]>>> {
    if select.group_by.is_empty() {
        return Ok(vec![matching_rows.to_vec()]);
    }

    let mut groups: BTreeMap<Vec<Value>, Vec<&'a [Value]>> = BTreeMap::new();
    for row in matching_rows {
        let key = select.group_by.iter()
            .map(|expr| evaluate_expr(expr, row, context, matching_rows.len() as i64))
            .collect::<Result<Vec<_>>>()?;
        groups.entry(key).or_default().push(*row);
    }

    Ok(groups.into_values().collect())
}

fn result_column_contains_aggregate(column: &ResultColumn) -> bool {
    match column {
        ResultColumn::Expr(expr, _) => expr_contains_aggregate(expr),
        _ => false,
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(name, args) => {
            is_aggregate_function(name) || args.iter().any(expr_contains_aggregate)
        }
        Expr::Unary(_, inner)
        | Expr::Parenthesized(inner)
        | Expr::IsNull { expr: inner, .. } => expr_contains_aggregate(inner),
        Expr::Binary(_, left, right)
        | Expr::Like { expr: left, pattern: right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::Subquery(_) => false,
        Expr::Exists(_) => false,
        Expr::In { expr, source, .. } => {
            expr_contains_aggregate(expr)
                || match source {
                    InSource::List(list) => list.iter().any(expr_contains_aggregate),
                    InSource::Subquery(_) => false,
                }
        }
        Expr::Between { expr, low, high, .. } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        _ => false,
    }
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
}

fn compare_grouped_order_by_rows(
    left: &GroupedResultRow<'_>,
    right: &GroupedResultRow<'_>,
    context: &QueryContext<'_>,
    order_by: &[OrderByItem],
) -> Ordering {
    for item in order_by {
        let left_value = evaluate_expr_in_group(
            &item.expr,
            left.representative.as_slice(),
            &left.group_rows,
            context,
        )
        .unwrap_or(Value::Null);
        let right_value = evaluate_expr_in_group(
            &item.expr,
            right.representative.as_slice(),
            &right.group_rows,
            context,
        )
        .unwrap_or(Value::Null);
        let ordering = left_value.cmp(&right_value);

        if ordering != Ordering::Equal {
            return if item.ascending { ordering } else { ordering.reverse() };
        }
    }

    Ordering::Equal
}

fn deduplicate_grouped_rows(rows: &mut Vec<GroupedResultRow<'_>>) {
    let mut seen = BTreeSet::new();
    rows.retain(|row| seen.insert(row.row.clone()));
}

fn build_predicate(expr: &Expr, table: &Table) -> Result<Box<dyn Fn(&[Value]) -> bool>> {
    let expr = expr.clone();
    let table = table.clone();
    Ok(Box::new(move |row: &[Value]| {
        let context = QueryContext {
            unqualified_lookup: table
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| (column.name.to_lowercase(), index))
                .collect(),
            qualified_lookup: table
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| (format!("{}:{}", table.name.to_lowercase(), column.name.to_lowercase()), index))
                .collect(),
            sources: vec![QuerySource {
                name: table.name.clone(),
                alias: None,
                start: 0,
                table: &table,
            }],
        };
        evaluate_condition(&expr, row, &context)
    }))
}

fn evaluate_condition(expr: &Expr, row: &[Value], context: &QueryContext<'_>) -> bool {
    match expr {
        Expr::Unary(UnaryOp::Not, inner) => !evaluate_condition(inner, row, context),
        Expr::IsNull { expr: inner, not } => {
            let value = evaluate_expr(inner, row, context, 1).unwrap_or(Value::Null);
            if *not { !value.is_null() } else { value.is_null() }
        }
        Expr::Like { expr: inner, not, pattern } => {
            let value = evaluate_expr(inner, row, context, 1).unwrap_or(Value::Null);
            let pattern = evaluate_expr(pattern, row, context, 1).unwrap_or(Value::Null);
            let matches = match (value.as_text(), pattern.as_text()) {
                (Some(text), Some(pattern)) => like_match(text, pattern),
                _ => false,
            };
            if *not { !matches } else { matches }
        }
        Expr::In { expr: inner, not, source } => {
            let value = evaluate_expr(inner, row, context, 1).unwrap_or(Value::Null);
            let matches = match source {
                InSource::List(list) => list.iter().any(|item| {
                    evaluate_expr(item, row, context, 1)
                        .map(|candidate| values_equal(&value, &candidate))
                        .unwrap_or(false)
                }),
                InSource::Subquery(_) => false,
            };
            if *not { !matches } else { matches }
        }
        Expr::Between { expr: inner, not, low, high } => {
            let value = evaluate_expr(inner, row, context, 1).unwrap_or(Value::Null);
            let low_value = evaluate_expr(low, row, context, 1).unwrap_or(Value::Null);
            let high_value = evaluate_expr(high, row, context, 1).unwrap_or(Value::Null);
            let matches = matches!(compare_values_cmp(&value, &low_value), Some(Ordering::Greater | Ordering::Equal))
                && matches!(compare_values_cmp(&value, &high_value), Some(Ordering::Less | Ordering::Equal));
            if *not { !matches } else { matches }
        }
        Expr::Binary(op, left, right) => {
            match op {
                BinaryOp::And => {
                    evaluate_condition(left, row, context) && evaluate_condition(right, row, context)
                }
                BinaryOp::Or => {
                    evaluate_condition(left, row, context) || evaluate_condition(right, row, context)
                }
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::Less
                | BinaryOp::LessEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterEqual => {
                    let left_val = evaluate_expr(left, row, context, 1).unwrap_or(Value::Null);
                    let right_val = evaluate_expr(right, row, context, 1).unwrap_or(Value::Null);
                    compare_values(&left_val, &right_val, *op)
                }
                _ => {
                    let value = evaluate_expr(expr, row, context, 1).unwrap_or(Value::Null);
                    value_to_bool(&value)
                }
            }
        }
        _ => {
            let value = evaluate_expr(expr, row, context, 1).unwrap_or(Value::Null);
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

fn evaluate_expr_in_group(
    expr: &Expr,
    row: &[Value],
    group_rows: &[&[Value]],
    context: &QueryContext<'_>,
) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Null => Ok(Value::Null),
        Expr::Bool(b) => Ok(Value::integer(if *b { 1 } else { 0 })),
        Expr::Identifier(name) => {
            if let Some(idx) = context.resolve_identifier(name) {
                Ok(row.get(idx).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        Expr::QualifiedIdentifier(table_name, col_name) => {
            if let Some(idx) = context.resolve_qualified_identifier(table_name, col_name) {
                Ok(row.get(idx).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Unary(op, inner) => {
            let value = evaluate_expr_in_group(inner, row, group_rows, context)?;
            match op {
                UnaryOp::Negate => match value {
                    Value::Integer(i) => Ok(Value::integer(-i)),
                    Value::Float(f) => Ok(Value::float(-f)),
                    _ => Ok(Value::Null),
                },
                UnaryOp::Not => Ok(Value::integer((!value_to_bool(&value)) as i64)),
                UnaryOp::BitNot => match value {
                    Value::Integer(i) => Ok(Value::integer(!i)),
                    _ => Ok(Value::Null),
                },
            }
        }
        Expr::Binary(op, left, right) => {
            let left_val = evaluate_expr_in_group(left, row, group_rows, context)?;
            let right_val = evaluate_expr_in_group(right, row, group_rows, context)?;

            match op {
                BinaryOp::Add => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a + b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::float(a + b)),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 + b)),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a + *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Subtract => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a - b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::float(a - b)),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 - b)),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a - *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Multiply => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) => Ok(Value::integer(a * b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::float(a * b)),
                    (Value::Integer(a), Value::Float(b)) => Ok(Value::float(*a as f64 * b)),
                    (Value::Float(a), Value::Integer(b)) => Ok(Value::float(a * *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Divide => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) if *b != 0 => Ok(Value::float(*a as f64 / *b as f64)),
                    (Value::Float(a), Value::Float(b)) if *b != 0.0 => Ok(Value::float(a / b)),
                    (Value::Integer(a), Value::Float(b)) if *b != 0.0 => Ok(Value::float(*a as f64 / b)),
                    (Value::Float(a), Value::Integer(b)) if *b != 0 => Ok(Value::float(a / *b as f64)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Modulo => match (&left_val, &right_val) {
                    (Value::Integer(a), Value::Integer(b)) if *b != 0 => Ok(Value::integer(a % b)),
                    _ => Ok(Value::Null),
                },
                BinaryOp::Concat => Ok(Value::text(format!("{}{}", left_val.to_string_value(), right_val.to_string_value()))),
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::Less
                | BinaryOp::LessEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterEqual => Ok(Value::integer(compare_values(&left_val, &right_val, *op) as i64)),
                BinaryOp::And => Ok(Value::integer((value_to_bool(&left_val) && value_to_bool(&right_val)) as i64)),
                BinaryOp::Or => Ok(Value::integer((value_to_bool(&left_val) || value_to_bool(&right_val)) as i64)),
            }
        }
        Expr::Function(name, args) => {
            if is_aggregate_function(name) {
                evaluate_aggregate_function(name, args, group_rows, context)
            } else {
                evaluate_scalar_function(name, args, row, group_rows, context)
            }
        }
        Expr::Parenthesized(inner) => evaluate_expr_in_group(inner, row, group_rows, context),
        Expr::Subquery(_) => Ok(Value::Null),
        Expr::Exists(_) => Ok(Value::Null),
        Expr::In { expr, not, source } => {
            let value = evaluate_expr_in_group(expr, row, group_rows, context)?;
            let matches = match source {
                InSource::List(list) => list.iter().any(|item| {
                    evaluate_expr_in_group(item, row, group_rows, context)
                        .map(|candidate| values_equal(&value, &candidate))
                        .unwrap_or(false)
                }),
                InSource::Subquery(_) => false,
            };
            Ok(Value::integer((if *not { !matches } else { matches }) as i64))
        }
        Expr::Between { expr, not, low, high } => {
            let value = evaluate_expr_in_group(expr, row, group_rows, context)?;
            let low_value = evaluate_expr_in_group(low, row, group_rows, context)?;
            let high_value = evaluate_expr_in_group(high, row, group_rows, context)?;
            let matches = matches!(compare_values_cmp(&value, &low_value), Some(Ordering::Greater | Ordering::Equal))
                && matches!(compare_values_cmp(&value, &high_value), Some(Ordering::Less | Ordering::Equal));
            Ok(Value::integer((if *not { !matches } else { matches }) as i64))
        }
        Expr::IsNull { expr, not } => {
            let value = evaluate_expr_in_group(expr, row, group_rows, context)?;
            Ok(Value::integer((if *not { !value.is_null() } else { value.is_null() }) as i64))
        }
        Expr::Like { expr, not, pattern } => {
            let value = evaluate_expr_in_group(expr, row, group_rows, context)?;
            let pattern = evaluate_expr_in_group(pattern, row, group_rows, context)?;
            let matches = match (value.as_text(), pattern.as_text()) {
                (Some(text), Some(pattern)) => like_match(text, pattern),
                _ => false,
            };
            Ok(Value::integer((if *not { !matches } else { matches }) as i64))
        }
    }
}

fn evaluate_aggregate_function(
    name: &str,
    args: &[Expr],
    group_rows: &[&[Value]],
    context: &QueryContext<'_>,
) -> Result<Value> {
    let func_name = name.to_uppercase();

    match func_name.as_str() {
        "COUNT" => {
            if args.is_empty() {
                Ok(Value::integer(group_rows.len() as i64))
            } else {
                let count = group_rows.iter()
                    .filter_map(|row| evaluate_expr(&args[0], row, context, group_rows.len() as i64).ok())
                    .filter(|value| !value.is_null())
                    .count();
                Ok(Value::integer(count as i64))
            }
        }
        "SUM" => {
            let mut sum = 0.0;
            let mut found = false;
            for row in group_rows {
                match evaluate_expr(&args[0], row, context, group_rows.len() as i64)? {
                    Value::Integer(value) => {
                        sum += value as f64;
                        found = true;
                    }
                    Value::Float(value) => {
                        sum += value;
                        found = true;
                    }
                    _ => {}
                }
            }
            Ok(if found { Value::float(sum) } else { Value::Null })
        }
        "AVG" => {
            let mut sum = 0.0;
            let mut count = 0usize;
            for row in group_rows {
                match evaluate_expr(&args[0], row, context, group_rows.len() as i64)? {
                    Value::Integer(value) => {
                        sum += value as f64;
                        count += 1;
                    }
                    Value::Float(value) => {
                        sum += value;
                        count += 1;
                    }
                    _ => {}
                }
            }
            Ok(if count == 0 { Value::Null } else { Value::float(sum / count as f64) })
        }
        "MIN" => {
            let mut minimum: Option<Value> = None;
            for row in group_rows {
                let value = evaluate_expr(&args[0], row, context, group_rows.len() as i64)?;
                if value.is_null() {
                    continue;
                }
                if minimum.as_ref().map_or(true, |current| value < *current) {
                    minimum = Some(value);
                }
            }
            Ok(minimum.unwrap_or(Value::Null))
        }
        "MAX" => {
            let mut maximum: Option<Value> = None;
            for row in group_rows {
                let value = evaluate_expr(&args[0], row, context, group_rows.len() as i64)?;
                if value.is_null() {
                    continue;
                }
                if maximum.as_ref().map_or(true, |current| value > *current) {
                    maximum = Some(value);
                }
            }
            Ok(maximum.unwrap_or(Value::Null))
        }
        _ => Ok(Value::Null),
    }
}

fn evaluate_scalar_function(
    name: &str,
    args: &[Expr],
    row: &[Value],
    group_rows: &[&[Value]],
    context: &QueryContext<'_>,
) -> Result<Value> {
    let func_name = name.to_uppercase();

    match func_name.as_str() {
        "UPPER" => match args.first() {
            Some(arg) => match evaluate_expr_in_group(arg, row, group_rows, context)? {
                Value::Text(text) => Ok(Value::text(text.to_uppercase())),
                _ => Ok(Value::Null),
            },
            None => Ok(Value::Null),
        },
        "LOWER" => match args.first() {
            Some(arg) => match evaluate_expr_in_group(arg, row, group_rows, context)? {
                Value::Text(text) => Ok(Value::text(text.to_lowercase())),
                _ => Ok(Value::Null),
            },
            None => Ok(Value::Null),
        },
        "LENGTH" => match args.first() {
            Some(arg) => match evaluate_expr_in_group(arg, row, group_rows, context)? {
                Value::Text(text) => Ok(Value::integer(text.len() as i64)),
                Value::Blob(bytes) => Ok(Value::integer(bytes.len() as i64)),
                _ => Ok(Value::Null),
            },
            None => Ok(Value::Null),
        },
        "ABS" => match args.first() {
            Some(arg) => match evaluate_expr_in_group(arg, row, group_rows, context)? {
                Value::Integer(value) => Ok(Value::integer(value.abs())),
                Value::Float(value) => Ok(Value::float(value.abs())),
                _ => Ok(Value::Null),
            },
            None => Ok(Value::Null),
        },
        "COALESCE" | "IFNULL" => {
            for arg in args {
                let value = evaluate_expr_in_group(arg, row, group_rows, context)?;
                if !value.is_null() {
                    return Ok(value);
                }
            }
            Ok(Value::Null)
        }
        "TYPEOF" => {
            let value = match args.first() {
                Some(arg) => evaluate_expr_in_group(arg, row, group_rows, context)?,
                None => Value::Null,
            };
            let type_name = match value {
                Value::Null => "null",
                Value::Integer(_) => "integer",
                Value::Float(_) => "real",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
            };
            Ok(Value::text(type_name.to_string()))
        }
        _ => Ok(Value::Null),
    }
}

/// Evaluate an expression against a row
fn evaluate_expr(
    expr: &Expr,
    row: &[Value],
    context: &QueryContext<'_>,
    _row_count: i64,
) -> Result<Value> {
    evaluate_expr_in_group(expr, row, &[row], context)
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
        Rows::next(self).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql;

    fn test_column(name: &str, col_type: &str) -> Column {
        Column {
            name: name.to_string(),
            col_type: col_type.to_string(),
            not_null: false,
            primary_key: false,
            unique: false,
            default_value: None,
        }
    }

    fn seeded_select_storage() -> Storage {
        let mut storage = Storage::new();
        storage
            .create_table(
                "records",
                vec![
                    test_column("id", "INTEGER"),
                    test_column("category", "TEXT"),
                    test_column("score", "REAL"),
                    test_column("note", "TEXT"),
                ],
            )
            .unwrap();

        for row in [
            vec![Value::integer(1), Value::text("alpha"), Value::float(10.0), Value::Null],
            vec![Value::integer(2), Value::text("alpha"), Value::float(15.0), Value::text("eligible")],
            vec![Value::integer(3), Value::text("beta"), Value::float(22.5), Value::text("beta-only")],
            vec![Value::integer(4), Value::text("alpha"), Value::float(25.0), Value::text("priority")],
            vec![Value::integer(5), Value::text("gamma"), Value::float(30.0), Value::Null],
            vec![Value::integer(6), Value::text("alpha"), Value::float(28.0), Value::text("latest")],
        ] {
            storage.insert("records", row).unwrap();
        }

        storage
    }

    fn seeded_multi_table_storage() -> Storage {
        let mut storage = Storage::new();
        storage
            .create_table(
                "users",
                vec![
                    test_column("id", "INTEGER"),
                    test_column("name", "TEXT"),
                    test_column("team", "TEXT"),
                ],
            )
            .unwrap();
        storage
            .create_table(
                "orders",
                vec![
                    test_column("id", "INTEGER"),
                    test_column("user_id", "INTEGER"),
                    test_column("amount", "REAL"),
                ],
            )
            .unwrap();
        storage
            .create_table(
                "profiles",
                vec![
                    test_column("id", "INTEGER"),
                    test_column("nickname", "TEXT"),
                ],
            )
            .unwrap();

        for row in [
            vec![Value::integer(1), Value::text("Alice"), Value::text("red")],
            vec![Value::integer(2), Value::text("Bob"), Value::text("blue")],
        ] {
            storage.insert("users", row).unwrap();
        }

        for row in [
            vec![Value::integer(101), Value::integer(1), Value::float(19.5)],
            vec![Value::integer(102), Value::integer(1), Value::float(42.25)],
            vec![Value::integer(103), Value::integer(2), Value::float(15.75)],
        ] {
            storage.insert("orders", row).unwrap();
        }

        storage
            .insert("profiles", vec![Value::integer(1), Value::text("ally")])
            .unwrap();

        storage
    }

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

        let result = stmt.step();
        assert!(result.is_err());
        assert_eq!(stmt.state(), StatementState::Error);
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

    #[test]
    fn test_execute_select_distinct_order_limit_offset() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT DISTINCT category FROM records ORDER BY category DESC LIMIT 2 OFFSET 1"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::text("beta")],
            vec![Value::text("alpha")],
        ]);
    }

    #[test]
    fn test_execute_select_filters_and_decimal_division() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id, score / 2 AS half_score FROM records WHERE id IN (1, 2, 4, 6) AND score BETWEEN 10 AND 30 AND note IS NOT NULL AND category LIKE 'a%' ORDER BY score DESC LIMIT 2"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(6), Value::float(14.0)],
            vec![Value::integer(4), Value::float(12.5)],
        ]);
    }

    #[test]
    fn test_execute_select_without_from_uses_float_division() {
        let storage = Storage::new();
        let AstStatement::Select(select) = parse_sql(
            "SELECT 1 / 2 AS half, 5 / 2 AS two_point_five, 3.0 / 2 AS one_point_five"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![vec![
            Value::float(0.5),
            Value::float(2.5),
            Value::float(1.5),
        ]]);
    }

    #[test]
    fn test_execute_select_group_by_having_and_aggregates() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT category, COUNT(*) AS item_count, SUM(score) AS total_score, AVG(score) AS average_score FROM records GROUP BY category HAVING SUM(score) >= 20 ORDER BY SUM(score) DESC LIMIT 2"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![
                Value::text("alpha"),
                Value::integer(4),
                Value::float(78.0),
                Value::float(19.5),
            ],
            vec![
                Value::text("gamma"),
                Value::integer(1),
                Value::float(30.0),
                Value::float(30.0),
            ],
        ]);
    }

    #[test]
    fn test_execute_select_group_by_without_aggregates() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT category FROM records GROUP BY category HAVING category LIKE 'a%' ORDER BY category"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![vec![Value::text("alpha")]]);
    }

    #[test]
    fn test_execute_select_multiple_tables_with_aliases() {
        let storage = seeded_multi_table_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT u.name AS user_name, o.amount FROM users AS u, orders AS o WHERE u.id = o.user_id ORDER BY o.amount DESC LIMIT 2"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::text("Alice"), Value::float(42.25)],
            vec![Value::text("Alice"), Value::float(19.5)],
        ]);
    }

    #[test]
    fn test_execute_select_multiple_tables_with_table_star() {
        let storage = seeded_multi_table_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT users.*, orders.amount FROM users, orders WHERE users.id = orders.user_id AND orders.id = 103"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![vec![
            Value::integer(2),
            Value::text("Bob"),
            Value::text("blue"),
            Value::float(15.75),
        ]]);
    }

    #[test]
    fn test_execute_select_explicit_join_on() {
        let storage = seeded_multi_table_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT u.name AS user_name, o.amount FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE o.amount >= 19 ORDER BY o.amount DESC"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::text("Alice"), Value::float(42.25)],
            vec![Value::text("Alice"), Value::float(19.5)],
        ]);
    }

    #[test]
    fn test_execute_select_cross_join_count() {
        let storage = seeded_multi_table_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT COUNT(*) AS pair_count FROM users CROSS JOIN orders"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![vec![Value::integer(6)]]);
    }

    #[test]
    fn test_execute_select_left_join_on_preserves_unmatched_rows() {
        let storage = seeded_multi_table_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT u.name, p.nickname FROM users AS u LEFT JOIN profiles AS p ON u.id = p.id ORDER BY u.id"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::text("Alice"), Value::text("ally")],
            vec![Value::text("Bob"), Value::Null],
        ]);
    }

    #[test]
    fn test_execute_select_join_using_matches_shared_columns() {
        let storage = seeded_multi_table_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT users.id, profiles.nickname FROM users LEFT JOIN profiles USING (id) ORDER BY users.id"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(1), Value::text("ally")],
            vec![Value::integer(2), Value::Null],
        ]);
    }

    #[test]
    fn test_execute_union_distinct_rows() {
        let storage = Storage::new();
        let AstStatement::CompoundSelect(compound) = parse_sql(
            "SELECT 1 AS value UNION SELECT 1 AS value UNION SELECT 2 AS value"
        )
        .unwrap() else {
            panic!("expected compound select statement");
        };

        let rows = execute_compound_select_query(&compound, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(1)],
            vec![Value::integer(2)],
        ]);
    }

    #[test]
    fn test_execute_union_all_keeps_duplicates() {
        let storage = Storage::new();
        let AstStatement::CompoundSelect(compound) = parse_sql(
            "SELECT 1 AS value UNION ALL SELECT 1 AS value UNION ALL SELECT 2 AS value"
        )
        .unwrap() else {
            panic!("expected compound select statement");
        };

        let rows = execute_compound_select_query(&compound, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(1)],
            vec![Value::integer(1)],
            vec![Value::integer(2)],
        ]);
    }

    #[test]
    fn test_execute_intersect_returns_common_rows() {
        let storage = Storage::new();
        let AstStatement::CompoundSelect(compound) = parse_sql(
            "SELECT 1 AS value UNION ALL SELECT 2 AS value INTERSECT SELECT 2 AS value"
        )
        .unwrap() else {
            panic!("expected compound select statement");
        };

        let rows = execute_compound_select_query(&compound, &storage).unwrap();
        assert_eq!(rows, vec![vec![Value::integer(2)]]);
    }

    #[test]
    fn test_execute_except_removes_matching_rows() {
        let storage = Storage::new();
        let AstStatement::CompoundSelect(compound) = parse_sql(
            "SELECT 1 AS value UNION ALL SELECT 2 AS value EXCEPT SELECT 1 AS value"
        )
        .unwrap() else {
            panic!("expected compound select statement");
        };

        let rows = execute_compound_select_query(&compound, &storage).unwrap();
        assert_eq!(rows, vec![vec![Value::integer(2)]]);
    }

    #[test]
    fn test_execute_compound_order_by_limit_offset() {
        let storage = Storage::new();
        let AstStatement::CompoundSelect(compound) = parse_sql(
            "SELECT 1 AS value UNION ALL SELECT 3 AS value UNION ALL SELECT 2 AS value ORDER BY value DESC LIMIT 2 OFFSET 1"
        )
        .unwrap() else {
            panic!("expected compound select statement");
        };

        let rows = execute_compound_select_query(&compound, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(2)],
            vec![Value::integer(1)],
        ]);
    }

    #[test]
    fn test_execute_select_with_in_subquery() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id FROM records WHERE id IN (SELECT id FROM records WHERE category = 'alpha' AND note IS NOT NULL) ORDER BY id"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(2)],
            vec![Value::integer(4)],
            vec![Value::integer(6)],
        ]);
    }

    #[test]
    fn test_execute_select_with_in_subquery_rejects_multiple_columns() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id FROM records WHERE id IN (SELECT id, score FROM records)"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let err = execute_select_query(&select, &storage).unwrap_err();
        assert!(err.to_string().contains("subquery for IN must return exactly one column"));
    }

    #[test]
    fn test_execute_select_with_exists_subquery() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id FROM records WHERE EXISTS (SELECT id FROM records WHERE category = 'gamma') ORDER BY id LIMIT 2"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(1)],
            vec![Value::integer(2)],
        ]);
    }

    #[test]
    fn test_execute_select_with_not_exists_subquery() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id FROM records WHERE NOT EXISTS (SELECT id FROM records WHERE category = 'missing') ORDER BY id DESC LIMIT 1"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![vec![Value::integer(6)]]);
    }

    #[test]
    fn test_execute_select_with_scalar_subquery_in_projection() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT (SELECT category FROM records WHERE id = 3) AS picked_category"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![vec![Value::text("beta")]]);
    }

    #[test]
    fn test_execute_select_with_scalar_subquery_comparison() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id FROM records WHERE score > (SELECT 20.0) ORDER BY id"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let rows = execute_select_query(&select, &storage).unwrap();
        assert_eq!(rows, vec![
            vec![Value::integer(3)],
            vec![Value::integer(4)],
            vec![Value::integer(5)],
            vec![Value::integer(6)],
        ]);
    }

    #[test]
    fn test_execute_select_with_scalar_subquery_rejects_multiple_columns() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id FROM records WHERE id = (SELECT id, score FROM records WHERE id = 1)"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let err = execute_select_query(&select, &storage).unwrap_err();
        assert!(err.to_string().contains("scalar subquery must return exactly one column"));
    }

    #[test]
    fn test_execute_select_with_scalar_subquery_rejects_multiple_rows() {
        let storage = seeded_select_storage();
        let AstStatement::Select(select) = parse_sql(
            "SELECT id FROM records WHERE id = (SELECT id FROM records WHERE category = 'alpha')"
        )
        .unwrap() else {
            panic!("expected select statement");
        };

        let err = execute_select_query(&select, &storage).unwrap_err();
        assert!(err.to_string().contains("scalar subquery must return at most one row"));
    }

    #[test]
    fn test_execute_compound_rejects_mismatched_column_counts() {
        let storage = Storage::new();
        let AstStatement::CompoundSelect(compound) = parse_sql(
            "SELECT 1 AS value UNION SELECT 1 AS value, 2 AS other"
        )
        .unwrap() else {
            panic!("expected compound select statement");
        };

        let err = execute_compound_select_query(&compound, &storage).unwrap_err();
        assert!(err.to_string().contains("compound query do not have the same number of result columns"));
    }
}
