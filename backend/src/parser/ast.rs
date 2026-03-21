//! Abstract Syntax Tree definitions for SQL

#![allow(missing_docs)]

use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A SQL statement (simplified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    /// SELECT statement
    Select(SelectStmt),
    /// Compound SELECT statement
    CompoundSelect(CompoundSelectStmt),
    /// INSERT statement
    Insert(InsertStmt),
    /// UPDATE statement
    Update(UpdateStmt),
    /// DELETE statement
    Delete(DeleteStmt),
    /// CREATE TABLE statement
    CreateTable(CreateTableStmt),
    /// CREATE INDEX statement
    CreateIndex(CreateIndexStmt),
    /// CREATE VIEW statement
    CreateView(CreateViewStmt),
    /// CREATE TRIGGER statement
    CreateTrigger(CreateTriggerStmt),
    /// DROP statement
    Drop(DropStmt),
    /// ALTER TABLE statement
    AlterTable(AlterTableStmt),
    /// BEGIN TRANSACTION
    Begin(BeginStmt),
    /// COMMIT
    Commit,
    /// ROLLBACK
    Rollback(RollbackStmt),
    /// PRAGMA statement
    Pragma(PragmaStmt),
    /// EXPLAIN statement
    Explain(ExplainStmt),
    /// VACUUM statement
    Vacuum(VacuumStmt),
    /// ANALYZE statement
    Analyze(AnalyzeStmt),
    /// ATTACH DATABASE
    Attach(AttachStmt),
    /// DETACH DATABASE
    Detach(DetachStmt),
}

/// SELECT statement (simplified)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStmt {
    /// DISTINCT modifier
    pub distinct: bool,
    /// Result columns
    pub columns: Vec<ResultColumn>,
    /// FROM clause
    pub from: Option<FromClause>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
    /// GROUP BY clause
    pub group_by: Vec<Expr>,
    /// HAVING clause
    pub having: Option<Expr>,
    /// ORDER BY clause
    pub order_by: Vec<OrderByItem>,
    /// LIMIT clause
    pub limit: Option<Expr>,
    /// OFFSET clause
    pub offset: Option<Expr>,
}

/// Compound SELECT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompoundSelectStmt {
    /// Left-most select
    pub left: SelectStmt,
    /// Remaining compound parts
    pub parts: Vec<CompoundSelectPart>,
    /// ORDER BY clause applied after compound evaluation
    pub order_by: Vec<OrderByItem>,
    /// LIMIT clause applied after compound evaluation
    pub limit: Option<Expr>,
    /// OFFSET clause applied after compound evaluation
    pub offset: Option<Expr>,
}

/// IN expression right-hand side
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InSource {
    /// Explicit expression list
    List(Vec<Expr>),
    /// SELECT subquery
    Subquery(Box<Statement>),
}

/// One compound SELECT segment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompoundSelectPart {
    /// Compound operator
    pub operator: CompoundOperator,
    /// Right-hand select
    pub select: SelectStmt,
}

/// Supported compound operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompoundOperator {
    /// UNION DISTINCT
    Union,
    /// UNION ALL
    UnionAll,
    /// INTERSECT DISTINCT
    Intersect,
    /// EXCEPT DISTINCT
    Except,
}

/// Result column in SELECT
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResultColumn {
    /// *
    Star,
    /// table.*
    TableStar(String),
    /// expr [AS alias]
    Expr(Expr, Option<String>),
}

/// FROM clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FromClause {
    /// Table references
    pub tables: Vec<TableRef>,
    /// Join operators between adjacent table references
    pub joins: Vec<JoinClause>,
}

/// Join operator between adjacent table references
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    /// Join kind
    pub kind: JoinKind,
    /// Join constraint
    pub constraint: Option<JoinConstraint>,
}

/// Supported join kinds
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind {
    /// Comma join or CROSS JOIN
    Cross,
    /// JOIN or INNER JOIN
    Inner,
    /// LEFT JOIN or LEFT OUTER JOIN
    Left,
}

/// Supported join constraints
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinConstraint {
    /// ON predicate
    On(Expr),
    /// USING column list
    Using(Vec<String>),
}

/// Table reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableRef {
    /// Table name
    pub name: String,
    /// Alias
    pub alias: Option<String>,
    /// Schema/database name
    pub schema: Option<String>,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: Option<bool>,
}

/// Expression (simplified to avoid recursive type issues)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Literal value
    Literal(Value),
    /// NULL
    Null,
    /// Boolean literal
    Bool(bool),
    /// Identifier (column, table, etc.)
    Identifier(String),
    /// Qualified identifier (table.column)
    QualifiedIdentifier(String, String),
    /// Unary operator
    Unary(UnaryOp, Box<Expr>),
    /// Binary operator
    Binary(BinaryOp, Box<Expr>, Box<Expr>),
    /// Function call
    Function(String, Vec<Expr>),
    /// Parenthesized expression
    Parenthesized(Box<Expr>),
    /// Scalar subquery
    Subquery(Box<Statement>),
    /// EXISTS subquery
    Exists(Box<Statement>),
    /// IN expression
    In {
        expr: Box<Expr>,
        not: bool,
        source: InSource,
    },
    /// BETWEEN expression
    Between {
        expr: Box<Expr>,
        not: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expr>,
        not: bool,
    },
    /// LIKE / GLOB
    Like {
        expr: Box<Expr>,
        not: bool,
        pattern: Box<Expr>,
    },
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    Negate,
    Not,
    BitNot,
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Concat,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStmt {
    /// Table name
    pub table: String,
    /// Schema name
    pub schema: Option<String>,
    /// Column names
    pub columns: Vec<String>,
    /// Insert source
    pub source: InsertSource,
    /// Optional UPSERT clause
    pub on_conflict: Option<OnConflictClause>,
}

/// INSERT source
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InsertSource {
    /// VALUES clause
    Values(Vec<Vec<Expr>>),
    /// INSERT FROM SELECT / compound SELECT
    Select(Box<Statement>),
}

/// UPSERT clause attached to INSERT
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnConflictClause {
    /// Optional conflict target column names
    pub target_columns: Vec<String>,
    /// Action when a uniqueness conflict is found
    pub action: OnConflictAction,
}

/// UPSERT conflict action
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OnConflictAction {
    /// Skip the conflicting row
    DoNothing,
    /// Update the conflicting row
    DoUpdate {
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    },
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStmt {
    /// Table name
    pub table: String,
    /// Schema name
    pub schema: Option<String>,
    /// SET assignments
    pub assignments: Vec<(String, Expr)>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStmt {
    /// Table name
    pub table: String,
    /// Schema name
    pub schema: Option<String>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStmt {
    /// IF NOT EXISTS
    pub if_not_exists: bool,
    /// Table name
    pub name: String,
    /// Schema name
    pub schema: Option<String>,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: Option<String>,
    pub constraints: Vec<ColumnConstraint>,
}

/// Column constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnConstraint {
    PrimaryKey { auto_increment: bool },
    NotNull,
    Unique,
    Check(Expr),
    Default(Expr),
    ForeignKey(ForeignKeyClause),
}

/// Foreign key clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKeyClause {
    pub table: String,
    pub schema: Option<String>,
    pub columns: Vec<String>,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateIndexStmt {
    pub unique: bool,
    pub if_not_exists: bool,
    pub name: String,
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<IndexedColumn>,
    pub where_clause: Option<Expr>,
}

/// Indexed column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedColumn {
    pub name: String,
    pub collation: Option<String>,
    pub ascending: Option<bool>,
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateViewStmt {
    pub if_not_exists: bool,
    pub temp: bool,
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<String>,
    pub select: SelectStmt,
}

/// CREATE TRIGGER statement (stub)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTriggerStmt {
    pub name: String,
}

/// DROP statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropStmt {
    pub object_type: ObjectType,
    pub if_exists: bool,
    pub name: String,
    pub schema: Option<String>,
}

/// Object type for DROP
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    Table,
    Index,
    View,
    Trigger,
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStmt {
    pub table: String,
    pub schema: Option<String>,
    pub action: AlterAction,
}

/// Alter action
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterAction {
    RenameTo(String),
    AddColumn(ColumnDef),
    DropColumn(String),
    RenameColumn { old: String, new: String },
}

/// BEGIN statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeginStmt {
    pub transaction_type: Option<TransactionType>,
}

/// Transaction type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionType {
    Deferred,
    Immediate,
    Exclusive,
}

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollbackStmt {
    pub savepoint: Option<String>,
}

/// PRAGMA statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PragmaStmt {
    pub schema: Option<String>,
    pub name: String,
    pub value: Option<PragmaValue>,
}

/// PRAGMA value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PragmaValue {
    Expr(Expr),
    Equals(Expr),
}

/// EXPLAIN statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainStmt {
    pub query_plan: bool,
    pub statement: Box<Statement>,
}

/// VACUUM statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VacuumStmt {
    pub schema: Option<String>,
    pub into: Option<String>,
}

/// ANALYZE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyzeStmt {
    pub schema: Option<String>,
    pub table: Option<String>,
    pub index: Option<String>,
}

/// ATTACH statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttachStmt {
    pub database: Expr,
    pub name: String,
    pub key: Option<Expr>,
}

/// DETACH statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetachStmt {
    pub name: String,
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(_) => write!(f, "SELECT ..."),
            Statement::CompoundSelect(_) => write!(f, "SELECT ... UNION ..."),
            Statement::Insert(i) => write!(f, "INSERT INTO {} ...", i.table),
            Statement::Update(u) => write!(f, "UPDATE {} ...", u.table),
            Statement::Delete(d) => write!(f, "DELETE FROM {} ...", d.table),
            Statement::CreateTable(t) => write!(f, "CREATE TABLE {} ...", t.name),
            Statement::CreateIndex(i) => write!(f, "CREATE INDEX {} ...", i.name),
            Statement::Drop(d) => write!(f, "DROP {} ...", d.object_type),
            Statement::Commit => write!(f, "COMMIT"),
            Statement::Rollback(_) => write!(f, "ROLLBACK"),
            _ => write!(f, "{:?}", self),
        }
    }
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectType::Table => write!(f, "TABLE"),
            ObjectType::Index => write!(f, "INDEX"),
            ObjectType::View => write!(f, "VIEW"),
            ObjectType::Trigger => write!(f, "TRIGGER"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement_display() {
        let stmt = Statement::Commit;
        assert_eq!(format!("{}", stmt), "COMMIT");
    }

    #[test]
    fn test_select_stmt() {
        let stmt = SelectStmt {
            distinct: false,
            columns: vec![ResultColumn::Star],
            from: None,
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        assert!(!stmt.distinct);
    }
}
