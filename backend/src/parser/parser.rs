//! SQL Parser implementation (simplified)

use crate::error::{Error, Result};
use crate::tokenizer::{Lexer, Token, TokenType};
use super::ast::*;

/// SQL Parser
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,
    peek: Token,
}

impl<'a> Parser<'a> {
    /// Create a new parser for the given input
    pub fn new(input: &'a str) -> Result<Self> {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token()?;
        let peek = lexer.next_token()?;

        Ok(Parser {
            lexer,
            current,
            peek,
        })
    }

    /// Parse a SQL statement
    pub fn parse(&mut self) -> Result<Statement> {
        match self.current.ty {
            TokenType::Select => self.parse_select(),
            TokenType::Insert => self.parse_insert(),
            TokenType::Update => self.parse_update(),
            TokenType::Delete => self.parse_delete(),
            TokenType::Create => self.parse_create(),
            TokenType::Drop => self.parse_drop(),
            TokenType::Alter => self.parse_alter(),
            TokenType::Begin => self.parse_begin(),
            TokenType::Commit => {
                self.advance();
                Ok(Statement::Commit)
            }
            TokenType::Rollback => self.parse_rollback(),
            TokenType::Pragma => self.parse_pragma(),
            TokenType::Explain => self.parse_explain(),
            TokenType::Vacuum => self.parse_vacuum(),
            TokenType::Analyze => self.parse_analyze(),
            TokenType::Attached => self.parse_attach(),
            TokenType::Detach => self.parse_detach(),
            _ => Err(Error::Parse(format!(
                "Unexpected token: {}",
                self.current.value
            ))),
        }
    }

    /// Parse SELECT statement
    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(TokenType::Select)?;

        let distinct = self.consume(TokenType::Distinct)?;
        let _ = self.consume(TokenType::All);

        let columns = self.parse_result_columns()?;

        let from = if self.consume(TokenType::From)? {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.consume(TokenType::Group)? {
            self.expect(TokenType::By)?;
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.consume(TokenType::Having)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.consume(TokenType::Order)? {
            self.expect(TokenType::By)?;
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        let limit = if self.consume(TokenType::Limit)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.consume(TokenType::Offset)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStmt {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        }))
    }

    /// Parse result columns
    fn parse_result_columns(&mut self) -> Result<Vec<ResultColumn>> {
        let mut columns = vec![self.parse_result_column()?];

        while self.consume(TokenType::Comma)? {
            columns.push(self.parse_result_column()?);
        }

        Ok(columns)
    }

    /// Parse result column
    fn parse_result_column(&mut self) -> Result<ResultColumn> {
        if self.consume(TokenType::Star)? {
            return Ok(ResultColumn::Star);
        }

        let expr = self.parse_expr()?;

        if self.consume(TokenType::Dot)? && self.current.ty == TokenType::Star {
            self.advance();
            if let Expr::Identifier(table) = expr {
                return Ok(ResultColumn::TableStar(table));
            }
        }

        let alias = if self.consume(TokenType::As)? {
            Some(self.parse_identifier()?)
        } else if matches!(self.current.ty, TokenType::Identifier | TokenType::QuotedIdentifier) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(ResultColumn::Expr(expr, alias))
    }

    /// Parse FROM clause
    fn parse_from_clause(&mut self) -> Result<FromClause> {
        let mut tables = vec![self.parse_table_ref()?];

        while self.consume(TokenType::Comma)? {
            tables.push(self.parse_table_ref()?);
        }

        Ok(FromClause { tables })
    }

    /// Parse table reference
    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let name = self.parse_identifier()?;
        let alias = if self.consume(TokenType::As)? {
            Some(self.parse_identifier()?)
        } else if matches!(self.current.ty, TokenType::Identifier | TokenType::QuotedIdentifier) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(TableRef {
            name,
            alias,
            schema: None,
        })
    }

    /// Parse ORDER BY list
    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = vec![self.parse_order_by_item()?];

        while self.consume(TokenType::Comma)? {
            items.push(self.parse_order_by_item()?);
        }

        Ok(items)
    }

    /// Parse ORDER BY item
    fn parse_order_by_item(&mut self) -> Result<OrderByItem> {
        let expr = self.parse_expr()?;

        let ascending = if self.consume(TokenType::Desc)? {
            false
        } else {
            let _ = self.consume(TokenType::Asc);
            true
        };

        Ok(OrderByItem {
            expr,
            ascending,
            nulls_first: None,
        })
    }

    /// Parse expression
    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    /// Parse OR expression
    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;

        while self.consume(TokenType::Or)? {
            let right = self.parse_and_expr()?;
            left = Expr::Binary(BinaryOp::Or, Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expr()?;

        while self.consume(TokenType::And)? {
            let right = self.parse_not_expr()?;
            left = Expr::Binary(BinaryOp::And, Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    /// Parse NOT expression
    fn parse_not_expr(&mut self) -> Result<Expr> {
        if self.consume(TokenType::Not)? {
            Ok(Expr::Unary(UnaryOp::Not, Box::new(self.parse_not_expr()?)))
        } else {
            self.parse_comparison_expr()
        }
    }

    /// Parse comparison expression
    fn parse_comparison_expr(&mut self) -> Result<Expr> {
        let left = self.parse_additive_expr()?;

        let op = match self.current.ty {
            TokenType::Equal => BinaryOp::Equal,
            TokenType::NotEqual => BinaryOp::NotEqual,
            TokenType::Less => BinaryOp::Less,
            TokenType::LessEqual => BinaryOp::LessEqual,
            TokenType::Greater => BinaryOp::Greater,
            TokenType::GreaterEqual => BinaryOp::GreaterEqual,
            _ => return Ok(left),
        };

        self.advance();
        let right = self.parse_additive_expr()?;
        Ok(Expr::Binary(op, Box::new(left), Box::new(right)))
    }

    /// Parse additive expression
    fn parse_additive_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = match self.current.ty {
                TokenType::Plus => BinaryOp::Add,
                TokenType::Minus => BinaryOp::Subtract,
                TokenType::Concat => BinaryOp::Concat,
                _ => break,
            };

            self.advance();
            let right = self.parse_multiplicative_expr()?;
            left = Expr::Binary(op, Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    /// Parse multiplicative expression
    fn parse_multiplicative_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expr()?;

        loop {
            let op = match self.current.ty {
                TokenType::Star => BinaryOp::Multiply,
                TokenType::Slash => BinaryOp::Divide,
                TokenType::Percent => BinaryOp::Modulo,
                _ => break,
            };

            self.advance();
            let right = self.parse_unary_expr()?;
            left = Expr::Binary(op, Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    /// Parse unary expression
    fn parse_unary_expr(&mut self) -> Result<Expr> {
        let op = match self.current.ty {
            TokenType::Minus => UnaryOp::Negate,
            TokenType::Plus => return self.parse_primary_expr(),
            TokenType::BitNot => UnaryOp::BitNot,
            _ => return self.parse_primary_expr(),
        };

        self.advance();
        Ok(Expr::Unary(op, Box::new(self.parse_unary_expr()?)))
    }

    /// Parse primary expression
    fn parse_primary_expr(&mut self) -> Result<Expr> {
        match self.current.ty {
            TokenType::Integer => {
                let value = self.current.value.parse::<i64>().map_err(|_| {
                    Error::Parse(format!("Invalid integer: {}", self.current.value))
                })?;
                self.advance();
                Ok(Expr::Literal(crate::types::Value::integer(value)))
            }
            TokenType::Float => {
                let value = self.current.value.parse::<f64>().map_err(|_| {
                    Error::Parse(format!("Invalid float: {}", self.current.value))
                })?;
                self.advance();
                Ok(Expr::Literal(crate::types::Value::float(value)))
            }
            TokenType::String => {
                let value = self.current.value.clone();
                self.advance();
                Ok(Expr::Literal(crate::types::Value::text(value)))
            }
            TokenType::Null => {
                self.advance();
                Ok(Expr::Null)
            }
            TokenType::True => {
                self.advance();
                Ok(Expr::Bool(true))
            }
            TokenType::False => {
                self.advance();
                Ok(Expr::Bool(false))
            }
            TokenType::Identifier | TokenType::QuotedIdentifier => {
                let name = self.parse_identifier()?;

                if self.consume(TokenType::Dot)? {
                    let col_name = self.parse_identifier()?;
                    return Ok(Expr::QualifiedIdentifier(name, col_name));
                }

                if self.current.ty == TokenType::LeftParen {
                    return self.parse_function_call(name);
                }

                Ok(Expr::Identifier(name))
            }
            TokenType::LeftParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(TokenType::RightParen)?;
                Ok(Expr::Parenthesized(Box::new(expr)))
            }
            _ => Err(Error::Parse(format!(
                "Unexpected token in expression: {}",
                self.current.value
            ))),
        }
    }

    /// Parse function call
    fn parse_function_call(&mut self, name: String) -> Result<Expr> {
        self.expect(TokenType::LeftParen)?;

        // Handle COUNT(*) and other aggregate functions with *
        let args = if self.current.ty != TokenType::RightParen {
            // Check for * in aggregate functions
            if name.eq_ignore_ascii_case("COUNT") && self.current.ty == TokenType::Star {
                self.advance(); // consume *
                vec![] // Empty args means COUNT(*)
            } else {
                self.parse_expr_list()?
            }
        } else {
            vec![]
        };

        self.expect(TokenType::RightParen)?;
        Ok(Expr::Function(name, args))
    }

    /// Parse expression list
    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = vec![self.parse_expr()?];

        while self.consume(TokenType::Comma)? {
            exprs.push(self.parse_expr()?);
        }

        Ok(exprs)
    }

    /// Parse identifier
    fn parse_identifier(&mut self) -> Result<String> {
        match self.current.ty {
            TokenType::Identifier | TokenType::QuotedIdentifier => {
                let name = self.current.value.clone();
                self.advance();
                Ok(name)
            }
            _ => Err(Error::Parse(format!(
                "Expected identifier, got {}",
                self.current.ty
            ))),
        }
    }

    // Simplified implementations for other statement types

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(TokenType::Insert)?;
        self.expect(TokenType::Into)?;

        let table = self.parse_identifier()?;

        let columns = if self.consume(TokenType::LeftParen)? {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenType::RightParen)?;
            cols
        } else {
            vec![]
        };

        let values = if self.consume(TokenType::Values)? {
            let mut rows = Vec::new();
            loop {
                self.expect(TokenType::LeftParen)?;
                let row = self.parse_expr_list()?;
                self.expect(TokenType::RightParen)?;
                rows.push(row);

                if !self.consume(TokenType::Comma)? {
                    break;
                }
            }
            rows
        } else {
            vec![]
        };

        Ok(Statement::Insert(InsertStmt {
            table,
            schema: None,
            columns,
            values,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(TokenType::Update)?;
        let table = self.parse_identifier()?;

        self.expect(TokenType::Set)?;

        let mut assignments = Vec::new();
        loop {
            let col = self.parse_identifier()?;
            self.expect(TokenType::Equal)?;
            let expr = self.parse_expr()?;
            assignments.push((col, expr));

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStmt {
            table,
            schema: None,
            assignments,
            where_clause,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(TokenType::Delete)?;
        self.expect(TokenType::From)?;

        let table = self.parse_identifier()?;

        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStmt {
            table,
            schema: None,
            where_clause,
        }))
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(TokenType::Create)?;

        match self.current.ty {
            TokenType::Table => self.parse_create_table(),
            TokenType::Index => self.parse_create_index(),
            TokenType::Unique => {
                self.advance();
                self.expect(TokenType::Index)?;
                self.parse_create_index()
            }
            _ => Err(Error::Parse(format!(
                "Expected TABLE or INDEX, got {}",
                self.current.ty
            ))),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect(TokenType::Table)?;

        let if_not_exists = self.consume(TokenType::If)?
            && {
                self.expect(TokenType::Not)?;
                self.expect(TokenType::Exists)?;
                true
            };

        let name = self.parse_identifier()?;

        self.expect(TokenType::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_column_def()?);

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        self.expect(TokenType::RightParen)?;

        Ok(Statement::CreateTable(CreateTableStmt {
            if_not_exists,
            name,
            schema: None,
            columns,
        }))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.parse_identifier()?;
        let type_name = if matches!(self.current.ty, TokenType::Identifier | TokenType::QuotedIdentifier) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        let mut constraints = Vec::new();
        loop {
            match self.current.ty {
                TokenType::Primary => {
                    self.advance();
                    self.expect(TokenType::Key)?;
                    constraints.push(ColumnConstraint::PrimaryKey { auto_increment: false });
                }
                TokenType::Not => {
                    self.advance();
                    self.expect(TokenType::Null)?;
                    constraints.push(ColumnConstraint::NotNull);
                }
                TokenType::Unique => {
                    self.advance();
                    constraints.push(ColumnConstraint::Unique);
                }
                _ => break,
            }
        }

        Ok(ColumnDef {
            name,
            type_name,
            constraints,
        })
    }

    fn parse_create_index(&mut self) -> Result<Statement> {
        let if_not_exists = self.consume(TokenType::If)?
            && {
                self.expect(TokenType::Not)?;
                self.expect(TokenType::Exists)?;
                true
            };

        let name = self.parse_identifier()?;

        self.expect(TokenType::On)?;
        let table = self.parse_identifier()?;

        self.expect(TokenType::LeftParen)?;
        let columns = self.parse_indexed_columns()?;
        self.expect(TokenType::RightParen)?;

        Ok(Statement::CreateIndex(CreateIndexStmt {
            unique: false,
            if_not_exists,
            name,
            schema: None,
            table,
            columns,
            where_clause: None,
        }))
    }

    fn parse_indexed_columns(&mut self) -> Result<Vec<IndexedColumn>> {
        let mut columns = vec![self.parse_indexed_column()?];

        while self.consume(TokenType::Comma)? {
            columns.push(self.parse_indexed_column()?);
        }

        Ok(columns)
    }

    fn parse_indexed_column(&mut self) -> Result<IndexedColumn> {
        let name = self.parse_identifier()?;

        let ascending = if self.consume(TokenType::Desc)? {
            Some(false)
        } else if self.consume(TokenType::Asc)? {
            Some(true)
        } else {
            None
        };

        Ok(IndexedColumn {
            name,
            collation: None,
            ascending,
        })
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(TokenType::Drop)?;

        let object_type = match self.current.ty {
            TokenType::Table => {
                self.advance();
                ObjectType::Table
            }
            TokenType::Index => {
                self.advance();
                ObjectType::Index
            }
            _ => return Err(Error::Parse("Expected TABLE or INDEX".into())),
        };

        let if_exists = self.consume(TokenType::If)?
            && {
                self.expect(TokenType::Exists)?;
                true
            };

        let name = self.parse_identifier()?;

        Ok(Statement::Drop(DropStmt {
            object_type,
            if_exists,
            name,
            schema: None,
        }))
    }

    fn parse_alter(&mut self) -> Result<Statement> {
        self.expect(TokenType::Alter)?;
        self.expect(TokenType::Table)?;

        let table = self.parse_identifier()?;

        let action = if self.consume(TokenType::Rename)? {
            if self.consume(TokenType::To)? {
                let new_name = self.parse_identifier()?;
                AlterAction::RenameTo(new_name)
            } else {
                self.expect(TokenType::Column)?;
                let old = self.parse_identifier()?;
                self.expect(TokenType::To)?;
                let new = self.parse_identifier()?;
                AlterAction::RenameColumn { old, new }
            }
        } else if self.consume(TokenType::Add)? {
            self.expect(TokenType::Column)?;
            let col = self.parse_column_def()?;
            AlterAction::AddColumn(col)
        } else {
            return Err(Error::Parse("Expected RENAME or ADD".into()));
        };

        Ok(Statement::AlterTable(AlterTableStmt {
            table,
            schema: None,
            action,
        }))
    }

    fn parse_begin(&mut self) -> Result<Statement> {
        self.expect(TokenType::Begin)?;
        let _ = self.consume(TokenType::Transaction);

        let transaction_type = if self.consume(TokenType::Deferred)? {
            Some(TransactionType::Deferred)
        } else if self.consume(TokenType::Immediate)? {
            Some(TransactionType::Immediate)
        } else if self.consume(TokenType::Exclusive)? {
            Some(TransactionType::Exclusive)
        } else {
            None
        };

        Ok(Statement::Begin(BeginStmt { transaction_type }))
    }

    fn parse_rollback(&mut self) -> Result<Statement> {
        self.expect(TokenType::Rollback)?;
        let _ = self.consume(TokenType::Transaction);

        let savepoint = if self.consume(TokenType::To)? {
            let _ = self.consume(TokenType::Savepoint);
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Statement::Rollback(RollbackStmt { savepoint }))
    }

    fn parse_pragma(&mut self) -> Result<Statement> {
        self.expect(TokenType::Pragma)?;

        let name = self.parse_identifier()?;

        let value = if self.consume(TokenType::Equal)? {
            Some(PragmaValue::Equals(self.parse_expr()?))
        } else if self.consume(TokenType::LeftParen)? {
            let expr = self.parse_expr()?;
            self.expect(TokenType::RightParen)?;
            Some(PragmaValue::Expr(expr))
        } else {
            None
        };

        Ok(Statement::Pragma(PragmaStmt {
            schema: None,
            name,
            value,
        }))
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        self.expect(TokenType::Explain)?;

        let query_plan = self.consume(TokenType::Query)?
            && {
                self.expect(TokenType::Plan)?;
                true
            };

        let statement = self.parse()?;

        Ok(Statement::Explain(ExplainStmt {
            query_plan,
            statement: Box::new(statement),
        }))
    }

    fn parse_vacuum(&mut self) -> Result<Statement> {
        self.expect(TokenType::Vacuum)?;

        let schema = if matches!(self.current.ty, TokenType::Identifier | TokenType::QuotedIdentifier) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Statement::Vacuum(VacuumStmt {
            schema,
            into: None,
        }))
    }

    fn parse_analyze(&mut self) -> Result<Statement> {
        self.expect(TokenType::Analyze)?;

        Ok(Statement::Analyze(AnalyzeStmt {
            schema: None,
            table: None,
            index: None,
        }))
    }

    fn parse_attach(&mut self) -> Result<Statement> {
        self.expect(TokenType::Attached)?;
        self.expect(TokenType::Database)?;

        let database = self.parse_expr()?;
        self.expect(TokenType::As)?;
        let name = self.parse_identifier()?;

        Ok(Statement::Attach(AttachStmt {
            database,
            name,
            key: None,
        }))
    }

    fn parse_detach(&mut self) -> Result<Statement> {
        self.expect(TokenType::Detach)?;
        let _ = self.consume(TokenType::Database);

        let name = self.parse_identifier()?;

        Ok(Statement::Detach(DetachStmt { name }))
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut ids = vec![self.parse_identifier()?];

        while self.consume(TokenType::Comma)? {
            ids.push(self.parse_identifier()?);
        }

        Ok(ids)
    }

    /// Consume a token if it matches the expected type
    fn consume(&mut self, ty: TokenType) -> Result<bool> {
        if self.current.ty == ty {
            self.advance();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Expect a specific token type
    fn expect(&mut self, ty: TokenType) -> Result<()> {
        if self.current.ty == ty {
            self.advance();
            Ok(())
        } else {
            Err(Error::Parse(format!(
                "Expected {}, got {}",
                ty, self.current.ty
            )))
        }
    }

    /// Advance to the next token
    fn advance(&mut self) {
        self.current = std::mem::replace(&mut self.peek, self.lexer.next_token().unwrap_or_else(|_| {
            Token::eof(self.current.end, self.current.line, self.current.column)
        }));
    }
}

/// Parse SQL text into a statement
pub fn parse_sql(sql: &str) -> Result<Statement> {
    let mut parser = Parser::new(sql)?;
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_star() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        assert!(matches!(stmt, Statement::CreateTable(_)));
    }

    #[test]
    fn test_parse_begin() {
        let stmt = parse_sql("BEGIN TRANSACTION").unwrap();
        assert!(matches!(stmt, Statement::Begin(_)));
    }

    #[test]
    fn test_parse_commit() {
        let stmt = parse_sql("COMMIT").unwrap();
        assert!(matches!(stmt, Statement::Commit));
    }
}
