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

        let left = self.parse_select_core(false)?;
        let mut parts = Vec::new();

        while let Some(operator) = self.parse_compound_operator()? {

            self.expect(TokenType::Select)?;
            let select = self.parse_select_core(false)?;
            parts.push(CompoundSelectPart { operator, select });
        }

        if parts.is_empty() {
            return Ok(Statement::Select(self.parse_select_tail(left)?));
        }

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

        Ok(Statement::CompoundSelect(CompoundSelectStmt {
            left,
            parts,
            order_by,
            limit,
            offset,
        }))
    }

    fn parse_compound_operator(&mut self) -> Result<Option<CompoundOperator>> {
        if self.consume(TokenType::Union)? {
            let operator = if self.consume(TokenType::All)? {
                CompoundOperator::UnionAll
            } else {
                CompoundOperator::Union
            };
            return Ok(Some(operator));
        }

        if self.consume(TokenType::Intersect)? {
            if self.consume(TokenType::All)? {
                return Err(Error::Parse("INTERSECT ALL is not currently supported".into()));
            }
            return Ok(Some(CompoundOperator::Intersect));
        }

        if self.consume(TokenType::Except)? {
            if self.consume(TokenType::All)? {
                return Err(Error::Parse("EXCEPT ALL is not currently supported".into()));
            }
            return Ok(Some(CompoundOperator::Except));
        }

        Ok(None)
    }

    fn parse_select_core(&mut self, parse_trailing_clauses: bool) -> Result<SelectStmt> {

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

        let select = SelectStmt {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        if parse_trailing_clauses {
            self.parse_select_tail(select)
        } else {
            Ok(select)
        }
    }

    fn parse_select_tail(&mut self, mut select: SelectStmt) -> Result<SelectStmt> {
        select.order_by = if self.consume(TokenType::Order)? {
            self.expect(TokenType::By)?;
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        select.limit = if self.consume(TokenType::Limit)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        select.offset = if self.consume(TokenType::Offset)? {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(select)
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

        if matches!(self.current.ty, TokenType::Identifier | TokenType::QuotedIdentifier)
            && self.peek.ty == TokenType::Dot
        {
            let qualifier = self.parse_identifier()?;
            self.expect(TokenType::Dot)?;

            if self.consume(TokenType::Star)? {
                return Ok(ResultColumn::TableStar(qualifier));
            }

            let column = self.parse_identifier()?;
            let expr = Expr::QualifiedIdentifier(qualifier, column);
            let alias = if self.consume(TokenType::As)? {
                Some(self.parse_identifier()?)
            } else if matches!(self.current.ty, TokenType::Identifier | TokenType::QuotedIdentifier) {
                Some(self.parse_identifier()?)
            } else {
                None
            };

            return Ok(ResultColumn::Expr(expr, alias));
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
        let mut joins = Vec::new();

        loop {
            if self.consume(TokenType::Comma)? {
                tables.push(self.parse_table_ref()?);
                joins.push(JoinClause {
                    kind: JoinKind::Cross,
                    constraint: None,
                });
                continue;
            }

            let join_kind = if self.consume(TokenType::Join)? {
                Some(JoinKind::Inner)
            } else if self.consume(TokenType::Inner)? {
                self.expect(TokenType::Join)?;
                Some(JoinKind::Inner)
            } else if self.consume(TokenType::Cross)? {
                self.expect(TokenType::Join)?;
                Some(JoinKind::Cross)
            } else if self.consume(TokenType::Left)? {
                let _ = self.consume(TokenType::Outer)?;
                self.expect(TokenType::Join)?;
                Some(JoinKind::Left)
            } else if matches!(self.current.ty, TokenType::Right | TokenType::Full | TokenType::Outer | TokenType::Natural) {
                return Err(Error::Parse(
                    "Only JOIN, INNER JOIN, LEFT JOIN, and CROSS JOIN are currently supported".into(),
                ));
            } else {
                None
            };

            let Some(join_kind) = join_kind else {
                break;
            };

            tables.push(self.parse_table_ref()?);
            let constraint = match join_kind {
                JoinKind::Cross => {
                    if matches!(self.current.ty, TokenType::On | TokenType::Using) {
                        return Err(Error::Parse(
                            "CROSS JOIN does not accept ON or USING in the current implementation".into(),
                        ));
                    }
                    None
                }
                JoinKind::Inner | JoinKind::Left => {
                    if self.consume(TokenType::On)? {
                        Some(JoinConstraint::On(self.parse_expr()?))
                    } else if self.consume(TokenType::Using)? {
                        self.expect(TokenType::LeftParen)?;
                        let columns = self.parse_identifier_list()?;
                        self.expect(TokenType::RightParen)?;
                        Some(JoinConstraint::Using(columns))
                    } else {
                        return Err(Error::Parse("Expected ON or USING after JOIN target".into()));
                    }
                }
            };

            joins.push(JoinClause { kind: join_kind, constraint });
        }

        Ok(FromClause { tables, joins })
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
        let mut expr = self.parse_additive_expr()?;

        loop {
            if self.consume(TokenType::IsNull)? {
                expr = Expr::IsNull {
                    expr: Box::new(expr),
                    not: false,
                };
                continue;
            }

            if self.consume(TokenType::NotNull)? {
                expr = Expr::IsNull {
                    expr: Box::new(expr),
                    not: true,
                };
                continue;
            }

            if self.consume(TokenType::Is)? {
                let not = self.consume(TokenType::Not)?;

                if self.consume(TokenType::Null)? || self.consume(TokenType::IsNull)? {
                    expr = Expr::IsNull {
                        expr: Box::new(expr),
                        not,
                    };
                    continue;
                }

                if self.consume(TokenType::NotNull)? {
                    expr = Expr::IsNull {
                        expr: Box::new(expr),
                        not: !not,
                    };
                    continue;
                }

                let right = self.parse_additive_expr()?;
                expr = Expr::Binary(
                    if not {
                        BinaryOp::NotEqual
                    } else {
                        BinaryOp::Equal
                    },
                    Box::new(expr),
                    Box::new(right),
                );
                continue;
            }

            let not = if self.current.ty == TokenType::Not
                && matches!(self.peek.ty, TokenType::In | TokenType::Between | TokenType::Like | TokenType::Glob)
            {
                self.advance();
                true
            } else {
                false
            };

            if self.consume(TokenType::In)? {
                self.expect(TokenType::LeftParen)?;
                let source = if self.current.ty == TokenType::Select {
                    InSource::Subquery(Box::new(self.parse_select()?))
                } else if self.current.ty == TokenType::RightParen {
                    InSource::List(Vec::new())
                } else {
                    InSource::List(self.parse_expr_list()?)
                };
                self.expect(TokenType::RightParen)?;
                expr = Expr::In {
                    expr: Box::new(expr),
                    not,
                    source,
                };
                continue;
            }

            if self.consume(TokenType::Between)? {
                let low = self.parse_additive_expr()?;
                self.expect(TokenType::And)?;
                let high = self.parse_additive_expr()?;
                expr = Expr::Between {
                    expr: Box::new(expr),
                    not,
                    low: Box::new(low),
                    high: Box::new(high),
                };
                continue;
            }

            if self.consume(TokenType::Like)? || self.consume(TokenType::Glob)? {
                let pattern = self.parse_additive_expr()?;
                expr = Expr::Like {
                    expr: Box::new(expr),
                    not,
                    pattern: Box::new(pattern),
                };
                continue;
            }

            let op = match self.current.ty {
                TokenType::Equal => BinaryOp::Equal,
                TokenType::NotEqual => BinaryOp::NotEqual,
                TokenType::Less => BinaryOp::Less,
                TokenType::LessEqual => BinaryOp::LessEqual,
                TokenType::Greater => BinaryOp::Greater,
                TokenType::GreaterEqual => BinaryOp::GreaterEqual,
                _ => break,
            };

            self.advance();
            let right = self.parse_additive_expr()?;
            expr = Expr::Binary(op, Box::new(expr), Box::new(right));
        }

        Ok(expr)
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
            TokenType::Exists => {
                self.advance();
                self.expect(TokenType::LeftParen)?;
                let statement = self.parse_select()?;
                self.expect(TokenType::RightParen)?;
                Ok(Expr::Exists(Box::new(statement)))
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

                if self.current.ty == TokenType::Select {
                    let statement = self.parse_select()?;
                    self.expect(TokenType::RightParen)?;
                    return Ok(Expr::Subquery(Box::new(statement)));
                }

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

        let source = if self.consume(TokenType::Values)? {
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
            InsertSource::Values(rows)
        } else if self.current.ty == TokenType::Select {
            InsertSource::Select(Box::new(self.parse_select()?))
        } else {
            return Err(Error::Parse("Expected VALUES or SELECT after INSERT INTO target".into()));
        };

        let on_conflict = if self.consume(TokenType::On)? {
            self.expect(TokenType::Conflict)?;
            Some(self.parse_on_conflict_clause()?)
        } else {
            None
        };

        Ok(Statement::Insert(InsertStmt {
            table,
            schema: None,
            columns,
            source,
            on_conflict,
        }))
    }

    fn parse_on_conflict_clause(&mut self) -> Result<OnConflictClause> {
        let target_columns = if self.consume(TokenType::LeftParen)? {
            let columns = self.parse_identifier_list()?;
            self.expect(TokenType::RightParen)?;
            columns
        } else {
            Vec::new()
        };

        self.expect(TokenType::Do)?;
        let action = if self.consume(TokenType::Nothing)? {
            OnConflictAction::DoNothing
        } else {
            self.expect(TokenType::Update)?;
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

            OnConflictAction::DoUpdate {
                assignments,
                where_clause,
            }
        };

        Ok(OnConflictClause {
            target_columns,
            action,
        })
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
                    let auto_increment = self.consume(TokenType::AutoIncrement)?;
                    constraints.push(ColumnConstraint::PrimaryKey { auto_increment });
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
                TokenType::Check => {
                    self.advance();
                    self.expect(TokenType::LeftParen)?;
                    constraints.push(ColumnConstraint::Check(self.parse_expr()?));
                    self.expect(TokenType::RightParen)?;
                }
                TokenType::Default => {
                    self.advance();
                    constraints.push(ColumnConstraint::Default(self.parse_expr()?));
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
    let statement = parser.parse()?;

    while parser.consume(TokenType::Semicolon)? {}

    if parser.current.ty != TokenType::Eof {
        return Err(Error::Parse(format!(
            "Unexpected trailing token: {}",
            parser.current.value
        )));
    }

    Ok(statement)
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
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };

        assert!(matches!(insert.source, InsertSource::Values(_)));
        assert!(insert.on_conflict.is_none());
    }

    #[test]
    fn test_parse_insert_from_select() {
        let stmt = parse_sql("INSERT INTO archived_users (id, name) SELECT id, name FROM users WHERE active = 1").unwrap();
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };

        assert!(matches!(insert.source, InsertSource::Select(_)));
        assert!(insert.on_conflict.is_none());
    }

    #[test]
    fn test_parse_insert_with_on_conflict_do_nothing() {
        let stmt = parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT(id) DO NOTHING").unwrap();
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };

        let on_conflict = insert.on_conflict.expect("expected on conflict clause");
        assert_eq!(on_conflict.target_columns, vec!["id"]);
        assert!(matches!(on_conflict.action, OnConflictAction::DoNothing));
    }

    #[test]
    fn test_parse_insert_with_on_conflict_do_update() {
        let stmt = parse_sql(
            "INSERT INTO users (id, name, visits) VALUES (1, 'Alice', 2) ON CONFLICT(id) DO UPDATE SET name = excluded.name, visits = visits + excluded.visits WHERE excluded.visits > 0"
        ).unwrap();
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };

        let on_conflict = insert.on_conflict.expect("expected on conflict clause");
        assert_eq!(on_conflict.target_columns, vec!["id"]);
        assert!(matches!(
            on_conflict.action,
            OnConflictAction::DoUpdate {
                assignments,
                where_clause: Some(_),
            } if assignments.len() == 2
        ));
    }

    #[test]
    fn test_parse_create_table_with_check() {
        let stmt = parse_sql("CREATE TABLE users (age INTEGER CHECK (age >= 18), score REAL CHECK (score <= 100))").unwrap();
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };

        assert!(matches!(create.columns[0].constraints[0], ColumnConstraint::Check(_)));
        assert!(matches!(create.columns[1].constraints[0], ColumnConstraint::Check(_)));
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

    #[test]
    fn test_parse_select_with_filters_and_paging() {
        let stmt = parse_sql(
            "SELECT DISTINCT id, name FROM users WHERE score BETWEEN 1.5 AND 9.5 AND name LIKE 'A%' AND id IN (1, 2, 3) AND note IS NOT NULL ORDER BY id DESC LIMIT 5 OFFSET 10"
        )
        .unwrap();

        let Statement::Select(select) = stmt else {
            panic!("expected select statement");
        };

        assert!(select.distinct);
        assert_eq!(select.order_by.len(), 1);
        assert!(select.limit.is_some());
        assert!(select.offset.is_some());
        assert!(select.where_clause.is_some());
    }

    #[test]
    fn test_parse_select_with_table_star_and_qualified_column() {
        let stmt = parse_sql("SELECT users.*, orders.amount FROM users, orders").unwrap();

        let Statement::Select(select) = stmt else {
            panic!("expected select statement");
        };

        assert_eq!(select.columns.len(), 2);
        assert!(matches!(select.columns[0], ResultColumn::TableStar(ref name) if name == "users"));
        assert!(matches!(
            select.columns[1],
            ResultColumn::Expr(Expr::QualifiedIdentifier(ref table, ref column), None)
                if table == "orders" && column == "amount"
        ));
    }

    #[test]
    fn test_parse_select_with_join_on_and_cross_join() {
        let join_stmt = parse_sql(
            "SELECT u.name, o.amount FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE o.amount > 10"
        )
        .unwrap();

        let Statement::Select(join_select) = join_stmt else {
            panic!("expected select statement");
        };

        assert_eq!(join_select.from.as_ref().unwrap().tables.len(), 2);
    assert_eq!(join_select.from.as_ref().unwrap().joins.len(), 1);
        assert_eq!(join_select.from.as_ref().unwrap().tables[0].alias.as_deref(), Some("u"));
        assert_eq!(join_select.from.as_ref().unwrap().tables[1].alias.as_deref(), Some("o"));
        assert!(join_select.where_clause.is_some());

        let cross_stmt = parse_sql("SELECT COUNT(*) FROM users CROSS JOIN orders").unwrap();
        let Statement::Select(cross_select) = cross_stmt else {
            panic!("expected select statement");
        };

        assert_eq!(cross_select.from.as_ref().unwrap().tables.len(), 2);
        assert!(matches!(
            cross_select.from.as_ref().unwrap().joins[0].kind,
            JoinKind::Cross
        ));
        assert!(cross_select.where_clause.is_none());
    }

    #[test]
    fn test_parse_select_with_left_join_and_using() {
        let stmt = parse_sql(
            "SELECT u.id, p.nickname FROM users AS u LEFT OUTER JOIN profiles AS p USING (id)"
        )
        .unwrap();

        let Statement::Select(select) = stmt else {
            panic!("expected select statement");
        };

        let from = select.from.as_ref().unwrap();
        assert_eq!(from.tables.len(), 2);
        assert_eq!(from.joins.len(), 1);
        assert!(matches!(from.joins[0].kind, JoinKind::Left));
        assert!(matches!(
            from.joins[0].constraint,
            Some(JoinConstraint::Using(ref columns)) if columns == &vec!["id".to_string()]
        ));
    }

    #[test]
    fn test_parse_union_all_select() {
        let stmt = parse_sql("SELECT 1 AS value UNION ALL SELECT 2 AS value UNION SELECT 2 AS value").unwrap();

        let Statement::CompoundSelect(compound) = stmt else {
            panic!("expected compound select statement");
        };

        assert_eq!(compound.parts.len(), 2);
        assert!(matches!(compound.parts[0].operator, CompoundOperator::UnionAll));
        assert!(matches!(compound.parts[1].operator, CompoundOperator::Union));
    }

    #[test]
    fn test_parse_intersect_and_except_select() {
        let stmt = parse_sql("SELECT 1 AS value INTERSECT SELECT 1 AS value EXCEPT SELECT 2 AS value").unwrap();

        let Statement::CompoundSelect(compound) = stmt else {
            panic!("expected compound select statement");
        };

        assert_eq!(compound.parts.len(), 2);
        assert!(matches!(compound.parts[0].operator, CompoundOperator::Intersect));
        assert!(matches!(compound.parts[1].operator, CompoundOperator::Except));
    }

    #[test]
    fn test_parse_compound_select_with_outer_order_limit_offset() {
        let stmt = parse_sql(
            "SELECT 1 AS value UNION ALL SELECT 2 AS value ORDER BY value DESC LIMIT 1 OFFSET 0"
        )
        .unwrap();

        let Statement::CompoundSelect(compound) = stmt else {
            panic!("expected compound select statement");
        };

        assert_eq!(compound.parts.len(), 1);
        assert_eq!(compound.order_by.len(), 1);
        assert!(compound.limit.is_some());
        assert!(compound.offset.is_some());
        assert!(compound.left.order_by.is_empty());
        assert!(compound.parts[0].select.order_by.is_empty());
    }

    #[test]
    fn test_parse_in_subquery() {
        let stmt = parse_sql(
            "SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE team = 'red')"
        )
        .unwrap();

        let Statement::Select(select) = stmt else {
            panic!("expected select statement");
        };

        assert!(matches!(
            select.where_clause,
            Some(Expr::In {
                source: InSource::Subquery(_),
                ..
            })
        ));
    }

    #[test]
    fn test_parse_exists_subquery() {
        let stmt = parse_sql(
            "SELECT id FROM users WHERE EXISTS (SELECT id FROM users WHERE team = 'red')"
        )
        .unwrap();

        let Statement::Select(select) = stmt else {
            panic!("expected select statement");
        };

        assert!(matches!(
            select.where_clause,
            Some(Expr::Exists(_))
        ));
    }

    #[test]
    fn test_parse_scalar_subquery() {
        let stmt = parse_sql(
            "SELECT id FROM users WHERE id = (SELECT id FROM users WHERE team = 'red')"
        )
        .unwrap();

        let Statement::Select(select) = stmt else {
            panic!("expected select statement");
        };

        assert!(matches!(
            select.where_clause,
            Some(Expr::Binary(BinaryOp::Equal, _, right)) if matches!(*right, Expr::Subquery(_))
        ));
    }

    #[test]
    fn test_parse_alter_table_rename_to() {
        let stmt = parse_sql("ALTER TABLE users RENAME TO members").unwrap();

        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        assert_eq!(alter.table, "users");
        assert!(matches!(alter.action, AlterAction::RenameTo(ref name) if name == "members"));
    }

    #[test]
    fn test_parse_alter_table_rename_column() {
        let stmt = parse_sql("ALTER TABLE users RENAME COLUMN name TO display_name").unwrap();

        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        assert!(matches!(
            alter.action,
            AlterAction::RenameColumn { ref old, ref new }
                if old == "name" && new == "display_name"
        ));
    }

    #[test]
    fn test_parse_alter_table_add_column() {
        let stmt = parse_sql("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'").unwrap();

        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        assert!(matches!(alter.action, AlterAction::AddColumn(ref col) if col.name == "status"));
    }

    #[test]
    fn test_parse_sql_allows_trailing_semicolon() {
        let stmt = parse_sql("SELECT 1 AS value;").unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_sql_rejects_trailing_tokens() {
        let err = parse_sql("SELECT 1 value extra").unwrap_err();
        assert!(err.to_string().contains("Unexpected trailing token: extra"));
    }
}
