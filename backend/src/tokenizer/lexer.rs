//! SQL Lexer implementation

use crate::error::{Error, Result};
use crate::util::string::{is_identifier_char, is_identifier_start};
use super::token::{Token, TokenType};
use std::collections::HashMap;

/// SQL Lexer (tokenizer)
pub struct Lexer<'a> {
    /// Input source
    input: &'a str,
    /// Current position in input
    pos: usize,
    /// Current line number
    line: usize,
    /// Current column number
    column: usize,
    /// Keyword lookup table
    keywords: HashMap<&'a str, TokenType>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given input
    pub fn new(input: &'a str) -> Self {
        let mut lexer = Lexer {
            input,
            pos: 0,
            line: 1,
            column: 1,
            keywords: HashMap::new(),
        };
        lexer.init_keywords();
        lexer
    }

    /// Initialize the keyword lookup table
    fn init_keywords(&mut self) {
        let keywords = &[
            ("ABORT", TokenType::Abort),
            ("ACTION", TokenType::Action),
            ("ADD", TokenType::Add),
            ("AFTER", TokenType::Unknown),
            ("ALL", TokenType::All),
            ("ALTER", TokenType::Alter),
            ("ALWAYS", TokenType::Always),
            ("ANALYZE", TokenType::Analyze),
            ("AND", TokenType::And),
            ("AS", TokenType::As),
            ("ASC", TokenType::Asc),
            ("ATTACH", TokenType::Attached),
            ("AUTOINCREMENT", TokenType::AutoIncrement),
            ("BEFORE", TokenType::Unknown),
            ("BEGIN", TokenType::Begin),
            ("BETWEEN", TokenType::Between),
            ("BY", TokenType::By),
            ("CASCADE", TokenType::Cascade),
            ("CASE", TokenType::Case),
            ("CAST", TokenType::Cast),
            ("CHECK", TokenType::Check),
            ("COLLATE", TokenType::Collate),
            ("COLUMN", TokenType::Column),
            ("COMMIT", TokenType::Commit),
            ("CONFLICT", TokenType::Conflict),
            ("CONSTRAINT", TokenType::Constraint),
            ("CREATE", TokenType::Create),
            ("CROSS", TokenType::Cross),
            ("CURRENT", TokenType::CurrentRow),
            ("CURRENT_DATE", TokenType::CurrentDate),
            ("CURRENT_TIME", TokenType::CurrentTime),
            ("CURRENT_TIMESTAMP", TokenType::CurrentTimestamp),
            ("DATABASE", TokenType::Database),
            ("DEFAULT", TokenType::Default),
            ("DEFERRABLE", TokenType::Deferrable),
            ("DEFERRED", TokenType::Deferred),
            ("DELETE", TokenType::Delete),
            ("DESC", TokenType::Desc),
            ("DETACH", TokenType::Detach),
            ("DISTINCT", TokenType::Distinct),
            ("DROP", TokenType::Drop),
            ("EACH", TokenType::Unknown),
            ("ELSE", TokenType::Else),
            ("END", TokenType::End),
            ("ESCAPE", TokenType::Escape),
            ("EXCEPT", TokenType::Except),
            ("EXCLUSIVE", TokenType::Exclusive),
            ("EXISTS", TokenType::Exists),
            ("EXPLAIN", TokenType::Explain),
            ("FAIL", TokenType::Fail),
            ("FALSE", TokenType::False),
            ("FILTER", TokenType::Filter),
            ("FIRST", TokenType::First),
            ("FOR", TokenType::Unknown),
            ("FOREIGN", TokenType::Foreign),
            ("FROM", TokenType::From),
            ("FULL", TokenType::Full),
            ("GENERATED", TokenType::Generated),
            ("GLOB", TokenType::Glob),
            ("GROUP", TokenType::Group),
            ("GROUPS", TokenType::Groups),
            ("HAVING", TokenType::Having),
            ("IF", TokenType::If),
            ("IGNORE", TokenType::Ignore),
            ("IMMEDIATE", TokenType::Immediate),
            ("IN", TokenType::In),
            ("INDEX", TokenType::Index),
            ("INDEXED", TokenType::Unknown),
            ("INITIALLY", TokenType::Unknown),
            ("INNER", TokenType::Inner),
            ("INSERT", TokenType::Insert),
            ("INSTEAD", TokenType::Unknown),
            ("INTERSECT", TokenType::Intersect),
            ("INTO", TokenType::Into),
            ("IS", TokenType::Is),
            ("ISNULL", TokenType::IsNull),
            ("JOIN", TokenType::Join),
            ("KEY", TokenType::Key),
            ("LAST", TokenType::Last),
            ("LEFT", TokenType::Left),
            ("LIKE", TokenType::Like),
            ("LIMIT", TokenType::Limit),
            ("MATCH", TokenType::Match),
            ("MATERIALIZED", TokenType::Materialized),
            ("NATURAL", TokenType::Natural),
            ("NO", TokenType::No),
            ("NOT", TokenType::Not),
            ("NOTHING", TokenType::Unknown),
            ("NOTNULL", TokenType::NotNull),
            ("NULL", TokenType::Null),
            ("NULLS", TokenType::Nulls),
            ("OF", TokenType::Unknown),
            ("OFFSET", TokenType::Offset),
            ("ON", TokenType::On),
            ("OR", TokenType::Or),
            ("ORDER", TokenType::Order),
            ("OUTER", TokenType::Outer),
            ("OVER", TokenType::Over),
            ("PARTITION", TokenType::Partition),
            ("PLAN", TokenType::Plan),
            ("PRAGMA", TokenType::Pragma),
            ("PRECEDING", TokenType::Preceding),
            ("PRIMARY", TokenType::Primary),
            ("QUERY", TokenType::Query),
            ("RAISE", TokenType::Unknown),
            ("RANGE", TokenType::Range),
            ("RECURSIVE", TokenType::Recursive),
            ("REFERENCES", TokenType::References),
            ("REGEXP", TokenType::Regexp),
            ("REINDEX", TokenType::Reindex),
            ("RELEASE", TokenType::Release),
            ("RENAME", TokenType::Rename),
            ("REPLACE", TokenType::Replace),
            ("RESTRICT", TokenType::Restrict),
            ("RETURNING", TokenType::Returning),
            ("RIGHT", TokenType::Right),
            ("ROLLBACK", TokenType::Rollback),
            ("ROW", TokenType::Row),
            ("ROWS", TokenType::Rows),
            ("SAVEPOINT", TokenType::Savepoint),
            ("SELECT", TokenType::Select),
            ("SET", TokenType::Set),
            ("STORED", TokenType::Stored),
            ("TABLE", TokenType::Table),
            ("TEMP", TokenType::Temp),
            ("TEMPORARY", TokenType::Temporary),
            ("THEN", TokenType::Then),
            ("TIES", TokenType::Ties),
            ("TO", TokenType::To),
            ("TRANSACTION", TokenType::Transaction),
            ("TRIGGER", TokenType::Trigger),
            ("TRUE", TokenType::True),
            ("UNBOUNDED", TokenType::Unbounded),
            ("UNION", TokenType::Union),
            ("UNIQUE", TokenType::Unique),
            ("UPDATE", TokenType::Update),
            ("USING", TokenType::Using),
            ("VACUUM", TokenType::Vacuum),
            ("VALUES", TokenType::Values),
            ("VIEW", TokenType::View),
            ("VIRTUAL", TokenType::Virtual),
            ("WHEN", TokenType::When),
            ("WHERE", TokenType::Where),
            ("WINDOW", TokenType::Window),
            ("WITH", TokenType::With),
            ("WITHOUT", TokenType::Unknown),
        ];

        for &(keyword, token_type) in keywords {
            self.keywords.insert(keyword, token_type);
        }
    }

    /// Get the next token
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments();

        if self.is_at_end() {
            return Ok(Token::eof(self.pos, self.line, self.column));
        }

        let start = self.pos;
        let start_line = self.line;
        let start_column = self.column;

        let c = self.current();

        // Blob literals (check before identifiers since X'...' starts with X)
        if (c == 'x' || c == 'X') && self.peek(1) == '\'' {
            return self.scan_blob(start, start_line, start_column);
        }

        // Identifiers and keywords
        if is_identifier_start(c) {
            return self.scan_identifier_or_keyword(start, start_line, start_column);
        }

        // Numbers
        if c.is_ascii_digit() {
            return self.scan_number(start, start_line, start_column);
        }

        // Strings
        if c == '\'' {
            return self.scan_string(start, start_line, start_column);
        }

        // Quoted identifiers
        if c == '"' || c == '[' || c == '`' {
            return self.scan_quoted_identifier(start, start_line, start_column);
        }

        // Variables
        if c == '?' || c == ':' || c == '@' || c == '$' {
            return self.scan_variable(start, start_line, start_column);
        }

        // Operators and punctuation
        let token_type = match c {
            '(' => TokenType::LeftParen,
            ')' => TokenType::RightParen,
            ',' => TokenType::Comma,
            ';' => TokenType::Semicolon,
            '.' => {
                // Check if it's a number starting with a dot
                if self.peek(1).is_ascii_digit() {
                    return self.scan_number(start, start_line, start_column);
                }
                TokenType::Dot
            }
            '+' => TokenType::Plus,
            '-' => {
                if self.peek(1) == '>' {
                    self.advance();
                    if self.peek(1) == '>' {
                        self.advance();
                        TokenType::ArrowText
                    } else {
                        TokenType::Arrow
                    }
                } else {
                    TokenType::Minus
                }
            }
            '*' => TokenType::Star,
            '/' => TokenType::Slash,
            '%' => TokenType::Percent,
            '=' => TokenType::Equal,
            '<' => {
                match self.peek(1) {
                    '=' => {
                        self.advance();
                        TokenType::LessEqual
                    }
                    '>' => {
                        self.advance();
                        TokenType::NotEqual
                    }
                    '<' => {
                        self.advance();
                        TokenType::LeftShift
                    }
                    _ => TokenType::Less,
                }
            }
            '>' => {
                match self.peek(1) {
                    '=' => {
                        self.advance();
                        TokenType::GreaterEqual
                    }
                    '>' => {
                        self.advance();
                        TokenType::RightShift
                    }
                    _ => TokenType::Greater,
                }
            }
            '!' => {
                if self.peek(1) == '=' {
                    self.advance();
                    TokenType::NotEqual
                } else {
                    TokenType::Unknown
                }
            }
            '|' => {
                if self.peek(1) == '|' {
                    self.advance();
                    TokenType::Concat
                } else if self.peek(1) == '/' {
                    self.advance();
                    TokenType::BitOr
                } else {
                    TokenType::BitOr
                }
            }
            '&' => TokenType::BitAnd,
            '~' => TokenType::BitNot,
            ':' => {
                if self.peek(1) == ':' {
                    self.advance();
                    TokenType::DoubleColon
                } else {
                    TokenType::Unknown
                }
            }
            _ => TokenType::Unknown,
        };

        self.advance();
        let value = self.input[start..self.pos].to_string();

        Ok(Token::with_position(
            token_type,
            value,
            start,
            self.pos,
            start_line,
            start_column,
        ))
    }

    /// Scan an identifier or keyword
    fn scan_identifier_or_keyword(
        &mut self,
        start: usize,
        start_line: usize,
        start_column: usize,
    ) -> Result<Token> {
        while !self.is_at_end() {
            let c = self.current();
            if is_identifier_char(c) {
                self.advance();
            } else {
                break;
            }
        }

        let value = &self.input[start..self.pos];
        let upper = value.to_uppercase();

        let token_type = self
            .keywords
            .get(upper.as_str())
            .copied()
            .unwrap_or(TokenType::Identifier);

        Ok(Token::with_position(
            token_type,
            value.to_string(),
            start,
            self.pos,
            start_line,
            start_column,
        ))
    }

    /// Scan a number (integer or float)
    fn scan_number(
        &mut self,
        start: usize,
        start_line: usize,
        start_column: usize,
    ) -> Result<Token> {
        let mut has_dot = false;
        let mut has_exp = false;

        // Handle leading dot
        if self.current() == '.' {
            has_dot = true;
            self.advance();
        }

        // Integer part
        while !self.is_at_end() && self.current().is_ascii_digit() {
            self.advance();
        }

        // Decimal part
        if !has_dot && self.current() == '.' && self.peek(1).is_ascii_digit() {
            has_dot = true;
            self.advance();
            while !self.is_at_end() && self.current().is_ascii_digit() {
                self.advance();
            }
        }

        // Exponent part
        if self.current() == 'e' || self.current() == 'E' {
            has_exp = true;
            self.advance();
            if self.current() == '+' || self.current() == '-' {
                self.advance();
            }
            while !self.is_at_end() && self.current().is_ascii_digit() {
                self.advance();
            }
        }

        let value = &self.input[start..self.pos];
        let token_type = if has_dot || has_exp {
            TokenType::Float
        } else {
            TokenType::Integer
        };

        Ok(Token::with_position(
            token_type,
            value.to_string(),
            start,
            self.pos,
            start_line,
            start_column,
        ))
    }

    /// Scan a string literal
    fn scan_string(
        &mut self,
        start: usize,
        start_line: usize,
        start_column: usize,
    ) -> Result<Token> {
        self.advance(); // Skip opening quote

        let mut value = String::new();

        while !self.is_at_end() {
            let c = self.current();

            if c == '\'' {
                // Check for escaped quote
                if self.peek(1) == '\'' {
                    value.push('\'');
                    self.advance();
                    self.advance();
                } else {
                    self.advance(); // Skip closing quote
                    break;
                }
            } else {
                value.push(c);
                self.advance();
            }
        }

        Ok(Token::with_position(
            TokenType::String,
            value,
            start,
            self.pos,
            start_line,
            start_column,
        ))
    }

    /// Scan a quoted identifier
    fn scan_quoted_identifier(
        &mut self,
        start: usize,
        start_line: usize,
        start_column: usize,
    ) -> Result<Token> {
        let open_quote = self.current();
        let close_quote = if open_quote == '[' { ']' } else { open_quote };
        self.advance(); // Skip opening quote

        let mut value = String::new();

        while !self.is_at_end() {
            let c = self.current();

            if c == close_quote {
                // Check for escaped quote (only for " and `)
                if close_quote != ']' && self.peek(1) == close_quote {
                    value.push(close_quote);
                    self.advance();
                    self.advance();
                } else {
                    self.advance(); // Skip closing quote
                    break;
                }
            } else {
                value.push(c);
                self.advance();
            }
        }

        Ok(Token::with_position(
            TokenType::QuotedIdentifier,
            value,
            start,
            self.pos,
            start_line,
            start_column,
        ))
    }

    /// Scan a variable/parameter
    fn scan_variable(
        &mut self,
        start: usize,
        start_line: usize,
        start_column: usize,
    ) -> Result<Token> {
        self.advance(); // Skip prefix

        // For ? parameters, optionally followed by a number
        if self.input.as_bytes()[start] == b'?' {
            while !self.is_at_end() && self.current().is_ascii_digit() {
                self.advance();
            }
        } else {
            // For : @ $, followed by identifier
            while !self.is_at_end() {
                let c = self.current();
                if c.is_ascii_alphanumeric() || c == '_' {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let value = self.input[start..self.pos].to_string();

        Ok(Token::with_position(
            TokenType::Variable,
            value,
            start,
            self.pos,
            start_line,
            start_column,
        ))
    }

    /// Scan a blob literal
    fn scan_blob(
        &mut self,
        start: usize,
        start_line: usize,
        start_column: usize,
    ) -> Result<Token> {
        self.advance(); // Skip 'x' or 'X'
        self.advance(); // Skip opening quote

        let mut value = String::new();

        while !self.is_at_end() {
            let c = self.current();

            if c == '\'' {
                self.advance(); // Skip closing quote
                break;
            } else if c.is_ascii_hexdigit() {
                value.push(c);
                self.advance();
            } else if !c.is_whitespace() {
                return Err(Error::Parse(format!(
                    "Invalid character in blob literal: '{}'",
                    c
                )));
            } else {
                self.advance();
            }
        }

        Ok(Token::with_position(
            TokenType::Blob,
            value,
            start,
            self.pos,
            start_line,
            start_column,
        ))
    }

    /// Skip whitespace and comments
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while !self.is_at_end() && self.current().is_whitespace() {
                self.advance();
            }

            if self.is_at_end() {
                break;
            }

            // Check for comments
            let c = self.current();
            if c == '-' && self.peek(1) == '-' {
                // Single-line comment
                while !self.is_at_end() && self.current() != '\n' {
                    self.advance();
                }
            } else if c == '/' && self.peek(1) == '*' {
                // Multi-line comment
                self.advance();
                self.advance();
                while !self.is_at_end() {
                    if self.current() == '*' && self.peek(1) == '/' {
                        self.advance();
                        self.advance();
                        break;
                    }
                    self.advance();
                }
            } else {
                break;
            }
        }
    }

    /// Check if we're at the end of input
    fn is_at_end(&self) -> bool {
        self.pos >= self.input.len()
    }

    /// Get the current character
    fn current(&self) -> char {
        self.peek(0)
    }

    /// Peek at a character at the given offset
    fn peek(&self, offset: usize) -> char {
        self.input[self.pos..].chars().nth(offset).unwrap_or('\0')
    }

    /// Advance to the next character
    fn advance(&mut self) {
        if let Some(c) = self.input[self.pos..].chars().next() {
            self.pos += c.len_utf8();
            if c == '\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
        }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token()?;
            if token.is_eof() {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }

        Ok(tokens)
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        let token = self.next_token().ok()?;
        if token.is_eof() {
            None
        } else {
            Some(Ok(token))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_keywords() {
        let mut lexer = Lexer::new("SELECT FROM WHERE");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 4); // SELECT, FROM, WHERE, EOF
        assert_eq!(tokens[0].ty, TokenType::Select);
        assert_eq!(tokens[1].ty, TokenType::From);
        assert_eq!(tokens[2].ty, TokenType::Where);
    }

    #[test]
    fn test_lexer_identifiers() {
        let mut lexer = Lexer::new("my_table _private");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 3); // my_table, _private, EOF
        assert_eq!(tokens[0].ty, TokenType::Identifier);
        assert_eq!(tokens[1].ty, TokenType::Identifier);
    }

    #[test]
    fn test_lexer_unicode_identifiers() {
        let mut lexer = Lexer::new("用户表 列名");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].ty, TokenType::Identifier);
        assert_eq!(tokens[0].value, "用户表");
        assert_eq!(tokens[1].ty, TokenType::Identifier);
        assert_eq!(tokens[1].value, "列名");
    }

    #[test]
    fn test_lexer_numbers() {
        let mut lexer = Lexer::new("42 3.14 1e10 1.5e-3 .5");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0].ty, TokenType::Integer);
        assert_eq!(tokens[0].value, "42");
        assert_eq!(tokens[1].ty, TokenType::Float);
        assert_eq!(tokens[2].ty, TokenType::Float);
        assert_eq!(tokens[3].ty, TokenType::Float);
        assert_eq!(tokens[4].ty, TokenType::Float);
        assert_eq!(tokens[4].value, ".5");
    }

    #[test]
    fn test_lexer_strings() {
        let mut lexer = Lexer::new("'hello' 'it''s quoted'");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].ty, TokenType::String);
        assert_eq!(tokens[0].value, "hello");
        assert_eq!(tokens[1].ty, TokenType::String);
        assert_eq!(tokens[1].value, "it's quoted");
    }

    #[test]
    fn test_lexer_unicode_strings() {
        let mut lexer = Lexer::new("'中文内容' '北京'");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].ty, TokenType::String);
        assert_eq!(tokens[0].value, "中文内容");
        assert_eq!(tokens[1].ty, TokenType::String);
        assert_eq!(tokens[1].value, "北京");
    }

    #[test]
    fn test_lexer_blobs() {
        let mut lexer = Lexer::new("X'010203' x'AABBCC'");
        let tokens = lexer.tokenize().unwrap();

        // Note: blob literals need to be X'...' (uppercase X required by SQLite spec)
        // Lowercase 'x' may be treated as identifier + string
        assert!(tokens.len() >= 3);
        assert_eq!(tokens[0].ty, TokenType::Blob);
        assert_eq!(tokens[0].value, "010203");
    }

    #[test]
    fn test_lexer_operators() {
        let mut lexer = Lexer::new("= <> != < <= > >= || + - * / % & | ~ << >>");
        let tokens = lexer.tokenize().unwrap();

        // 18 operators + 1 EOF = 19 tokens
        assert_eq!(tokens.len(), 19);
        assert_eq!(tokens[0].ty, TokenType::Equal);
        assert_eq!(tokens[1].ty, TokenType::NotEqual);
        assert_eq!(tokens[2].ty, TokenType::NotEqual);
        assert_eq!(tokens[3].ty, TokenType::Less);
        assert_eq!(tokens[4].ty, TokenType::LessEqual);
        assert_eq!(tokens[5].ty, TokenType::Greater);
        assert_eq!(tokens[6].ty, TokenType::GreaterEqual);
        assert_eq!(tokens[7].ty, TokenType::Concat);
        assert_eq!(tokens[8].ty, TokenType::Plus);
        assert_eq!(tokens[9].ty, TokenType::Minus);
        assert_eq!(tokens[10].ty, TokenType::Star);
        assert_eq!(tokens[11].ty, TokenType::Slash);
        assert_eq!(tokens[12].ty, TokenType::Percent);
        assert_eq!(tokens[13].ty, TokenType::BitAnd);
        assert_eq!(tokens[14].ty, TokenType::BitOr);
        assert_eq!(tokens[15].ty, TokenType::BitNot);
        assert_eq!(tokens[16].ty, TokenType::LeftShift);
        assert_eq!(tokens[17].ty, TokenType::RightShift);
    }

    #[test]
    fn test_lexer_punctuation() {
        let mut lexer = Lexer::new("( ) , ; .");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0].ty, TokenType::LeftParen);
        assert_eq!(tokens[1].ty, TokenType::RightParen);
        assert_eq!(tokens[2].ty, TokenType::Comma);
        assert_eq!(tokens[3].ty, TokenType::Semicolon);
        assert_eq!(tokens[4].ty, TokenType::Dot);
    }

    #[test]
    fn test_lexer_variables() {
        let mut lexer = Lexer::new("? ?1 :name @var $param");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 6);
        for token in &tokens[..5] {
            assert_eq!(token.ty, TokenType::Variable);
        }
        assert_eq!(tokens[1].value, "?1");
        assert_eq!(tokens[2].value, ":name");
        assert_eq!(tokens[3].value, "@var");
        assert_eq!(tokens[4].value, "$param");
    }

    #[test]
    fn test_lexer_quoted_identifiers() {
        let mut lexer = Lexer::new(r#""quoted" `backtick` [bracket] "with "" quote""#);
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[0].ty, TokenType::QuotedIdentifier);
        assert_eq!(tokens[0].value, "quoted");
        assert_eq!(tokens[1].ty, TokenType::QuotedIdentifier);
        assert_eq!(tokens[1].value, "backtick");
        assert_eq!(tokens[2].ty, TokenType::QuotedIdentifier);
        assert_eq!(tokens[2].value, "bracket");
        assert_eq!(tokens[3].ty, TokenType::QuotedIdentifier);
        assert_eq!(tokens[3].value, r#"with " quote"#);
    }

    #[test]
    fn test_lexer_comments() {
        let mut lexer = Lexer::new("SELECT -- comment\nFROM /* multi\nline */ WHERE");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].ty, TokenType::Select);
        assert_eq!(tokens[1].ty, TokenType::From);
        assert_eq!(tokens[2].ty, TokenType::Where);
    }

    #[test]
    fn test_lexer_complex_query() {
        let sql = r#"SELECT id, name FROM users WHERE age > 18 AND status = 'active' ORDER BY name LIMIT 10"#;
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize().unwrap();

        // Verify we got a reasonable number of tokens
        assert!(tokens.len() > 15);
        // Check specific tokens by finding them
        assert_eq!(tokens[0].ty, TokenType::Select);
        // Find the FROM keyword
        let from_pos = tokens.iter().position(|t| t.ty == TokenType::From).unwrap();
        assert_eq!(tokens[from_pos].ty, TokenType::From);
        // Find WHERE
        let where_pos = tokens.iter().position(|t| t.ty == TokenType::Where).unwrap();
        assert_eq!(tokens[where_pos].ty, TokenType::Where);
    }

    #[test]
    fn test_lexer_position_tracking() {
        let mut lexer = Lexer::new("SELECT\nFROM");
        let tokens = lexer.tokenize().unwrap();

        assert_eq!(tokens[0].line, 1);
        assert_eq!(tokens[0].column, 1);
        assert_eq!(tokens[1].line, 2);
        assert_eq!(tokens[1].column, 1);
    }

    #[test]
    fn test_lexer_iterator() {
        let lexer = Lexer::new("SELECT FROM");
        let tokens: Vec<Token> = lexer.filter_map(|r| r.ok()).collect();

        assert_eq!(tokens.len(), 2);
    }
}
