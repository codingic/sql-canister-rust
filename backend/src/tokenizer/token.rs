//! Token types for SQL lexer

#![allow(missing_docs)]

use std::fmt;

/// Token type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenType {
    // Literals
    /// Integer literal
    Integer,
    /// Floating point literal
    Float,
    /// String literal
    String,
    /// Blob literal (X'...')
    Blob,
    /// NULL keyword
    Null,
    /// Boolean literals
    True,
    False,

    // Identifiers and keywords
    /// Regular identifier
    Identifier,
    /// Quoted identifier "..."
    QuotedIdentifier,
    /// Variable/parameter ?1, :name, @name, $name
    Variable,

    // Keywords
    // DDL Keywords
    Create,
    Table,
    Index,
    View,
    Trigger,
    Drop,
    Alter,
    Add,
    Column,
    Rename,
    To,

    // DML Keywords
    Select,
    Insert,
    Update,
    Delete,
    From,
    Into,
    Values,
    Set,
    Where,
    And,
    Or,
    Not,

    // Query keywords
    Distinct,
    All,
    As,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    Group,
    Having,
    Union,
    Intersect,
    Except,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    Cross,
    Natural,
    On,
    Using,

    // Constraint keywords
    Primary,
    Key,
    Unique,
    Check,
    Constraint,
    Foreign,
    References,
    Default,
    Collate,
    Generated,
    Always,
    Stored,
    Virtual,

    // Type keywords
    Text,
    Int,
    Real,
    BlobType,
    Numeric,
    Varchar,
    Char,
    FloatType,
    Double,
    Boolean,
    Date,
    Time,
    Datetime,

    // Transaction keywords
    Begin,
    Commit,
    Rollback,
    Transaction,
    Savepoint,
    Release,

    // Other keywords
    Nulls,
    First,
    Last,
    Between,
    Like,
    Glob,
    In,
    Is,
    IsNull,
    NotNull,
    Case,
    When,
    Then,
    Else,
    End,
    Cast,
    Exists,
    With,
    Recursive,
    Return,
    Returning,
    AutoIncrement,
    Temporary,
    Temp,
    If,
    Replace,
    Ignore,
    Do,
    Nothing,
    Conflict,
    Abort,
    Fail,
    Cascade,
    Restrict,
    No,
    Action,
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
    Deferrable,
    Deferred,
    Immediate,
    Exclusive,
    Attached,
    Detach,
    Database,
    Pragma,
    Explain,
    Query,
    Plan,
    Vacuum,
    Reindex,
    Analyze,
    Match,
    Regexp,
    Escape,

    // Function keywords
    Over,
    Partition,
    Filter,
    Window,
    Frame,
    Range,
    Row,
    Rows,
    Groups,
    Unbounded,
    Preceding,
    Following,
    Ties,
    Others,
    Exclude,
    CurrentRow,
    Materialized,

    // Operators
    /// =
    Equal,
    /// <> or !=
    NotEqual,
    /// <
    Less,
    /// <=
    LessEqual,
    /// >
    Greater,
    /// >=
    GreaterEqual,
    /// ||
    Concat,
    /// +
    Plus,
    /// -
    Minus,
    /// *
    Star,
    /// /
    Slash,
    /// %
    Percent,
    /// <<
    LeftShift,
    /// >>
    RightShift,
    /// &
    BitAnd,
    /// |
    BitOr,
    /// ~
    BitNot,
    /// ->
    Arrow,
    /// ->>
    ArrowText,

    // Punctuation
    /// (
    LeftParen,
    /// )
    RightParen,
    /// [
    LeftBracket,
    /// ]
    RightBracket,
    /// ,
    Comma,
    /// ;
    Semicolon,
    /// .
    Dot,
    /// ::
    DoubleColon,

    // Special
    /// End of input
    Eof,
    /// Unknown token
    Unknown,
}

impl TokenType {
    /// Check if this is a literal token
    pub fn is_literal(&self) -> bool {
        matches!(
            self,
            TokenType::Integer
                | TokenType::Float
                | TokenType::String
                | TokenType::Blob
                | TokenType::Null
                | TokenType::True
                | TokenType::False
        )
    }

    /// Check if this is an operator token
    pub fn is_operator(&self) -> bool {
        matches!(
            self,
            TokenType::Equal
                | TokenType::NotEqual
                | TokenType::Less
                | TokenType::LessEqual
                | TokenType::Greater
                | TokenType::GreaterEqual
                | TokenType::Concat
                | TokenType::Plus
                | TokenType::Minus
                | TokenType::Star
                | TokenType::Slash
                | TokenType::Percent
                | TokenType::LeftShift
                | TokenType::RightShift
                | TokenType::BitAnd
                | TokenType::BitOr
                | TokenType::BitNot
        )
    }
}

impl fmt::Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenType::Integer => write!(f, "INTEGER"),
            TokenType::Float => write!(f, "FLOAT"),
            TokenType::String => write!(f, "STRING"),
            TokenType::Blob => write!(f, "BLOB"),
            TokenType::Null => write!(f, "NULL"),
            TokenType::True => write!(f, "TRUE"),
            TokenType::False => write!(f, "FALSE"),
            TokenType::Identifier => write!(f, "IDENTIFIER"),
            TokenType::QuotedIdentifier => write!(f, "QUOTED_IDENTIFIER"),
            TokenType::Variable => write!(f, "VARIABLE"),
            TokenType::Create => write!(f, "CREATE"),
            TokenType::Table => write!(f, "TABLE"),
            TokenType::Select => write!(f, "SELECT"),
            TokenType::Insert => write!(f, "INSERT"),
            TokenType::Update => write!(f, "UPDATE"),
            TokenType::Delete => write!(f, "DELETE"),
            TokenType::From => write!(f, "FROM"),
            TokenType::Where => write!(f, "WHERE"),
            TokenType::Equal => write!(f, "="),
            TokenType::NotEqual => write!(f, "<>"),
            TokenType::Less => write!(f, "<"),
            TokenType::LessEqual => write!(f, "<="),
            TokenType::Greater => write!(f, ">"),
            TokenType::GreaterEqual => write!(f, ">="),
            TokenType::Concat => write!(f, "||"),
            TokenType::Plus => write!(f, "+"),
            TokenType::Minus => write!(f, "-"),
            TokenType::Star => write!(f, "*"),
            TokenType::Slash => write!(f, "/"),
            TokenType::Percent => write!(f, "%"),
            TokenType::LeftParen => write!(f, "("),
            TokenType::RightParen => write!(f, ")"),
            TokenType::Comma => write!(f, ","),
            TokenType::Semicolon => write!(f, ";"),
            TokenType::Dot => write!(f, "."),
            TokenType::Eof => write!(f, "EOF"),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// A token with its type, value, and position
#[derive(Debug, Clone)]
pub struct Token {
    /// Token type
    pub ty: TokenType,
    /// Token value as string
    pub value: String,
    /// Start position in source
    pub start: usize,
    /// End position in source
    pub end: usize,
    /// Line number (1-based)
    pub line: usize,
    /// Column number (1-based)
    pub column: usize,
}

impl Token {
    /// Create a new token
    pub fn new(ty: TokenType, value: impl Into<String>, start: usize, end: usize) -> Self {
        Token {
            ty,
            value: value.into(),
            start,
            end,
            line: 1,
            column: 1,
        }
    }

    /// Create a token with position information
    pub fn with_position(
        ty: TokenType,
        value: impl Into<String>,
        start: usize,
        end: usize,
        line: usize,
        column: usize,
    ) -> Self {
        Token {
            ty,
            value: value.into(),
            start,
            end,
            line,
            column,
        }
    }

    /// Create an EOF token
    pub fn eof(pos: usize, line: usize, column: usize) -> Self {
        Token {
            ty: TokenType::Eof,
            value: String::new(),
            start: pos,
            end: pos,
            line,
            column,
        }
    }

    /// Get the length of the token
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if this is an EOF token
    pub fn is_eof(&self) -> bool {
        self.ty == TokenType::Eof
    }

    /// Get integer value if this is an integer token
    pub fn as_integer(&self) -> Option<i64> {
        if self.ty == TokenType::Integer {
            self.value.parse().ok()
        } else {
            None
        }
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}('{}' at {}:{})",
            self.ty, self.value, self.line, self.column
        )
    }
}

impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        self.ty == other.ty && self.value == other.value
    }
}

impl Eq for Token {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_type_is_literal() {
        assert!(TokenType::Integer.is_literal());
        assert!(TokenType::Float.is_literal());
        assert!(TokenType::String.is_literal());
        assert!(TokenType::Null.is_literal());
        assert!(!TokenType::Identifier.is_literal());
    }

    #[test]
    fn test_token_new() {
        let token = Token::new(TokenType::Integer, "42", 0, 2);
        assert_eq!(token.ty, TokenType::Integer);
        assert_eq!(token.value, "42");
    }

    #[test]
    fn test_token_eof() {
        let token = Token::eof(10, 2, 5);
        assert!(token.is_eof());
    }
}
