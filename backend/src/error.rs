//! Error types for SQLite-RS
//!
//! This module defines all error types used throughout the library.

#![allow(missing_docs)]

use std::fmt;
use std::result;
use thiserror::Error;

/// Primary result type for SQLite operations
pub type Result<T> = result::Result<T, Error>;

/// Error codes matching SQLite's error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorCode {
    /// Successful result
    Ok = 0,
    /// Generic error
    Error = 1,
    /// Internal logic error
    Internal = 2,
    /// Access permission denied
    Perm = 3,
    /// Callback routine requested an abort
    Abort = 4,
    /// Database file is busy
    Busy = 5,
    /// Database table is locked
    Locked = 6,
    /// Out of memory
    NoMem = 7,
    /// Attempt to write a readonly database
    ReadOnly = 8,
    /// Operation terminated by interrupt
    Interrupt = 9,
    /// Disk I/O error
    IoErr = 10,
    /// Database disk image is malformed
    Corrupt = 11,
    /// Unknown opcode in sqlite3_file_control
    NotFound = 12,
    /// Insertion failed because database is full
    Full = 13,
    /// Unable to open the database file
    CantOpen = 14,
    /// Database lock protocol error
    Protocol = 15,
    /// Internal use only
    Empty = 16,
    /// Database schema changed
    Schema = 17,
    /// String or blob exceeds size limit
    TooBig = 18,
    /// Abort due to constraint violation
    Constraint = 19,
    /// Data type mismatch
    Mismatch = 20,
    /// Library used incorrectly
    Misuse = 21,
    /// Uses OS features not supported on host
    NoLfs = 22,
    /// Authorization denied
    Auth = 23,
    /// Auxiliary database format error
    Format = 24,
    /// 2nd parameter to sqlite3_bind out of range
    Range = 25,
    /// File opened that is not a database file
    NotADb = 26,
    /// Notifications from sqlite3_log
    Notice = 27,
    /// Warnings from sqlite3_log
    Warning = 28,
    /// sqlite3_step() has another row ready
    Row = 100,
    /// sqlite3_step() has finished executing
    Done = 101,
}

impl ErrorCode {
    /// Get the error code as an integer
    pub fn code(&self) -> i32 {
        *self as i32
    }

    /// Check if this is a successful result code
    pub fn is_ok(&self) -> bool {
        matches!(self, ErrorCode::Ok | ErrorCode::Row | ErrorCode::Done)
    }

    /// Get the error name as a string
    pub fn name(&self) -> &'static str {
        match self {
            ErrorCode::Ok => "SQLITE_OK",
            ErrorCode::Error => "SQLITE_ERROR",
            ErrorCode::Internal => "SQLITE_INTERNAL",
            ErrorCode::Perm => "SQLITE_PERM",
            ErrorCode::Abort => "SQLITE_ABORT",
            ErrorCode::Busy => "SQLITE_BUSY",
            ErrorCode::Locked => "SQLITE_LOCKED",
            ErrorCode::NoMem => "SQLITE_NOMEM",
            ErrorCode::ReadOnly => "SQLITE_READONLY",
            ErrorCode::Interrupt => "SQLITE_INTERRUPT",
            ErrorCode::IoErr => "SQLITE_IOERR",
            ErrorCode::Corrupt => "SQLITE_CORRUPT",
            ErrorCode::NotFound => "SQLITE_NOTFOUND",
            ErrorCode::Full => "SQLITE_FULL",
            ErrorCode::CantOpen => "SQLITE_CANTOPEN",
            ErrorCode::Protocol => "SQLITE_PROTOCOL",
            ErrorCode::Empty => "SQLITE_EMPTY",
            ErrorCode::Schema => "SQLITE_SCHEMA",
            ErrorCode::TooBig => "SQLITE_TOOBIG",
            ErrorCode::Constraint => "SQLITE_CONSTRAINT",
            ErrorCode::Mismatch => "SQLITE_MISMATCH",
            ErrorCode::Misuse => "SQLITE_MISUSE",
            ErrorCode::NoLfs => "SQLITE_NOLFS",
            ErrorCode::Auth => "SQLITE_AUTH",
            ErrorCode::Format => "SQLITE_FORMAT",
            ErrorCode::Range => "SQLITE_RANGE",
            ErrorCode::NotADb => "SQLITE_NOTADB",
            ErrorCode::Notice => "SQLITE_NOTICE",
            ErrorCode::Warning => "SQLITE_WARNING",
            ErrorCode::Row => "SQLITE_ROW",
            ErrorCode::Done => "SQLITE_DONE",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Default for ErrorCode {
    fn default() -> Self {
        ErrorCode::Ok
    }
}

/// Extended error codes for more specific error information
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ExtendedErrorCode {
    // IO Error extensions
    IoErrRead = (ErrorCode::IoErr as i32) | (1 << 8),
    IoErrShortRead = (ErrorCode::IoErr as i32) | (2 << 8),
    IoErrWrite = (ErrorCode::IoErr as i32) | (3 << 8),
    IoErrFsync = (ErrorCode::IoErr as i32) | (4 << 8),
    IoErrDirFsync = (ErrorCode::IoErr as i32) | (5 << 8),
    IoErrTruncate = (ErrorCode::IoErr as i32) | (6 << 8),
    IoErrFstat = (ErrorCode::IoErr as i32) | (7 << 8),
    IoErrUnlock = (ErrorCode::IoErr as i32) | (8 << 8),
    IoErrRdlock = (ErrorCode::IoErr as i32) | (9 << 8),
    IoErrDelete = (ErrorCode::IoErr as i32) | (10 << 8),
    IoErrBlocked = (ErrorCode::IoErr as i32) | (11 << 8),
    IoErrNoMem = (ErrorCode::IoErr as i32) | (12 << 8),
    IoErrAccess = (ErrorCode::IoErr as i32) | (13 << 8),
    IoErrCheckReservedLock = (ErrorCode::IoErr as i32) | (14 << 8),
    IoErrLock = (ErrorCode::IoErr as i32) | (15 << 8),
    IoErrClose = (ErrorCode::IoErr as i32) | (16 << 8),
    IoErrDirClose = (ErrorCode::IoErr as i32) | (17 << 8),
    IoErrShmOpen = (ErrorCode::IoErr as i32) | (18 << 8),
    IoErrShmSize = (ErrorCode::IoErr as i32) | (19 << 8),
    IoErrShmLock = (ErrorCode::IoErr as i32) | (20 << 8),
    IoErrShmMap = (ErrorCode::IoErr as i32) | (21 << 8),
    IoErrSeek = (ErrorCode::IoErr as i32) | (22 << 8),
    IoErrDeleteNoEnt = (ErrorCode::IoErr as i32) | (23 << 8),
    IoErrMmap = (ErrorCode::IoErr as i32) | (24 << 8),
    IoErrGetTempPath = (ErrorCode::IoErr as i32) | (25 << 8),
    IoErrConvPath = (ErrorCode::IoErr as i32) | (26 << 8),
    IoErrVnode = (ErrorCode::IoErr as i32) | (27 << 8),
    IoErrAuth = (ErrorCode::IoErr as i32) | (28 << 8),
    IoErrBeginAtomic = (ErrorCode::IoErr as i32) | (29 << 8),
    IoErrCommitAtomic = (ErrorCode::IoErr as i32) | (30 << 8),
    IoErrRollbackAtomic = (ErrorCode::IoErr as i32) | (31 << 8),
    IoErrData = (ErrorCode::IoErr as i32) | (32 << 8),
    IoErrCorruptFs = (ErrorCode::IoErr as i32) | (33 << 8),

    // Constraint error extensions
    ConstraintCheck = (ErrorCode::Constraint as i32) | (1 << 8),
    ConstraintCommitHook = (ErrorCode::Constraint as i32) | (2 << 8),
    ConstraintForeignKey = (ErrorCode::Constraint as i32) | (3 << 8),
    ConstraintFunction = (ErrorCode::Constraint as i32) | (4 << 8),
    ConstraintNotNull = (ErrorCode::Constraint as i32) | (5 << 8),
    ConstraintPrimaryKey = (ErrorCode::Constraint as i32) | (6 << 8),
    ConstraintTrigger = (ErrorCode::Constraint as i32) | (7 << 8),
    ConstraintUnique = (ErrorCode::Constraint as i32) | (8 << 8),
    ConstraintVtab = (ErrorCode::Constraint as i32) | (9 << 8),
    ConstraintRowId = (ErrorCode::Constraint as i32) | (10 << 8),
    ConstraintPinned = (ErrorCode::Constraint as i32) | (11 << 8),

    // Busy error extensions
    BusyRecovery = (ErrorCode::Busy as i32) | (1 << 8),
    BusySnapshot = (ErrorCode::Busy as i32) | (2 << 8),
    BusyTimeout = (ErrorCode::Busy as i32) | (3 << 8),

    // CantOpen error extensions
    CantOpenNoTempDir = (ErrorCode::CantOpen as i32) | (1 << 8),
    CantOpenIsDir = (ErrorCode::CantOpen as i32) | (2 << 8),
    CantOpenFullPath = (ErrorCode::CantOpen as i32) | (3 << 8),
    CantOpenConvPath = (ErrorCode::CantOpen as i32) | (4 << 8),
    CantOpenDirtyWal = (ErrorCode::CantOpen as i32) | (5 << 8),
    CantOpenSymlink = (ErrorCode::CantOpen as i32) | (6 << 8),

    // Corrupt error extensions
    CorruptVtab = (ErrorCode::Corrupt as i32) | (1 << 8),
    CorruptSequence = (ErrorCode::Corrupt as i32) | (2 << 8),
    CorruptIndex = (ErrorCode::Corrupt as i32) | (3 << 8),

    // ReadOnly error extensions
    ReadOnlyRecovery = (ErrorCode::ReadOnly as i32) | (1 << 8),
    ReadOnlyCantLock = (ErrorCode::ReadOnly as i32) | (2 << 8),
    ReadOnlyRollback = (ErrorCode::ReadOnly as i32) | (3 << 8),
    ReadOnlyDbMoved = (ErrorCode::ReadOnly as i32) | (4 << 8),
    ReadOnlyCantInit = (ErrorCode::ReadOnly as i32) | (5 << 8),
    ReadOnlyDirectory = (ErrorCode::ReadOnly as i32) | (6 << 8),

    // Abort error extensions
    AbortRollback = (ErrorCode::Abort as i32) | (2 << 8),

    // Auth error extensions
    AuthUser = (ErrorCode::Auth as i32) | (1 << 8),

    // Lock error extensions
    LockedSharedCache = (ErrorCode::Locked as i32) | (1 << 8),
    LockedVtab = (ErrorCode::Locked as i32) | (2 << 8),
}

impl ExtendedErrorCode {
    /// Get the primary error code from the extended code
    pub fn primary_code(&self) -> ErrorCode {
        let code = *self as i32;
        match code & 0xFF {
            0 => ErrorCode::Ok,
            1 => ErrorCode::Error,
            10 => ErrorCode::IoErr,
            19 => ErrorCode::Constraint,
            5 => ErrorCode::Busy,
            14 => ErrorCode::CantOpen,
            11 => ErrorCode::Corrupt,
            8 => ErrorCode::ReadOnly,
            4 => ErrorCode::Abort,
            23 => ErrorCode::Auth,
            6 => ErrorCode::Locked,
            _ => ErrorCode::Error,
        }
    }
}

/// Main error type for SQLite operations
#[derive(Debug, Error)]
pub enum Error {
    /// Generic SQLite error with code and message
    #[error("SQLite error (code {code}): {message}")]
    Sqlite {
        /// Error code
        code: ErrorCode,
        /// Error message
        message: String,
    },

    /// Extended SQLite error with extended code
    #[error("SQLite error ({extended:?}): {message}")]
    ExtendedSqlite {
        /// Extended error code
        extended: ExtendedErrorCode,
        /// Error message
        message: String,
    },

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Parse error
    #[error("Parse error: {0}")]
    Parse(String),

    /// Type mismatch error
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// Expected type
        expected: String,
        /// Actual type
        actual: String,
    },

    /// Column not found error
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// Table not found error
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Index not found error
    #[error("Index not found: {0}")]
    IndexNotFound(String),

    /// Constraint violation
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    /// Invalid SQL
    #[error("Invalid SQL: {0}")]
    InvalidSql(String),

    /// Out of range
    #[error("Value out of range: {0}")]
    OutOfRange(String),

    /// Database is locked
    #[error("Database is locked")]
    DatabaseLocked,

    /// Out of memory
    #[error("Out of memory")]
    OutOfMemory,

    /// Database is corrupt
    #[error("Database is corrupt: {0}")]
    Corrupt(String),

    /// Not a database
    #[error("Not a database file")]
    NotADatabase,

    /// Operation cancelled
    #[error("Operation cancelled")]
    Cancelled,

    /// Invalid parameter
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    /// Unsupported operation
    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Create a new SQLite error with the given code and message
    pub fn sqlite(code: ErrorCode, message: impl Into<String>) -> Self {
        Error::Sqlite {
            code,
            message: message.into(),
        }
    }

    /// Create an error error
    pub fn error(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Error, msg)
    }

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Internal, msg)
    }

    /// Create a busy error
    pub fn busy(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Busy, msg)
    }

    /// Create a locked error
    pub fn locked(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Locked, msg)
    }

    /// Create a no memory error
    pub fn no_mem() -> Self {
        Self::sqlite(ErrorCode::NoMem, "out of memory")
    }

    /// Create a read-only error
    pub fn read_only(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::ReadOnly, msg)
    }

    /// Create an I/O error
    pub fn io_err(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::IoErr, msg)
    }

    /// Create a corrupt error
    pub fn corrupt(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Corrupt, msg)
    }

    /// Create a full error
    pub fn full(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Full, msg)
    }

    /// Create a can't open error
    pub fn cant_open(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::CantOpen, msg)
    }

    /// Create a constraint error
    pub fn constraint(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Constraint, msg)
    }

    /// Create a mismatch error
    pub fn mismatch(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Mismatch, msg)
    }

    /// Create a misuse error
    pub fn misuse(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Misuse, msg)
    }

    /// Create a range error
    pub fn range(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Range, msg)
    }

    /// Create a schema error
    pub fn schema(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Schema, msg)
    }

    /// Create a too big error
    pub fn too_big(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::TooBig, msg)
    }

    /// Create an abort error
    pub fn abort(msg: impl Into<String>) -> Self {
        Self::sqlite(ErrorCode::Abort, msg)
    }

    /// Get the error code if this is a SQLite error
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Sqlite { code, .. } => *code,
            Error::ExtendedSqlite { extended, .. } => extended.primary_code(),
            Error::Io(_) => ErrorCode::IoErr,
            Error::OutOfMemory => ErrorCode::NoMem,
            Error::DatabaseLocked => ErrorCode::Busy,
            Error::Corrupt(_) => ErrorCode::Corrupt,
            Error::NotADatabase => ErrorCode::NotADb,
            Error::ConstraintViolation(_) => ErrorCode::Constraint,
            Error::TypeMismatch { .. } => ErrorCode::Mismatch,
            Error::OutOfRange(_) => ErrorCode::Range,
            _ => ErrorCode::Error,
        }
    }

    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self.code(),
            ErrorCode::Busy
                | ErrorCode::Locked
                | ErrorCode::Constraint
                | ErrorCode::Range
                | ErrorCode::TooBig
        )
    }
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        let msg = match code {
            ErrorCode::Ok => "success",
            ErrorCode::Error => "generic error",
            ErrorCode::Internal => "internal error",
            ErrorCode::Perm => "permission denied",
            ErrorCode::Abort => "operation aborted",
            ErrorCode::Busy => "database is busy",
            ErrorCode::Locked => "database is locked",
            ErrorCode::NoMem => "out of memory",
            ErrorCode::ReadOnly => "attempt to write to read-only database",
            ErrorCode::Interrupt => "operation interrupted",
            ErrorCode::IoErr => "I/O error",
            ErrorCode::Corrupt => "database is corrupt",
            ErrorCode::NotFound => "not found",
            ErrorCode::Full => "database is full",
            ErrorCode::CantOpen => "unable to open database",
            ErrorCode::Protocol => "lock protocol error",
            ErrorCode::Empty => "empty database",
            ErrorCode::Schema => "schema has changed",
            ErrorCode::TooBig => "string or blob too big",
            ErrorCode::Constraint => "constraint violation",
            ErrorCode::Mismatch => "data type mismatch",
            ErrorCode::Misuse => "library misuse",
            ErrorCode::NoLfs => "large file support not available",
            ErrorCode::Auth => "authorization denied",
            ErrorCode::Format => "database format error",
            ErrorCode::Range => "parameter index out of range",
            ErrorCode::NotADb => "not a database file",
            ErrorCode::Notice => "notice",
            ErrorCode::Warning => "warning",
            ErrorCode::Row => "row available",
            ErrorCode::Done => "execution complete",
        };
        Error::sqlite(code, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_values() {
        assert_eq!(ErrorCode::Ok as i32, 0);
        assert_eq!(ErrorCode::Error as i32, 1);
        assert_eq!(ErrorCode::Busy as i32, 5);
        assert_eq!(ErrorCode::IoErr as i32, 10);
        assert_eq!(ErrorCode::Corrupt as i32, 11);
        assert_eq!(ErrorCode::Constraint as i32, 19);
        assert_eq!(ErrorCode::Row as i32, 100);
        assert_eq!(ErrorCode::Done as i32, 101);
    }

    #[test]
    fn test_error_code_names() {
        assert_eq!(ErrorCode::Ok.name(), "SQLITE_OK");
        assert_eq!(ErrorCode::Error.name(), "SQLITE_ERROR");
        assert_eq!(ErrorCode::Busy.name(), "SQLITE_BUSY");
        assert_eq!(ErrorCode::Row.name(), "SQLITE_ROW");
        assert_eq!(ErrorCode::Done.name(), "SQLITE_DONE");
    }

    #[test]
    fn test_error_code_is_ok() {
        assert!(ErrorCode::Ok.is_ok());
        assert!(ErrorCode::Row.is_ok());
        assert!(ErrorCode::Done.is_ok());
        assert!(!ErrorCode::Error.is_ok());
        assert!(!ErrorCode::Busy.is_ok());
    }

    #[test]
    fn test_error_creation() {
        let err = Error::sqlite(ErrorCode::Busy, "database is locked");
        assert_eq!(err.code(), ErrorCode::Busy);
        assert!(err.is_recoverable());

        let err = Error::constraint("UNIQUE constraint failed");
        assert_eq!(err.code(), ErrorCode::Constraint);
        assert!(err.is_recoverable());
    }

    #[test]
    fn test_error_display() {
        let err = Error::sqlite(ErrorCode::Busy, "database is locked");
        let msg = format!("{}", err);
        assert!(msg.contains("SQLITE_BUSY"));
        assert!(msg.contains("database is locked"));
    }

    #[test]
    fn test_extended_error_codes() {
        assert_eq!(
            ExtendedErrorCode::IoErrRead.primary_code(),
            ErrorCode::IoErr
        );
        assert_eq!(
            ExtendedErrorCode::ConstraintUnique.primary_code(),
            ErrorCode::Constraint
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert_eq!(err.code(), ErrorCode::IoErr);
    }
}
