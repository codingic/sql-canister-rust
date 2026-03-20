//! VDBE Opcodes
//!
//! Defines all VDBE bytecode opcodes for the virtual machine.

#![allow(missing_docs)]

/// VDBE Opcode enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Op {
    // Control flow
    Goto = 0x01,
    Gosub = 0x02,
    Return = 0x03,
    Yield = 0x04,
    Halt = 0x05,
    Noop = 0x06,

    // Constants
    Integer = 0x10,
    Int64 = 0x11,
    Real = 0x12,
    String8 = 0x13,
    Blob = 0x14,
    Null = 0x15,
    NullRow = 0x16,
    Zero = 0x17,
    One = 0x18,

    // Arithmetic
    Add = 0x20,
    Subtract = 0x21,
    Multiply = 0x22,
    Divide = 0x23,
    Remainder = 0x24,
    Concat = 0x25,
    BitAnd = 0x26,
    BitOr = 0x27,
    BitNot = 0x28,
    ShiftLeft = 0x29,
    ShiftRight = 0x2A,
    Abs = 0x2B,

    // Comparison
    Eq = 0x30,
    Ne = 0x31,
    Lt = 0x32,
    Le = 0x33,
    Gt = 0x34,
    Ge = 0x35,
    IsNull = 0x36,
    NotNull = 0x37,
    Is = 0x38,
    IsNot = 0x39,
    Like = 0x3A,
    Glob = 0x3B,
    Between = 0x3C,
    In = 0x3D,
    Compare = 0x3E,

    // Data movement
    Copy = 0x40,
    SCopy = 0x41,
    IntCopy = 0x42,
    Move = 0x43,

    // Cursors
    OpenRead = 0x50,
    OpenWrite = 0x51,
    OpenDup = 0x52,
    OpenAutoindex = 0x53,
    OpenEphemeral = 0x54,
    Close = 0x55,
    SeekLT = 0x56,
    SeekLE = 0x57,
    SeekGE = 0x58,
    SeekGT = 0x59,
    SeekRowid = 0x5A,
    NotFound = 0x5B,
    Found = 0x5C,
    Last = 0x5D,
    First = 0x5E,
    Next = 0x5F,
    Prev = 0x60,
    Rewind = 0x61,
    Rowid = 0x62,
    Column = 0x63,
    MakeRecord = 0x64,
    Insert = 0x65,
    Delete = 0x66,
    ResetCount = 0x67,
    SorterSort = 0x68,
    SorterInsert = 0x69,
    SorterData = 0x6A,
    SorterOpen = 0x6B,
    SorterNext = 0x6C,
    IdxInsert = 0x6D,
    IdxDelete = 0x6E,
    IdxGE = 0x6F,
    IdxGT = 0x70,
    IdxLT = 0x71,
    IdxLE = 0x72,

    // Functions
    Function = 0x80,
    AggStep = 0x81,
    AggFinal = 0x82,
    AggValue = 0x83,

    // Transactions
    Transaction = 0x90,
    AutoCommit = 0x91,
    ReadCookie = 0x92,
    SetCookie = 0x93,
    Savepoint = 0x94,

    // DDL
    CreateTable = 0xA0,
    CreateIndex = 0xA1,
    DropTable = 0xA2,
    DropIndex = 0xA3,
    DropTrigger = 0xA4,
    ParseSchema = 0xA5,

    // Result
    ResultRow = 0xB0,
}

impl Op {
    /// Get opcode name
    pub fn name(&self) -> &'static str {
        match self {
            Op::Goto => "Goto",
            Op::Gosub => "Gosub",
            Op::Return => "Return",
            Op::Halt => "Halt",
            Op::Noop => "Noop",
            Op::Integer => "Integer",
            Op::Int64 => "Int64",
            Op::Real => "Real",
            Op::String8 => "String8",
            Op::Blob => "Blob",
            Op::Null => "Null",
            Op::Add => "Add",
            Op::Subtract => "Subtract",
            Op::Multiply => "Multiply",
            Op::Divide => "Divide",
            Op::Remainder => "Remainder",
            Op::Concat => "Concat",
            Op::Eq => "Eq",
            Op::Ne => "Ne",
            Op::Lt => "Lt",
            Op::Le => "Le",
            Op::Gt => "Gt",
            Op::Ge => "Ge",
            Op::IsNull => "IsNull",
            Op::NotNull => "NotNull",
            Op::Copy => "Copy",
            Op::SCopy => "SCopy",
            Op::OpenRead => "OpenRead",
            Op::OpenWrite => "OpenWrite",
            Op::Close => "Close",
            Op::Column => "Column",
            Op::Rowid => "Rowid",
            Op::Next => "Next",
            Op::Prev => "Prev",
            Op::Rewind => "Rewind",
            Op::Last => "Last",
            Op::First => "First",
            Op::SeekLT => "SeekLT",
            Op::SeekLE => "SeekLE",
            Op::SeekGE => "SeekGE",
            Op::SeekGT => "SeekGT",
            Op::SeekRowid => "SeekRowid",
            Op::NotFound => "NotFound",
            Op::Found => "Found",
            Op::MakeRecord => "MakeRecord",
            Op::Insert => "Insert",
            Op::Delete => "Delete",
            Op::IdxInsert => "IdxInsert",
            Op::IdxDelete => "IdxDelete",
            Op::Function => "Function",
            Op::AggStep => "AggStep",
            Op::AggFinal => "AggFinal",
            Op::ResultRow => "ResultRow",
            Op::SorterSort => "SorterSort",
            Op::SorterInsert => "SorterInsert",
            Op::SorterOpen => "SorterOpen",
            Op::SorterNext => "SorterNext",
            Op::Transaction => "Transaction",
            Op::AutoCommit => "AutoCommit",
            Op::CreateTable => "CreateTable",
            Op::CreateIndex => "CreateIndex",
            Op::DropTable => "DropTable",
            Op::DropIndex => "DropIndex",
            Op::ParseSchema => "ParseSchema",
            _ => "Unknown",
        }
    }

    /// Check if this opcode is a jump
    pub fn is_jump(&self) -> bool {
        matches!(
            self,
            Op::Goto
                | Op::Gosub
                | Op::Return
                | Op::Eq
                | Op::Ne
                | Op::Lt
                | Op::Le
                | Op::Gt
                | Op::Ge
                | Op::IsNull
                | Op::NotNull
                | Op::NotFound
                | Op::Found
                | Op::Next
                | Op::Prev
                | Op::Rewind
                | Op::SorterNext
        )
    }

    /// Check if this opcode writes
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Op::Insert
                | Op::Delete
                | Op::IdxInsert
                | Op::IdxDelete
                | Op::OpenWrite
                | Op::CreateTable
                | Op::CreateIndex
                | Op::DropTable
                | Op::DropIndex
        )
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_names() {
        assert_eq!(Op::Goto.name(), "Goto");
        assert_eq!(Op::Integer.name(), "Integer");
        assert_eq!(Op::Add.name(), "Add");
    }

    #[test]
    fn test_op_is_jump() {
        assert!(Op::Goto.is_jump());
        assert!(Op::Eq.is_jump());
        assert!(!Op::Integer.is_jump());
    }

    #[test]
    fn test_op_is_write() {
        assert!(Op::Insert.is_write());
        assert!(Op::Delete.is_write());
        assert!(!Op::OpenRead.is_write());
    }
}
