//! Virtual Database Engine (VDBE)
//!
//! The VDBE is a bytecode interpreter that executes compiled SQL statements.

pub mod op;
pub mod mem;
pub mod vm;

pub use op::*;
pub use mem::*;
pub use vm::*;
