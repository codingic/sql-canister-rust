//! Code generator module
//!
//! Transforms parsed SQL AST into VDBE bytecode.

use crate::error::Result;
use crate::parser::ast::Statement;
use crate::vdbe::{Vdbe, Op};

/// Code generator context
pub struct CodeGen {
    vdbe: Vdbe,
}

impl CodeGen {
    /// Create a new code generator
    pub fn new() -> Self {
        CodeGen {
            vdbe: Vdbe::new(),
        }
    }

    /// Generate VDBE bytecode for a statement
    pub fn generate(&mut self, _stmt: &Statement) -> Result<Vdbe> {
        // Generate a simple halt program for now
        self.vdbe.add_op(Op::Halt);
        Ok(std::mem::replace(&mut self.vdbe, Vdbe::new()))
    }
}

impl Default for CodeGen {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate VDBE bytecode from a statement
pub fn codegen(stmt: &Statement) -> Result<Vdbe> {
    let mut gen = CodeGen::new();
    gen.generate(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codegen_new() {
        let gen = CodeGen::new();
        assert!(gen.vdbe.instructions().is_empty());
    }
}
