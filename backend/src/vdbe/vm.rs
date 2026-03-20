//! VDBE Virtual Machine
//!
//! Executes bytecode programs.

use crate::error::{Error, ErrorCode, Result};
use super::op::Op;
use super::mem::Mem;
use crate::types::Value;

/// VDBE instruction
#[derive(Debug, Clone)]
pub struct Instruction {
    /// Opcode
    pub opcode: Op,
    /// First operand
    pub p1: i32,
    /// Second operand
    pub p2: i32,
    /// Third operand
    pub p3: i32,
    /// Fourth operand (polymorphic)
    pub p4: P4Value,
    /// Fifth operand (flags)
    pub p5: u16,
}

impl Instruction {
    /// Create a new instruction
    pub fn new(opcode: Op) -> Self {
        Instruction {
            opcode,
            p1: 0,
            p2: 0,
            p3: 0,
            p4: P4Value::None,
            p5: 0,
        }
    }

    /// Set p1
    pub fn p1(mut self, p1: i32) -> Self {
        self.p1 = p1;
        self
    }

    /// Set p2
    pub fn p2(mut self, p2: i32) -> Self {
        self.p2 = p2;
        self
    }

    /// Set p3
    pub fn p3(mut self, p3: i32) -> Self {
        self.p3 = p3;
        self
    }

    /// Set p4
    pub fn p4(mut self, p4: P4Value) -> Self {
        self.p4 = p4;
        self
    }
}

/// P4 value (fourth operand of instruction)
#[derive(Debug, Clone)]
pub enum P4Value {
    /// No value
    None,
    /// Integer value
    Int(i64),
    /// Float value
    Float(f64),
    /// String value
    String(String),
    /// Blob value
    Blob(Vec<u8>),
    /// Table reference
    Table(i32),
    /// Function pointer index
    Func(i32),
    /// Key info
    KeyInfo,
}

impl Default for P4Value {
    fn default() -> Self {
        P4Value::None
    }
}

/// VDBE program
#[derive(Debug, Clone, Default)]
pub struct Vdbe {
    /// Instructions
    instructions: Vec<Instruction>,
    /// Memory cells (registers)
    memory: Vec<Mem>,
    /// Program counter
    pc: usize,
    /// Last inserted rowid
    last_rowid: i64,
    /// Number of changes
    changes: u64,
    /// Is halted
    halted: bool,
    /// Has row available
    has_row: bool,
}

impl Vdbe {
    /// Create a new VDBE program
    pub fn new() -> Self {
        Vdbe {
            instructions: Vec::new(),
            memory: Vec::new(),
            pc: 0,
            last_rowid: 0,
            changes: 0,
            halted: false,
            has_row: false,
        }
    }

    /// Add an instruction
    pub fn add_op(&mut self, opcode: Op) {
        self.instructions.push(Instruction::new(opcode));
    }

    /// Add an instruction with operands
    pub fn add_op3(&mut self, opcode: Op, p1: i32, p2: i32, p3: i32) {
        self.instructions.push(Instruction::new(opcode).p1(p1).p2(p2).p3(p3));
    }

    /// Add an instruction with p4
    pub fn add_op4(&mut self, opcode: Op, p1: i32, p2: i32, p3: i32, p4: P4Value) {
        self.instructions.push(Instruction::new(opcode).p1(p1).p2(p2).p3(p3).p4(p4));
    }

    /// Get instructions
    pub fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }

    /// Ensure memory cell exists
    pub fn ensure_memory(&mut self, idx: usize) {
        if idx >= self.memory.len() {
            self.memory.resize(idx + 1, Mem::new());
        }
    }

    /// Get memory cell
    pub fn get_memory(&self, idx: usize) -> Option<&Mem> {
        self.memory.get(idx)
    }

    /// Get mutable memory cell
    pub fn get_memory_mut(&mut self, idx: usize) -> Option<&mut Mem> {
        self.memory.get_mut(idx)
    }

    /// Set memory cell value
    pub fn set_memory(&mut self, idx: usize, value: Value) {
        self.ensure_memory(idx);
        self.memory[idx].value = value;
        self.memory[idx].flags.is_null = self.memory[idx].value.is_null();
    }

    /// Execute one step of the program
    pub fn step(&mut self) -> Result<bool> {
        if self.halted {
            return Ok(false);
        }

        if self.pc >= self.instructions.len() {
            self.halted = true;
            return Ok(false);
        }

        let instr = self.instructions[self.pc].clone();
        let next_pc = self.pc + 1;

        match instr.opcode {
            Op::Goto => {
                self.pc = instr.p2 as usize;
                return Ok(true);
            }
            Op::Halt => {
                self.halted = true;
                return Ok(false);
            }
            Op::Noop => {
                // Do nothing
            }
            Op::Integer => {
                self.set_memory(instr.p1 as usize, Value::integer(instr.p2 as i64));
            }
            Op::Null => {
                self.set_memory(instr.p1 as usize, Value::Null);
            }
            Op::Add => {
                let a = self.get_memory(instr.p2 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(0);
                let b = self.get_memory(instr.p3 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(0);
                self.set_memory(instr.p1 as usize, Value::integer(a + b));
            }
            Op::Subtract => {
                let a = self.get_memory(instr.p2 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(0);
                let b = self.get_memory(instr.p3 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(0);
                self.set_memory(instr.p1 as usize, Value::integer(a - b));
            }
            Op::Multiply => {
                let a = self.get_memory(instr.p2 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(0);
                let b = self.get_memory(instr.p3 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(0);
                self.set_memory(instr.p1 as usize, Value::integer(a * b));
            }
            Op::Divide => {
                let a = self.get_memory(instr.p2 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(0);
                let b = self.get_memory(instr.p3 as usize)
                    .and_then(|m| m.value.as_integer())
                    .unwrap_or(1);
                if b == 0 {
                    return Err(Error::sqlite(ErrorCode::Error, "division by zero"));
                }
                self.set_memory(instr.p1 as usize, Value::integer(a / b));
            }
            Op::Copy => {
                if let Some(src) = self.get_memory(instr.p2 as usize) {
                    let value = src.value.clone();
                    self.set_memory(instr.p1 as usize, value);
                }
            }
            Op::ResultRow => {
                self.has_row = true;
                self.pc = next_pc;
                return Ok(true);
            }
            Op::Transaction => {
                // Start transaction
            }
            Op::AutoCommit => {
                // Commit/rollback transaction
            }
            _ => {
                // Unknown opcode, skip
            }
        }

        self.pc = next_pc;
        Ok(!self.halted)
    }

    /// Reset the program
    pub fn reset(&mut self) {
        self.pc = 0;
        self.halted = false;
        self.has_row = false;
        self.memory.clear();
    }

    /// Check if row is available
    pub fn has_row(&self) -> bool {
        self.has_row
    }

    /// Get last rowid
    pub fn last_rowid(&self) -> i64 {
        self.last_rowid
    }

    /// Get changes count
    pub fn changes(&self) -> u64 {
        self.changes
    }

    /// Check if program is done
    pub fn is_done(&self) -> bool {
        self.halted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vdbe_new() {
        let vdbe = Vdbe::new();
        assert!(vdbe.instructions().is_empty());
        assert!(!vdbe.has_row());
    }

    #[test]
    fn test_vdbe_add_op() {
        let mut vdbe = Vdbe::new();
        vdbe.add_op(Op::Integer);
        assert_eq!(vdbe.instructions().len(), 1);
    }

    #[test]
    fn test_vdbe_halt() {
        let mut vdbe = Vdbe::new();
        vdbe.add_op(Op::Halt);

        let result = vdbe.step().unwrap();
        assert!(!result);
        assert!(vdbe.is_done());
    }

    #[test]
    fn test_vdbe_goto() {
        let mut vdbe = Vdbe::new();
        vdbe.add_op(Op::Integer);
        vdbe.add_op(Op::Goto);
        vdbe.instructions[1].p2 = 0;

        assert_eq!(vdbe.instructions().len(), 2);
    }
}
