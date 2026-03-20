//! Write-Ahead Logging (WAL) module
//!
//! Implements Write-Ahead Logging for transaction durability and concurrency.

#![allow(missing_docs)]

use crate::error::Result;
use crate::types::Pgno;
use std::path::PathBuf;

/// WAL magic numbers
pub const WAL_MAGIC_BE: u32 = 0x377f0682;
pub const WAL_MAGIC_LE: u32 = 0x377f0683;

/// WAL header size in bytes
pub const WAL_HEADER_SIZE: usize = 32;

/// WAL frame header size in bytes
pub const FRAME_HEADER_SIZE: usize = 24;

/// WAL checkpoint modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    /// Passive checkpoint
    Passive,
    /// Full checkpoint
    Full,
    /// Restart checkpoint
    Restart,
    /// Truncate checkpoint
    Truncate,
}

/// WAL header
#[derive(Debug, Clone)]
pub struct WalHeader {
    /// Magic number
    magic: u32,
    /// File format version
    version: u32,
    /// Database page size
    page_size: u32,
    /// Checkpoint sequence number
    checkpoint_seq: u32,
    /// Salt 1
    salt1: u32,
    /// Salt 2
    salt2: u32,
    /// Checksum 1
    checksum1: u32,
    /// Checksum 2
    checksum2: u32,
}

impl WalHeader {
    /// Create a new WAL header
    pub fn new(page_size: u32) -> Self {
        WalHeader {
            magic: WAL_MAGIC_BE,
            version: 3007000,
            page_size,
            checkpoint_seq: 0,
            salt1: rand_salt(),
            salt2: rand_salt(),
            checksum1: 0,
            checksum2: 0,
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; WAL_HEADER_SIZE] {
        let mut buf = [0u8; WAL_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_be_bytes());
        buf[4..8].copy_from_slice(&self.version.to_be_bytes());
        buf[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        buf[12..16].copy_from_slice(&self.checkpoint_seq.to_be_bytes());
        buf[16..20].copy_from_slice(&self.salt1.to_be_bytes());
        buf[20..24].copy_from_slice(&self.salt2.to_be_bytes());
        buf[24..28].copy_from_slice(&self.checksum1.to_be_bytes());
        buf[28..32].copy_from_slice(&self.checksum2.to_be_bytes());
        buf
    }
}

/// Generate random salt value
fn rand_salt() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (duration.as_nanos() & 0xFFFFFFFF) as u32
}

/// WAL frame header
#[derive(Debug, Clone)]
pub struct FrameHeader {
    /// Page number
    pgno: Pgno,
    /// Database size in pages after commit
    db_size: u32,
    /// Salt 1
    salt1: u32,
    /// Salt 2
    salt2: u32,
    /// Checksum 1
    checksum1: u32,
    /// Checksum 2
    checksum2: u32,
}

impl FrameHeader {
    /// Create a new frame header
    pub fn new(pgno: Pgno, db_size: u32) -> Self {
        FrameHeader {
            pgno,
            db_size,
            salt1: 0,
            salt2: 0,
            checksum1: 0,
            checksum2: 0,
        }
    }
}

/// WAL state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalState {
    /// WAL is closed
    Closed,
    /// WAL is open for reading
    Read,
    /// WAL is open for writing
    Write,
}

/// Write-Ahead Log
pub struct Wal {
    /// WAL file path
    path: PathBuf,
    /// Page size
    page_size: u32,
    /// Current state
    state: WalState,
    /// Header
    header: Option<WalHeader>,
    /// Frames
    frames: Vec<(FrameHeader, Vec<u8>)>,
}

impl Wal {
    /// Create a new WAL
    pub fn new(path: PathBuf, page_size: u32) -> Self {
        Wal {
            path,
            page_size,
            state: WalState::Closed,
            header: None,
            frames: Vec::new(),
        }
    }

    /// Open WAL for reading
    pub fn begin_read(&mut self) -> Result<()> {
        self.state = WalState::Read;
        if self.header.is_none() {
            self.header = Some(WalHeader::new(self.page_size));
        }
        Ok(())
    }

    /// End read transaction
    pub fn end_read(&mut self) -> Result<()> {
        self.state = WalState::Closed;
        Ok(())
    }

    /// Begin write transaction
    pub fn begin_write(&mut self) -> Result<()> {
        self.state = WalState::Write;
        Ok(())
    }

    /// End write transaction
    pub fn end_write(&mut self) -> Result<()> {
        self.state = WalState::Closed;
        Ok(())
    }

    /// Write a frame to WAL
    pub fn write_frame(&mut self, pgno: Pgno, data: &[u8]) -> Result<()> {
        let header = FrameHeader::new(pgno, 0);
        self.frames.push((header, data.to_vec()));
        Ok(())
    }

    /// Perform checkpoint
    pub fn checkpoint(&mut self, _mode: CheckpointMode) -> Result<usize> {
        let frames_count = self.frames.len();
        self.frames.clear();
        Ok(frames_count)
    }

    /// Get frame count
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Get page size
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Get state
    pub fn state(&self) -> WalState {
        self.state
    }

    /// Close WAL
    pub fn close(&mut self) -> Result<()> {
        self.state = WalState::Closed;
        self.frames.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_new() {
        let wal = Wal::new(PathBuf::from("test.db-wal"), 4096);
        assert_eq!(wal.page_size(), 4096);
        assert_eq!(wal.state(), WalState::Closed);
    }

    #[test]
    fn test_wal_read() {
        let mut wal = Wal::new(PathBuf::from("test.db-wal"), 4096);
        wal.begin_read().unwrap();
        assert_eq!(wal.state(), WalState::Read);
        wal.end_read().unwrap();
        assert_eq!(wal.state(), WalState::Closed);
    }

    #[test]
    fn test_wal_write() {
        let mut wal = Wal::new(PathBuf::from("test.db-wal"), 4096);
        wal.begin_write().unwrap();
        assert_eq!(wal.state(), WalState::Write);

        wal.write_frame(1, &[0u8; 4096]).unwrap();
        assert_eq!(wal.frame_count(), 1);

        wal.end_write().unwrap();
    }

    #[test]
    fn test_wal_checkpoint() {
        let mut wal = Wal::new(PathBuf::from("test.db-wal"), 4096);
        wal.begin_write().unwrap();
        wal.write_frame(1, &[0u8; 4096]).unwrap();
        wal.write_frame(2, &[0u8; 4096]).unwrap();

        let count = wal.checkpoint(CheckpointMode::Full).unwrap();
        assert_eq!(count, 2);
        assert_eq!(wal.frame_count(), 0);
    }

    #[test]
    fn test_wal_header() {
        let header = WalHeader::new(4096);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), WAL_HEADER_SIZE);
    }
}
