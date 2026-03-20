//! Virtual File System (VFS) module
//!
//! Abstracts operating system differences through a virtual file system interface.

use crate::error::{Error, ErrorCode, Result};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};

/// VFS file open flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlags {
    /// Read access
    pub read: bool,
    /// Write access
    pub write: bool,
    /// Create if not exists
    pub create: bool,
    /// Create exclusively
    pub exclusive: bool,
    /// Delete on close
    pub delete_on_close: bool,
    /// Append only
    pub append: bool,
}

impl Default for OpenFlags {
    fn default() -> Self {
        OpenFlags {
            read: true,
            write: true,
            create: true,
            exclusive: false,
            delete_on_close: false,
            append: false,
        }
    }
}

/// VFS file lock levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockLevel {
    /// No lock
    None,
    /// Shared lock (read)
    Shared,
    /// Reserved lock (intent to write)
    Reserved,
    /// Pending lock
    Pending,
    /// Exclusive lock (write)
    Exclusive,
}

impl Default for LockLevel {
    fn default() -> Self {
        LockLevel::None
    }
}

/// VFS file
pub struct VfsFile {
    /// File path
    path: PathBuf,
    /// Open flags
    flags: OpenFlags,
    /// Current lock level
    lock_level: LockLevel,
    /// File handle (simplified)
    content: Vec<u8>,
    /// Current position
    position: u64,
}

impl VfsFile {
    /// Create a new VFS file
    pub fn new(path: PathBuf, flags: OpenFlags) -> Self {
        VfsFile {
            path,
            flags,
            lock_level: LockLevel::None,
            content: Vec::new(),
            position: 0,
        }
    }

    /// Read from file
    pub fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let offset = offset as usize;
        if offset >= self.content.len() {
            return Ok(0);
        }

        let available = self.content.len() - offset;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&self.content[offset..offset + to_read]);
        self.position = offset as u64 + to_read as u64;
        Ok(to_read)
    }

    /// Write to file
    pub fn write(&mut self, buf: &[u8], offset: u64) -> Result<usize> {
        let offset = offset as usize;

        // Extend file if necessary
        if offset + buf.len() > self.content.len() {
            self.content.resize(offset + buf.len(), 0);
        }

        self.content[offset..offset + buf.len()].copy_from_slice(buf);
        self.position = offset as u64 + buf.len() as u64;
        Ok(buf.len())
    }

    /// Sync file to disk
    pub fn sync(&self) -> Result<()> {
        // In-memory implementation, nothing to sync
        Ok(())
    }

    /// Truncate file
    pub fn truncate(&mut self, size: u64) -> Result<()> {
        self.content.truncate(size as usize);
        Ok(())
    }

    /// Get file size
    pub fn size(&self) -> u64 {
        self.content.len() as u64
    }

    /// Lock file
    pub fn lock(&mut self, level: LockLevel) -> Result<()> {
        self.lock_level = level;
        Ok(())
    }

    /// Unlock file
    pub fn unlock(&mut self, level: LockLevel) -> Result<()> {
        self.lock_level = level;
        Ok(())
    }

    /// Check if file exists
    pub fn exists(&self) -> bool {
        !self.content.is_empty()
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get lock level
    pub fn lock_level(&self) -> LockLevel {
        self.lock_level
    }
}

/// VFS implementation
pub struct Vfs {
    /// VFS name
    name: String,
    /// Maximum pathname length
    max_pathname: usize,
}

impl Vfs {
    /// Create a new VFS
    pub fn new(name: &str) -> Self {
        Vfs {
            name: name.to_string(),
            max_pathname: 1024,
        }
    }

    /// Get VFS name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Open a file
    pub fn open(&self, path: &Path, flags: OpenFlags) -> Result<VfsFile> {
        Ok(VfsFile::new(path.to_path_buf(), flags))
    }

    /// Delete a file
    pub fn delete(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    /// Check if file exists
    pub fn access(&self, _path: &Path) -> bool {
        true
    }

    /// Get full pathname
    pub fn full_pathname(&self, path: &Path) -> Result<PathBuf> {
        Ok(path.to_path_buf())
    }

    /// Get maximum pathname length
    pub fn max_pathname(&self) -> usize {
        self.max_pathname
    }
}

impl Default for Vfs {
    fn default() -> Self {
        Self::new("default")
    }
}

/// Operating system backed VFS file
pub struct OsVfsFile {
    /// File path
    path: PathBuf,
    /// File handle
    file: File,
    /// Open flags
    flags: OpenFlags,
    /// Current lock level
    lock_level: LockLevel,
}

impl OsVfsFile {
    /// Open a file with the given flags
    pub fn open(path: &Path, flags: OpenFlags) -> Result<Self> {
        let mut options = OpenOptions::new();

        if flags.read && flags.write {
            options.read(true).write(true);
        } else if flags.read {
            options.read(true);
        } else if flags.write {
            options.write(true);
        }

        if flags.create {
            options.create(true);
        }

        if flags.exclusive {
            options.create_new(true);
        }

        if flags.append {
            options.append(true);
        }

        let file = options.open(path)
            .map_err(|e| Error::sqlite(ErrorCode::CantOpen, &format!("cannot open file '{}': {}", path.display(), e)))?;

        Ok(OsVfsFile {
            path: path.to_path_buf(),
            file,
            flags,
            lock_level: LockLevel::None,
        })
    }

    /// Read from file at offset
    pub fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("seek error: {}", e)))?;
        self.file.read(buf)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("read error: {}", e)))
    }

    /// Write to file at offset
    pub fn write(&mut self, buf: &[u8], offset: u64) -> Result<usize> {
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("seek error: {}", e)))?;
        self.file.write(buf)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("write error: {}", e)))
    }

    /// Sync file to disk
    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("sync error: {}", e)))
    }

    /// Sync file data only (no metadata)
    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data()
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("sync_data error: {}", e)))
    }

    /// Truncate file
    pub fn truncate(&mut self, size: u64) -> Result<()> {
        self.file.set_len(size)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("truncate error: {}", e)))
    }

    /// Get file size
    pub fn size(&self) -> Result<u64> {
        let metadata = self.file.metadata()
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("metadata error: {}", e)))?;
        Ok(metadata.len())
    }

    /// Lock file
    pub fn lock(&mut self, level: LockLevel) -> Result<()> {
        // Simplified locking - in a real implementation, use file locking
        self.lock_level = level;
        Ok(())
    }

    /// Unlock file
    pub fn unlock(&mut self, level: LockLevel) -> Result<()> {
        self.lock_level = level;
        Ok(())
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get lock level
    pub fn lock_level(&self) -> LockLevel {
        self.lock_level
    }
}

/// OsVfs - VFS that uses real file system operations
pub struct OsVfs {
    /// VFS name
    name: String,
    /// Maximum pathname length
    max_pathname: usize,
}

impl OsVfs {
    /// Create a new OS VFS
    pub fn new(name: &str) -> Self {
        OsVfs {
            name: name.to_string(),
            max_pathname: 4096, // Typical max path length
        }
    }

    /// Get VFS name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Open a file
    pub fn open(&self, path: &Path, flags: OpenFlags) -> Result<OsVfsFile> {
        OsVfsFile::open(path, flags)
    }

    /// Delete a file
    pub fn delete(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path)
            .map_err(|e| Error::sqlite(ErrorCode::IoErr, &format!("delete error: {}", e)))
    }

    /// Check if file exists
    pub fn access(&self, path: &Path) -> bool {
        path.exists()
    }

    /// Get full pathname
    pub fn full_pathname(&self, path: &Path) -> Result<PathBuf> {
        path.canonicalize()
            .map_err(|e| Error::sqlite(ErrorCode::CantOpen, &format!("cannot resolve path: {}", e)))
    }

    /// Get maximum pathname length
    pub fn max_pathname(&self) -> usize {
        self.max_pathname
    }
}

impl Default for OsVfs {
    fn default() -> Self {
        Self::new("os-default")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vfs_new() {
        let vfs = Vfs::default();
        assert_eq!(vfs.name(), "default");
    }

    #[test]
    fn test_vfs_file_read_write() {
        let vfs = Vfs::default();
        let mut file = vfs.open(Path::new("test.db"), OpenFlags::default()).unwrap();

        // Write
        let written = file.write(b"hello", 0).unwrap();
        assert_eq!(written, 5);

        // Read
        let mut buf = [0u8; 5];
        let read = file.read(&mut buf, 0).unwrap();
        assert_eq!(read, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_vfs_file_truncate() {
        let vfs = Vfs::default();
        let mut file = vfs.open(Path::new("test.db"), OpenFlags::default()).unwrap();

        file.write(b"sample text", 0).unwrap();
        assert_eq!(file.size(), 11);

        file.truncate(5).unwrap();
        assert_eq!(file.size(), 5);
    }

    #[test]
    fn test_vfs_file_lock() {
        let vfs = Vfs::default();
        let mut file = vfs.open(Path::new("test.db"), OpenFlags::default()).unwrap();

        assert_eq!(file.lock_level(), LockLevel::None);

        file.lock(LockLevel::Exclusive).unwrap();
        assert_eq!(file.lock_level(), LockLevel::Exclusive);

        file.unlock(LockLevel::None).unwrap();
        assert_eq!(file.lock_level(), LockLevel::None);
    }
}
