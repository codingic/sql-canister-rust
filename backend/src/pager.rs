//! Pager module
//!
//! Manages page-level I/O, transactions, and recovery.

use crate::error::Result;
use crate::types::Pgno;
use std::path::PathBuf;

/// Page content
pub type PageContent = Vec<u8>;

/// Database page
#[derive(Debug, Clone)]
pub struct Page {
    /// Page number (1-indexed)
    pub pgno: Pgno,
    /// Page content
    pub content: PageContent,
    /// Is dirty
    pub dirty: bool,
    /// Reference count
    pub ref_count: u32,
}

impl Page {
    /// Create a new page
    pub fn new(pgno: Pgno, size: usize) -> Self {
        Page {
            pgno,
            content: vec![0u8; size],
            dirty: false,
            ref_count: 0,
        }
    }

    /// Get page data
    pub fn data(&self) -> &[u8] {
        &self.content
    }

    /// Get mutable page data
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        &mut self.content
    }
}

/// Pager state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PagerState {
    /// Unlocked
    Unlocked,
    /// Shared lock (read)
    Shared,
    /// Reserved lock (intent to write)
    Reserved,
    /// Pending lock
    Pending,
    /// Exclusive lock (write)
    Exclusive,
}

/// Journal mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// Delete journal after commit
    Delete,
    /// Truncate journal
    Truncate,
    /// Persist journal header
    Persist,
    /// Memory journal
    Memory,
    /// Write-ahead logging
    Wal,
    /// No journal
    Off,
}

impl Default for JournalMode {
    fn default() -> Self {
        JournalMode::Delete
    }
}

/// Pager - manages database pages
pub struct Pager {
    /// Database file path
    db_path: PathBuf,
    /// Page size
    page_size: u32,
    /// Total pages
    total_pages: u32,
    /// Current state
    state: PagerState,
    /// Journal mode
    journal_mode: JournalMode,
    /// Pages in memory
    pages: Vec<Option<Page>>,
}

impl Pager {
    /// Create a new pager
    pub fn new(db_path: PathBuf, page_size: u32) -> Self {
        Pager {
            db_path,
            page_size,
            total_pages: 0,
            state: PagerState::Unlocked,
            journal_mode: JournalMode::Delete,
            pages: Vec::new(),
        }
    }

    /// Get page size
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Get total pages
    pub fn total_pages(&self) -> u32 {
        self.total_pages
    }

    /// Get pager state
    pub fn state(&self) -> PagerState {
        self.state
    }

    /// Get journal mode
    pub fn journal_mode(&self) -> JournalMode {
        self.journal_mode
    }

    /// Set journal mode
    pub fn set_journal_mode(&mut self, mode: JournalMode) {
        self.journal_mode = mode;
    }

    /// Begin read transaction
    pub fn begin_read(&mut self) -> Result<()> {
        self.state = PagerState::Shared;
        Ok(())
    }

    /// Begin write transaction
    pub fn begin_write(&mut self) -> Result<()> {
        self.state = PagerState::Exclusive;
        Ok(())
    }

    /// Commit transaction
    pub fn commit(&mut self) -> Result<()> {
        self.state = PagerState::Unlocked;
        Ok(())
    }

    /// Rollback transaction
    pub fn rollback(&mut self) -> Result<()> {
        self.state = PagerState::Unlocked;
        Ok(())
    }

    /// Get a page
    pub fn get_page(&mut self, pgno: Pgno) -> Result<&Page> {
        let idx = (pgno - 1) as usize;
        if idx >= self.pages.len() {
            self.pages.resize(idx + 1, None);
        }

        if self.pages[idx].is_none() {
            self.pages[idx] = Some(Page::new(pgno, self.page_size as usize));
        }

        Ok(self.pages[idx].as_ref().unwrap())
    }

    /// Get mutable page
    pub fn get_page_mut(&mut self, pgno: Pgno) -> Result<&mut Page> {
        let idx = (pgno - 1) as usize;
        if idx >= self.pages.len() {
            self.pages.resize(idx + 1, None);
        }

        if self.pages[idx].is_none() {
            self.pages[idx] = Some(Page::new(pgno, self.page_size as usize));
        }

        Ok(self.pages[idx].as_mut().unwrap())
    }

    /// Mark page as dirty
    pub fn mark_dirty(&mut self, pgno: Pgno) {
        if let Some(page) = self.pages.get_mut((pgno - 1) as usize) {
            if let Some(p) = page {
                p.dirty = true;
            }
        }
    }

    /// Allocate a new page
    pub fn allocate_page(&mut self) -> Result<Pgno> {
        self.total_pages += 1;
        Ok(self.total_pages)
    }

    /// Sync to disk
    pub fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pager_new() {
        let pager = Pager::new(PathBuf::from("test.db"), 4096);
        assert_eq!(pager.page_size(), 4096);
    }

    #[test]
    fn test_pager_transaction() {
        let mut pager = Pager::new(PathBuf::from("test.db"), 4096);
        pager.begin_read().unwrap();
        assert_eq!(pager.state(), PagerState::Shared);

        pager.commit().unwrap();
        assert_eq!(pager.state(), PagerState::Unlocked);
    }

    #[test]
    fn test_pager_get_page() {
        let mut pager = Pager::new(PathBuf::from("test.db"), 4096);
        let page = pager.get_page(1).unwrap();
        assert_eq!(page.pgno, 1);
    }

    #[test]
    fn test_pager_journal_mode() {
        let mut pager = Pager::new(PathBuf::from("test.db"), 4096);
        pager.set_journal_mode(JournalMode::Wal);
        assert_eq!(pager.journal_mode(), JournalMode::Wal);
    }
}
