//! Hash table implementation for SQLite

use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Simple hash function for strings (used for keyword lookup)
pub fn hash_string(s: &str) -> u32 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish() as u32
}

/// Case-insensitive hash for SQL identifiers
pub fn hash_identifier(s: &str) -> u32 {
    let mut hasher = DefaultHasher::new();
    for c in s.chars() {
        c.to_ascii_uppercase().hash(&mut hasher);
    }
    hasher.finish() as u32
}

/// Simple multiplicative hash for integers
pub fn hash_int(n: u64) -> u32 {
    const PRIME: u64 = 0x51_7C_C1_B7_27_12_20_A5;
    (n.wrapping_mul(PRIME) >> 32) as u32
}

/// Hash combiner for multiple values
pub fn combine_hashes(a: u32, b: u32) -> u32 {
    a ^ (b.wrapping_add(0x9e37_79b9).wrapping_add(a << 6).wrapping_add(a >> 2))
}

/// A simple hash table with linear probing
#[derive(Debug, Clone)]
pub struct SimpleHashTable<K, V> {
    entries: Vec<Option<(K, V)>>,
    size: usize,
}

impl<K, V> SimpleHashTable<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Create a new hash table with the given capacity
    pub fn new(capacity: usize) -> Self {
        SimpleHashTable {
            entries: vec![None; capacity],
            size: 0,
        }
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.size * 2 >= self.entries.len() {
            self.resize(self.entries.len() * 2);
        }

        let hash = hash_key(&key);
        let index = self.find_slot(&key, hash);

        match &mut self.entries[index] {
            Some((_, v)) => {
                let old = std::mem::replace(v, value);
                Some(old)
            }
            slot @ None => {
                *slot = Some((key, value));
                self.size += 1;
                None
            }
        }
    }

    /// Get a value by key
    pub fn get(&self, key: &K) -> Option<&V> {
        let hash = hash_key(key);
        let index = self.find_slot(key, hash);

        self.entries[index].as_ref().map(|(_, v)| v)
    }

    /// Remove a key-value pair
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let hash = hash_key(key);
        let index = self.find_slot(key, hash);

        if self.entries[index].is_some() {
            let (_, value) = self.entries[index].take().unwrap();
            self.size -= 1;
            self.rehash_after_removal(index);
            Some(value)
        } else {
            None
        }
    }

    /// Check if the table contains a key
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.size
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        for entry in &mut self.entries {
            *entry = None;
        }
        self.size = 0;
    }

    /// Resize the hash table
    fn resize(&mut self, new_capacity: usize) {
        let old_entries = std::mem::take(&mut self.entries);
        self.entries = vec![None; new_capacity];
        self.size = 0;

        for entry in old_entries {
            if let Some((k, v)) = entry {
                self.insert(k, v);
            }
        }
    }

    /// Find the slot for a key
    fn find_slot(&self, key: &K, hash: u32) -> usize {
        let len = self.entries.len();
        let mut index = (hash as usize) % len;
        let start = index;

        loop {
            match &self.entries[index] {
                Some((k, _)) if k == key => return index,
                None => return index,
                _ => {
                    index = (index + 1) % len;
                    if index == start {
                        panic!("Hash table is full");
                    }
                }
            }
        }
    }

    /// Rehash after removal to maintain linear probing invariants
    fn rehash_after_removal(&mut self, removed_index: usize) {
        let len = self.entries.len();
        let mut current = (removed_index + 1) % len;

        loop {
            if self.entries[current].is_none() {
                break;
            }

            let (key, value) = self.entries[current].clone().unwrap();
            let hash = hash_key(&key);
            let ideal_index = (hash as usize) % len;

            // Check if the entry needs to be moved
            let needs_move = if ideal_index <= removed_index {
                current > removed_index || current < ideal_index
            } else {
                current > removed_index && current < ideal_index
            };

            if needs_move {
                self.entries[removed_index] = Some((key, value));
                self.entries[current] = None;
                self.rehash_after_removal(current);
                break;
            }

            current = (current + 1) % len;
            if current == (removed_index + 1) % len {
                break;
            }
        }
    }
}

/// Hash a key
fn hash_key<K: Hash>(key: &K) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_string() {
        let h1 = hash_string("hello");
        let h2 = hash_string("hello");
        let h3 = hash_string("world");

        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_hash_identifier() {
        let h1 = hash_identifier("table1");
        let h2 = hash_identifier("TABLE1");
        let h3 = hash_identifier("Table1");

        // Case-insensitive
        assert_eq!(h1, h2);
        assert_eq!(h1, h3);
    }

    #[test]
    fn test_hash_int() {
        let h1 = hash_int(1);
        let h2 = hash_int(2);
        let h3 = hash_int(1);

        assert_ne!(h1, h2);
        assert_eq!(h1, h3);
    }

    #[test]
    fn test_combine_hashes() {
        let h1 = hash_string("hello");
        let h2 = hash_string("world");
        let combined = combine_hashes(h1, h2);

        assert_ne!(combined, h1);
        assert_ne!(combined, h2);
    }

    #[test]
    fn test_simple_hash_table_basic() {
        let mut table = SimpleHashTable::new(16);

        table.insert("key1".to_string(), 1);
        table.insert("key2".to_string(), 2);

        assert_eq!(table.get(&"key1".to_string()), Some(&1));
        assert_eq!(table.get(&"key2".to_string()), Some(&2));
        assert_eq!(table.get(&"key3".to_string()), None);
    }

    #[test]
    fn test_simple_hash_table_update() {
        let mut table = SimpleHashTable::new(16);

        table.insert("key".to_string(), 1);
        let old = table.insert("key".to_string(), 2);

        assert_eq!(old, Some(1));
        assert_eq!(table.get(&"key".to_string()), Some(&2));
    }

    #[test]
    fn test_simple_hash_table_remove() {
        let mut table = SimpleHashTable::new(16);

        table.insert("key1".to_string(), 1);
        table.insert("key2".to_string(), 2);

        let removed = table.remove(&"key1".to_string());
        assert_eq!(removed, Some(1));
        assert_eq!(table.get(&"key1".to_string()), None);
        assert_eq!(table.get(&"key2".to_string()), Some(&2));
    }

    #[test]
    fn test_simple_hash_table_resize() {
        let mut table = SimpleHashTable::new(4);

        // Insert more items than initial capacity
        for i in 0..10 {
            table.insert(format!("key{}", i), i);
        }

        for i in 0..10 {
            assert_eq!(table.get(&format!("key{}", i)), Some(&i));
        }
    }

    #[test]
    fn test_simple_hash_table_clear() {
        let mut table = SimpleHashTable::new(16);

        table.insert("key1".to_string(), 1);
        table.insert("key2".to_string(), 2);
        table.clear();

        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_simple_hash_table_collision() {
        let mut table = SimpleHashTable::new(4);

        // Insert multiple keys that may collide
        for i in 0..10 {
            table.insert(i, format!("value{}", i));
        }

        for i in 0..10 {
            assert_eq!(table.get(&i), Some(&format!("value{}", i)));
        }
    }
}
