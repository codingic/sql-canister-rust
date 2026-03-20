//! Checksum calculation utilities

/// Calculate a simple checksum (similar to SQLite's WAL checksum)
///
/// This is a cumulative checksum used in the WAL file format.
pub fn wal_checksum(data: &[u8], initial: (u32, u32)) -> (u32, u32) {
    let (mut s0, mut s1) = initial;

    // Process 8 bytes at a time
    let chunks = data.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let a = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let b = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        s0 = s0.wrapping_add(a).wrapping_add(s1);
        s1 = s1.wrapping_add(b).wrapping_add(s0);
    }

    // Handle remainder (pad with zeros)
    if !remainder.is_empty() {
        let mut a_bytes = [0u8; 4];
        let mut b_bytes = [0u8; 4];
        let len = remainder.len();
        a_bytes[..len.min(4)].copy_from_slice(&remainder[..len.min(4)]);
        if len > 4 {
            b_bytes[..len - 4].copy_from_slice(&remainder[4..]);
        }
        let a = u32::from_le_bytes(a_bytes);
        let b = u32::from_le_bytes(b_bytes);
        s0 = s0.wrapping_add(a).wrapping_add(s1);
        s1 = s1.wrapping_add(b).wrapping_add(s0);
    }

    (s0, s1)
}

/// Simple CRC32 implementation
pub fn crc32(data: &[u8]) -> u32 {
    const CRC32_TABLE: [u32; 256] = generate_crc32_table();

    let mut crc = 0xFFFFFFFFu32;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = CRC32_TABLE[index] ^ (crc >> 8);
    }
    !crc
}

/// Generate CRC32 lookup table at compile time
const fn generate_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Adler-32 checksum
pub fn adler32(data: &[u8]) -> u32 {
    const MOD_ADLER: u32 = 65521;

    let mut a: u32 = 1;
    let mut b: u32 = 0;

    for &byte in data {
        a = (a + byte as u32) % MOD_ADLER;
        b = (b + a) % MOD_ADLER;
    }

    (b << 16) | a
}

/// Simple hash function for page numbers
pub fn page_hash(pgno: u32) -> u32 {
    // FNV-1a hash
    let mut hash: u32 = 2166136261;
    let bytes = pgno.to_le_bytes();
    for &byte in &bytes {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_checksum_empty() {
        let (s0, s1) = wal_checksum(&[], (0, 0));
        assert_eq!(s0, 0);
        assert_eq!(s1, 0);
    }

    #[test]
    fn test_wal_checksum_simple() {
        let data = [1, 2, 3, 4, 5, 6, 7, 8];
        let (s0, s1) = wal_checksum(&data, (0, 0));

        // Verify the checksum is deterministic
        let (s0_2, s1_2) = wal_checksum(&data, (0, 0));
        assert_eq!(s0, s0_2);
        assert_eq!(s1, s1_2);
    }

    #[test]
    fn test_wal_checksum_cumulative() {
        let data1 = [1, 2, 3, 4, 5, 6, 7, 8];
        let data2 = [9, 10, 11, 12, 13, 14, 15, 16];

        let (s0, s1) = wal_checksum(&data1, (0, 0));
        let (s0_c, s1_c) = wal_checksum(&data2, (s0, s1));

        // Combined checksum should differ from individual
        let combined: Vec<u8> = data1.iter().chain(data2.iter()).cloned().collect();
        let (s0_full, s1_full) = wal_checksum(&combined, (0, 0));

        assert_eq!(s0_c, s0_full);
        assert_eq!(s1_c, s1_full);
    }

    #[test]
    fn test_wal_checksum_remainder() {
        // Test with non-multiple of 8 bytes
        let data = [1, 2, 3, 4, 5];
        let (s0, s1) = wal_checksum(&data, (0, 0));
        assert!(s0 != 0 || s1 != 0); // Should have some checksum
    }

    #[test]
    fn test_crc32() {
        // Standard test vectors
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
        assert_eq!(crc32(b""), 0x00000000);
        assert_eq!(crc32(b"a"), 0xE8B7BE43);
    }

    #[test]
    fn test_adler32() {
        assert_eq!(adler32(b"123456789"), 0x091E01DE);
        assert_eq!(adler32(b""), 0x00000001);
        assert_eq!(adler32(b"a"), 0x00620062);
    }

    #[test]
    fn test_page_hash() {
        let h1 = page_hash(1);
        let h2 = page_hash(2);
        let h3 = page_hash(1);

        assert_ne!(h1, h2);
        assert_eq!(h1, h3);
    }

    #[test]
    fn test_crc32_deterministic() {
        let data = b"checksum-test";
        let h1 = crc32(data);
        let h2 = crc32(data);
        assert_eq!(h1, h2);
    }
}
