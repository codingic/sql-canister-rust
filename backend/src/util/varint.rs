//! Variable-length integer encoding (varint)
//!
//! SQLite uses a variable-length encoding for integers in the database format.
//! This module provides encoding and decoding functions.

use crate::error::{Error, Result};

/// Maximum number of bytes in a varint
pub const MAX_VARINT_SIZE: usize = 9;

/// Encode a 64-bit unsigned integer as a varint
///
/// Returns the number of bytes written to the buffer.
///
/// # Format (SQLite style)
/// - First 8 bytes: MSB = 1 (continuation), lower 7 bits = data
/// - 9th byte (if needed): all 8 bits = data
pub fn encode_varint(mut value: u64, buf: &mut [u8]) -> usize {
    if value == 0 {
        buf[0] = 0;
        return 1;
    }

    // Check if value fits in 8 bytes (56 bits)
    if value < (1u64 << 56) {
        // Value fits in at most 8 bytes
        let mut len = 0;
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80; // Set continuation bit
            }
            buf[len] = byte;
            len += 1;
            if value == 0 {
                break;
            }
        }
        len
    } else {
        // Value needs 9 bytes
        // First 8 bytes: 7 bits each with continuation bit set
        for i in 0..8 {
            buf[i] = ((value >> (7 * i)) as u8 & 0x7F) | 0x80;
        }
        // 9th byte: all 8 bits of the high byte
        buf[8] = (value >> 56) as u8;
        9
    }
}

/// Encode a 64-bit unsigned integer as a varint into a Vec
pub fn encode_varint_vec(value: u64) -> Vec<u8> {
    let mut buf = vec![0u8; MAX_VARINT_SIZE];
    let len = encode_varint(value, &mut buf);
    buf.truncate(len);
    buf
}

/// Decode a varint from a byte slice
///
/// Returns the decoded value and the number of bytes consumed.
pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize)> {
    if buf.is_empty() {
        return Err(Error::corrupt("empty buffer for varint decode"));
    }

    let mut result: u64 = 0;
    let mut i = 0;

    for _ in 0..8 {
        if i >= buf.len() {
            return Err(Error::corrupt("incomplete varint"));
        }

        let byte = buf[i];
        i += 1;

        if byte & 0x80 == 0 {
            result |= (byte as u64) << (7 * (i - 1));
            return Ok((result, i));
        }

        result |= ((byte & 0x7F) as u64) << (7 * (i - 1));
    }

    // 9th byte uses all 8 bits
    if i >= buf.len() {
        return Err(Error::corrupt("incomplete varint"));
    }

    result |= (buf[i] as u64) << 56;
    Ok((result, 9))
}

/// Encode a 64-bit signed integer as a varint
///
/// This uses zigzag encoding to handle negative numbers efficiently.
pub fn encode_signed_varint(value: i64, buf: &mut [u8]) -> usize {
    // Zigzag encoding: (n << 1) ^ (n >> 63)
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zigzag, buf)
}

/// Decode a signed varint
pub fn decode_signed_varint(buf: &[u8]) -> Result<(i64, usize)> {
    let (zigzag, len) = decode_varint(buf)?;
    // Reverse zigzag: (n >> 1) ^ -(n & 1)
    let value = ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64));
    Ok((value, len))
}

/// Get the size of a varint without encoding it
pub fn varint_size(value: u64) -> usize {
    if value == 0 {
        return 1;
    }

    let mut size = 0;
    let mut v = value;

    for i in 0..8 {
        if v == 0 {
            break;
        }
        size += 1;
        v >>= 7;
    }

    if v != 0 {
        size = 9;
    }

    size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_varint_zero() {
        let mut buf = [0u8; MAX_VARINT_SIZE];
        let len = encode_varint(0, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0);

        let (value, decoded_len) = decode_varint(&buf[..len]).unwrap();
        assert_eq!(value, 0);
        assert_eq!(decoded_len, 1);
    }

    #[test]
    fn test_encode_decode_varint_small() {
        for i in 0..=127u64 {
            let mut buf = [0u8; MAX_VARINT_SIZE];
            let len = encode_varint(i, &mut buf);
            assert_eq!(len, 1);
            assert!(buf[0] <= 127);

            let (value, decoded_len) = decode_varint(&buf[..len]).unwrap();
            assert_eq!(value, i);
            assert_eq!(decoded_len, 1);
        }
    }

    #[test]
    fn test_encode_decode_varint_medium() {
        let test_values = [
            128u64, 255, 256, 127, 128, 16383, 16384, 16385,
            65535, 65536, 1000000, 16777215, 16777216,
        ];

        for &value in &test_values {
            let mut buf = [0u8; MAX_VARINT_SIZE];
            let len = encode_varint(value, &mut buf);

            let (decoded, decoded_len) = decode_varint(&buf[..len]).unwrap();
            assert_eq!(decoded, value, "Failed for value {}", value);
            assert_eq!(decoded_len, len);
        }
    }

    #[test]
    fn test_encode_decode_varint_large() {
        let test_values = [
            u32::MAX as u64,
            (u32::MAX as u64) + 1,
            u64::MAX / 2,
            u64::MAX - 1,
            u64::MAX,
        ];

        for &value in &test_values {
            let mut buf = [0u8; MAX_VARINT_SIZE];
            let len = encode_varint(value, &mut buf);

            let (decoded, decoded_len) = decode_varint(&buf[..len]).unwrap();
            assert_eq!(decoded, value, "Failed for value {}", value);
            assert_eq!(decoded_len, len);
        }
    }

    #[test]
    fn test_varint_size() {
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(1), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(varint_size(16383), 2);
        assert_eq!(varint_size(16384), 3);
        assert_eq!(varint_size(2097151), 3);
        assert_eq!(varint_size(2097152), 4);
    }

    #[test]
    fn test_encode_decode_signed() {
        let test_values = [
            0i64, 1, -1, 127, -127, 128, -128,
            16383, -16383, 16384, -16384,
            i32::MAX as i64, i32::MIN as i64,
            i64::MAX, i64::MIN,
        ];

        for &value in &test_values {
            let mut buf = [0u8; MAX_VARINT_SIZE];
            let len = encode_signed_varint(value, &mut buf);

            let (decoded, decoded_len) = decode_signed_varint(&buf[..len]).unwrap();
            assert_eq!(decoded, value, "Failed for value {}", value);
            assert_eq!(decoded_len, len);
        }
    }

    #[test]
    fn test_encode_varint_vec() {
        let v = encode_varint_vec(42);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0], 42);

        let v = encode_varint_vec(128);
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn test_decode_varint_empty() {
        let result = decode_varint(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_varint_incomplete() {
        let result = decode_varint(&[0x80]);
        assert!(result.is_err());
    }

    #[test]
    fn test_varint_roundtrip() {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        for _ in 0..1000 {
            let value = rng.gen::<u64>();
            let mut buf = [0u8; MAX_VARINT_SIZE];
            let len = encode_varint(value, &mut buf);

            let (decoded, decoded_len) = decode_varint(&buf[..len]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(decoded_len, len);
        }
    }
}
