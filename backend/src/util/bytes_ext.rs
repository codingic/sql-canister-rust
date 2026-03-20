//! Byte manipulation utilities


/// Read a big-endian 16-bit unsigned integer
#[inline]
pub fn read_be_u16(buf: &[u8]) -> u16 {
    u16::from_be_bytes([buf[0], buf[1]])
}

/// Read a big-endian 32-bit unsigned integer
#[inline]
pub fn read_be_u32(buf: &[u8]) -> u32 {
    u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])
}

/// Read a big-endian 64-bit unsigned integer
#[inline]
pub fn read_be_u64(buf: &[u8]) -> u64 {
    u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3],
        buf[4], buf[5], buf[6], buf[7],
    ])
}

/// Write a big-endian 16-bit unsigned integer
#[inline]
pub fn write_be_u16(buf: &mut [u8], value: u16) {
    let bytes = value.to_be_bytes();
    buf[0] = bytes[0];
    buf[1] = bytes[1];
}

/// Write a big-endian 32-bit unsigned integer
#[inline]
pub fn write_be_u32(buf: &mut [u8], value: u32) {
    let bytes = value.to_be_bytes();
    buf[0] = bytes[0];
    buf[1] = bytes[1];
    buf[2] = bytes[2];
    buf[3] = bytes[3];
}

/// Write a big-endian 64-bit unsigned integer
#[inline]
pub fn write_be_u64(buf: &mut [u8], value: u64) {
    let bytes = value.to_be_bytes();
    buf.copy_from_slice(&bytes);
}

/// Read a big-endian 24-bit signed integer
#[inline]
pub fn read_be_i24(buf: &[u8]) -> i32 {
    let b0 = buf[0] as i32;
    let b1 = buf[1] as i32;
    let b2 = buf[2] as i32;

    // Sign extend if the high bit is set
    let result = (b0 << 16) | (b1 << 8) | b2;
    if result & 0x0080_0000 != 0 {
        result | 0xFF00_0000u32 as i32
    } else {
        result
    }
}

/// Write a big-endian 24-bit signed integer
#[inline]
pub fn write_be_i24(buf: &mut [u8], value: i32) {
    buf[0] = ((value >> 16) & 0xFF) as u8;
    buf[1] = ((value >> 8) & 0xFF) as u8;
    buf[2] = (value & 0xFF) as u8;
}

/// Read a big-endian 48-bit signed integer
#[inline]
pub fn read_be_i48(buf: &[u8]) -> i64 {
    let result = ((buf[0] as i64) << 40)
        | ((buf[1] as i64) << 32)
        | ((buf[2] as i64) << 24)
        | ((buf[3] as i64) << 16)
        | ((buf[4] as i64) << 8)
        | (buf[5] as i64);

    // Sign extend if the high bit is set
    if result & 0x0000_8000_0000_0000 != 0 {
        result | 0xFFFF_0000_0000_0000u64 as i64
    } else {
        result
    }
}

/// Write a big-endian 48-bit signed integer
#[inline]
pub fn write_be_i48(buf: &mut [u8], value: i64) {
    buf[0] = ((value >> 40) & 0xFF) as u8;
    buf[1] = ((value >> 32) & 0xFF) as u8;
    buf[2] = ((value >> 24) & 0xFF) as u8;
    buf[3] = ((value >> 16) & 0xFF) as u8;
    buf[4] = ((value >> 8) & 0xFF) as u8;
    buf[5] = (value & 0xFF) as u8;
}

/// Read a big-endian 64-bit floating point number
#[inline]
pub fn read_be_f64(buf: &[u8]) -> f64 {
    f64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3],
        buf[4], buf[5], buf[6], buf[7],
    ])
}

/// Write a big-endian 64-bit floating point number
#[inline]
pub fn write_be_f64(buf: &mut [u8], value: f64) {
    let bytes = value.to_be_bytes();
    buf.copy_from_slice(&bytes);
}

/// Byte buffer extension trait
pub trait ByteBuffer {
    /// Read a u8 at the given position
    fn read_u8(&self, pos: usize) -> u8;

    /// Read a u16 at the given position (big-endian)
    fn read_u16(&self, pos: usize) -> u16;

    /// Read a u32 at the given position (big-endian)
    fn read_u32(&self, pos: usize) -> u32;

    /// Read a u64 at the given position (big-endian)
    fn read_u64(&self, pos: usize) -> u64;

    /// Read an i64 at the given position (big-endian)
    fn read_i64(&self, pos: usize) -> i64;

    /// Read an f64 at the given position (big-endian)
    fn read_f64(&self, pos: usize) -> f64;

    /// Write a u8 at the given position
    fn write_u8(&mut self, pos: usize, value: u8);

    /// Write a u16 at the given position (big-endian)
    fn write_u16(&mut self, pos: usize, value: u16);

    /// Write a u32 at the given position (big-endian)
    fn write_u32(&mut self, pos: usize, value: u32);

    /// Write a u64 at the given position (big-endian)
    fn write_u64(&mut self, pos: usize, value: u64);

    /// Write an i64 at the given position (big-endian)
    fn write_i64(&mut self, pos: usize, value: i64);

    /// Write an f64 at the given position (big-endian)
    fn write_f64(&mut self, pos: usize, value: f64);
}

impl ByteBuffer for [u8] {
    #[inline]
    fn read_u8(&self, pos: usize) -> u8 {
        self[pos]
    }

    #[inline]
    fn read_u16(&self, pos: usize) -> u16 {
        read_be_u16(&self[pos..pos + 2])
    }

    #[inline]
    fn read_u32(&self, pos: usize) -> u32 {
        read_be_u32(&self[pos..pos + 4])
    }

    #[inline]
    fn read_u64(&self, pos: usize) -> u64 {
        read_be_u64(&self[pos..pos + 8])
    }

    #[inline]
    fn read_i64(&self, pos: usize) -> i64 {
        self.read_u64(pos) as i64
    }

    #[inline]
    fn read_f64(&self, pos: usize) -> f64 {
        read_be_f64(&self[pos..pos + 8])
    }

    #[inline]
    fn write_u8(&mut self, pos: usize, value: u8) {
        self[pos] = value;
    }

    #[inline]
    fn write_u16(&mut self, pos: usize, value: u16) {
        write_be_u16(&mut self[pos..pos + 2], value);
    }

    #[inline]
    fn write_u32(&mut self, pos: usize, value: u32) {
        write_be_u32(&mut self[pos..pos + 4], value);
    }

    #[inline]
    fn write_u64(&mut self, pos: usize, value: u64) {
        write_be_u64(&mut self[pos..pos + 8], value);
    }

    #[inline]
    fn write_i64(&mut self, pos: usize, value: i64) {
        self.write_u64(pos, value as u64);
    }

    #[inline]
    fn write_f64(&mut self, pos: usize, value: f64) {
        write_be_f64(&mut self[pos..pos + 8], value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_write_be_u16() {
        let mut buf = [0u8; 2];
        write_be_u16(&mut buf, 0x1234);
        assert_eq!(buf, [0x12, 0x34]);
        assert_eq!(read_be_u16(&buf), 0x1234);
    }

    #[test]
    fn test_read_write_be_u32() {
        let mut buf = [0u8; 4];
        write_be_u32(&mut buf, 0x12345678);
        assert_eq!(buf, [0x12, 0x34, 0x56, 0x78]);
        assert_eq!(read_be_u32(&buf), 0x12345678);
    }

    #[test]
    fn test_read_write_be_u64() {
        let mut buf = [0u8; 8];
        write_be_u64(&mut buf, 0x0123_4567_89AB_CDEF);
        assert_eq!(buf, [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
        assert_eq!(read_be_u64(&buf), 0x0123_4567_89AB_CDEF);
    }

    #[test]
    fn test_read_write_be_i24() {
        let mut buf = [0u8; 3];

        // Positive value
        write_be_i24(&mut buf, 0x123456);
        assert_eq!(buf, [0x12, 0x34, 0x56]);
        assert_eq!(read_be_i24(&buf), 0x123456);

        // Negative value
        write_be_i24(&mut buf, -1);
        assert_eq!(buf, [0xFF, 0xFF, 0xFF]);
        assert_eq!(read_be_i24(&buf), -1);

        // Max positive
        write_be_i24(&mut buf, 0x7FFFFF);
        assert_eq!(read_be_i24(&buf), 0x7FFFFF);

        // Min negative
        write_be_i24(&mut buf, -0x800000);
        assert_eq!(read_be_i24(&buf), -0x800000);
    }

    #[test]
    fn test_read_write_be_i48() {
        let mut buf = [0u8; 6];

        // Positive value
        write_be_i48(&mut buf, 0x123456789ABC);
        assert_eq!(read_be_i48(&buf), 0x123456789ABC);

        // Negative value
        write_be_i48(&mut buf, -1);
        assert_eq!(read_be_i48(&buf), -1);
    }

    #[test]
    fn test_read_write_be_f64() {
        let mut buf = [0u8; 8];
        let value = 3.141592653589793;
        write_be_f64(&mut buf, value);
        let result = read_be_f64(&buf);
        assert!((result - value).abs() < 1e-15);
    }

    #[test]
    fn test_byte_buffer_trait() {
        let mut buf = vec![0u8; 64];

        buf.write_u8(0, 42);
        assert_eq!(buf.read_u8(0), 42);

        buf.write_u16(2, 0x1234);
        assert_eq!(buf.read_u16(2), 0x1234);

        buf.write_u32(4, 0x12345678);
        assert_eq!(buf.read_u32(4), 0x12345678);

        buf.write_u64(8, 0x0123_4567_89AB_CDEF);
        assert_eq!(buf.read_u64(8), 0x0123_4567_89AB_CDEF);

        buf.write_i64(16, -12345);
        assert_eq!(buf.read_i64(16), -12345);

        buf.write_f64(24, std::f64::consts::PI);
        assert!((buf.read_f64(24) - std::f64::consts::PI).abs() < 1e-15);
    }
}
