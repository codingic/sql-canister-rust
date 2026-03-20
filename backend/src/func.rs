//! Built-in SQL functions
//!
//! Implements scalar, aggregate, and window functions.

use crate::error::Result;
use crate::types::Value;

/// SQL function context
pub struct FunctionContext {
    /// Function name
    pub name: String,
    /// Number of arguments
    pub argc: usize,
    /// Result
    pub result: Option<Value>,
}

impl FunctionContext {
    /// Create a new function context
    pub fn new(name: &str, argc: usize) -> Self {
        FunctionContext {
            name: name.to_string(),
            argc,
            result: None,
        }
    }

    /// Set result
    pub fn set_result(&mut self, value: Value) {
        self.result = Some(value);
    }

    /// Get result
    pub fn result(&self) -> Option<&Value> {
        self.result.as_ref()
    }
}

/// Function implementation
pub type ScalarFunc = fn(&mut FunctionContext, &[Value]) -> Result<()>;

/// Built-in functions
pub struct BuiltinFunctions;

impl BuiltinFunctions {
    /// Get a function by name
    pub fn get(name: &str) -> Option<ScalarFunc> {
        match name.to_uppercase().as_str() {
            "ABS" => Some(Self::abs),
            "LOWER" => Some(Self::lower),
            "UPPER" => Some(Self::upper),
            "LENGTH" => Some(Self::length),
            "TYPEOF" => Some(Self::typeof_),
            "NULLIF" => Some(Self::nullif),
            "COALESCE" => Some(Self::coalesce),
            "IFNULL" => Some(Self::ifnull),
            "MAX" => Some(Self::max),
            "MIN" => Some(Self::min),
            "HEX" => Some(Self::hex),
            "ZEROBLOB" => Some(Self::zeroblob),
            "RANDOMBLOB" => Some(Self::randomblob),
            "RANDOM" => Some(Self::random),
            "ROUND" => Some(Self::round),
            "FLOOR" => Some(Self::floor),
            "CEIL" | "CEILING" => Some(Self::ceil),
            "LTRIM" => Some(Self::ltrim),
            "RTRIM" => Some(Self::rtrim),
            "TRIM" => Some(Self::trim),
            "REPLACE" => Some(Self::replace),
            "SUBSTR" | "SUBSTRING" => Some(Self::substr),
            "INSTR" => Some(Self::instr),
            "PRINTF" | "FORMAT" => Some(Self::printf),
            "UNICODE" => Some(Self::unicode),
            "CHAR" => Some(Self::char_),
            "DATE" => Some(Self::date),
            "TIME" => Some(Self::time),
            "DATETIME" => Some(Self::datetime),
            "JULIANDAY" => Some(Self::julianday),
            "STRFTIME" => Some(Self::strftime),
            "CHANGES" => Some(Self::changes),
            "TOTAL_CHANGES" => Some(Self::total_changes),
            "LAST_INSERT_ROWID" => Some(Self::last_insert_rowid),
            _ => None,
        }
    }

    /// ABS function
    pub fn abs(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let result = match &args[0] {
            Value::Integer(i) => Value::integer(i.abs()),
            Value::Float(f) => Value::float(f.abs()),
            _ => Value::Null,
        };
        ctx.set_result(result);
        Ok(())
    }

    /// LOWER function
    pub fn lower(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let result = match &args[0] {
            Value::Text(s) => Value::text(s.to_lowercase()),
            _ => args[0].clone(),
        };
        ctx.set_result(result);
        Ok(())
    }

    /// UPPER function
    pub fn upper(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let result = match &args[0] {
            Value::Text(s) => Value::text(s.to_uppercase()),
            _ => args[0].clone(),
        };
        ctx.set_result(result);
        Ok(())
    }

    /// LENGTH function
    pub fn length(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let len = match &args[0] {
            Value::Text(s) => s.len() as i64,
            Value::Blob(b) => b.len() as i64,
            _ => 0i64,
        };
        ctx.set_result(Value::integer(len));
        Ok(())
    }

    /// TYPEOF function
    pub fn typeof_(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() {
            ctx.set_result(Value::text("null"));
            return Ok(());
        }

        let type_name = match &args[0] {
            Value::Null => "null",
            Value::Integer(_) => "integer",
            Value::Float(_) => "real",
            Value::Text(_) => "text",
            Value::Blob(_) => "blob",
        };
        ctx.set_result(Value::text(type_name));
        Ok(())
    }

    /// NULLIF function
    pub fn nullif(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.len() < 2 {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        if args[0] == args[1] {
            ctx.set_result(Value::Null);
        } else {
            ctx.set_result(args[0].clone());
        }
        Ok(())
    }

    /// COALESCE function
    pub fn coalesce(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        for arg in args {
            if !arg.is_null() {
                ctx.set_result(arg.clone());
                return Ok(());
            }
        }
        ctx.set_result(Value::Null);
        Ok(())
    }

    /// IFNULL function
    pub fn ifnull(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.len() < 2 {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        if args[0].is_null() {
            ctx.set_result(args[1].clone());
        } else {
            ctx.set_result(args[0].clone());
        }
        Ok(())
    }

    /// MAX function (scalar version)
    pub fn max(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let mut max_val = &args[0];
        for arg in &args[1..] {
            if arg.compare(max_val) == std::cmp::Ordering::Greater {
                max_val = arg;
            }
        }
        ctx.set_result(max_val.clone());
        Ok(())
    }

    /// MIN function (scalar version)
    pub fn min(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let mut min_val = &args[0];
        for arg in &args[1..] {
            if arg.compare(min_val) == std::cmp::Ordering::Less {
                min_val = arg;
            }
        }
        ctx.set_result(min_val.clone());
        Ok(())
    }

    /// HEX function
    pub fn hex(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let hex_str = match &args[0] {
            Value::Blob(b) => b.iter().map(|byte| format!("{:02x}", byte)).collect(),
            Value::Text(s) => s.as_bytes().iter().map(|byte| format!("{:02x}", byte)).collect(),
            _ => String::new(),
        };
        ctx.set_result(Value::text(hex_str));
        Ok(())
    }

    /// ZEROBLOB function
    pub fn zeroblob(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() {
            ctx.set_result(Value::blob(vec![]));
            return Ok(());
        }

        let len = args[0].as_integer().unwrap_or(0) as usize;
        ctx.set_result(Value::blob(vec![0u8; len]));
        Ok(())
    }

    /// RANDOMBLOB function
    pub fn randomblob(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        let len = if args.is_empty() { 16 } else { args[0].as_integer().unwrap_or(16) as usize };

        let mut blob = vec![0u8; len];
        for byte in blob.iter_mut() {
            *byte = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u8)
                .unwrap_or(0))
                .wrapping_add(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| (d.as_nanos() >> 8) as u8)
                    .unwrap_or(0));
        }
        ctx.set_result(Value::blob(blob));
        Ok(())
    }

    /// RANDOM function
    pub fn random(ctx: &mut FunctionContext, _args: &[Value]) -> Result<()> {
        let random_val = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        ctx.set_result(Value::integer(random_val));
        Ok(())
    }

    /// ROUND function
    pub fn round(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let value = args[0].as_float().unwrap_or(0.0);
        let precision = if args.len() > 1 {
            args[1].as_integer().unwrap_or(0) as i32
        } else {
            0
        };

        let factor = 10f64.powi(precision);
        let rounded = (value * factor).round() / factor;
        ctx.set_result(Value::float(rounded));
        Ok(())
    }

    /// FLOOR function
    pub fn floor(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let value = args[0].as_float().unwrap_or(0.0);
        ctx.set_result(Value::float(value.floor()));
        Ok(())
    }

    /// CEIL function
    pub fn ceil(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let value = args[0].as_float().unwrap_or(0.0);
        ctx.set_result(Value::float(value.ceil()));
        Ok(())
    }

    /// LTRIM function
    pub fn ltrim(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let s = args[0].as_text().unwrap_or("");
        let chars = if args.len() > 1 {
            args[1].as_text().unwrap_or(" \t\n\r")
        } else {
            " \t\n\r"
        };

        let trimmed = s.trim_start_matches(|c| chars.contains(c));
        ctx.set_result(Value::text(trimmed));
        Ok(())
    }

    /// RTRIM function
    pub fn rtrim(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let s = args[0].as_text().unwrap_or("");
        let chars = if args.len() > 1 {
            args[1].as_text().unwrap_or(" \t\n\r")
        } else {
            " \t\n\r"
        };

        let trimmed = s.trim_end_matches(|c| chars.contains(c));
        ctx.set_result(Value::text(trimmed));
        Ok(())
    }

    /// TRIM function
    pub fn trim(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let s = args[0].as_text().unwrap_or("");
        let chars = if args.len() > 1 {
            args[1].as_text().unwrap_or(" \t\n\r")
        } else {
            " \t\n\r"
        };

        let trimmed = s.trim_matches(|c| chars.contains(c));
        ctx.set_result(Value::text(trimmed));
        Ok(())
    }

    /// REPLACE function
    pub fn replace(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.len() < 3 {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        if args[0].is_null() || args[1].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let s = args[0].as_text().unwrap_or("");
        let find = args[1].as_text().unwrap_or("");
        let replace_with = args[2].as_text().unwrap_or("");

        ctx.set_result(Value::text(s.replace(find, replace_with)));
        Ok(())
    }

    /// SUBSTR function
    pub fn substr(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.len() < 2 {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        if args[0].is_null() || args[1].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let s = args[0].as_text().unwrap_or("");
        let start = (args[1].as_integer().unwrap_or(1) - 1).max(0) as usize;
        let len = if args.len() > 2 {
            args[2].as_integer().unwrap_or(s.len() as i64) as usize
        } else {
            s.len()
        };

        let end = (start + len).min(s.len());
        ctx.set_result(Value::text(s[start..end].to_string()));
        Ok(())
    }

    /// INSTR function
    pub fn instr(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.len() < 2 {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        if args[0].is_null() || args[1].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let s = args[0].as_text().unwrap_or("");
        let find = args[1].as_text().unwrap_or("");

        let pos = s.find(find).map(|i| (i + 1) as i64).unwrap_or(0);
        ctx.set_result(Value::integer(pos));
        Ok(())
    }

    /// PRINTF function
    pub fn printf(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() {
            ctx.set_result(Value::text(String::new()));
            return Ok(());
        }

        // Simple implementation - just concatenate string representations
        let result: String = args.iter().map(|v| v.to_string_value()).collect();
        ctx.set_result(Value::text(result));
        Ok(())
    }

    /// UNICODE function
    pub fn unicode(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() || args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }

        let s = args[0].as_text().unwrap_or("");
        let code = s.chars().next().map(|c| c as i64).unwrap_or(0);
        ctx.set_result(Value::integer(code));
        Ok(())
    }

    /// CHAR function
    pub fn char_(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        let result: String = args
            .iter()
            .filter_map(|v| v.as_integer().and_then(|i| char::from_u32(i as u32)))
            .collect();
        ctx.set_result(Value::text(result));
        Ok(())
    }

    /// DATE function (stub)
    pub fn date(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if !args.is_empty() && args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }
        let now = Self::current_datetime();
        ctx.set_result(Value::text(now.format("%Y-%m-%d").to_string()));
        Ok(())
    }

    /// TIME function (stub)
    pub fn time(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if !args.is_empty() && args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }
        let now = Self::current_datetime();
        ctx.set_result(Value::text(now.format("%H:%M:%S").to_string()));
        Ok(())
    }

    /// DATETIME function (stub)
    pub fn datetime(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if !args.is_empty() && args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }
        let now = Self::current_datetime();
        ctx.set_result(Value::text(now.format("%Y-%m-%d %H:%M:%S").to_string()));
        Ok(())
    }

    /// JULIANDAY function (stub)
    pub fn julianday(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if !args.is_empty() && args[0].is_null() {
            ctx.set_result(Value::Null);
            return Ok(());
        }
        let timestamp = Self::current_unix_timestamp_seconds();
        ctx.set_result(Value::float(timestamp as f64 / 86400.0 + 2440587.5));
        Ok(())
    }

    /// STRFTIME function (stub)
    pub fn strftime(ctx: &mut FunctionContext, args: &[Value]) -> Result<()> {
        if args.is_empty() {
            ctx.set_result(Value::Null);
            return Ok(());
        }
        let format = args[0].as_text().unwrap_or("%Y-%m-%d");
        let now = Self::current_datetime();
        ctx.set_result(Value::text(now.format(format).to_string()));
        Ok(())
    }

    /// CHANGES function (stub)
    pub fn changes(ctx: &mut FunctionContext, _args: &[Value]) -> Result<()> {
        ctx.set_result(Value::integer(0));
        Ok(())
    }

    /// TOTAL_CHANGES function (stub)
    pub fn total_changes(ctx: &mut FunctionContext, _args: &[Value]) -> Result<()> {
        ctx.set_result(Value::integer(0));
        Ok(())
    }

    /// LAST_INSERT_ROWID function (stub)
    pub fn last_insert_rowid(ctx: &mut FunctionContext, _args: &[Value]) -> Result<()> {
        ctx.set_result(Value::integer(0));
        Ok(())
    }

    fn current_datetime() -> chrono::DateTime<chrono::Utc> {
        use chrono::TimeZone;

        chrono::Utc
            .timestamp_opt(Self::current_unix_timestamp_seconds(), 0)
            .single()
            .unwrap_or_else(|| {
                chrono::Utc
                    .timestamp_opt(0, 0)
                    .single()
                    .expect("unix epoch should be valid")
            })
    }

    fn current_unix_timestamp_seconds() -> i64 {
        #[cfg(target_arch = "wasm32")]
        {
            (ic_cdk::api::time() / 1_000_000_000) as i64
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            use std::time::{SystemTime, UNIX_EPOCH};

            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abs() {
        let mut ctx = FunctionContext::new("abs", 1);
        BuiltinFunctions::abs(&mut ctx, &[Value::integer(-5)]).unwrap();
        assert_eq!(ctx.result(), Some(&Value::integer(5)));
    }

    #[test]
    fn test_upper() {
        let mut ctx = FunctionContext::new("upper", 1);
        BuiltinFunctions::upper(&mut ctx, &[Value::text("hello")]).unwrap();
        assert_eq!(ctx.result(), Some(&Value::text("HELLO")));
    }

    #[test]
    fn test_lower() {
        let mut ctx = FunctionContext::new("lower", 1);
        BuiltinFunctions::lower(&mut ctx, &[Value::text("HELLO")]).unwrap();
        assert_eq!(ctx.result(), Some(&Value::text("hello")));
    }

    #[test]
    fn test_length() {
        let mut ctx = FunctionContext::new("length", 1);
        BuiltinFunctions::length(&mut ctx, &[Value::text("hello")]).unwrap();
        assert_eq!(ctx.result(), Some(&Value::integer(5)));
    }

    #[test]
    fn test_typeof() {
        let mut ctx = FunctionContext::new("typeof", 1);
        BuiltinFunctions::typeof_(&mut ctx, &[Value::integer(42)]).unwrap();
        assert_eq!(ctx.result(), Some(&Value::text("integer")));
    }

    #[test]
    fn test_coalesce() {
        let mut ctx = FunctionContext::new("coalesce", 3);
        BuiltinFunctions::coalesce(&mut ctx, &[Value::Null, Value::Null, Value::integer(3)]).unwrap();
        assert_eq!(ctx.result(), Some(&Value::integer(3)));
    }

    #[test]
    fn test_nullif() {
        let mut ctx = FunctionContext::new("nullif", 2);
        BuiltinFunctions::nullif(&mut ctx, &[Value::integer(1), Value::integer(1)]).unwrap();
        assert_eq!(ctx.result(), Some(&Value::Null));
    }
}
