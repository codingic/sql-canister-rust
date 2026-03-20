//! String utilities for SQLite

/// Check if a character is a valid SQL identifier start character
pub fn is_identifier_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

/// Check if a character is a valid SQL identifier character
pub fn is_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// Check if a string is a valid SQL identifier
pub fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    let mut chars = s.chars();
    if !is_identifier_start(chars.next().unwrap()) {
        return false;
    }

    chars.all(is_identifier_char)
}

/// Escape a string for SQL (single quotes)
pub fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Unescape a SQL string
pub fn unescape_sql_string(s: &str) -> String {
    s.replace("''", "'")
}

/// Quote an identifier if necessary
pub fn quote_identifier(s: &str) -> String {
    if needs_quoting(s) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Check if an identifier needs quoting
pub fn needs_quoting(s: &str) -> bool {
    if s.is_empty() {
        return true;
    }

    let mut chars = s.chars();
    if !is_identifier_start(chars.next().unwrap()) {
        return true;
    }

    if !chars.all(is_identifier_char) {
        return true;
    }

    // Check if it's a reserved keyword
    is_keyword(s)
}

/// Check if a string is a SQL keyword
pub fn is_keyword(s: &str) -> bool {
    let upper = s.to_uppercase();
    KEYWORDS.contains(&upper.as_str())
}

/// SQL keywords (subset of full list)
const KEYWORDS: &[&str] = &[
    "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE",
    "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN",
    "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT",
    "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE",
    "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DO", "DROP", "EACH",
    "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS",
    "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM",
    "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE",
    "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT",
    "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST",
    "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT",
    "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER",
    "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING",
    "PRIMARY", "QUERY", "RAISE", "RANGE", "RECURSIVE", "REFERENCES", "REGEXP",
    "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RETURNING", "RIGHT",
    "ROLLBACK", "ROW", "ROWS", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP",
    "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED",
    "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL",
    "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT",
];

/// Trim whitespace from both ends
pub fn trim(s: &str) -> &str {
    s.trim()
}

/// Convert to uppercase (ASCII only for SQL compatibility)
pub fn to_upper_ascii(s: &str) -> String {
    s.chars().map(|c| c.to_ascii_uppercase()).collect()
}

/// Convert to lowercase (ASCII only for SQL compatibility)
pub fn to_lower_ascii(s: &str) -> String {
    s.chars().map(|c| c.to_ascii_lowercase()).collect()
}

/// Check if string starts with (case-insensitive)
pub fn starts_with_ignore_case(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && to_upper_ascii(&s[..prefix.len()]) == to_upper_ascii(prefix)
}

/// Parse a number from a string
pub fn parse_number(s: &str) -> Option<f64> {
    // Try integer first
    if let Ok(i) = s.parse::<i64>() {
        return Some(i as f64);
    }

    // Try float
    s.parse::<f64>().ok()
}

/// Format a floating point number for SQL output
pub fn format_float(f: f64) -> String {
    if f.is_nan() {
        "NULL".to_string()
    } else if f.is_infinite() {
        if f > 0.0 {
            "1e999".to_string()
        } else {
            "-1e999".to_string()
        }
    } else if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.0}", f)
    } else {
        format!("{}", f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_identifier_start() {
        assert!(is_identifier_start('a'));
        assert!(is_identifier_start('Z'));
        assert!(is_identifier_start('表'));
        assert!(is_identifier_start('_'));
        assert!(!is_identifier_start('1'));
        assert!(!is_identifier_start('-'));
    }

    #[test]
    fn test_is_identifier_char() {
        assert!(is_identifier_char('a'));
        assert!(is_identifier_char('1'));
        assert!(is_identifier_char('名'));
        assert!(is_identifier_char('_'));
        assert!(is_identifier_char('$'));
        assert!(!is_identifier_char('-'));
        assert!(!is_identifier_char(' '));
    }

    #[test]
    fn test_is_valid_identifier() {
        assert!(is_valid_identifier("table1"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("my_table"));
        assert!(is_valid_identifier("用户表"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("1table"));
        assert!(!is_valid_identifier("my-table"));
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("hello"), "hello");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("'quoted'"), "''quoted''");
    }

    #[test]
    fn test_unescape_sql_string() {
        assert_eq!(unescape_sql_string("hello"), "hello");
        assert_eq!(unescape_sql_string("it''s"), "it's");
        assert_eq!(unescape_sql_string("''quoted''"), "'quoted'");
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("table"), "\"table\""); // "table" is a keyword
        assert_eq!(quote_identifier("my_table"), "my_table"); // not a keyword
        assert_eq!(quote_identifier("1table"), "\"1table\"");
        assert_eq!(quote_identifier("select"), "\"select\"");
    }

    #[test]
    fn test_needs_quoting() {
        assert!(!needs_quoting("my_table"));
        assert!(needs_quoting("1table"));
        assert!(needs_quoting("select"));
        assert!(needs_quoting(""));
    }

    #[test]
    fn test_is_keyword() {
        assert!(is_keyword("SELECT"));
        assert!(is_keyword("select"));
        assert!(is_keyword("Select"));
        assert!(!is_keyword("mytable"));
    }

    #[test]
    fn test_starts_with_ignore_case() {
        assert!(starts_with_ignore_case("SELECT * FROM", "select"));
        assert!(starts_with_ignore_case("SELECT * FROM", "SELECT"));
        assert!(!starts_with_ignore_case("INSERT", "select"));
    }

    #[test]
    fn test_parse_number() {
        assert_eq!(parse_number("42"), Some(42.0));
        assert_eq!(parse_number("-42"), Some(-42.0));
        assert_eq!(parse_number("3.14"), Some(3.14));
        assert_eq!(parse_number("not a number"), None);
    }

    #[test]
    fn test_format_float() {
        assert_eq!(format_float(42.0), "42");
        assert_eq!(format_float(3.14), "3.14");
        assert_eq!(format_float(f64::NAN), "NULL");
        assert_eq!(format_float(f64::INFINITY), "1e999");
        assert_eq!(format_float(f64::NEG_INFINITY), "-1e999");
    }

    #[test]
    fn test_to_upper_lower_ascii() {
        assert_eq!(to_upper_ascii("hello"), "HELLO");
        assert_eq!(to_lower_ascii("HELLO"), "hello");
    }

    #[test]
    fn test_trim() {
        assert_eq!(trim("  hello  "), "hello");
        assert_eq!(trim("\thello\n"), "hello");
    }
}
