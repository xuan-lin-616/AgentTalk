//! Duplicate-key-safe JSON parsing and frozen RFC 8785 / JCS canonicalization.
//!
//! `serde_json`'s default `Value` visitor is last-key-wins. This module never
//! uses that behavior for contract inputs: it deserializes with a recursive
//! visitor that stores every decoded object key and rejects the second
//! occurrence, including keys that differ only in JSON escaping.

use serde::de::{DeserializeSeed, Error as DeError, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use unicode_normalization::UnicodeNormalization;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const DUPLICATE_KEY_PREFIX: &str = "AGENTTALK_DUPLICATE_KEY:";

/// Errors produced while parsing JSON with duplicate-key rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonParseError {
    /// A decoded object key occurred more than once in the same object.
    DuplicateKey { path: String },
    /// The input is not syntactically valid JSON.
    Syntax { message: String },
}

impl Display for JsonParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateKey { path } => write!(f, "duplicate object key at {path}"),
            Self::Syntax { message } => f.write_str(message),
        }
    }
}

impl std::error::Error for JsonParseError {}

/// Errors produced by the contract JCS canonicalizer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalizationError {
    pub path: String,
    pub reason: CanonicalizationReason,
}

impl CanonicalizationError {
    #[must_use]
    pub const fn new(path: String, reason: CanonicalizationReason) -> Self {
        Self { path, reason }
    }
}

impl Display for CanonicalizationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            CanonicalizationReason::UnsafeInteger => {
                write!(
                    f,
                    "JCS contract rule violation at {}: number is not a non-negative safe integer",
                    self.path
                )
            }
            CanonicalizationReason::NonNfcString => {
                write!(
                    f,
                    "JCS contract rule violation at {}: non-ASCII string is not NFC",
                    self.path
                )
            }
            CanonicalizationReason::Serializer => {
                write!(
                    f,
                    "JCS serialization failed at {}: {}",
                    self.path, self.reason
                )
            }
        }
    }
}

impl std::error::Error for CanonicalizationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalizationReason {
    UnsafeInteger,
    NonNfcString,
    Serializer,
}

impl Display for CanonicalizationReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsafeInteger => f.write_str("number is not a non-negative safe integer"),
            Self::NonNfcString => f.write_str("non-ASCII string is not NFC"),
            Self::Serializer => f.write_str("the JCS serializer rejected the value"),
        }
    }
}

/// Parse a JSON document and reject duplicate object keys at every nesting
/// level. Unlike `serde_json::from_slice`, this never silently overwrites an
/// earlier value with a later one.
pub fn parse_duplicate_safe(bytes: &[u8]) -> Result<Value, JsonParseError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let seed = ValueSeed {
        path: "$".to_owned(),
    };
    let value = seed.deserialize(&mut deserializer).map_err(classify)?;
    // Reject trailing non-whitespace, including a second complete JSON value.
    deserializer.end().map_err(classify)?;
    Ok(value)
}

/// Parse a JSON string and reject duplicate object keys at every nesting
/// level.
pub fn parse_duplicate_safe_str(json: &str) -> Result<Value, JsonParseError> {
    parse_duplicate_safe(json.as_bytes())
}

fn classify(error: serde_json::Error) -> JsonParseError {
    if error.classify() == serde_json::error::Category::Data {
        let message = error.to_string();
        if let Some(path) = message.strip_prefix(DUPLICATE_KEY_PREFIX) {
            // serde_json appends " at line N column M" to Display output.
            let path = path.split(" at line ").next().unwrap_or(path);
            return JsonParseError::DuplicateKey {
                path: path.to_owned(),
            };
        }
    }
    JsonParseError::Syntax {
        message: format!(
            "{} at line {} column {}",
            error,
            error.line(),
            error.column()
        ),
    }
}

struct ValueSeed {
    path: String,
}

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor { path: self.path })
    }
}

struct ValueVisitor {
    path: String,
}

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Value, E>
    where
        E: DeError,
    {
        // The serde_json parser rejects non-finite input before this visitor
        // is called. Defend the invariant anyway.
        if value.is_finite() {
            Number::from_f64(value).map_or_else(
                || Err(E::custom("invalid JSON number")),
                |number| Ok(Value::Number(number)),
            )
        } else {
            Err(E::custom("non-finite JSON number"))
        }
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        ValueSeed { path: self.path }.deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        let mut index = 0usize;
        while let Some(value) = seq.next_element_seed(ValueSeed {
            path: format!("{}[{index}]", self.path),
        })? {
            values.push(value);
            index += 1;
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut entries: Vec<(String, Value)> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            let path = format!("{}.{key}", self.path);
            if entries.iter().any(|(existing, _)| existing == &key) {
                return Err(A::Error::custom(format!("{DUPLICATE_KEY_PREFIX}{path}")));
            }
            let value = map.next_value_seed(ValueSeed { path: path.clone() })?;
            entries.push((key, value));
        }

        let mut object = Map::with_capacity(entries.len());
        for (key, value) in entries {
            object.insert(key, value);
        }
        Ok(Value::Object(object))
    }
}

/// Order two strings by their UTF-16 code-unit sequence, as required by JCS
/// object-key sorting and by the frozen set-array sorting rules.
#[must_use]
pub fn utf16_order(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

/// Canonicalize a JSON value with the frozen contract JCS rules:
/// RFC 8785 serialization, object keys sorted by UTF-16 code units, no
/// insignificant whitespace, and contract extensions that reject duplicate
/// keys (enforced at parse time), non-NFC strings, and any number that is not
/// a non-negative safe integer.
pub fn canonicalize(value: &Value) -> Result<Vec<u8>, CanonicalizationError> {
    validate_contract_jcs_rules(value, "$")?;
    serde_jcs::to_vec(value).map_err(|_error| CanonicalizationError {
        path: "$".to_owned(),
        reason: CanonicalizationReason::Serializer,
    })
}

fn validate_contract_jcs_rules(value: &Value, path: &str) -> Result<(), CanonicalizationError> {
    match value {
        Value::Null | Value::Bool(_) => Ok(()),
        Value::Number(number) => {
            if is_non_negative_safe_integer(number) {
                Ok(())
            } else {
                Err(CanonicalizationError {
                    path: path.to_owned(),
                    reason: CanonicalizationReason::UnsafeInteger,
                })
            }
        }
        Value::String(string) => {
            if is_nfc(string) {
                Ok(())
            } else {
                Err(CanonicalizationError {
                    path: path.to_owned(),
                    reason: CanonicalizationReason::NonNfcString,
                })
            }
        }
        Value::Array(values) => {
            for (index, item) in values.iter().enumerate() {
                validate_contract_jcs_rules(item, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        Value::Object(object) => {
            for (key, item) in object {
                let key_path = format!("{path}.{key}");
                if !is_nfc(key) {
                    return Err(CanonicalizationError {
                        path: key_path,
                        reason: CanonicalizationReason::NonNfcString,
                    });
                }
                validate_contract_jcs_rules(item, &key_path)?;
            }
            Ok(())
        }
    }
}

/// Returns the normalized `u64` for the exact contract-safe numeric domain.
/// `1.0`, `1e0`, and `0e0` normalize to `1` / `0`.
#[must_use]
pub fn safe_integer_as_u64(number: &Number) -> Option<u64> {
    if let Some(unsigned) = number.as_u64() {
        return (unsigned <= MAX_SAFE_INTEGER).then_some(unsigned);
    }
    if let Some(signed) = number.as_i64() {
        return (signed >= 0 && signed <= MAX_SAFE_INTEGER as i64).then_some(signed as u64);
    }
    let float = number.as_f64()?;
    (float.is_finite() && float >= 0.0 && float <= MAX_SAFE_INTEGER as f64 && float.fract() == 0.0)
        .then_some(float as u64)
}

/// Returns the normalized `u64` for a JSON value in the contract-safe numeric
/// domain.
#[must_use]
pub fn value_as_safe_u64(value: &Value) -> Option<u64> {
    value.as_number().and_then(safe_integer_as_u64)
}

/// Returns true for the exact contract-safe numeric domain: a mathematical
/// integer `n` with `0 <= n <= 9007199254740991`. `1.0`, `1e0`, and `0e0`
/// therefore count as the safe integer `1` / `0`.
#[must_use]
pub fn is_non_negative_safe_integer(number: &Number) -> bool {
    if let Some(unsigned) = number.as_u64() {
        unsigned <= MAX_SAFE_INTEGER
    } else if let Some(signed) = number.as_i64() {
        signed >= 0 && signed <= MAX_SAFE_INTEGER as i64
    } else if let Some(float) = number.as_f64() {
        float.is_finite()
            && float >= 0.0
            && float <= MAX_SAFE_INTEGER as f64
            && float.fract() == 0.0
    } else {
        false
    }
}

/// `sha256Raw(bytes)` from ADR-001: SHA-256 over the exact sealed bytes.
#[must_use]
pub fn sha256_raw(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// `sha256Jcs(value)` from ADR-001: SHA-256 over RFC 8785 canonical bytes,
/// after enforcing the frozen contract JCS extensions.
pub fn sha256_jcs(value: &Value) -> Result<[u8; 32], CanonicalizationError> {
    Ok(sha256_raw(&canonicalize(value)?))
}

/// Lowercase hex encoding used by every frozen digest field.
#[must_use]
pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    output
}

/// `sha256Raw` as the lowercase hex string used in contract JSON.
#[must_use]
pub fn sha256_raw_hex(bytes: &[u8]) -> String {
    encode_hex(&sha256_raw(bytes))
}

/// `sha256Jcs` as the lowercase hex string used in contract JSON.
pub fn sha256_jcs_hex(value: &Value) -> Result<String, CanonicalizationError> {
    Ok(encode_hex(&sha256_jcs(value)?))
}

/// Unicode NFC predicate using `unicode-normalization`.
fn is_nfc(value: &str) -> bool {
    value.nfc().collect::<String>() == value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_keys_at_nesting_depth() {
        let input = br#"{"a":1,"nested":{"b":2,"b":3}}"#;
        let error = parse_duplicate_safe(input).unwrap_err();
        assert_eq!(
            error,
            JsonParseError::DuplicateKey {
                path: "$.nested.b".to_owned()
            }
        );
    }

    #[test]
    fn accepts_trailing_whitespace_only() {
        for input in [
            b"{}".as_slice(),
            b"{} \n\t\r ".as_slice(),
            b"[1,2]\n".as_slice(),
        ] {
            parse_duplicate_safe(input).expect("trailing whitespace must be accepted");
        }
    }

    #[test]
    fn rejects_trailing_second_json_value() {
        for input in [b"{} {}".as_slice(), b"{}[]".as_slice(), b"1 2".as_slice()] {
            let error = parse_duplicate_safe(input).expect_err("second value must be rejected");
            assert!(matches!(error, JsonParseError::Syntax { .. }), "{error}");
        }
    }

    #[test]
    fn rejects_trailing_non_whitespace_garbage() {
        for input in [
            b"{} nope".as_slice(),
            b"{} \x00".as_slice(),
            b"1 #".as_slice(),
        ] {
            let error = parse_duplicate_safe(input).expect_err("garbage tail must be rejected");
            assert!(matches!(error, JsonParseError::Syntax { .. }), "{error}");
        }
    }

    #[test]
    fn rejects_escaped_duplicate_keys() {
        let input = br#"{"a":1,"\u0061":2}"#;
        assert!(matches!(
            parse_duplicate_safe(input),
            Err(JsonParseError::DuplicateKey { .. })
        ));
    }

    #[test]
    fn utf16_order_precedes_codepoint_order_for_non_bmp() {
        // U+10000 is one surrogate pair (D800 DC00); U+E000 is one UTF-16
        // code unit. UTF-16 order puts U+10000 first, codepoint order does not.
        let left = "\u{10000}";
        let right = "\u{e000}";
        assert_eq!(utf16_order(left, right), Ordering::Less);
        assert_eq!(left.cmp(right), Ordering::Greater);
    }

    #[test]
    fn canonicalizes_decimal_forms_to_the_same_integer() {
        for literal in ["1.0", "1e0", "1"] {
            let value = parse_duplicate_safe_str(literal).unwrap();
            assert_eq!(canonicalize(&value).unwrap(), b"1");
        }
    }

    #[test]
    fn rejects_unsafe_and_negative_numbers() {
        for literal in ["9007199254740992", "-1", "1.5"] {
            let value = parse_duplicate_safe_str(literal).unwrap();
            assert_eq!(
                canonicalize(&value).unwrap_err().reason,
                CanonicalizationReason::UnsafeInteger
            );
        }
    }

    #[test]
    fn rejects_non_nfc_strings() {
        let value = parse_duplicate_safe_str(r#""e\u0301""#).unwrap();
        assert_eq!(
            canonicalize(&value).unwrap_err().reason,
            CanonicalizationReason::NonNfcString
        );
    }
}
