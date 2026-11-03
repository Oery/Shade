//! Typed conditions on JSON: whether a webhook payload, or later a watched
//! resource, matches.
//!
//! A condition picks one value with a JSON pointer (RFC 6901; empty or
//! omitted means the whole document) and compares it with a constant.
//! Anything it cannot decide exactly is an error, never `false`: a missing
//! value, an object or array, values of different types, ordering on
//! anything but numbers, or numbers too large to compare without rounding.

use std::cmp::Ordering;

use serde::Deserialize;
use serde_json::{Number, Value};

/// 2^53: past this, not every integer has its own floating-point value.
const EXACT: u128 = 1 << 53;

#[derive(Clone, Copy, Debug, PartialEq, Deserialize)]
pub enum Op {
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Ge,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Le,
}

impl Op {
    fn symbol(self) -> &'static str {
        match self {
            Self::Eq => "==",
            Self::Ne => "!=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Lt => "<",
            Self::Le => "<=",
        }
    }

    fn holds(self, ordering: Ordering) -> bool {
        match self {
            Self::Eq => ordering == Ordering::Equal,
            Self::Ne => ordering != Ordering::Equal,
            Self::Gt => ordering == Ordering::Greater,
            Self::Ge => ordering != Ordering::Less,
            Self::Lt => ordering == Ordering::Less,
            Self::Le => ordering != Ordering::Greater,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Condition {
    #[serde(default)]
    pub pointer: String,
    pub op: Op,
    pub value: Value,
}

impl Condition {
    /// What can be checked before any document: the pointer, and a constant
    /// the operator can compare.
    pub fn validate(&self) -> Result<(), String> {
        check_pointer(&self.pointer)?;

        match (&self.value, self.op) {
            (Value::Number(number), _) => exact(number).map(drop),
            (Value::String(_) | Value::Bool(_) | Value::Null, Op::Eq | Op::Ne) => Ok(()),
            (Value::String(_) | Value::Bool(_) | Value::Null, op) => Err(format!("`{}` compares numbers only", op.symbol())),
            (Value::Array(_) | Value::Object(_), _) => Err("the value must be a number, string, boolean or null".to_owned()),
        }
    }

    /// Whether `document` matches. An error means it could not be decided.
    pub fn matches(&self, document: &Value) -> Result<bool, String> {
        check_pointer(&self.pointer)?;

        let Some(found) = document.pointer(&self.pointer) else {
            return Err(format!("nothing at `{}`", self.pointer));
        };

        compare(found, self.op, &self.value)
    }
}

/// An RFC 6901 pointer: empty, or `/`-separated tokens where `~` is only
/// ever `~0` or `~1`.
fn check_pointer(pointer: &str) -> Result<(), String> {
    if !pointer.is_empty() && !pointer.starts_with('/') {
        return Err(format!("pointer `{pointer}` must be empty or start with `/`"));
    }

    let mut characters = pointer.chars();

    while let Some(character) = characters.next() {
        if character == '~' && !matches!(characters.next(), Some('0' | '1')) {
            return Err(format!("pointer `{pointer}` has a `~` not followed by 0 or 1"));
        }
    }

    Ok(())
}

fn compare(found: &Value, op: Op, expected: &Value) -> Result<bool, String> {
    match (found, expected) {
        (Value::Number(found), Value::Number(expected)) => Ok(op.holds(order(found, expected)?)),
        (Value::String(found), Value::String(expected)) => equality(op, found == expected),
        (Value::Bool(found), Value::Bool(expected)) => equality(op, found == expected),
        (Value::Null, Value::Null) => equality(op, true),
        (Value::Array(_) | Value::Object(_), _) => Err(format!("found {}, not a value to compare", kind(found))),
        _ => Err(format!("found {}, which cannot be compared with {}", kind(found), kind(expected))),
    }
}

fn equality(op: Op, equal: bool) -> Result<bool, String> {
    match op {
        Op::Eq => Ok(equal),
        Op::Ne => Ok(!equal),
        other => Err(format!("`{}` compares numbers only", other.symbol())),
    }
}

/// Integers compare exactly; anything involving a fraction compares as
/// floating point, but only where that cannot round.
fn order(found: &Number, expected: &Number) -> Result<Ordering, String> {
    if let (Some(found), Some(expected)) = (integer(found), integer(expected)) {
        return Ok(found.cmp(&expected));
    }

    let (found, expected) = (exact(found)?, exact(expected)?);

    match found.partial_cmp(&expected) {
        Some(ordering) => Ok(ordering),
        None => Err("the numbers cannot be ordered".to_owned()),
    }
}

fn integer(number: &Number) -> Option<i128> {
    match number.as_i64() {
        Some(integer) => Some(i128::from(integer)),
        None => number.as_u64().map(i128::from),
    }
}

/// The number as floating point, if it is small enough to be exact. A
/// large integer that JSON parsing already turned into floating point is
/// refused here too, rather than compared after rounding.
fn exact(number: &Number) -> Result<f64, String> {
    let small = match (integer(number), number.as_f64()) {
        (Some(integer), _) => integer.unsigned_abs() <= EXACT,
        (None, Some(float)) => float.is_finite() && float.abs() < 9_007_199_254_740_992.0,
        (None, None) => false,
    };

    match number.as_f64() {
        Some(float) if small => Ok(float),
        _ => Err(format!("{number} is too large to compare exactly")),
    }
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::Condition;

    fn condition(pointer: &str, op: &str, value: Value) -> Result<Condition, String> {
        let condition: Condition = match serde_json::from_value(json!({ "pointer": pointer, "op": op, "value": value })) {
            Ok(condition) => condition,
            Err(error) => return Err(error.to_string()),
        };

        condition.validate()?;

        Ok(condition)
    }

    /// What matches, what does not, and what is an error rather than `false`.
    #[test]
    fn conditions_decide_exactly_or_not_at_all() {
        let document = json!({
            "run": { "conclusion": "failure", "attempt": 2, "draft": false, "id": 9_007_199_254_740_993_u64, "ratio": 0.5, "tags": [] },
            "a/b": 1,
            "big": 1e300,
        });

        let cases: [(&str, &str, Value, Result<bool, ()>); 18] = [
            ("/run/conclusion", "==", json!("failure"), Ok(true)),
            ("/run/conclusion", "!=", json!("failure"), Ok(false)),
            ("/run/draft", "==", json!(false), Ok(true)),
            ("/run/attempt", ">=", json!(2), Ok(true)),
            ("/run/ratio", "<", json!(1), Ok(true)),
            // Integers beyond 2^53 still compare exactly with integers.
            ("/run/id", ">", json!(9_007_199_254_740_992_u64), Ok(true)),
            ("/run/id", "==", json!(9_007_199_254_740_992_u64), Ok(false)),
            // ...but not with a fraction, nor a huge number JSON already rounded.
            ("/run/id", ">", json!(0.5), Err(())),
            ("/big", ">", json!(1), Err(())),
            ("/a~1b", "==", json!(1), Ok(true)),
            ("/missing", "==", json!(1), Err(())),
            ("/run/tags", "==", json!(1), Err(())),
            ("/run", "==", json!("x"), Err(())),
            ("/run/attempt", "==", json!("2"), Err(())),
            ("/run/conclusion", ">", json!("a"), Err(())),
            ("/run/x~2", "==", json!(1), Err(())),
            ("run", "==", json!(1), Err(())),
            ("/run/conclusion", "==", json!(["failure"]), Err(())),
        ];

        for (pointer, op, value, expected) in cases {
            let decided = condition(pointer, op, value.clone()).and_then(|condition| condition.matches(&document));
            assert_eq!(decided.map_err(drop), expected, "{pointer} {op} {value}");
        }

        // The empty pointer is the whole document.
        assert_eq!(condition("", "==", json!(3)).and_then(|condition| condition.matches(&json!(3))), Ok(true));
    }
}
