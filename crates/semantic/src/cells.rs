//! Shared cell/embedding helpers for the semantic layer (used by the L1
//! retriever and the catalogue-edit path; one definition, not one per module —
//! AGENTS.md § DRY).

use consumer_engine_core::SemanticType;
use serde_json::Value;

/// A `JSON` cell as a string, defaulting to empty for null/non-string.
#[must_use]
pub fn cell_string(v: Option<&Value>) -> String {
    v.and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default()
}

/// An optional string cell (column names may be absent for table-level rows).
#[must_use]
pub fn cell_opt_string(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str).map(str::to_string)
}

/// Parse a `semantic_type` wire label; `None` for an unknown value (the caller
/// decides: retrieval defaults to Dimension, the edit path rejects).
#[must_use]
pub fn parse_semantic_type(v: Option<&Value>) -> Option<SemanticType> {
    v.and_then(Value::as_str).and_then(SemanticType::parse)
}

/// Convert a `FLOAT[]` `JSON` cell into a `Vec<f32>`.
#[must_use]
pub fn embedding_to_vec(v: &Value) -> Vec<f32> {
    v.as_array()
        .map(|arr| arr.iter().filter_map(num_to_f32).collect())
        .unwrap_or_default()
}

/// Map a `JSON` number to `f32`.
fn num_to_f32(v: &Value) -> Option<f32> {
    v.as_f64().map(|f| f as f32)
}

/// Cosine similarity; 0.0 if either vector has zero norm.
#[must_use]
pub fn cosine(a: &[f32], b: &[f32]) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let dot: f64 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let na: f64 = a
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    let nb: f64 = b
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_cosine_of_unit_vectors() {
        assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-9);
        assert!((cosine(&[1.0, 0.0], &[0.0, 1.0])).abs() < 1e-9);
        assert_eq!(cosine(&[], &[1.0]), 0.0);
    }

    #[test]
    fn test_should_parse_cells() {
        assert_eq!(cell_string(Some(&Value::String("x".into()))), "x");
        assert_eq!(cell_string(None), "");
        assert_eq!(cell_opt_string(None), None);
        assert_eq!(
            parse_semantic_type(Some(&Value::String("identifier".into()))),
            Some(SemanticType::Identifier)
        );
        assert_eq!(parse_semantic_type(None), None);
    }
}
