//! Embedded formal JSON Schema Draft 2020-12 validators.
//!
//! The schema documents live next to the IPC schema set in
//! `agenttalk-next/schemas/orchestration/v1/`. They use only local
//! `#/$defs/...` references, set `additionalProperties: false` at every
//! object level, and are compiled here once with remote/fetching features
//! disabled.

use jsonschema::{Draft, Validator};
use std::sync::OnceLock;

/// Frozen brief-root-manifest schema document.
pub const BRIEF_ROOT_MANIFEST_SCHEMA_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/orchestration/v1/brief-root-manifest.schema.json"
));

/// Frozen handoff-envelope schema document.
pub const HANDOFF_ENVELOPE_SCHEMA_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/orchestration/v1/handoff-envelope.schema.json"
));

/// Returns the compiled brief schema validator.
#[must_use]
pub fn brief_validator() -> &'static Validator {
    static BRIEF: OnceLock<Validator> = OnceLock::new();
    BRIEF.get_or_init(|| {
        compile(BRIEF_ROOT_MANIFEST_SCHEMA_JSON)
            .expect("embedded brief schema must be a valid Draft 2020-12 schema")
    })
}

/// Returns the compiled handoff schema validator.
#[must_use]
pub fn handoff_validator() -> &'static Validator {
    static HANDOFF: OnceLock<Validator> = OnceLock::new();
    HANDOFF.get_or_init(|| {
        compile(HANDOFF_ENVELOPE_SCHEMA_JSON)
            .expect("embedded handoff schema must be a valid Draft 2020-12 schema")
    })
}

fn compile(schema_json: &str) -> Result<Validator, jsonschema::ValidationError<'static>> {
    let schema = crate::json::parse_duplicate_safe(schema_json.as_bytes())
        .expect("embedded schema source must be duplicate-key free");
    jsonschema::options()
        .with_draft(Draft::Draft202012)
        .build(&schema)
}

/// Returns the first schema error rendered for diagnostics, if any.
pub(crate) fn first_schema_error(
    validator: &Validator,
    value: &serde_json::Value,
) -> Option<String> {
    validator
        .iter_errors(value)
        .next()
        .map(|error| error.to_string())
}
