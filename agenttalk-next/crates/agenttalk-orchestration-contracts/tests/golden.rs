mod common;

use agenttalk_orchestration_contracts::brief::{ParsedManifest, ShapeValidatedManifest};
use agenttalk_orchestration_contracts::handoff::{
    DeliveryDeclaration, HandoffVerificationContext, ParsedEnvelope, ShapeValidatedEnvelope,
};
use agenttalk_orchestration_contracts::json;
use common::{
    golden_dir, list_dirs, load_brief_registry, load_bytes_map, load_handoff_context, read_bytes,
    trimmed,
};

fn expected(path: &str) -> String {
    trimmed(path)
}

fn jcs_case_accepts(name: &str) {
    let raw = read_bytes(format!("jcs/{name}.input.json"));
    let value = json::parse_duplicate_safe(&raw).expect("JCS fixture must parse");
    assert_eq!(
        json::sha256_raw_hex(&raw),
        expected(&format!("jcs/{name}.expected.sha256.txt")),
        "{name}: sha256Raw(original input bytes) literal"
    );
    let canonical = json::canonicalize(&value).expect("JCS fixture must canonicalize");
    assert_eq!(
        canonical,
        read_bytes(format!("jcs/{name}.expected.canonical.json")),
        "{name}: canonical JCS bytes literal"
    );
    assert_eq!(
        json::sha256_jcs_hex(&value).unwrap(),
        expected(&format!("jcs/{name}.expected.sha256-jcs.txt")),
        "{name}: sha256Jcs literal"
    );
}

#[test]
fn jcs_golden_vectors_match_literal_reference() {
    for name in [
        "utf16-key-order",
        "unicode-escapes-nfc",
        "safe-integer-boundary",
        "array-order",
        "raw-number-literals",
    ] {
        jcs_case_accepts(name);
    }

    let raw_number_tokens = read_bytes("jcs/raw-number-literals.input.json");
    for token in [b"1.0".as_slice(), b"1e0".as_slice(), b"0e0".as_slice()] {
        assert!(
            raw_number_tokens
                .windows(token.len())
                .any(|window| window == token),
            "raw-number-literals must preserve the {token:?} token verbatim"
        );
    }

    for name in ["unsafe-integer", "negative-integer", "fractional"] {
        let value = json::parse_duplicate_safe(&read_bytes(format!("jcs/{name}.input.json")))
            .expect("must parse");
        assert_eq!(
            expected(&format!("jcs/{name}.expected.reason.txt")),
            "unsafe-integer"
        );
        assert_eq!(
            json::canonicalize(&value).unwrap_err().reason,
            json::CanonicalizationReason::UnsafeInteger,
            "{name}"
        );
    }
    let value = json::parse_duplicate_safe(&read_bytes("jcs/non-nfc.input.json")).unwrap();
    assert_eq!(
        expected("jcs/non-nfc.expected.reason.txt"),
        "non-nfc-string"
    );
    assert_eq!(
        json::canonicalize(&value).unwrap_err().reason,
        json::CanonicalizationReason::NonNfcString
    );
    assert_eq!(
        expected("jcs/duplicate-key.expected.reason.txt"),
        "duplicate-key"
    );
    assert!(matches!(
        json::parse_duplicate_safe(&read_bytes("jcs/duplicate-key.input.json")),
        Err(json::JsonParseError::DuplicateKey { .. })
    ));
}

fn verify_brief_literals(case: &str) {
    let raw = read_bytes(format!("{case}/input.json"));
    assert_eq!(
        json::sha256_raw_hex(&raw),
        expected(&format!("{case}/expected.sha256.txt")),
        "{case}: raw input sha256"
    );

    let parsed = ParsedManifest::parse(&raw).expect("brief fixture must parse");
    assert_eq!(
        json::canonicalize(parsed.as_value()).unwrap(),
        read_bytes(format!("{case}/expected.canonical.json")),
        "{case}: canonical input"
    );
    assert_eq!(
        json::sha256_jcs_hex(parsed.as_value()).unwrap(),
        expected(&format!("{case}/expected.sha256-jcs.txt")),
        "{case}: canonical input sha256"
    );

    let registry = load_brief_registry(case);
    let content = parsed
        .validate_shape()
        .expect("brief fixture must be shape valid")
        .validate_content(&registry, &load_bytes_map(case))
        .expect("brief fixture must be content valid");
    assert_eq!(
        content.canonical_tree_record_bytes(),
        read_bytes(format!("{case}/expected.tree-record.canonical.json")),
        "{case}: tree record canonical bytes"
    );
    assert_eq!(
        content.brief_tree_digest(),
        expected(&format!("{case}/expected.brief-tree-digest.txt")),
        "{case}: briefTreeDigest literal"
    );
}

#[test]
fn brief_golden_vectors_match_literal_reference() {
    verify_brief_literals("brief/valid-minimal");
    verify_brief_literals("brief/valid-schema-registry");

    // Literal briefTreeDigest oracle for every frozen tree-record field, plus
    // object-key and set-array ordering stability.
    let registry = load_brief_registry("brief/valid-schema-registry");
    let stable_digest = expected("brief/valid-minimal/expected.brief-tree-digest.txt");
    for name in list_dirs("brief/tree-digest-vectors") {
        let case = format!("brief/tree-digest-vectors/{name}");
        let raw = read_bytes(format!("{case}/input.json"));
        let content = ParsedManifest::parse(&raw)
            .unwrap_or_else(|error| panic!("{name} must parse: {error}"))
            .validate_shape()
            .unwrap_or_else(|error| panic!("{name} must be shape valid: {error}"))
            .validate_content(&registry, &load_bytes_map(&case))
            .unwrap_or_else(|error| panic!("{name} must be content valid: {error}"));
        let literal = expected(&format!("{case}/expected.brief-tree-digest.txt"));
        assert_eq!(
            content.brief_tree_digest(),
            literal,
            "{name}: briefTreeDigest literal mismatch"
        );
        if name == "object-key-order-shuffled" || name == "semantic-order-shuffled" {
            assert_eq!(literal, stable_digest, "{name} must be order stable");
        }
    }

    // Explicit positive coverage of the .env.example exception.
    let manifest =
        json::parse_duplicate_safe(&read_bytes("brief/valid-minimal/input.json")).unwrap();
    let has_env_example = manifest
        .get("files")
        .and_then(serde_json::Value::as_array)
        .unwrap()
        .iter()
        .any(|file| {
            file.get("path").and_then(serde_json::Value::as_str) == Some("plan/.env.example")
        });
    assert!(has_env_example);
}

fn verify_handoff_literals(case: &str, expect_impostor: bool) {
    let raw = read_bytes(format!("{case}/envelope.input.json"));
    assert_eq!(
        json::sha256_raw_hex(&raw),
        expected(&format!("{case}/expected.sha256.txt")),
        "{case}: raw envelope sha256"
    );

    let fixture = load_handoff_context(case);
    let parsed = ParsedEnvelope::parse(&raw).expect("handoff fixture must parse");
    assert_eq!(
        json::canonicalize(parsed.as_value()).unwrap(),
        read_bytes(format!("{case}/expected.canonical.json")),
        "{case}: canonical envelope"
    );
    assert_eq!(
        json::sha256_jcs_hex(parsed.as_value()).unwrap(),
        expected(&format!("{case}/expected.sha256-jcs.txt")),
        "{case}: canonical envelope sha256"
    );
    if expect_impostor {
        assert_eq!(
            parsed
                .as_value()
                .pointer("/producer/agentId")
                .and_then(serde_json::Value::as_str),
            Some("agent-impostor")
        );
    }

    let content = parsed
        .validate_shape()
        .expect("handoff fixture must be shape valid")
        .verify_content(&common::verification_context(&fixture))
        .expect("handoff fixture must be content verified");

    assert_eq!(
        content.declaration_digest(),
        expected(&format!("{case}/expected.declaration-digest.txt"))
    );
    assert_eq!(
        content.artifact_transfer_set_digest(),
        expected(&format!("{case}/expected.artifact-transfer-set-digest.txt"))
    );
    assert_eq!(
        content.idempotency_key(),
        expected(&format!("{case}/expected.idempotency-key.txt"))
    );
    assert_eq!(
        content.delivery_payload_digest(),
        expected(&format!("{case}/expected.delivery-payload-digest.txt"))
    );
    assert_eq!(
        content.envelope_sha256(),
        expected(&format!("{case}/expected.envelope-sha256.txt"))
    );
}

#[test]
fn handoff_golden_vectors_match_literal_reference() {
    verify_handoff_literals("handoff/valid-minimal", false);
    verify_handoff_literals("handoff/wrong-producer-valid", true);
    verify_handoff_literals("handoff/binding-order-reversed-valid", false);

    // Swapping only artifactBindings order must leave every semantic digest,
    // including envelopeSha256, equal to the valid-minimal literals.
    for digest_file in [
        "expected.declaration-digest.txt",
        "expected.artifact-transfer-set-digest.txt",
        "expected.idempotency-key.txt",
        "expected.delivery-payload-digest.txt",
        "expected.envelope-sha256.txt",
    ] {
        assert_eq!(
            expected(&format!(
                "handoff/binding-order-reversed-valid/{digest_file}"
            )),
            expected(&format!("handoff/valid-minimal/{digest_file}")),
            "{digest_file} must be order stable"
        );
    }
}

#[test]
fn brief_negative_fixture_matrix_has_positive_and_negative_pairs() {
    for name in list_dirs("brief/negative") {
        let raw = read_bytes(format!("brief/negative/{name}/input.json"));
        let expected_code = expected(&format!("brief/negative/{name}/expected.txt"));
        let parsed = match ParsedManifest::parse(&raw) {
            Ok(parsed) => parsed,
            Err(error) => {
                assert_eq!(error.code().as_str(), expected_code, "{name} parse error");
                continue;
            }
        };
        match parsed.validate_shape() {
            Err(error) => assert_eq!(error.code().as_str(), expected_code, "{name} shape error"),
            Ok(shape) => {
                let bytes = load_bytes_map(format!("brief/negative/{name}"));
                let error = shape
                    .validate_content(&load_brief_registry("brief/valid-schema-registry"), &bytes)
                    .expect_err("negative brief fixture must fail by content verification");
                assert_eq!(error.code().as_str(), expected_code, "{name} content error");
            }
        }
    }
}

#[test]
fn handoff_negative_fixture_matrix_has_positive_and_negative_pairs() {
    let valid = load_handoff_context("handoff/valid-minimal");
    for name in list_dirs("handoff/negative") {
        let raw = read_bytes(format!("handoff/negative/{name}/envelope.input.json"));
        let expected_code = expected(&format!("handoff/negative/{name}/expected.txt"));
        let parsed = match ParsedEnvelope::parse(&raw) {
            Ok(parsed) => parsed,
            Err(error) => {
                assert_eq!(error.code().as_str(), expected_code, "{name} parse error");
                continue;
            }
        };
        let shape = match parsed.validate_shape() {
            Ok(shape) => shape,
            Err(error) => {
                assert_eq!(error.code().as_str(), expected_code, "{name} shape error");
                continue;
            }
        };

        let declaration_path =
            golden_dir().join(format!("handoff/negative/{name}/declaration.input.json"));
        let declaration = if declaration_path.exists() {
            DeliveryDeclaration::parse(&read_bytes(format!(
                "handoff/negative/{name}/declaration.input.json"
            )))
            .expect("negative declaration fixture must parse")
        } else {
            valid.declaration.clone()
        };
        let context = HandoffVerificationContext::new(&valid.cas, &valid.registry, &declaration);
        let error = shape
            .verify_content(&context)
            .expect_err("negative handoff fixture must fail");
        assert_eq!(error.code().as_str(), expected_code, "{name} content error");
    }
}

// Keep rustc aware of the typed states used by golden tests even when test
// ordering is filtered.
#[allow(dead_code)]
fn typed_state_types(
    shape: ShapeValidatedManifest,
    envelope: ShapeValidatedEnvelope<
        agenttalk_orchestration_contracts::handoff::AuthorityUnchecked,
    >,
) {
    let _ = (shape, envelope);
}
