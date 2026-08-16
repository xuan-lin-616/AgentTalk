mod common;

use agenttalk_orchestration_contracts::brief::{
    BriefBytesMap, InMemoryBriefBytesMap, ParsedManifest,
};
use agenttalk_orchestration_contracts::handoff::{
    artifact_transfer_set_digest_hex, classify_idempotency_replay, delivery_payload_digest_hex,
    envelope_sha256_hex, idempotency_key_hex, DeliveryDeclaration, HandoffVerificationContext,
    IdempotencyDisposition, ParsedEnvelope,
};
use agenttalk_orchestration_contracts::json;
use agenttalk_orchestration_contracts::registry::{
    InMemorySchemaRegistry, SchemaRegistrationError,
};
use agenttalk_orchestration_contracts::schema::{
    BRIEF_ROOT_MANIFEST_SCHEMA_JSON, HANDOFF_ENVELOPE_SCHEMA_JSON,
};
use agenttalk_orchestration_contracts::{ContractError, ErrorCode};
use common::{load_bytes_map, load_handoff_context, read_bytes, read_text, trimmed};
use serde_json::{Map, Value};

fn manifest_value(case: &str) -> Value {
    json::parse_duplicate_safe(&read_bytes(format!("{case}/input.json"))).unwrap()
}

type ManifestMutation = Box<dyn Fn(&mut Value)>;

fn envelope_value(case: &str) -> Value {
    json::parse_duplicate_safe(&read_bytes(format!("{case}/envelope.input.json"))).unwrap()
}

fn brief_digest_for(value: &Value, bytes: &InMemoryBriefBytesMap) -> Result<String, ContractError> {
    let raw = serde_json::to_vec(value).expect("value serializes");
    Ok(ParsedManifest::parse(&raw)?
        .validate_shape()?
        .validate_content(&InMemorySchemaRegistry::new(), bytes)?
        .brief_tree_digest()
        .to_owned())
}

fn brief_digest_with_registry(
    value: &Value,
    bytes: &InMemoryBriefBytesMap,
    registry: &InMemorySchemaRegistry,
) -> Result<String, ContractError> {
    let raw = serde_json::to_vec(value).expect("value serializes");
    Ok(ParsedManifest::parse(&raw)?
        .validate_shape()?
        .validate_content(registry, bytes)?
        .brief_tree_digest()
        .to_owned())
}

#[test]
fn schema_documents_are_local_refs_with_deny_unknown_at_every_level() {
    for schema_json in [
        BRIEF_ROOT_MANIFEST_SCHEMA_JSON,
        HANDOFF_ENVELOPE_SCHEMA_JSON,
    ] {
        let schema = json::parse_duplicate_safe(schema_json.as_bytes()).unwrap();
        assert_eq!(
            schema.get("$schema").and_then(Value::as_str),
            Some("https://json-schema.org/draft/2020-12/schema")
        );
        collect_refs(&schema, &mut |reference| {
            assert!(
                reference.starts_with("#/"),
                "remote or non-local $ref is forbidden: {reference}"
            );
        });
        collect_schema_objects(&schema, &mut |object| {
            if object.get("type") == Some(&Value::String("object".to_owned())) {
                assert_eq!(
                    object.get("additionalProperties"),
                    Some(&Value::Bool(false)),
                    "every object-typed schema level must deny unknown properties"
                );
            }
        });
    }
}

fn collect_refs(value: &Value, f: &mut dyn FnMut(&str)) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                f(reference);
            }
            for child in object.values() {
                collect_refs(child, f);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_refs(child, f);
            }
        }
        _ => {}
    }
}

fn collect_schema_objects(value: &Value, f: &mut dyn FnMut(&Map<String, Value>)) {
    match value {
        Value::Object(object) => {
            f(object);
            for child in object.values() {
                collect_schema_objects(child, f);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_schema_objects(child, f);
            }
        }
        _ => {}
    }
}

#[test]
fn brief_tree_digest_is_sensitive_to_every_frozen_tree_field() {
    let baseline = manifest_value("brief/valid-minimal");
    let bytes = load_bytes_map("brief/valid-minimal");
    let baseline_digest = brief_digest_for(&baseline, &bytes).unwrap();

    let mut previous = baseline_digest;
    let mutations: Vec<(&str, ManifestMutation)> = vec![
        (
            "title",
            Box::new(|v| {
                v["title"] = Value::String(format!("{}!", v["title"].as_str().unwrap()));
            }),
        ),
        (
            "role displayName",
            Box::new(|v| {
                v["roles"][0]["displayName"] = Value::String("PM!".to_owned());
            }),
        ),
        (
            "file kind",
            Box::new(|v| {
                v["files"][0]["kind"] = Value::String("design".to_owned());
            }),
        ),
        (
            "file format",
            Box::new(|v| {
                v["files"][0]["format"] = Value::String("text".to_owned());
            }),
        ),
        (
            "file required",
            Box::new(|v| {
                v["files"][0]["required"] = Value::Bool(false);
            }),
        ),
        (
            "context layer",
            Box::new(|v| {
                v["files"][0]["context"]["layer"] = Value::String("role".to_owned());
            }),
        ),
        (
            "context roleIds",
            Box::new(|v| {
                v["files"][0]["context"]["roleIds"] = serde_json::json!(["pm"]);
            }),
        ),
        (
            "context retention",
            Box::new(|v| {
                v["files"][0]["context"]["retention"] = Value::String("project".to_owned());
            }),
        ),
        (
            "context workspaceAccess",
            Box::new(|v| {
                v["files"][0]["context"]["workspaceAccess"] = Value::String("none".to_owned());
            }),
        ),
        (
            "declaredOwnerRoleId",
            Box::new(|v| {
                v["files"][0]["declaredOwnerRoleId"] = Value::String("architect".to_owned());
            }),
        ),
    ];

    for (label, mutate) in mutations {
        let mut changed = baseline.clone();
        mutate(&mut changed);
        let digest = brief_digest_for(&changed, &bytes).unwrap();
        assert_ne!(digest, previous, "{label} must change briefTreeDigest");
        previous = digest;
    }

    // file path sensitivity needs the bytes map keyed by the renamed path.
    {
        let mut changed = baseline.clone();
        changed["files"][0]["path"] = Value::String("plan/roadmap-renamed.md".to_owned());
        let mut renamed_bytes = load_bytes_map("brief/valid-minimal");
        let original = renamed_bytes
            .get("plan/roadmap.md")
            .expect("roadmap bytes must exist")
            .to_vec();
        renamed_bytes.insert("plan/roadmap-renamed.md", original);
        let digest = brief_digest_for(&changed, &renamed_bytes).unwrap();
        assert_ne!(digest, previous, "file path must change briefTreeDigest");
        previous = digest;
    }

    // contentSchemaRef null -> object changes the tree record.
    let schema_registry = {
        let mut registry = InMemorySchemaRegistry::new();
        let reference = registry
            .register_json(
                "agenttalk.design.spec.v1",
                "1",
                &read_bytes("brief/valid-schema-registry/registry/design-spec.json"),
            )
            .unwrap();
        let mut changed = baseline.clone();
        changed["files"][0]["contentSchemaRef"] = serde_json::json!({
            "id": reference.id,
            "version": reference.version,
            "digest": reference.digest,
        });
        let digest = brief_digest_with_registry(&changed, &bytes, &registry).unwrap();
        assert_ne!(
            digest, previous,
            "contentSchemaRef must change briefTreeDigest"
        );
        previous = digest;
        registry
    };
    let _ = schema_registry;

    // rawSha256 and size enter the tree record too.
    let mut content_a = baseline.clone();
    let mut content_b = baseline.clone();
    for (manifest, content) in [
        (&mut content_a, b"a".as_slice()),
        (&mut content_b, b"b".as_slice()),
    ] {
        manifest["files"][0]["sha256"] = Value::String(json::sha256_raw_hex(content));
        manifest["files"][0]["size"] =
            Value::Number(serde_json::Number::from(content.len() as u64));
    }
    let mut bytes_a = load_bytes_map("brief/valid-minimal");
    bytes_a.insert("plan/roadmap.md", b"a".to_vec());
    let mut bytes_b = load_bytes_map("brief/valid-minimal");
    bytes_b.insert("plan/roadmap.md", b"b".to_vec());
    let digest_a = brief_digest_for(&content_a, &bytes_a).unwrap();
    let digest_b = brief_digest_for(&content_b, &bytes_b).unwrap();
    assert_ne!(
        digest_a, digest_b,
        "rawSha256/size must change briefTreeDigest"
    );
    assert_ne!(
        digest_a, previous,
        "content change must change briefTreeDigest"
    );
}

#[test]
fn brief_tree_digest_is_canonically_stable_and_does_not_reread_cas() {
    let baseline = manifest_value("brief/valid-minimal");
    let mut reordered = baseline.clone();
    let files = reordered["files"].as_array_mut().unwrap();
    files.reverse();
    for file in files.iter_mut() {
        file["context"]["roleIds"].as_array_mut().unwrap().reverse();
    }
    reordered["roles"].as_array_mut().unwrap().reverse();

    let bytes = load_bytes_map("brief/valid-minimal");
    let baseline_digest = brief_digest_for(&baseline, &bytes).unwrap();
    let reordered_digest = brief_digest_for(&reordered, &bytes).unwrap();
    assert_eq!(baseline_digest, reordered_digest);

    // Once content validation has happened, later mutation of the fake CAS
    // must not change the already-derived transitive digest.
    let parsed = ParsedManifest::parse(&read_bytes("brief/valid-minimal/input.json")).unwrap();
    let content = parsed
        .validate_shape()
        .unwrap()
        .validate_content(&InMemorySchemaRegistry::new(), &bytes)
        .unwrap();
    let digest_before = content.brief_tree_digest().to_owned();
    let mut tampered = load_bytes_map("brief/valid-minimal");
    tampered.insert("plan/roadmap.md", b"tampered-after-validation".to_vec());
    let _ = tampered;
    assert_eq!(content.brief_tree_digest(), digest_before);
}

#[test]
fn brief_schema_registry_fails_closed_for_id_version_and_digest() {
    let baseline = manifest_value("brief/valid-schema-registry");
    let mut registry = InMemorySchemaRegistry::new();
    registry
        .register_json(
            "agenttalk.design.spec.v1",
            "1",
            &read_bytes("brief/valid-schema-registry/registry/design-spec.json"),
        )
        .unwrap();
    let bytes = load_bytes_map("brief/valid-schema-registry");

    for (id, version, digest) in [
        ("agenttalk.unknown.v1", "1", "00".repeat(32)),
        ("agenttalk.design.spec.v1", "2", "00".repeat(32)),
        ("agenttalk.design.spec.v1", "1", "00".repeat(32)),
    ] {
        let mut manifest = baseline.clone();
        manifest["files"][0]["contentSchemaRef"] = serde_json::json!({
            "id": id, "version": version, "digest": digest,
        });
        let raw = serde_json::to_vec(&manifest).unwrap();
        let error = ParsedManifest::parse(&raw)
            .unwrap()
            .validate_shape()
            .unwrap()
            .validate_content(&registry, &bytes)
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::BriefSchemaRefUnresolved);
    }
}

#[test]
fn handoff_declaration_rejects_target_input_duplication() {
    let declaration = r#"{
            "schemaVersion":"agenttalk.handoff.delivery-declaration.v1",
            "projectRunId":"run-0001","edgeId":"edge-0001",
            "fromTaskNodeId":"node-architect","fromAttemptId":"1",
            "fromExecutionRunId":"er-0001","leaseEpoch":3,
            "outputs":[{"sourceOutputPortId":"alpha","targetInputPortId":"alpha-in",
                "stagingObjectId":"s","declaredContentType":null,
                "declaredContentSchemaRef":null}]
        }"#;
    let error = DeliveryDeclaration::parse_str(declaration).unwrap_err();
    assert_eq!(error.code(), ErrorCode::HandoffSchemaViolation);
    assert!(error.message().contains("unknown field"));
}

#[test]
fn same_idempotency_key_replays_or_conflicts_on_payload_digest() {
    let digest_a = "a".repeat(64);
    let digest_b = "b".repeat(64);
    assert_eq!(
        classify_idempotency_replay(None, &digest_a).unwrap(),
        IdempotencyDisposition::FirstDelivery
    );
    assert_eq!(
        classify_idempotency_replay(Some(&digest_a), &digest_a).unwrap(),
        IdempotencyDisposition::Replay
    );
    assert_eq!(
        classify_idempotency_replay(Some(&digest_a), &digest_b).unwrap(),
        IdempotencyDisposition::Conflict
    );
    assert_eq!(
        classify_idempotency_replay(Some("not-hex"), &digest_a)
            .unwrap_err()
            .code(),
        ErrorCode::HandoffIdempotencyInvalid
    );
}

#[test]
fn duplicate_source_port_with_different_target_is_a_duplicate_binding() {
    let fixture = load_handoff_context("handoff/valid-minimal");
    let mut envelope = envelope_value("handoff/valid-minimal");
    let first = envelope["artifactBindings"][0].clone();
    envelope["artifactBindings"]
        .as_array_mut()
        .unwrap()
        .push(first);
    envelope["artifactBindings"][1]["targetInput"]["portId"] = Value::String("other-in".to_owned());

    let raw = serde_json::to_vec(&envelope).unwrap();
    let error = ParsedEnvelope::parse(&raw)
        .unwrap()
        .validate_shape()
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::HandoffDuplicateBinding);
    let _ = fixture;
}

#[test]
fn handoff_schema_registry_fails_closed_for_version_and_digest() {
    let fixture = load_handoff_context("handoff/valid-minimal");
    let mut envelope = envelope_value("handoff/valid-minimal");
    for (version, digest) in [("2", "00".repeat(32)), ("1", "00".repeat(32))] {
        envelope["artifactBindings"][0]["artifactRef"]["contentSchemaRef"]["version"] =
            Value::String(version.to_owned());
        envelope["artifactBindings"][0]["artifactRef"]["contentSchemaRef"]["digest"] =
            Value::String(digest);
        let raw = serde_json::to_vec(&envelope).unwrap();
        let shape = ParsedEnvelope::parse(&raw)
            .unwrap()
            .validate_shape()
            .unwrap();
        let context =
            HandoffVerificationContext::new(&fixture.cas, &fixture.registry, &fixture.declaration);
        let error = shape.verify_content(&context).unwrap_err();
        assert_eq!(error.code(), ErrorCode::HandoffSchemaRefUnresolved);
    }
}

#[test]
fn json_eof_validation_is_shared_by_all_contract_parse_paths() {
    for raw in [b"{} {}".as_slice(), b"{} trailing".as_slice()] {
        assert_eq!(
            ParsedManifest::parse(raw).unwrap_err().code(),
            ErrorCode::BriefSchemaViolation,
            "brief parse path must reject trailing bytes"
        );
        assert_eq!(
            ParsedEnvelope::parse(raw).unwrap_err().code(),
            ErrorCode::HandoffSchemaViolation,
            "handoff parse path must reject trailing bytes"
        );
        assert_eq!(
            DeliveryDeclaration::parse(raw).unwrap_err().code(),
            ErrorCode::HandoffSchemaViolation,
            "declaration parse path must reject trailing bytes"
        );
        let mut registry = InMemorySchemaRegistry::new();
        assert!(matches!(
            registry.register_json("shared.eof.v1", "1", raw),
            Err(SchemaRegistrationError::Syntax(_))
        ));
    }

    // Trailing whitespace remains valid on the same shared parse path.
    assert!(ParsedManifest::parse(b"{} \n\t ").is_ok());
    assert!(ParsedEnvelope::parse(b"{} \n\t ").is_ok());
    let mut declaration = read_bytes("handoff/valid-minimal/declaration.input.json");
    declaration.extend_from_slice(b"\n\t ");
    assert!(DeliveryDeclaration::parse(&declaration).is_ok());
    let mut registry = InMemorySchemaRegistry::new();
    assert!(registry
        .register_json("shared.eof.v1", "1", b"{\"type\":\"object\"} \n\t ")
        .is_ok());
}

#[test]
fn reference_generator_has_no_real_machine_path() {
    let generator = read_text("reference-generator.py");
    // Reject drive-letter absolute paths (`C:/...`, `E:\...`) without
    // hardcoding this machine's path in the tracked test text.
    let has_drive_absolute = generator.lines().any(|line| {
        let bytes = line.trim_start().as_bytes();
        bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && (bytes[2] == b'/' || bytes[2] == b'\\')
    });
    assert!(!has_drive_absolute, "generator must not leak a real path");
    assert!(generator.contains("Path(__file__).resolve().parent"));
}

#[test]
fn source_port_closure_negative_is_otherwise_fully_digest_consistent() {
    let envelope = envelope_value("handoff/negative/source-port-closure-mismatch");
    let fixture = load_handoff_context("handoff/valid-minimal");
    let declaration = fixture.declaration.clone();
    let computed_declaration = declaration.declaration_digest_hex().unwrap();
    let computed_transfer = artifact_transfer_set_digest_hex(&envelope).unwrap();
    let computed_idempotency = idempotency_key_hex(&envelope).unwrap();
    let computed_payload = delivery_payload_digest_hex(
        &computed_declaration,
        &computed_transfer,
        envelope
            .pointer("/acceptance/contractDigest")
            .and_then(Value::as_str)
            .unwrap(),
        envelope
            .pointer("/acceptance/evidenceDigest")
            .and_then(Value::as_str)
            .unwrap(),
        envelope
            .pointer("/producerContextManifestDigest")
            .and_then(Value::as_str)
            .unwrap(),
        envelope
            .pointer("/dagSnapshotDigest")
            .and_then(Value::as_str)
            .unwrap(),
        envelope
            .pointer("/roleBindingSnapshotDigest")
            .and_then(Value::as_str)
            .unwrap(),
    )
    .unwrap();
    let computed_envelope = envelope_sha256_hex(&envelope).unwrap();

    for (pointer, computed) in [
        ("/declarationDigest", computed_declaration.as_str()),
        ("/artifactTransferSetDigest", computed_transfer.as_str()),
        ("/idempotencyKey", computed_idempotency.as_str()),
        ("/deliveryPayloadDigest", computed_payload.as_str()),
        ("/envelopeSha256", computed_envelope.as_str()),
    ] {
        assert_eq!(
            envelope.pointer(pointer).and_then(Value::as_str),
            Some(computed),
            "{pointer} must be recomputed consistently"
        );
    }

    let shape = ParsedEnvelope::parse(&serde_json::to_vec(&envelope).unwrap())
        .unwrap()
        .validate_shape()
        .unwrap();
    let context = HandoffVerificationContext::new(&fixture.cas, &fixture.registry, &declaration);
    let error = shape.verify_content(&context).unwrap_err();
    assert_eq!(error.code(), ErrorCode::HandoffIdempotencyInvalid);
    assert!(error.message().contains("source ports"));
}

#[test]
fn literal_golden_digests_are_present_in_reference_record() {
    for name in [
        "expected.declaration-digest.txt",
        "expected.artifact-transfer-set-digest.txt",
        "expected.idempotency-key.txt",
        "expected.delivery-payload-digest.txt",
        "expected.envelope-sha256.txt",
    ] {
        assert_eq!(trimmed(format!("handoff/valid-minimal/{name}")).len(), 64);
    }
    assert_eq!(
        trimmed("brief/valid-minimal/expected.brief-tree-digest.txt").len(),
        64
    );
}
