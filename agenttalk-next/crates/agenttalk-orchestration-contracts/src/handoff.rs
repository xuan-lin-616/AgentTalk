//! Handoff envelope contract: shape -> content verification.
//!
//! `declarationDigest` is a digest of the Provider's attempt-scoped staging
//! claim, which is **not** duplicated inside the envelope. Content
//! verification therefore requires the parsed [`DeliveryDeclaration`]
//! alongside the envelope; the envelope alone does not contain
//! `stagingObjectId`, `declaredContentType`, or
//! `declaredContentSchemaRef`.
//!
//! Journal authority (producer/consumer authority, lease fencing, receipt
//! replay, `HANDOFF_STALE_LEASE`, and construction of
//! [`JournalAuthorizedEnvelope`]) is intentionally deferred to Core journal.

use crate::error::{ContractError, ErrorCode};
use crate::json::{self, utf16_order};
use crate::registry::{SchemaReference, SchemaRegistry};
use crate::schema;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;

const DECLARATION_SCHEMA_VERSION: &str = "agenttalk.handoff.delivery-declaration.v1";
const DECLARATION_ALLOWED_KEYS: [&str; 8] = [
    "schemaVersion",
    "projectRunId",
    "edgeId",
    "fromTaskNodeId",
    "fromAttemptId",
    "fromExecutionRunId",
    "leaseEpoch",
    "outputs",
];
const DECLARATION_OUTPUT_ALLOWED_KEYS: [&str; 4] = [
    "sourceOutputPortId",
    "stagingObjectId",
    "declaredContentType",
    "declaredContentSchemaRef",
];
const SCHEMA_REF_ALLOWED_KEYS: [&str; 3] = ["id", "version", "digest"];

/// Attempt-scoped Provider staging claim described by the frozen
/// `declarationDigest` formula.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryDeclaration {
    project_run_id: String,
    edge_id: String,
    from_task_node_id: String,
    from_attempt_id: String,
    from_execution_run_id: String,
    lease_epoch: u64,
    outputs: Vec<DeclarationOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclarationOutput {
    pub source_output_port_id: String,
    pub staging_object_id: String,
    pub declared_content_type: Option<String>,
    pub declared_content_schema_ref: Option<SchemaReference>,
}

impl DeliveryDeclaration {
    pub fn parse(bytes: &[u8]) -> Result<Self, ContractError> {
        let value = json::parse_duplicate_safe(bytes).map_err(handoff_parse_error)?;
        let object = value.as_object().ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "declaration root must be an object",
            )
        })?;

        reject_unknown_keys(object, &DECLARATION_ALLOWED_KEYS)?;

        let schema_version = required_string(object, "schemaVersion")?;
        if schema_version != DECLARATION_SCHEMA_VERSION {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                format!("declaration schemaVersion must be {DECLARATION_SCHEMA_VERSION}"),
            ));
        }

        let project_run_id = required_string(object, "projectRunId")?.to_owned();
        let edge_id = required_string(object, "edgeId")?.to_owned();
        let from_task_node_id = required_string(object, "fromTaskNodeId")?.to_owned();
        let from_attempt_id = required_string(object, "fromAttemptId")?.to_owned();
        let from_execution_run_id = required_string(object, "fromExecutionRunId")?.to_owned();
        let lease_epoch = json::value_as_safe_u64(object.get("leaseEpoch").ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "declaration leaseEpoch is required",
            )
        })?)
        .ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "declaration leaseEpoch must be a non-negative safe integer",
            )
        })?;

        let raw_outputs = object
            .get("outputs")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ContractError::new(
                    ErrorCode::HandoffSchemaViolation,
                    "declaration outputs must be an array",
                )
            })?;

        let mut outputs = Vec::with_capacity(raw_outputs.len());
        let mut seen_ports = HashSet::with_capacity(raw_outputs.len());
        for output in raw_outputs {
            let output = parse_declaration_output(output)?;
            if !seen_ports.insert(output.source_output_port_id.clone()) {
                return Err(ContractError::new(
                    ErrorCode::HandoffDuplicateBinding,
                    format!(
                        "duplicate source output in declaration outputs: {}",
                        output.source_output_port_id
                    ),
                ));
            }
            outputs.push(output);
        }
        outputs.sort_by(|left, right| {
            utf16_order(&left.source_output_port_id, &right.source_output_port_id)
        });

        Ok(Self {
            project_run_id,
            edge_id,
            from_task_node_id,
            from_attempt_id,
            from_execution_run_id,
            lease_epoch,
            outputs,
        })
    }

    pub fn parse_str(json: &str) -> Result<Self, ContractError> {
        Self::parse(json.as_bytes())
    }

    #[must_use]
    pub fn project_run_id(&self) -> &str {
        &self.project_run_id
    }

    #[must_use]
    pub fn edge_id(&self) -> &str {
        &self.edge_id
    }

    #[must_use]
    pub fn from_task_node_id(&self) -> &str {
        &self.from_task_node_id
    }

    #[must_use]
    pub fn from_attempt_id(&self) -> &str {
        &self.from_attempt_id
    }

    #[must_use]
    pub fn from_execution_run_id(&self) -> &str {
        &self.from_execution_run_id
    }

    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }

    #[must_use]
    pub fn outputs(&self) -> &[DeclarationOutput] {
        &self.outputs
    }

    /// Frozen `declarationDigest = sha256Jcs(...)`.
    pub fn declaration_digest(&self) -> Result<[u8; 32], ContractError> {
        let outputs = self
            .outputs
            .iter()
            .map(|output| {
                let declared_content_schema_ref = output
                    .declared_content_schema_ref
                    .as_ref()
                    .map_or(Value::Null, |schema_reference| {
                        schema_reference_value(schema_reference)
                    });
                Value::Object(Map::from_iter([
                    (
                        "sourceOutputPortId".to_owned(),
                        Value::String(output.source_output_port_id.clone()),
                    ),
                    (
                        "stagingObjectId".to_owned(),
                        Value::String(output.staging_object_id.clone()),
                    ),
                    (
                        "declaredContentType".to_owned(),
                        output
                            .declared_content_type
                            .clone()
                            .map_or(Value::Null, Value::String),
                    ),
                    (
                        "declaredContentSchemaRef".to_owned(),
                        declared_content_schema_ref,
                    ),
                ]))
            })
            .collect::<Vec<Value>>();

        let record = Value::Object(Map::from_iter([
            (
                "schemaVersion".to_owned(),
                Value::String(DECLARATION_SCHEMA_VERSION.to_owned()),
            ),
            (
                "projectRunId".to_owned(),
                Value::String(self.project_run_id.clone()),
            ),
            ("edgeId".to_owned(), Value::String(self.edge_id.clone())),
            (
                "fromTaskNodeId".to_owned(),
                Value::String(self.from_task_node_id.clone()),
            ),
            (
                "fromAttemptId".to_owned(),
                Value::String(self.from_attempt_id.clone()),
            ),
            (
                "fromExecutionRunId".to_owned(),
                Value::String(self.from_execution_run_id.clone()),
            ),
            (
                "leaseEpoch".to_owned(),
                Value::Number(serde_json::Number::from(self.lease_epoch)),
            ),
            ("outputs".to_owned(), Value::Array(outputs)),
        ]));

        json::sha256_jcs(&record).map_err(|error| {
            ContractError::new(ErrorCode::HandoffCanonicalEncoding, error.to_string())
        })
    }

    /// Lowercase hex `declarationDigest`.
    pub fn declaration_digest_hex(&self) -> Result<String, ContractError> {
        Ok(json::encode_hex(&self.declaration_digest()?))
    }
}

fn parse_declaration_output(output: &Value) -> Result<DeclarationOutput, ContractError> {
    let object = output.as_object().ok_or_else(|| {
        ContractError::new(
            ErrorCode::HandoffSchemaViolation,
            "declaration output must be an object",
        )
    })?;
    reject_unknown_keys(object, &DECLARATION_OUTPUT_ALLOWED_KEYS)?;
    let source_output_port_id = required_string(object, "sourceOutputPortId")?.to_owned();
    let staging_object_id = required_string(object, "stagingObjectId")?.to_owned();
    for required_key in ["declaredContentType", "declaredContentSchemaRef"] {
        if !object.contains_key(required_key) {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                format!("declaration output is missing required field {required_key}"),
            ));
        }
    }
    let declared_content_type = match object.get("declaredContentType") {
        Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.clone()),
        None => {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "declaration output is missing required field declaredContentType",
            ));
        }
        Some(_) => {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "declaredContentType must be null or a string",
            ));
        }
    };
    let declared_content_schema_ref = match object.get("declaredContentSchemaRef") {
        Some(Value::Null) => None,
        Some(Value::Object(object)) => Some(parse_schema_reference(object)?),
        None => {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "declaration output is missing required field declaredContentSchemaRef",
            ));
        }
        Some(_) => {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "declaredContentSchemaRef must be null or an object",
            ));
        }
    };
    Ok(DeclarationOutput {
        source_output_port_id,
        staging_object_id,
        declared_content_type,
        declared_content_schema_ref,
    })
}

fn parse_schema_reference(object: &Map<String, Value>) -> Result<SchemaReference, ContractError> {
    reject_unknown_keys(object, &SCHEMA_REF_ALLOWED_KEYS)?;
    Ok(SchemaReference::new(
        required_string(object, "id")?,
        required_string(object, "version")?,
        required_string(object, "digest")?,
    ))
}

fn reject_unknown_keys(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), ContractError> {
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                format!("unknown field in delivery declaration: {key}"),
            ));
        }
    }
    Ok(())
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ContractError> {
    object.get(key).and_then(Value::as_str).ok_or_else(|| {
        ContractError::new(
            ErrorCode::HandoffSchemaViolation,
            format!("{key} must be a string"),
        )
    })
}

/// Authority is a type-level marker, not a runtime boolean.
///
/// ```compile_fail
/// use agenttalk_orchestration_contracts::handoff::{AuthorityVerified, JournalAuthorizedEnvelope};
///
/// fn forge_authority() {
///     // No public constructor exists; C1 cannot fabricate this wrapper.
///     let _ = JournalAuthorizedEnvelope::<AuthorityVerified> {
///         envelope: todo!(),
///     };
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorityUnchecked {
    _private: (),
}

/// Marker for the future Core journal authority layer. This crate exposes no
/// constructor for it and no constructor for
/// [`JournalAuthorizedEnvelope`]`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorityVerified {
    _private: (),
}

/// Duplicate-key-safe parsed handoff envelope.
#[derive(Clone, Debug)]
pub struct ParsedEnvelope {
    value: Value,
}

impl ParsedEnvelope {
    pub fn parse(bytes: &[u8]) -> Result<Self, ContractError> {
        match json::parse_duplicate_safe(bytes) {
            Ok(value) => Ok(Self { value }),
            Err(json::JsonParseError::DuplicateKey { path }) => Err(ContractError::new(
                ErrorCode::HandoffDuplicateKey,
                format!("duplicate object key at {path}"),
            )),
            Err(other) => Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                other.to_string(),
            )),
        }
    }

    pub fn parse_str(json: &str) -> Result<Self, ContractError> {
        Self::parse(json.as_bytes())
    }

    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.value
    }

    pub fn validate_shape(
        self,
    ) -> Result<ShapeValidatedEnvelope<AuthorityUnchecked>, ContractError> {
        if let Some(message) = schema::first_schema_error(schema::handoff_validator(), &self.value)
        {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                message,
            ));
        }

        validate_allowed_consumers(&self.value)?;
        validate_bindings(&self.value)?;

        json::canonicalize(&self.value).map_err(|error| {
            ContractError::new(ErrorCode::HandoffCanonicalEncoding, error.to_string())
        })?;

        Ok(ShapeValidatedEnvelope {
            value: self.value,
            authority: PhantomData,
        })
    }
}

/// Schema- and semantic-shape-validated envelope.
#[derive(Clone, Debug)]
pub struct ShapeValidatedEnvelope<A> {
    value: Value,
    authority: PhantomData<A>,
}

impl<A> ShapeValidatedEnvelope<A> {
    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.value
    }
}

impl ShapeValidatedEnvelope<AuthorityUnchecked> {
    /// Re-verify CAS objects and every frozen derived digest. The caller must
    /// also supply the Provider staging declaration because the envelope
    /// deliberately does not duplicate staging claims.
    pub fn verify_content(
        self,
        context: &HandoffVerificationContext<'_>,
    ) -> Result<ContentVerifiedEnvelope<AuthorityUnchecked>, ContractError> {
        verify_envelope_content(&self.value, context)?;
        let derived = DerivedDigests::compute(&self.value, context.declaration)?;

        Ok(ContentVerifiedEnvelope {
            value: self.value,
            derived,
            authority: PhantomData,
        })
    }
}

/// Content-verified envelope. This state is still `AuthorityUnchecked`;
/// scheduler/context-assembler admission requires a future
/// `JournalAuthorizedEnvelope<AuthorityVerified>`.
#[derive(Clone, Debug)]
pub struct ContentVerifiedEnvelope<A> {
    value: Value,
    derived: DerivedDigests,
    authority: PhantomData<A>,
}

impl<A> ContentVerifiedEnvelope<A> {
    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.value
    }

    #[must_use]
    pub fn envelope_sha256(&self) -> &str {
        &self.derived.envelope_sha256
    }

    #[must_use]
    pub fn declaration_digest(&self) -> &str {
        &self.derived.declaration_digest
    }

    #[must_use]
    pub fn artifact_transfer_set_digest(&self) -> &str {
        &self.derived.artifact_transfer_set_digest
    }

    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.derived.idempotency_key
    }

    #[must_use]
    pub fn delivery_payload_digest(&self) -> &str {
        &self.derived.delivery_payload_digest
    }
}

/// Zero-constructor type-level placeholder for the Core journal authority
/// layer. C1 deliberately cannot build one; future Core code will add the
/// journal-gated constructor.
pub struct JournalAuthorizedEnvelope<A> {
    #[allow(dead_code)] // C1 placeholder; the field becomes journal-gated API in Core.
    envelope: ContentVerifiedEnvelope<A>,
}

#[doc(hidden)]
mod scheduler_seal {
    pub trait Sealed {}
}

/// Compile-time scheduler gate. It is sealed so downstream code cannot
/// implement it for `ContentVerifiedEnvelope<AuthorityUnchecked>`.
///
/// ```compile_fail
/// use agenttalk_orchestration_contracts::handoff::{
///     AuthorityUnchecked, ContentVerifiedEnvelope, SchedulerReady,
/// };
///
/// fn assert_scheduler_ready<T: SchedulerReady>() {}
///
/// fn wrong_producer_cannot_enter_scheduler() {
///     assert_scheduler_ready::<ContentVerifiedEnvelope<AuthorityUnchecked>>();
/// }
/// ```
///
/// `JournalAuthorizedEnvelope<AuthorityUnchecked>` is equally excluded; only
/// the future Core journal layer may wrap an envelope in `AuthorityVerified`.
///
/// ```compile_fail
/// use agenttalk_orchestration_contracts::handoff::{
///     AuthorityUnchecked, JournalAuthorizedEnvelope, SchedulerReady,
/// };
///
/// fn assert_scheduler_ready<T: SchedulerReady>() {}
///
/// fn unauthorized_journal_wrapper_cannot_enter_scheduler() {
///     assert_scheduler_ready::<JournalAuthorizedEnvelope<AuthorityUnchecked>>();
/// }
/// ```
#[allow(private_bounds)]
pub trait SchedulerReady: scheduler_seal::Sealed {}

impl scheduler_seal::Sealed for JournalAuthorizedEnvelope<AuthorityVerified> {}
impl SchedulerReady for JournalAuthorizedEnvelope<AuthorityVerified> {}

#[cfg(test)]
mod authority_tests {
    use super::*;

    #[test]
    fn only_authority_verified_journal_wrapper_is_scheduler_ready() {
        fn assert_scheduler_ready<T: SchedulerReady>() {}
        assert_scheduler_ready::<JournalAuthorizedEnvelope<AuthorityVerified>>();
    }
}

/// Pure in-memory CAS abstraction used by C1 verification.
pub trait ObjectStore {
    fn get(&self, object_ref: &str) -> Option<&[u8]>;
}

/// Fake CAS for fixtures and unit tests. Production Core CAS is deferred.
#[derive(Clone, Debug, Default)]
pub struct InMemoryObjectStore {
    objects: HashMap<String, Vec<u8>>,
}

impl InMemoryObjectStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, object_ref: impl Into<String>, bytes: impl Into<Vec<u8>>) {
        self.objects.insert(object_ref.into(), bytes.into());
    }
}

impl ObjectStore for InMemoryObjectStore {
    fn get(&self, object_ref: &str) -> Option<&[u8]> {
        self.objects.get(object_ref).map(Vec::as_slice)
    }
}

/// Everything needed for pure content verification.
#[derive(Clone, Copy)]
pub struct HandoffVerificationContext<'a> {
    pub cas: &'a dyn ObjectStore,
    pub schema_registry: &'a dyn SchemaRegistry,
    pub declaration: &'a DeliveryDeclaration,
}

impl<'a> HandoffVerificationContext<'a> {
    #[must_use]
    pub const fn new(
        cas: &'a dyn ObjectStore,
        schema_registry: &'a dyn SchemaRegistry,
        declaration: &'a DeliveryDeclaration,
    ) -> Self {
        Self {
            cas,
            schema_registry,
            declaration,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DerivedDigests {
    envelope_sha256: String,
    declaration_digest: String,
    artifact_transfer_set_digest: String,
    idempotency_key: String,
    delivery_payload_digest: String,
}

impl DerivedDigests {
    fn compute(envelope: &Value, declaration: &DeliveryDeclaration) -> Result<Self, ContractError> {
        let computed_declaration = declaration.declaration_digest_hex()?;
        let declared_declaration = envelope_string(envelope, "declarationDigest")?;
        if computed_declaration != declared_declaration {
            return Err(ContractError::new(
                ErrorCode::HandoffIdempotencyInvalid,
                format!(
                    "declarationDigest mismatch: envelope {declared_declaration}, computed {computed_declaration}"
                ),
            ));
        }

        let computed_transfer = artifact_transfer_set_digest_hex(envelope)?;
        let declared_transfer = envelope_string(envelope, "artifactTransferSetDigest")?;
        if computed_transfer != declared_transfer {
            return Err(ContractError::new(
                ErrorCode::HandoffDigestMismatch,
                format!(
                    "artifactTransferSetDigest mismatch: envelope {declared_transfer}, computed {computed_transfer}"
                ),
            ));
        }

        let computed_idempotency_key = idempotency_key_hex(envelope)?;
        let declared_idempotency_key = envelope_string(envelope, "idempotencyKey")?;
        if computed_idempotency_key != declared_idempotency_key {
            return Err(ContractError::new(
                ErrorCode::HandoffIdempotencyInvalid,
                format!(
                    "idempotencyKey mismatch: envelope {declared_idempotency_key}, computed {computed_idempotency_key}"
                ),
            ));
        }

        let computed_payload = delivery_payload_digest_hex(
            &computed_declaration,
            &computed_transfer,
            envelope_string(envelope, "acceptance.contractDigest")?,
            envelope_string(envelope, "acceptance.evidenceDigest")?,
            envelope_string(envelope, "producerContextManifestDigest")?,
            envelope_string(envelope, "dagSnapshotDigest")?,
            envelope_string(envelope, "roleBindingSnapshotDigest")?,
        )?;
        let declared_payload = envelope_string(envelope, "deliveryPayloadDigest")?;
        if computed_payload != declared_payload {
            return Err(ContractError::new(
                ErrorCode::HandoffIdempotencyInvalid,
                format!(
                    "deliveryPayloadDigest mismatch: envelope {declared_payload}, computed {computed_payload}"
                ),
            ));
        }

        let computed_envelope = envelope_sha256_hex(envelope)?;
        let declared_envelope = envelope_string(envelope, "envelopeSha256")?;
        if computed_envelope != declared_envelope {
            return Err(ContractError::new(
                ErrorCode::HandoffEnvelopeHashMismatch,
                format!(
                    "envelopeSha256 mismatch: envelope {declared_envelope}, computed {computed_envelope}"
                ),
            ));
        }

        Ok(Self {
            envelope_sha256: computed_envelope,
            declaration_digest: computed_declaration,
            artifact_transfer_set_digest: computed_transfer,
            idempotency_key: computed_idempotency_key,
            delivery_payload_digest: computed_payload,
        })
    }
}

fn verify_envelope_content(
    envelope: &Value,
    context: &HandoffVerificationContext<'_>,
) -> Result<(), ContractError> {
    verify_declaration_identity(envelope, context.declaration)?;
    verify_artifact_objects(envelope, context.cas, context.schema_registry)?;
    verify_acceptance_objects(envelope, context.cas)?;
    Ok(())
}

fn verify_declaration_identity(
    envelope: &Value,
    declaration: &DeliveryDeclaration,
) -> Result<(), ContractError> {
    let mismatches = [
        (
            "projectRunId",
            envelope_string(envelope, "projectRunId").unwrap_or_default(),
            declaration.project_run_id(),
        ),
        (
            "edgeId",
            envelope_string(envelope, "edgeId").unwrap_or_default(),
            declaration.edge_id(),
        ),
        (
            "fromTaskNodeId",
            envelope_string(envelope, "from.taskNodeId").unwrap_or_default(),
            declaration.from_task_node_id(),
        ),
        (
            "fromAttemptId",
            envelope_string(envelope, "from.attemptId").unwrap_or_default(),
            declaration.from_attempt_id(),
        ),
        (
            "fromExecutionRunId",
            envelope_string(envelope, "from.executionRunId").unwrap_or_default(),
            declaration.from_execution_run_id(),
        ),
    ];
    for (label, envelope_value, declaration_value) in mismatches {
        if envelope_value != declaration_value {
            return Err(ContractError::new(
                ErrorCode::HandoffIdempotencyInvalid,
                format!("{label} differs between envelope and declaration"),
            ));
        }
    }
    let envelope_lease_epoch = json::value_as_safe_u64(
        envelope
            .get("leaseEpoch")
            .expect("shape validation guarantees leaseEpoch"),
    )
    .unwrap_or_default();
    if envelope_lease_epoch != declaration.lease_epoch() {
        return Err(ContractError::new(
            ErrorCode::HandoffIdempotencyInvalid,
            "leaseEpoch differs between envelope and declaration",
        ));
    }

    verify_source_port_closure(envelope, declaration)
}

/// The Provider declaration and the envelope must describe exactly the same
/// set of source output ports. `targetInputPortId` remains transfer-set/DAG
/// provenance and is never accepted in the declaration.
fn verify_source_port_closure(
    envelope: &Value,
    declaration: &DeliveryDeclaration,
) -> Result<(), ContractError> {
    let mut declaration_ports = HashSet::new();
    for output in declaration.outputs() {
        declaration_ports.insert(output.source_output_port_id.as_str());
    }

    let mut binding_ports = HashSet::new();
    for binding in envelope_array(envelope, "artifactBindings")? {
        let source = binding
            .pointer("/sourceOutput/portId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ContractError::new(
                    ErrorCode::HandoffSchemaViolation,
                    "binding sourceOutput.portId must be a string",
                )
            })?;
        binding_ports.insert(source);
    }

    let binding_set = binding_ports.iter().copied().collect::<HashSet<_>>();
    if declaration_ports.len() != binding_ports.len() || declaration_ports != binding_set {
        return Err(ContractError::new(
            ErrorCode::HandoffIdempotencyInvalid,
            format!(
                "declaration source ports do not exactly match envelope binding source ports: declaration={:?}, bindings={:?}",
                declaration_ports, binding_ports
            ),
        ));
    }
    Ok(())
}

fn verify_artifact_objects(
    envelope: &Value,
    cas: &dyn ObjectStore,
    schema_registry: &dyn SchemaRegistry,
) -> Result<(), ContractError> {
    for binding in envelope_array(envelope, "artifactBindings")? {
        let artifact_ref = binding
            .get("artifactRef")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ContractError::new(
                    ErrorCode::HandoffSchemaViolation,
                    "artifactRef must be an object",
                )
            })?;
        verify_schema_reference(artifact_ref, schema_registry)?;

        let object_ref = required_envelope_string(artifact_ref, "objectRef")?;
        let sha256 = required_envelope_string(artifact_ref, "sha256")?;
        if object_ref != format!("sha256:{sha256}") {
            return Err(ContractError::new(
                ErrorCode::HandoffObjectRefMismatch,
                format!("artifact objectRef {object_ref} does not equal sha256:{sha256}"),
            ));
        }
        let bytes = cas.get(object_ref).ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffObjectUnknown,
                format!("artifact object is absent from the fake CAS: {object_ref}"),
            )
        })?;
        let actual = json::sha256_raw_hex(bytes);
        if actual != sha256 {
            return Err(ContractError::new(
                ErrorCode::HandoffDigestMismatch,
                format!("artifact CAS digest mismatch for {object_ref}: expected {sha256}, actual {actual}"),
            ));
        }
        let declared_size = json::value_as_safe_u64(
            artifact_ref
                .get("size")
                .expect("shape validation guarantees artifactRef.size"),
        )
        .ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "artifact size must be a non-negative safe integer",
            )
        })?;
        if bytes.len() as u64 != declared_size {
            return Err(ContractError::new(
                ErrorCode::HandoffDigestMismatch,
                format!(
                    "artifact size mismatch for {object_ref}: expected {declared_size}, actual {}",
                    bytes.len()
                ),
            ));
        }
    }
    Ok(())
}

fn verify_acceptance_objects(envelope: &Value, cas: &dyn ObjectStore) -> Result<(), ContractError> {
    let acceptance = envelope
        .get("acceptance")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "acceptance must be an object",
            )
        })?;
    for (ref_field, digest_field) in [
        ("contractRef", "contractDigest"),
        ("evidenceRef", "evidenceDigest"),
    ] {
        let object_ref = required_envelope_string(acceptance, ref_field)?;
        let digest = required_envelope_string(acceptance, digest_field)?;
        if object_ref != format!("sha256:{digest}") {
            return Err(ContractError::new(
                ErrorCode::HandoffDigestMismatch,
                format!("acceptance {ref_field} {object_ref} does not equal sha256:{digest}"),
            ));
        }
        let bytes = cas.get(object_ref).ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffObjectUnknown,
                format!("acceptance object is absent from the fake CAS: {object_ref}"),
            )
        })?;
        let actual = json::sha256_raw_hex(bytes);
        if actual != digest {
            return Err(ContractError::new(
                ErrorCode::HandoffDigestMismatch,
                format!("acceptance CAS digest mismatch for {object_ref}: expected {digest}, actual {actual}"),
            ));
        }
    }
    Ok(())
}

fn verify_schema_reference(
    artifact_ref: &Map<String, Value>,
    schema_registry: &dyn SchemaRegistry,
) -> Result<(), ContractError> {
    let schema_ref = artifact_ref
        .get("contentSchemaRef")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "artifact contentSchemaRef must be a non-null object",
            )
        })?;
    let reference = parse_schema_reference(schema_ref)?;
    if schema_registry.resolve(&reference).is_none() {
        return Err(ContractError::new(
            ErrorCode::HandoffSchemaRefUnresolved,
            format!(
                "contentSchemaRef {} version {} digest {} is not resolvable",
                reference.id, reference.version, reference.digest
            ),
        ));
    }
    Ok(())
}

fn validate_allowed_consumers(envelope: &Value) -> Result<(), ContractError> {
    let to = envelope
        .get("to")
        .ok_or_else(|| ContractError::new(ErrorCode::HandoffSchemaViolation, "to is missing"))?;
    let allowed = envelope_array(envelope, "allowedConsumers")?;
    let Some(only) = allowed.first() else {
        return Err(ContractError::new(
            ErrorCode::HandoffSchemaViolation,
            "allowedConsumers must contain exactly one consumer",
        ));
    };
    if allowed.len() != 1 || only != to {
        return Err(ContractError::new(
            ErrorCode::HandoffSchemaViolation,
            "allowedConsumers must contain exactly one entry equal to to",
        ));
    }
    Ok(())
}

fn validate_bindings(envelope: &Value) -> Result<(), ContractError> {
    let bindings = envelope_array(envelope, "artifactBindings")?;
    let mut exact_bindings = HashSet::with_capacity(bindings.len());
    let mut source_ports = HashSet::with_capacity(bindings.len());
    for binding in bindings {
        let source_port = binding
            .pointer("/sourceOutput/portId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ContractError::new(
                    ErrorCode::HandoffSchemaViolation,
                    "binding sourceOutput.portId must be a string",
                )
            })?;
        let target_port = binding
            .pointer("/targetInput/portId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ContractError::new(
                    ErrorCode::HandoffSchemaViolation,
                    "binding targetInput.portId must be a string",
                )
            })?;
        if !source_ports.insert(source_port.to_owned()) {
            return Err(ContractError::new(
                ErrorCode::HandoffDuplicateBinding,
                format!("duplicate source output port in artifactBindings: {source_port}"),
            ));
        }
        if !exact_bindings.insert((source_port.to_owned(), target_port.to_owned())) {
            return Err(ContractError::new(
                ErrorCode::HandoffDuplicateBinding,
                format!("duplicate binding {source_port} -> {target_port}"),
            ));
        }
    }
    Ok(())
}

fn envelope_array<'a>(envelope: &'a Value, key: &str) -> Result<&'a Vec<Value>, ContractError> {
    envelope.get(key).and_then(Value::as_array).ok_or_else(|| {
        ContractError::new(
            ErrorCode::HandoffSchemaViolation,
            format!("{key} must be an array"),
        )
    })
}

fn required_envelope_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ContractError> {
    object.get(key).and_then(Value::as_str).ok_or_else(|| {
        ContractError::new(
            ErrorCode::HandoffSchemaViolation,
            format!("{key} must be a string"),
        )
    })
}

fn envelope_string<'a>(envelope: &'a Value, path: &str) -> Result<&'a str, ContractError> {
    let pointer = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{}", path.replace('.', "/"))
    };
    envelope
        .pointer(&pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                format!("{path} must be a string"),
            )
        })
}

/// Frozen `artifactTransferSetDigest = sha256Jcs(...)`.
pub fn artifact_transfer_set_digest(envelope: &Value) -> Result<[u8; 32], ContractError> {
    json::sha256_jcs(&artifact_transfer_set_record(envelope)?)
        .map_err(|error| ContractError::new(ErrorCode::HandoffCanonicalEncoding, error.to_string()))
}

pub fn artifact_transfer_set_digest_hex(envelope: &Value) -> Result<String, ContractError> {
    Ok(json::encode_hex(&artifact_transfer_set_digest(envelope)?))
}

fn sorted_binding_values(envelope: &Value) -> Result<Vec<Value>, ContractError> {
    let bindings = envelope_array(envelope, "artifactBindings")?;
    let mut exact_bindings = HashSet::with_capacity(bindings.len());
    let mut source_ports = HashSet::with_capacity(bindings.len());
    for binding in bindings {
        let source = binding
            .pointer("/sourceOutput/portId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ContractError::new(
                    ErrorCode::HandoffSchemaViolation,
                    "binding sourceOutput.portId must be a string",
                )
            })?;
        let target = binding
            .pointer("/targetInput/portId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ContractError::new(
                    ErrorCode::HandoffSchemaViolation,
                    "binding targetInput.portId must be a string",
                )
            })?;
        if !source_ports.insert(source.to_owned()) {
            return Err(ContractError::new(
                ErrorCode::HandoffDuplicateBinding,
                format!("duplicate source output port in artifactBindings: {source}"),
            ));
        }
        if !exact_bindings.insert((source.to_owned(), target.to_owned())) {
            return Err(ContractError::new(
                ErrorCode::HandoffDuplicateBinding,
                format!("duplicate binding {source} -> {target}"),
            ));
        }
    }

    let mut sorted = bindings.clone();
    sorted.sort_by(|left, right| {
        let left_source = binding_port(left, "/sourceOutput/portId");
        let right_source = binding_port(right, "/sourceOutput/portId");
        utf16_order(left_source, right_source).then_with(|| {
            utf16_order(
                binding_port(left, "/targetInput/portId"),
                binding_port(right, "/targetInput/portId"),
            )
        })
    });
    Ok(sorted)
}

fn artifact_transfer_set_record(envelope: &Value) -> Result<Value, ContractError> {
    let sorted = sorted_binding_values(envelope)?;

    let records = sorted
        .iter()
        .map(|binding| {
            let source_output_port_id = binding_port(binding, "/sourceOutput/portId").to_owned();
            let target_input_port_id = binding_port(binding, "/targetInput/portId").to_owned();
            let artifact_ref = binding
                .get("artifactRef")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    ContractError::new(
                        ErrorCode::HandoffSchemaViolation,
                        "binding artifactRef must be an object",
                    )
                })?;
            let content_schema_ref = artifact_ref
                .get("contentSchemaRef")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    ContractError::new(
                        ErrorCode::HandoffSchemaViolation,
                        "artifact contentSchemaRef must be a non-null object",
                    )
                })?;
            let reference = parse_schema_reference(content_schema_ref)?;
            Ok(Value::Object(Map::from_iter([
                (
                    "sourceOutputPortId".to_owned(),
                    Value::String(source_output_port_id),
                ),
                (
                    "targetInputPortId".to_owned(),
                    Value::String(target_input_port_id),
                ),
                (
                    "artifactRef".to_owned(),
                    Value::Object(Map::from_iter([
                        (
                            "objectRef".to_owned(),
                            artifact_ref
                                .get("objectRef")
                                .cloned()
                                .expect("shape validation guarantees objectRef"),
                        ),
                        (
                            "sha256".to_owned(),
                            artifact_ref
                                .get("sha256")
                                .cloned()
                                .expect("shape validation guarantees sha256"),
                        ),
                        (
                            "size".to_owned(),
                            artifact_ref
                                .get("size")
                                .cloned()
                                .expect("shape validation guarantees size"),
                        ),
                        (
                            "contentSchemaRef".to_owned(),
                            schema_reference_value(&reference),
                        ),
                        (
                            "normalizedContentType".to_owned(),
                            artifact_ref
                                .get("normalizedContentType")
                                .cloned()
                                .expect("shape validation guarantees normalizedContentType"),
                        ),
                        (
                            "normalizedContentTypePolicyVersion".to_owned(),
                            artifact_ref
                                .get("normalizedContentTypePolicyVersion")
                                .cloned()
                                .expect("shape validation guarantees normalizedContentTypePolicyVersion"),
                        ),
                    ])),
                ),
            ])))
        })
        .collect::<Result<Vec<Value>, ContractError>>()?;

    Ok(Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String("agenttalk.handoff.artifact-transfer-set.v1".to_owned()),
        ),
        ("bindings".to_owned(), Value::Array(records)),
    ])))
}

fn binding_port<'a>(binding: &'a Value, pointer: &str) -> &'a str {
    binding
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// Frozen `idempotencyKey = sha256Jcs(...)`.
pub fn idempotency_key(envelope: &Value) -> Result<[u8; 32], ContractError> {
    let record = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String("agenttalk.handoff.delivery-identity.v1".to_owned()),
        ),
        (
            "projectRunId".to_owned(),
            envelope
                .get("projectRunId")
                .cloned()
                .expect("shape validation guarantees projectRunId"),
        ),
        (
            "edgeId".to_owned(),
            envelope
                .get("edgeId")
                .cloned()
                .expect("shape validation guarantees edgeId"),
        ),
        (
            "fromTaskNodeId".to_owned(),
            envelope
                .pointer("/from/taskNodeId")
                .cloned()
                .expect("shape validation guarantees from.taskNodeId"),
        ),
        (
            "fromAttemptId".to_owned(),
            envelope
                .pointer("/from/attemptId")
                .cloned()
                .expect("shape validation guarantees from.attemptId"),
        ),
        (
            "fromExecutionRunId".to_owned(),
            envelope
                .pointer("/from/executionRunId")
                .cloned()
                .expect("shape validation guarantees from.executionRunId"),
        ),
        (
            "toTaskNodeId".to_owned(),
            envelope
                .pointer("/to/taskNodeId")
                .cloned()
                .expect("shape validation guarantees to.taskNodeId"),
        ),
        (
            "leaseEpoch".to_owned(),
            envelope
                .get("leaseEpoch")
                .cloned()
                .expect("shape validation guarantees leaseEpoch"),
        ),
    ]));
    json::sha256_jcs(&record)
        .map_err(|error| ContractError::new(ErrorCode::HandoffCanonicalEncoding, error.to_string()))
}

pub fn idempotency_key_hex(envelope: &Value) -> Result<String, ContractError> {
    Ok(json::encode_hex(&idempotency_key(envelope)?))
}

/// Frozen `deliveryPayloadDigest = sha256Jcs(...)`.
pub fn delivery_payload_digest(
    declaration_digest: &str,
    artifact_transfer_set_digest: &str,
    acceptance_contract_digest: &str,
    acceptance_evidence_digest: &str,
    producer_context_manifest_digest: &str,
    dag_snapshot_digest: &str,
    role_binding_snapshot_digest: &str,
) -> Result<[u8; 32], ContractError> {
    for digest in [
        declaration_digest,
        artifact_transfer_set_digest,
        acceptance_contract_digest,
        acceptance_evidence_digest,
        producer_context_manifest_digest,
        dag_snapshot_digest,
        role_binding_snapshot_digest,
    ] {
        if !is_lower_hex64(digest) {
            return Err(ContractError::new(
                ErrorCode::HandoffIdempotencyInvalid,
                "digest arguments must be lowercase hex64 values",
            ));
        }
    }

    let record = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String("agenttalk.handoff.delivery-payload.v1".to_owned()),
        ),
        (
            "declarationDigest".to_owned(),
            Value::String(declaration_digest.to_owned()),
        ),
        (
            "artifactTransferSetDigest".to_owned(),
            Value::String(artifact_transfer_set_digest.to_owned()),
        ),
        (
            "acceptanceContractDigest".to_owned(),
            Value::String(acceptance_contract_digest.to_owned()),
        ),
        (
            "acceptanceEvidenceDigest".to_owned(),
            Value::String(acceptance_evidence_digest.to_owned()),
        ),
        (
            "producerContextManifestDigest".to_owned(),
            Value::String(producer_context_manifest_digest.to_owned()),
        ),
        (
            "dagSnapshotDigest".to_owned(),
            Value::String(dag_snapshot_digest.to_owned()),
        ),
        (
            "roleBindingSnapshotDigest".to_owned(),
            Value::String(role_binding_snapshot_digest.to_owned()),
        ),
    ]));
    json::sha256_jcs(&record)
        .map_err(|error| ContractError::new(ErrorCode::HandoffCanonicalEncoding, error.to_string()))
}

pub fn delivery_payload_digest_hex(
    declaration_digest: &str,
    artifact_transfer_set_digest: &str,
    acceptance_contract_digest: &str,
    acceptance_evidence_digest: &str,
    producer_context_manifest_digest: &str,
    dag_snapshot_digest: &str,
    role_binding_snapshot_digest: &str,
) -> Result<String, ContractError> {
    Ok(json::encode_hex(&delivery_payload_digest(
        declaration_digest,
        artifact_transfer_set_digest,
        acceptance_contract_digest,
        acceptance_evidence_digest,
        producer_context_manifest_digest,
        dag_snapshot_digest,
        role_binding_snapshot_digest,
    )?))
}

/// Frozen `envelopeSha256 = sha256Jcs(canonical(envelope without
/// envelopeSha256))`. `artifactBindings` is a set array, so the canonical
/// preimage is normalized by `(sourceOutputPortId, targetInputPortId)`
/// exactly like `artifactTransferSetDigest`; swapping binding order must not
/// change the envelope hash.
pub fn envelope_sha256(envelope: &Value) -> Result<[u8; 32], ContractError> {
    let sorted_bindings = sorted_binding_values(envelope)?;
    let mut without_hash = envelope.clone();
    if let Some(object) = without_hash.as_object_mut() {
        if object.remove("envelopeSha256").is_none() {
            return Err(ContractError::new(
                ErrorCode::HandoffSchemaViolation,
                "envelopeSha256 is missing",
            ));
        }
        object.insert("artifactBindings".to_owned(), Value::Array(sorted_bindings));
    } else {
        return Err(ContractError::new(
            ErrorCode::HandoffSchemaViolation,
            "envelope must be an object",
        ));
    }
    json::sha256_jcs(&without_hash)
        .map_err(|error| ContractError::new(ErrorCode::HandoffCanonicalEncoding, error.to_string()))
}

pub fn envelope_sha256_hex(envelope: &Value) -> Result<String, ContractError> {
    Ok(json::encode_hex(&envelope_sha256(envelope)?))
}

/// Pure replay/conflict classifier for the future Core journal. It has no
/// storage and does **not** implement journal authority; the caller must only
/// invoke it after proving the idempotency keys are equal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyDisposition {
    FirstDelivery,
    Replay,
    Conflict,
}

pub fn classify_idempotency_replay(
    previous_payload_digest: Option<&str>,
    candidate_payload_digest: &str,
) -> Result<IdempotencyDisposition, ContractError> {
    for digest in previous_payload_digest
        .into_iter()
        .chain([candidate_payload_digest])
    {
        if !is_lower_hex64(digest) {
            return Err(ContractError::new(
                ErrorCode::HandoffIdempotencyInvalid,
                "payload digests must be lowercase hex64 values",
            ));
        }
    }
    Ok(match previous_payload_digest {
        None => IdempotencyDisposition::FirstDelivery,
        Some(previous) if previous == candidate_payload_digest => IdempotencyDisposition::Replay,
        Some(_) => IdempotencyDisposition::Conflict,
    })
}

fn handoff_parse_error(error: json::JsonParseError) -> ContractError {
    match error {
        json::JsonParseError::DuplicateKey { path } => ContractError::new(
            ErrorCode::HandoffDuplicateKey,
            format!("duplicate object key at {path}"),
        ),
        other => ContractError::new(ErrorCode::HandoffSchemaViolation, other.to_string()),
    }
}

fn is_lower_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn schema_reference_value(reference: &SchemaReference) -> Value {
    Value::Object(Map::from_iter([
        ("id".to_owned(), Value::String(reference.id.clone())),
        (
            "version".to_owned(),
            Value::String(reference.version.clone()),
        ),
        ("digest".to_owned(), Value::String(reference.digest.clone())),
    ]))
}
