use std::fmt::{Display, Formatter};

/// Frozen orchestration contract error codes for the two planes owned by this
/// crate.
///
/// Filesystem-seal codes and journal-authority codes are intentionally **not**
/// defined here. They are owned by the future Core sealer and Core journal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorCode {
    // Brief: contract shape.
    BriefSchemaViolation,
    BriefDuplicateKey,
    BriefCanonicalEncoding,
    BriefEnumInvalid,
    BriefUnknownRole,
    BriefDuplicatePath,
    BriefPathLexicalInvalid,
    BriefPathAlias,
    BriefCasReference,
    BriefSensitiveSourceForbidden,
    // Brief: contract content.
    BriefHashMismatch,
    BriefSizeMismatch,
    BriefDeclaredFileMissing,
    BriefSchemaRefUnresolved,
    // Handoff: contract shape.
    HandoffSchemaViolation,
    HandoffDuplicateKey,
    HandoffCanonicalEncoding,
    HandoffEnumInvalid,
    HandoffDuplicateBinding,
    // Handoff: contract content.
    HandoffDigestMismatch,
    HandoffObjectRefMismatch,
    HandoffObjectUnknown,
    HandoffEnvelopeHashMismatch,
    HandoffIdempotencyInvalid,
    HandoffSchemaRefUnresolved,
}

impl ErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BriefSchemaViolation => "BRIEF_SCHEMA_VIOLATION",
            Self::BriefDuplicateKey => "BRIEF_DUPLICATE_KEY",
            Self::BriefCanonicalEncoding => "BRIEF_CANONICAL_ENCODING",
            Self::BriefEnumInvalid => "BRIEF_ENUM_INVALID",
            Self::BriefUnknownRole => "BRIEF_UNKNOWN_ROLE",
            Self::BriefDuplicatePath => "BRIEF_DUPLICATE_PATH",
            Self::BriefPathLexicalInvalid => "BRIEF_PATH_LEXICAL_INVALID",
            Self::BriefPathAlias => "BRIEF_PATH_ALIAS",
            Self::BriefCasReference => "BRIEF_CAS_REFERENCE",
            Self::BriefSensitiveSourceForbidden => "BRIEF_SENSITIVE_SOURCE_FORBIDDEN",
            Self::BriefHashMismatch => "BRIEF_HASH_MISMATCH",
            Self::BriefSizeMismatch => "BRIEF_SIZE_MISMATCH",
            Self::BriefDeclaredFileMissing => "BRIEF_DECLARED_FILE_MISSING",
            Self::BriefSchemaRefUnresolved => "BRIEF_SCHEMA_REF_UNRESOLVED",
            Self::HandoffSchemaViolation => "HANDOFF_SCHEMA_VIOLATION",
            Self::HandoffDuplicateKey => "HANDOFF_DUPLICATE_KEY",
            Self::HandoffCanonicalEncoding => "HANDOFF_CANONICAL_ENCODING",
            Self::HandoffEnumInvalid => "HANDOFF_ENUM_INVALID",
            Self::HandoffDuplicateBinding => "HANDOFF_DUPLICATE_BINDING",
            Self::HandoffDigestMismatch => "HANDOFF_DIGEST_MISMATCH",
            Self::HandoffObjectRefMismatch => "HANDOFF_OBJECT_REF_MISMATCH",
            Self::HandoffObjectUnknown => "HANDOFF_OBJECT_UNKNOWN",
            Self::HandoffEnvelopeHashMismatch => "HANDOFF_ENVELOPE_HASH_MISMATCH",
            Self::HandoffIdempotencyInvalid => "HANDOFF_IDEMPOTENCY_INVALID",
            Self::HandoffSchemaRefUnresolved => "HANDOFF_SCHEMA_REF_UNRESOLVED",
        }
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A typed contract error. `code` is always one of the frozen error codes and
/// `message` is for human diagnostics only; callers must branch on `code`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractError {
    code: ErrorCode,
    message: String,
}

impl ContractError {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for ContractError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ContractError {}
