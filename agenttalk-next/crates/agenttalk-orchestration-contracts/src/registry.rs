//! Pure in-memory `contentSchemaRef` registry.
//!
//! The frozen registry rule is fail-closed: a reference resolves only when
//! `(id, version)` is known **and** the requested digest exactly equals
//! `sha256Jcs(canonical schema JSON)`. Unknown id, unknown version, and
//! digest mismatch are all `None` here and are converted to
//! `BRIEF_SCHEMA_REF_UNRESOLVED` / `HANDOFF_SCHEMA_REF_UNRESOLVED` by the
//! respective validators.

use crate::json::{self, CanonicalizationError, JsonParseError};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

/// Frozen wire shape `{id, version, digest}`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SchemaReference {
    pub id: String,
    pub version: String,
    pub digest: String,
}

impl SchemaReference {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        version: impl Into<String>,
        digest: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            version: version.into(),
            digest: digest.into(),
        }
    }
}

/// A registry entry whose digest is computed over canonical JCS schema bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaDescriptor {
    reference: SchemaReference,
    canonical_bytes: Vec<u8>,
}

impl SchemaDescriptor {
    #[must_use]
    pub const fn reference(&self) -> &SchemaReference {
        &self.reference
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.reference.id
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.reference.version
    }

    #[must_use]
    pub fn digest(&self) -> &str {
        &self.reference.digest
    }

    /// Canonical JCS bytes the Core sealer will eventually co-seal.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

/// Pure registry abstraction. Implementations must be in-memory and must not
/// perform IO, network access, or fallback resolution.
pub trait SchemaRegistry {
    /// Fail-closed lookup. `None` means unknown id/version or digest
    /// mismatch; callers must never attempt a second source.
    fn resolve(&self, reference: &SchemaReference) -> Option<&SchemaDescriptor>;
}

/// A fake registry used by contract tests and by future Core in-memory
/// fixtures. It intentionally has no persistence.
#[derive(Clone, Debug, Default)]
pub struct InMemorySchemaRegistry {
    entries: HashMap<(String, String), SchemaDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaRegistrationError {
    DuplicateKey(JsonParseError),
    Syntax(JsonParseError),
    Canonicalization(CanonicalizationError),
    DuplicateEntry { id: String, version: String },
}

impl Display for SchemaRegistrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateKey(error) => write!(f, "duplicate key in schema JSON: {error}"),
            Self::Syntax(error) => write!(f, "schema JSON syntax error: {error}"),
            Self::Canonicalization(error) => {
                write!(f, "schema JSON canonicalization error: {error}")
            }
            Self::DuplicateEntry { id, version } => {
                write!(f, "schema registry already contains {id} version {version}")
            }
        }
    }
}

impl std::error::Error for SchemaRegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DuplicateKey(error) | Self::Syntax(error) => Some(error),
            Self::Canonicalization(error) => Some(error),
            Self::DuplicateEntry { .. } => None,
        }
    }
}

impl InMemorySchemaRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse duplicate-key-safely, canonicalize, digest, and register a schema
    /// JSON document. The returned reference is the only reference that will
    /// resolve for this entry.
    pub fn register_json(
        &mut self,
        id: impl Into<String>,
        version: impl Into<String>,
        schema_json: &[u8],
    ) -> Result<SchemaReference, SchemaRegistrationError> {
        let id = id.into();
        let version = version.into();
        let value = json::parse_duplicate_safe(schema_json).map_err(|error| match error {
            JsonParseError::DuplicateKey { .. } => SchemaRegistrationError::DuplicateKey(error),
            other => SchemaRegistrationError::Syntax(other),
        })?;
        self.register_value(id, version, value)
    }

    /// Register an already parsed schema value with the same canonical
    /// digest discipline.
    pub fn register_value(
        &mut self,
        id: impl Into<String>,
        version: impl Into<String>,
        schema: Value,
    ) -> Result<SchemaReference, SchemaRegistrationError> {
        let id = id.into();
        let version = version.into();
        if self.entries.contains_key(&(id.clone(), version.clone())) {
            return Err(SchemaRegistrationError::DuplicateEntry { id, version });
        }
        let canonical_bytes =
            json::canonicalize(&schema).map_err(SchemaRegistrationError::Canonicalization)?;
        let digest = json::encode_hex(&json::sha256_raw(&canonical_bytes));
        let reference = SchemaReference {
            id,
            version,
            digest,
        };
        let descriptor = SchemaDescriptor {
            reference: reference.clone(),
            canonical_bytes,
        };
        self.entries.insert(
            (reference.id.clone(), reference.version.clone()),
            descriptor,
        );
        Ok(reference)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl SchemaRegistry for InMemorySchemaRegistry {
    fn resolve(&self, reference: &SchemaReference) -> Option<&SchemaDescriptor> {
        self.entries
            .get(&(reference.id.clone(), reference.version.clone()))
            .filter(|descriptor| descriptor.digest() == reference.digest)
    }
}
