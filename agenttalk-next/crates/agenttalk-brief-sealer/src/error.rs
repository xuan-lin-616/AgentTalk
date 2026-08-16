//! Filesystem-seal errors for the C3-A brief sealer.

use agenttalk_orchestration_contracts::ContractError;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

#[derive(Debug)]
pub enum BriefSealError {
    /// The contract layer rejected the manifest shape/content. The inner
    /// error already carries one of the frozen contract error codes.
    Contract(ContractError),
    /// A reparse point / symlink / junction was found on a protected path.
    ReparsePoint { path: PathBuf },
    /// A declared path is outside the closed authoring tree after
    /// filesystem-side checks.
    PathEscape { path: String },
    /// The final open handle identity changed between open and read.
    SourceHandleChanged { path: String },
    /// Two declared files or a file and the root manifest resolve to the
    /// same physical file identity.
    PhysicalAlias { path: String },
    /// The CAS layer rejected a publish or read.
    Cas(crate::cas::CasError),
    /// Generic filesystem IO failure while sealing.
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl Display for BriefSealError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(error) => write!(f, "{error}"),
            Self::ReparsePoint { path } => write!(f, "reparse point forbidden: {}", path.display()),
            Self::PathEscape { path } => write!(f, "path escapes the authoring tree: {path}"),
            Self::SourceHandleChanged { path } => {
                write!(f, "source handle identity changed during seal: {path}")
            }
            Self::PhysicalAlias { path } => {
                write!(f, "physical file alias detected during seal: {path}")
            }
            Self::Cas(error) => write!(f, "{error}"),
            Self::Io { path, source } => write!(f, "io error at {}: {source}", path.display()),
        }
    }
}

impl std::error::Error for BriefSealError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Cas(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl BriefSealError {
    /// Frozen error-code string for filesystem-plane failures. Contract
    /// failures delegate to the frozen contract code.
    #[must_use]
    pub fn code_str(&self) -> &'static str {
        match self {
            Self::Contract(error) => error.code().as_str(),
            Self::ReparsePoint { .. } => "BRIEF_REPARSE_POINT",
            Self::PathEscape { .. } => "BRIEF_PATH_ESCAPE",
            Self::SourceHandleChanged { .. } => "BRIEF_SOURCE_HANDLE_CHANGED",
            Self::PhysicalAlias { .. } => "BRIEF_PATH_ALIAS",
            Self::Cas(error) => error.code_str(),
            Self::Io { .. } => "BRIEF_IO",
        }
    }
}

impl From<ContractError> for BriefSealError {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<crate::cas::CasError> for BriefSealError {
    fn from(error: crate::cas::CasError) -> Self {
        Self::Cas(error)
    }
}
