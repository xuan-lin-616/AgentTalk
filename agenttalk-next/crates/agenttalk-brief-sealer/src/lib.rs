//! C3-A filesystem brief sealer and Core-owned CAS.
//!
//! This crate implements ADR-001 §2 steps 1-4 plus the `.agenttalk/objects/`
//! CAS boundary. Step 5 (journal Run creation) is intentionally absent:
//! at the C3-A baseline there is no `orchestration_runs` storage table or
//! writer API. The product of this crate is [`sealer::PreparedBriefSeal`],
//! which is immutable and is **not** an OrchestrationRun and must not be
//! passed to the scheduler.
//!
//! Status: `C3-A PARTIAL / JOURNAL_PERSISTENCE_BLOCKED`
//!
//! CAS publication durability is atomic no-replace; full Windows directory
//! entry durability is explicitly not claimed in this work package.

pub mod cas;
pub mod error;
pub mod fs_guard;
pub mod sealer;

pub use cas::{CasObject, CoreCas};
pub use error::BriefSealError;
pub use sealer::{BriefSealer, PreparedBriefSeal, SealedBriefFile};
