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
//! CAS publication is atomic no-replace. Directory bootstrap, temp create,
//! link, delete, and flush are performed through handle-relative Windows
//! APIs; no absolute CAS path is used for I/O.

pub mod cas;
pub mod error;
pub mod fs_guard;
pub mod sealer;

pub use cas::{CasObject, CoreCas};
pub use error::BriefSealError;
pub use sealer::{BriefSealer, PreparedBriefSeal, SealedBriefFile};

/// Test-only failure injection for temp-file deletion. Production code paths
/// must never consult this module.
#[doc(hidden)]
pub mod test_support {
    use std::cell::Cell;

    thread_local! {
        static FAIL_NEXT_TEMP_DELETE: Cell<bool> = const { Cell::new(false) };
    }

    pub fn set_fail_next_temp_delete(value: bool) {
        FAIL_NEXT_TEMP_DELETE.with(|flag| flag.set(value));
    }

    pub(crate) fn should_fail_temp_delete() -> bool {
        FAIL_NEXT_TEMP_DELETE.with(|flag| flag.replace(false))
    }
}
