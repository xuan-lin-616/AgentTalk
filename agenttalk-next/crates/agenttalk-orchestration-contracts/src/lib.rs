#![forbid(unsafe_code)]
#![doc = "Frozen orchestration contract primitives (C1)."]

//! # Orchestration contract planes
//!
//! This crate implements exactly the two pure contract planes for C1:
//!
//! | Plane | Implemented here | Boundary |
//! |---|---|---|
//! | contract shape | duplicate-key-safe JSON parse, Draft 2020-12 schema validation, pure semantic shape rules | `Parsed*` -> `ShapeValidated*` |
//! | contract content | in-memory fake CAS / bytes-map re-verification, frozen digest formulas | `ShapeValidated*` -> `ContentValidated*` / `ContentVerified*<AuthorityUnchecked>` |
//! | filesystem seal | **deferred** | `BRIEF_REPARSE_POINT`, `BRIEF_PATH_ESCAPE`, `BRIEF_SOURCE_HANDLE_CHANGED` belong to the future Core sealer and are intentionally absent from this crate |
//! | journal authority | **deferred** | lease fencing, producer/consumer authority, `HANDOFF_STALE_LEASE`, receipt replay/conflict persistence, and `JournalAuthorizedEnvelope<AuthorityVerified>` construction belong to Core journal |
//!
//! A `ContentVerifiedEnvelope<AuthorityUnchecked>` can be produced for a
//! syntactically and content-consistent envelope even when the producer is
//! wrong. That type deliberately cannot satisfy [`handoff::SchedulerReady`];
//! only a future Core-journal-constructed
//! [`handoff::JournalAuthorizedEnvelope`] can.

pub mod brief;
pub mod error;
pub mod handoff;
pub mod json;
pub mod registry;
pub mod schema;

pub use error::{ContractError, ErrorCode};
