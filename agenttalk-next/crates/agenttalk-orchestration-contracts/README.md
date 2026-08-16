# agenttalk-orchestration-contracts

C1 implementation of the frozen orchestration contract planes.

## Implemented (pure, zero IO at runtime)

- Duplicate-key-safe JSON parsing (`json::parse_duplicate_safe`) that never
  relies on `serde_json`'s last-key-wins `Value` visitor.
- RFC 8785/JCS canonicalization with the frozen contract extensions:
  duplicate-key rejection, complete JSON EOF rejection, non-NFC string
  rejection, non-negative safe integers only, UTF-16 code-unit key ordering,
  and explicit set-array sorting with duplicate rejection.
- `sha256Raw` and `sha256Jcs`.
- Embedded Draft 2020-12 validators for the two formal schemas in
  `agenttalk-next/schemas/orchestration/v1/`.
- Brief typestate:
  `ParsedManifest -> ShapeValidatedManifest -> ContentValidatedManifest`,
  including path lexical/CAS/sensitive rules and the in-memory bytes-map
  re-verification. `briefTreeDigest` is only available on
  `ContentValidatedManifest`.
- Handoff typestate:
  `ParsedEnvelope -> ShapeValidatedEnvelope<AuthorityUnchecked> ->
  ContentVerifiedEnvelope<AuthorityUnchecked>`, including exact source-port
  closure between `DeliveryDeclaration.outputs` and `artifactBindings`,
  fake-CAS object re-verification, and the frozen `declarationDigest`,
  `artifactTransferSetDigest`, `idempotencyKey`, `deliveryPayloadDigest`, and
  `envelopeSha256` formulas. `artifactBindings` is normalized as a set for
  both transfer-set and envelope-hash preimages. `declarationDigest`
  verification requires the separate `DeliveryDeclaration` because the
  envelope intentionally does not duplicate staging claims.
- Type-level scheduler gate: `ContentVerifiedEnvelope<AuthorityUnchecked>`
  and `JournalAuthorizedEnvelope<AuthorityUnchecked>` cannot satisfy
  `handoff::SchedulerReady`; only the zero-constructor
  `JournalAuthorizedEnvelope<AuthorityVerified>` placeholder implements it.

## Explicitly deferred

- Filesystem seal: reparse/junction, FileId alias identity, same-open-handle
  reads, atomic CAS publication.
- Journal authority: producer/consumer authority, lease fencing,
  `HANDOFF_STALE_LEASE`, receipt replay/conflict persistence, construction of
  `JournalAuthorizedEnvelope<AuthorityVerified>`.

## Golden vectors

`tests/fixtures/golden/` contains literal expected files generated
independently with Python `jcs==0.2.1`; see `REFERENCE.md` and
`reference-generator.py` in that directory. Tests never generate expected
hashes from this crate.
