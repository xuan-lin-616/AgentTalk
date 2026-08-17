use crate::{SqliteStore, StorageError, SCHEMA_VERSION, V15_SCHEMA_VERSION};
use agenttalk_brief_sealer::PreparedBriefSeal;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_hex64(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_object_ref(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(is_hex64)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrchestrationRunSeed {
    pub run_id: String,
    pub project_id: String,
    pub brief_snapshot_id: String,
    pub brief_tree_digest: String,
    pub dag_snapshot_digest: String,
    pub role_binding_snapshot_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrchestrationRunRecord {
    pub run_id: String,
    pub project_id: String,
    pub status: String,
    pub version: i64,
    pub brief_snapshot_id: String,
    pub brief_tree_digest: String,
    pub dag_snapshot_digest: String,
    pub role_binding_snapshot_digest: String,
    pub coordinator_generation: i64,
    pub terminal_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HumanReceiptRecord {
    pub receipt_id: String,
    pub run_id: String,
    pub milestone_id: String,
    pub request_id: String,
    pub semantic_payload_hash: String,
    pub decision: String,
    pub expected_version: i64,
    pub brief_tree_digest: String,
    pub presented_artifact_set_digest: String,
    pub acceptance_evidence_digest: String,
    pub authenticated_principal: String,
    pub core_timestamp: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffDeliveryRecord {
    pub delivery_id: String,
    pub run_id: String,
    pub attempt_id: String,
    pub edge_id: String,
    pub lease_epoch: i64,
    pub declaration_digest: String,
    pub artifact_transfer_set_digest: String,
    pub idempotency_key: String,
    pub delivery_payload_digest: String,
    pub envelope_object_ref: String,
    pub envelope_raw_sha256: String,
    pub envelope_sha256_jcs: String,
    pub acceptance_contract_ref: String,
    pub acceptance_contract_digest: String,
    pub acceptance_evidence_ref: String,
    pub acceptance_evidence_digest: String,
    pub producer_context_manifest_digest: String,
    pub replay_receipt_json: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskReadyToRunningOutcome {
    pub node_id: String,
    pub attempt_id: String,
    pub attempt_no: i64,
    pub lease_epoch: i64,
}

impl SqliteStore {
    pub fn create_orchestration_run(
        &mut self,
        seed: OrchestrationRunSeed,
    ) -> Result<(), StorageError> {
        if seed.run_id.is_empty() || seed.project_id.is_empty() {
            return Err(StorageError::ModelSnapshotInvalid {
                reason: "run_id and project_id are required".into(),
            });
        }
        if !is_object_ref(&seed.brief_snapshot_id) || !is_hex64(&seed.brief_tree_digest) {
            return Err(StorageError::ModelSnapshotInvalid {
                reason:
                    "brief_snapshot_id must be sha256:<64hex> and brief_tree_digest must be 64hex"
                        .into(),
            });
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = tx
            .query_row(
                "SELECT brief_snapshot_id, brief_tree_digest FROM orchestration_runs WHERE run_id = ?1",
                [&seed.run_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            if existing.0 != seed.brief_snapshot_id || existing.1 != seed.brief_tree_digest {
                return Err(StorageError::OrchestrationRunConflict {
                    run_id: seed.run_id,
                });
            }
            tx.commit()?;
            return Ok(());
        }
        if tx
            .query_row(
                "SELECT 1 FROM orchestration_runs WHERE brief_snapshot_id = ?1",
                [&seed.brief_snapshot_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(StorageError::OrchestrationRunConflict {
                run_id: seed.run_id,
            });
        }
        tx.execute(
            "INSERT INTO orchestration_runs(
               run_id, project_id, status, version, brief_snapshot_id,
               brief_tree_digest, dag_snapshot_digest,
               role_binding_snapshot_digest, coordinator_generation
             ) VALUES(?1, ?2, 'prepared', 1, ?3, ?4, ?5, ?6, 1)",
            params![
                seed.run_id,
                seed.project_id,
                seed.brief_snapshot_id,
                seed.brief_tree_digest,
                seed.dag_snapshot_digest,
                seed.role_binding_snapshot_digest,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn create_orchestration_run_from_prepared_brief_seal(
        &mut self,
        project_id: &str,
        run_id: &str,
        seal: &PreparedBriefSeal,
        dag_snapshot_digest: &str,
        role_binding_snapshot_digest: &str,
    ) -> Result<(), StorageError> {
        let brief_snapshot_id = seal.brief_snapshot_id();
        let brief_tree_digest = seal.brief_tree_digest();
        if brief_snapshot_id != seal.descriptor_object_ref() {
            return Err(StorageError::ModelSnapshotInvalid {
                reason: "brief_snapshot_id must equal descriptor object ref".into(),
            });
        }
        if !is_object_ref(brief_snapshot_id) || !is_hex64(brief_tree_digest) {
            return Err(StorageError::ModelSnapshotInvalid {
                reason: "brief snapshot digest binding is invalid".into(),
            });
        }
        self.create_orchestration_run(OrchestrationRunSeed {
            run_id: run_id.to_owned(),
            project_id: project_id.to_owned(),
            brief_snapshot_id: brief_snapshot_id.to_owned(),
            brief_tree_digest: brief_tree_digest.to_owned(),
            dag_snapshot_digest: dag_snapshot_digest.to_owned(),
            role_binding_snapshot_digest: role_binding_snapshot_digest.to_owned(),
        })
    }

    pub fn orchestration_run(&self, run_id: &str) -> Result<OrchestrationRunRecord, StorageError> {
        self.connection
            .query_row(
                "SELECT run_id, project_id, status, version, brief_snapshot_id,
                        brief_tree_digest, dag_snapshot_digest,
                        role_binding_snapshot_digest, coordinator_generation,
                        terminal_reason
                 FROM orchestration_runs WHERE run_id = ?1",
                [run_id],
                |row| {
                    Ok(OrchestrationRunRecord {
                        run_id: row.get(0)?,
                        project_id: row.get(1)?,
                        status: row.get(2)?,
                        version: row.get(3)?,
                        brief_snapshot_id: row.get(4)?,
                        brief_tree_digest: row.get(5)?,
                        dag_snapshot_digest: row.get(6)?,
                        role_binding_snapshot_digest: row.get(7)?,
                        coordinator_generation: row.get(8)?,
                        terminal_reason: row.get(9)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationRunNotFound {
                run_id: run_id.to_owned(),
            })
    }

    pub fn ensure_orchestration_milestone(
        &mut self,
        run_id: &str,
        milestone_id: &str,
        milestone_key: &str,
        brief_tree_digest: &str,
    ) -> Result<(), StorageError> {
        self.orchestration_run(run_id)?;
        self.connection.execute(
            "INSERT INTO orchestration_milestones(
               milestone_id, run_id, milestone_key, required, status, version,
               brief_tree_digest, presented_artifact_set_digest,
               acceptance_evidence_digest
             ) VALUES(?1, ?2, ?3, 1, 'ready', 1, ?4, '', '')
             ON CONFLICT(milestone_id) DO NOTHING",
            params![milestone_id, run_id, milestone_key, brief_tree_digest],
        )?;
        Ok(())
    }

    pub fn record_human_receipt(
        &mut self,
        receipt: HumanReceiptRecord,
    ) -> Result<bool, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let milestone_run: Option<String> = tx
            .query_row(
                "SELECT run_id FROM orchestration_milestones WHERE milestone_id = ?1",
                [&receipt.milestone_id],
                |row| row.get(0),
            )
            .optional()?;
        if milestone_run.is_none() {
            return Err(StorageError::OrchestrationMilestoneNotFound {
                milestone_id: receipt.milestone_id,
            });
        }
        let existing = tx
            .query_row(
                "SELECT semantic_payload_hash, decision, brief_tree_digest,
                        presented_artifact_set_digest, acceptance_evidence_digest,
                        authenticated_principal
                 FROM orchestration_human_receipts
                 WHERE milestone_id = ?1 AND request_id = ?2",
                params![receipt.milestone_id, receipt.request_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing.0 != receipt.semantic_payload_hash
                || existing.1 != receipt.decision
                || existing.2 != receipt.brief_tree_digest
                || existing.3 != receipt.presented_artifact_set_digest
                || existing.4 != receipt.acceptance_evidence_digest
                || existing.5 != receipt.authenticated_principal
            {
                return Err(StorageError::HumanReceiptConflict {
                    milestone_id: receipt.milestone_id,
                    request_id: receipt.request_id,
                });
            }
            tx.commit()?;
            return Ok(true);
        }
        tx.execute(
            "INSERT INTO orchestration_human_receipts(
               receipt_id, run_id, milestone_id, request_id,
               semantic_payload_hash, decision, expected_version,
               brief_tree_digest, presented_artifact_set_digest,
               acceptance_evidence_digest, authenticated_principal,
               core_timestamp
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                receipt.receipt_id,
                receipt.run_id,
                receipt.milestone_id,
                receipt.request_id,
                receipt.semantic_payload_hash,
                receipt.decision,
                receipt.expected_version,
                receipt.brief_tree_digest,
                receipt.presented_artifact_set_digest,
                receipt.acceptance_evidence_digest,
                receipt.authenticated_principal,
                receipt.core_timestamp,
            ],
        )?;
        tx.execute(
            "UPDATE orchestration_milestones
             SET status = 'human_approved',
                 presented_artifact_set_digest = ?2,
                 acceptance_evidence_digest = ?3
             WHERE milestone_id = ?1",
            params![
                receipt.milestone_id,
                receipt.presented_artifact_set_digest,
                receipt.acceptance_evidence_digest,
            ],
        )?;
        tx.commit()?;
        Ok(false)
    }

    pub fn record_handoff_delivery(
        &mut self,
        delivery: HandoffDeliveryRecord,
    ) -> Result<bool, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = tx
            .query_row(
                "SELECT delivery_id, delivery_payload_digest,
                        envelope_object_ref, envelope_raw_sha256,
                        envelope_sha256_jcs, acceptance_contract_digest,
                        acceptance_evidence_digest
                 FROM orchestration_handoff_deliveries
                 WHERE attempt_id = ?1 AND edge_id = ?2 AND lease_epoch = ?3",
                params![delivery.attempt_id, delivery.edge_id, delivery.lease_epoch],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing.1 != delivery.delivery_payload_digest
                || existing.2 != delivery.envelope_object_ref
                || existing.3 != delivery.envelope_raw_sha256
                || existing.4 != delivery.envelope_sha256_jcs
                || existing.5 != delivery.acceptance_contract_digest
                || existing.6 != delivery.acceptance_evidence_digest
            {
                return Err(StorageError::HandoffDeliveryConflict {
                    attempt_id: delivery.attempt_id,
                    edge_id: delivery.edge_id,
                    lease_epoch: delivery.lease_epoch,
                });
            }
            tx.commit()?;
            return Ok(true);
        }
        tx.execute(
            "INSERT INTO orchestration_handoff_deliveries(
               delivery_id, run_id, attempt_id, edge_id, lease_epoch,
               declaration_digest, artifact_transfer_set_digest,
               idempotency_key, delivery_payload_digest,
               envelope_object_ref, envelope_raw_sha256, envelope_sha256_jcs,
               acceptance_contract_ref, acceptance_contract_digest,
               acceptance_evidence_ref, acceptance_evidence_digest,
               producer_context_manifest_digest, replay_receipt_json
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                      ?13, ?14, ?15, ?16, ?17, ?18)",
            params![
                delivery.delivery_id,
                delivery.run_id,
                delivery.attempt_id,
                delivery.edge_id,
                delivery.lease_epoch,
                delivery.declaration_digest,
                delivery.artifact_transfer_set_digest,
                delivery.idempotency_key,
                delivery.delivery_payload_digest,
                delivery.envelope_object_ref,
                delivery.envelope_raw_sha256,
                delivery.envelope_sha256_jcs,
                delivery.acceptance_contract_ref,
                delivery.acceptance_contract_digest,
                delivery.acceptance_evidence_ref,
                delivery.acceptance_evidence_digest,
                delivery.producer_context_manifest_digest,
                delivery.replay_receipt_json,
            ],
        )?;
        tx.commit()?;
        Ok(false)
    }

    pub fn insert_orchestration_task_node(
        &mut self,
        run_id: &str,
        node_id: &str,
        node_key: &str,
    ) -> Result<(), StorageError> {
        self.orchestration_run(run_id)?;
        self.connection.execute(
            "INSERT INTO orchestration_task_nodes(
               node_id, run_id, node_key, required, status, version,
               attempt_count, max_attempts
             ) VALUES(?1, ?2, ?3, 1, 'ready', 1, 0, 1)
             ON CONFLICT(node_id) DO NOTHING",
            params![node_id, run_id, node_key],
        )?;
        Ok(())
    }

    pub fn transition_task_ready_to_running(
        &mut self,
        node_id: &str,
        lease_owner: &str,
    ) -> Result<TaskReadyToRunningOutcome, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let node = tx
            .query_row(
                "SELECT run_id, status, attempt_count FROM orchestration_task_nodes WHERE node_id = ?1",
                [node_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationTaskNotFound {
                node_id: node_id.to_owned(),
            })?;
        if matches!(
            node.1.as_str(),
            "completed" | "failed" | "cancelled" | "interrupted" | "running"
        ) {
            return Err(StorageError::OrchestrationTaskTerminal {
                node_id: node_id.to_owned(),
            });
        }
        let attempt_no = node.2 + 1;
        let attempt_id = format!("{node_id}:attempt:{attempt_no}");
        let lease_epoch = 1;
        let now = crate::orchestration::now_unix()?;
        tx.execute(
            "INSERT INTO orchestration_task_attempts(
               attempt_id, run_id, node_id, attempt_no, status, lease_epoch
             ) VALUES(?1, ?2, ?3, ?4, 'running', ?5)",
            params![attempt_id, node.0, node_id, attempt_no, lease_epoch],
        )?;
        tx.execute(
            "INSERT INTO orchestration_leases(
               attempt_id, run_id, node_id, lease_epoch, lease_owner,
               heartbeat_at, deadline, coordinator_generation, status
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?6 + 30000, 1, 'active')",
            params![attempt_id, node.0, node_id, lease_epoch, lease_owner, now],
        )?;
        tx.execute(
            "UPDATE orchestration_task_nodes
             SET status = 'running', active_attempt_id = ?2,
                 attempt_count = ?3, version = version + 1
             WHERE node_id = ?1",
            params![node_id, attempt_id, attempt_no],
        )?;
        tx.commit()?;
        Ok(TaskReadyToRunningOutcome {
            node_id: node_id.to_owned(),
            attempt_id,
            attempt_no,
            lease_epoch,
        })
    }

    pub fn bump_coordinator_generation(&mut self, run_id: &str) -> Result<i64, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let generation = tx
            .query_row(
                "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
                [run_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationRunNotFound {
                run_id: run_id.to_owned(),
            })?;
        let next = generation + 1;
        tx.execute(
            "UPDATE orchestration_runs SET coordinator_generation = ?2 WHERE run_id = ?1",
            params![run_id, next],
        )?;
        tx.commit()?;
        Ok(next)
    }

    pub fn assert_lease_epoch_current(
        &self,
        attempt_id: &str,
        requested_epoch: i64,
    ) -> Result<(), StorageError> {
        let latest: Option<i64> = self
            .connection
            .query_row(
                "SELECT MAX(lease_epoch) FROM orchestration_leases WHERE attempt_id = ?1",
                [attempt_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        if latest.is_some_and(|epoch| epoch > requested_epoch) {
            return Err(StorageError::StaleLease {
                attempt_id: attempt_id.to_owned(),
            });
        }
        Ok(())
    }

    pub fn orchestration_recovery_state(
        &self,
        run_id: &str,
    ) -> Result<Vec<(String, String, i64)>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT node_id, status, attempt_count FROM orchestration_task_nodes WHERE run_id = ?1 ORDER BY node_id",
        )?;
        let rows = statement
            .query_map([run_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn orchestration_migration_checksum(&self) -> String {
        hex_digest(crate::MIGRATION_V15_SQL.as_bytes())
    }

    pub fn orchestration_schema_version(&self) -> i64 {
        SCHEMA_VERSION
    }

    pub fn legacy_versions_unchanged(&self) -> Result<Vec<i64>, StorageError> {
        let mut statement = self
            .connection
            .prepare("SELECT version FROM schema_migrations WHERE version < ?1 ORDER BY version")?;
        let rows = statement
            .query_map([V15_SCHEMA_VERSION], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

pub(crate) fn now_unix() -> Result<i64, StorageError> {
    use std::time::{SystemTime, UNIX_EPOCH};
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| {
            StorageError::Sqlite(rusqlite::Error::InvalidParameterName(source.to_string()))
        })?
        .as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteStore;

    fn seed(run_id: &str) -> OrchestrationRunSeed {
        let hex = "0".repeat(64);
        OrchestrationRunSeed {
            run_id: run_id.to_owned(),
            project_id: "project-a".to_owned(),
            brief_snapshot_id: format!("sha256:{hex}"),
            brief_tree_digest: hex.clone(),
            dag_snapshot_digest: hex.clone(),
            role_binding_snapshot_digest: hex.clone(),
        }
    }

    #[test]
    fn v15_migration_checksum_version_and_legacy_tables_are_recorded() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.orchestration_schema_version(), 15);
        assert_eq!(store.orchestration_migration_checksum().len(), 64);
        assert_eq!(
            store.legacy_versions_unchanged().unwrap(),
            vec![11, 12, 13, 14]
        );
        let table_count: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'orchestration!_%' ESCAPE '!'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_count, 12);
    }

    #[test]
    fn run_creation_binds_brief_snapshot_and_rejects_conflict() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        let record = store.orchestration_run("run-1").unwrap();
        assert_eq!(record.status, "prepared");
        assert_eq!(record.coordinator_generation, 1);
        let mut conflict = seed("run-1");
        conflict.brief_tree_digest = "1".repeat(64);
        assert!(matches!(
            store.create_orchestration_run(conflict),
            Err(StorageError::OrchestrationRunConflict { .. })
        ));
    }

    #[test]
    fn human_receipt_replay_conflict_and_cas_ordering() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .ensure_orchestration_milestone(
                "run-1",
                "milestone-1",
                "m1",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap();
        let receipt = HumanReceiptRecord {
            receipt_id: "receipt-1".into(),
            run_id: "run-1".into(),
            milestone_id: "milestone-1".into(),
            request_id: "request-1".into(),
            semantic_payload_hash: "payload-hash-1".into(),
            decision: "approved".into(),
            expected_version: 1,
            brief_tree_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            presented_artifact_set_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            acceptance_evidence_digest:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            authenticated_principal: "human-a".into(),
            core_timestamp: 1000,
        };
        assert!(!store.record_human_receipt(receipt.clone()).unwrap());
        assert!(store.record_human_receipt(receipt.clone()).unwrap());
        let mut conflicting = receipt;
        conflicting.semantic_payload_hash = "different".into();
        assert!(matches!(
            store.record_human_receipt(conflicting),
            Err(StorageError::HumanReceiptConflict { .. })
        ));
    }

    #[test]
    fn handoff_delivery_replay_and_conflict_are_slot_scoped() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        let delivery = HandoffDeliveryRecord {
            delivery_id: "delivery-1".into(),
            run_id: "run-1".into(),
            attempt_id: "attempt-1".into(),
            edge_id: "edge-1".into(),
            lease_epoch: 1,
            declaration_digest: "decl-1".into(),
            artifact_transfer_set_digest: "artifact-set-1".into(),
            idempotency_key: "key-1".into(),
            delivery_payload_digest: "payload-1".into(),
            envelope_object_ref:
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
            envelope_raw_sha256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .into(),
            envelope_sha256_jcs: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .into(),
            acceptance_contract_ref: "contract-1".into(),
            acceptance_contract_digest: "contract-digest-1".into(),
            acceptance_evidence_ref: "evidence-1".into(),
            acceptance_evidence_digest: "evidence-digest-1".into(),
            producer_context_manifest_digest: "context-1".into(),
            replay_receipt_json: None,
        };
        assert!(!store.record_handoff_delivery(delivery.clone()).unwrap());
        assert!(store.record_handoff_delivery(delivery.clone()).unwrap());
        let mut conflicting = delivery;
        conflicting.delivery_payload_digest = "different".into();
        assert!(matches!(
            store.record_handoff_delivery(conflicting),
            Err(StorageError::HandoffDeliveryConflict { .. })
        ));
    }

    #[test]
    fn ready_to_running_is_single_shot_and_stale_lease_fencing_works() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        let outcome = store
            .transition_task_ready_to_running("node-1", "worker-a")
            .unwrap();
        assert_eq!(outcome.attempt_no, 1);
        assert!(matches!(
            store.transition_task_ready_to_running("node-1", "worker-b"),
            Err(StorageError::OrchestrationTaskTerminal { .. })
        ));
        store
            .assert_lease_epoch_current(&outcome.attempt_id, 1)
            .unwrap();
        assert!(matches!(
            store.assert_lease_epoch_current(&outcome.attempt_id, 0),
            Err(StorageError::StaleLease { .. })
        ));
        assert_eq!(store.bump_coordinator_generation("run-1").unwrap(), 2);
    }

    #[test]
    fn recovery_matrix_and_cas_before_journal_ordering_are_explicit() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-2", "key-2")
            .unwrap();
        store
            .transition_task_ready_to_running("node-1", "worker-a")
            .unwrap();
        let matrix = store.orchestration_recovery_state("run-1").unwrap();
        assert_eq!(matrix[0].0, "node-1");
        assert_eq!(matrix[0].1, "running");
        assert_eq!(matrix[1].0, "node-2");
        assert_eq!(matrix[1].1, "ready");
        let record = store.orchestration_run("run-1").unwrap();
        assert!(is_object_ref(&record.brief_snapshot_id));
        assert_eq!(record.brief_tree_digest.len(), 64);
    }
}
