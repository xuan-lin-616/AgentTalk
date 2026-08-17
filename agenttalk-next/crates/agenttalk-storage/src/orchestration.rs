use crate::{SqliteStore, StorageError, SCHEMA_VERSION, V16_SCHEMA_VERSION};
use agenttalk_brief_sealer::PreparedBriefSeal;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

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
    pub lease_owner: String,
    pub coordinator_generation: i64,
    pub envelope_handoff_id: String,
    pub from_task_node_id: String,
    pub from_execution_run_id: String,
    pub to_task_node_id: String,
    pub dag_snapshot_digest: String,
    pub role_binding_snapshot_digest: String,
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
pub struct ArtifactBindingInput {
    pub binding_id: String,
    pub edge_port_id: String,
    pub source_output_port_id: String,
    pub target_input_port_id: String,
    pub object_ref: String,
    pub sha256: String,
    pub size: i64,
    pub content_schema_id: String,
    pub content_schema_version: String,
    pub content_schema_digest: String,
    pub normalized_content_type: String,
    pub normalized_content_type_policy_version: String,
    pub content_schema_ref_json: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskReadyToRunningOutcome {
    pub node_id: String,
    pub attempt_id: String,
    pub attempt_no: i64,
    pub lease_epoch: i64,
}

pub trait CasVerifier {
    fn verify_object(&self, object_ref: &str) -> Result<Vec<u8>, StorageError>;
}

pub struct CoreCasVerifier<'a> {
    pub cas: &'a agenttalk_brief_sealer::CoreCas,
}

impl CasVerifier for CoreCasVerifier<'_> {
    fn verify_object(&self, object_ref: &str) -> Result<Vec<u8>, StorageError> {
        self.cas
            .read(object_ref)
            .map_err(|_error| StorageError::ArtifactBodyMismatch)
    }
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
        tx.execute(
            "INSERT INTO orchestration_runs(
               run_id, project_id, status, version, brief_snapshot_id,
               brief_tree_digest, dag_snapshot_digest,
               role_binding_snapshot_digest, coordinator_generation
             ) VALUES(?1, ?2, 'pending', 1, ?3, ?4, ?5, ?6, 1)",
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
        presented_artifact_set_digest: &str,
        acceptance_evidence_digest: &str,
    ) -> Result<(), StorageError> {
        self.orchestration_run(run_id)?;
        self.connection.execute(
            "INSERT INTO orchestration_milestones(
               milestone_id, run_id, milestone_key, required, status, version,
               brief_tree_digest, presented_artifact_set_digest,
               acceptance_evidence_digest
             ) VALUES(?1, ?2, ?3, 1, 'awaiting_approval', 1, ?4, ?5, ?6)
             ON CONFLICT(milestone_id) DO NOTHING",
            params![
                milestone_id,
                run_id,
                milestone_key,
                brief_tree_digest,
                presented_artifact_set_digest,
                acceptance_evidence_digest,
            ],
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
        // Step 1: replay or conflict on the immutable receipt slot.
        let existing = tx
            .query_row(
                "SELECT run_id, semantic_payload_hash, decision, brief_tree_digest,
                        presented_artifact_set_digest, acceptance_evidence_digest,
                        authenticated_principal, expected_version
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
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing.1 != receipt.semantic_payload_hash
                || existing.2 != receipt.decision
                || existing.3 != receipt.brief_tree_digest
                || existing.4 != receipt.presented_artifact_set_digest
                || existing.5 != receipt.acceptance_evidence_digest
                || existing.6 != receipt.authenticated_principal
                || existing.7 != receipt.expected_version
            {
                return Err(StorageError::HumanReceiptConflict {
                    milestone_id: receipt.milestone_id,
                    request_id: receipt.request_id,
                });
            }
            tx.commit()?;
            return Ok(true);
        }

        // Step 2: milestone ownership.
        let milestone = tx
            .query_row(
                "SELECT run_id, status, version, brief_tree_digest,
                        presented_artifact_set_digest, acceptance_evidence_digest
                 FROM orchestration_milestones WHERE milestone_id = ?1",
                [&receipt.milestone_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationMilestoneNotFound {
                milestone_id: receipt.milestone_id.clone(),
            })?;
        if milestone.0 != receipt.run_id {
            return Err(StorageError::OrchestrationMilestoneStateInvalid {
                milestone_id: receipt.milestone_id,
                status: "run_id mismatch".into(),
            });
        }
        if milestone.1 != "awaiting_approval" {
            return Err(StorageError::OrchestrationMilestoneStateInvalid {
                milestone_id: receipt.milestone_id,
                status: milestone.1,
            });
        }

        // Step 3: run state and active attempt guard.
        let run_status: String = tx.query_row(
            "SELECT status FROM orchestration_runs WHERE run_id = ?1",
            [&receipt.run_id],
            |row| row.get(0),
        )?;
        if run_status != "awaiting_approval" {
            return Err(StorageError::OrchestrationRunStatusInvalid {
                run_id: receipt.run_id,
                status: run_status,
            });
        }
        let now = crate::orchestration::now_unix()?;
        let active_attempt: Option<String> = tx
            .query_row(
                "SELECT attempt_id FROM orchestration_leases
                 WHERE run_id = ?1 AND status = 'active' AND deadline > ?2 LIMIT 1",
                params![receipt.run_id, now],
                |row| row.get(0),
            )
            .optional()?;
        if active_attempt.is_some() {
            return Err(StorageError::OrchestrationActiveAttemptExists {
                run_id: receipt.run_id,
            });
        }

        // Step 4: receipt field validation against the milestone view.
        if receipt.expected_version != milestone.2
            || receipt.brief_tree_digest != milestone.3.unwrap_or_default()
            || receipt.presented_artifact_set_digest != milestone.4.unwrap_or_default()
            || receipt.acceptance_evidence_digest != milestone.5.unwrap_or_default()
        {
            return Err(StorageError::OrchestrationMilestoneStateInvalid {
                milestone_id: receipt.milestone_id,
                status: "receipt digest/version mismatch".into(),
            });
        }

        // Step 5: write the immutable receipt and close the run/milestone.
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
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&receipt.run_id],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            &receipt.run_id,
            "human_receipt_recorded",
            "human_receipt",
            &receipt.receipt_id,
            "{\"decision\":\"recorded\"}",
            &format!(
                "human_receipt:{}:{}",
                receipt.milestone_id, receipt.request_id
            ),
            generation,
        )?;
        if receipt.decision == "approve" {
            tx.execute(
                "UPDATE orchestration_milestones
                 SET status = 'approved', version = version + 1
                 WHERE milestone_id = ?1",
                [&receipt.milestone_id],
            )?;
            let pending_milestones: i64 = tx.query_row(
                "SELECT count(*) FROM orchestration_milestones
                 WHERE run_id = ?1 AND required = 1 AND status != 'approved'",
                [&receipt.run_id],
                |row| row.get(0),
            )?;
            let pending_nodes: i64 = tx.query_row(
                "SELECT count(*) FROM orchestration_task_nodes
                 WHERE run_id = ?1 AND required = 1 AND status != 'completed'",
                [&receipt.run_id],
                |row| row.get(0),
            )?;
            let next_run_status = if pending_milestones == 0 && pending_nodes == 0 {
                "completed"
            } else {
                "running"
            };
            tx.execute(
                "UPDATE orchestration_runs
                 SET status = ?2, version = version + 1
                 WHERE run_id = ?1",
                params![receipt.run_id, next_run_status],
            )?;
            append_audit_event(
                &tx,
                &receipt.run_id,
                "milestone_state_changed",
                "milestone",
                &receipt.milestone_id,
                "{\"status\":\"approved\"}",
                &format!("milestone_approved:{}", receipt.milestone_id),
                generation,
            )?;
            append_audit_event(
                &tx,
                &receipt.run_id,
                "run_state_changed",
                "run",
                &receipt.run_id,
                &format!("{{\"status\":\"{next_run_status}\"}}"),
                &format!("run_state_changed:{}:{next_run_status}", receipt.run_id),
                generation,
            )?;
        } else if receipt.decision == "reject" {
            tx.execute(
                "UPDATE orchestration_milestones
                 SET status = 'rejected', version = version + 1
                 WHERE milestone_id = ?1",
                [&receipt.milestone_id],
            )?;
            tx.execute(
                "UPDATE orchestration_runs
                 SET status = 'failed', terminal_reason = 'milestone_rejected',
                     version = version + 1
                 WHERE run_id = ?1",
                [&receipt.run_id],
            )?;
            append_audit_event(
                &tx,
                &receipt.run_id,
                "milestone_state_changed",
                "milestone",
                &receipt.milestone_id,
                "{\"status\":\"rejected\"}",
                &format!("milestone_rejected:{}", receipt.milestone_id),
                generation,
            )?;
            append_audit_event(
                &tx,
                &receipt.run_id,
                "run_state_changed",
                "run",
                &receipt.run_id,
                "{\"status\":\"failed\",\"terminal_reason\":\"milestone_rejected\"}",
                &format!("run_state_changed:{}:failed", receipt.run_id),
                generation,
            )?;
        } else {
            return Err(StorageError::OrchestrationMilestoneStateInvalid {
                milestone_id: receipt.milestone_id,
                status: receipt.decision,
            });
        }
        tx.commit()?;
        Ok(false)
    }

    pub fn record_handoff_delivery(
        &mut self,
        delivery: HandoffDeliveryRecord,
        bindings: &[ArtifactBindingInput],
        cas: &dyn CasVerifier,
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
                    attempt_id: delivery.attempt_id.clone(),
                    edge_id: delivery.edge_id.clone(),
                    lease_epoch: delivery.lease_epoch,
                });
            }
            tx.commit()?;
            return Ok(true);
        }
        // New path: full authority closure in one Immediate transaction.
        Self::validate_handoff_authority(&tx, &delivery, bindings)?;
        SqliteStore::verify_cas_before_journal(cas, &delivery, bindings)?;
        for binding in bindings {
            if binding.edge_port_id.is_empty() || binding.content_schema_ref_json.is_empty() {
                return Err(StorageError::OrchestrationArtifactBindingInvalid {
                    reason: "edge_port_id and content_schema_ref_json are required".into(),
                });
            }
            if !is_object_ref(&binding.object_ref)
                || !is_hex64(&binding.sha256)
                || binding.object_ref != format!("sha256:{}", binding.sha256)
            {
                return Err(StorageError::OrchestrationArtifactBindingInvalid {
                    reason: "object_ref must equal sha256:<sha256>".into(),
                });
            }
        }
        tx.execute(
            "INSERT INTO orchestration_handoff_deliveries(
               delivery_id, run_id, attempt_id, edge_id, lease_epoch,
               envelope_handoff_id, from_task_node_id, from_execution_run_id,
               to_task_node_id, lease_owner, coordinator_generation,
               dag_snapshot_digest, role_binding_snapshot_digest,
               declaration_digest, artifact_transfer_set_digest,
               idempotency_key, delivery_payload_digest,
               envelope_object_ref, envelope_raw_sha256, envelope_sha256_jcs,
               acceptance_contract_ref, acceptance_contract_digest,
               acceptance_evidence_ref, acceptance_evidence_digest,
               producer_context_manifest_digest, replay_receipt_json
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                      ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22,
                      ?23, ?24, ?25, ?26)",
            params![
                delivery.delivery_id,
                delivery.run_id,
                delivery.attempt_id,
                delivery.edge_id,
                delivery.lease_epoch,
                delivery.envelope_handoff_id,
                delivery.from_task_node_id,
                delivery.from_execution_run_id,
                delivery.to_task_node_id,
                delivery.lease_owner,
                delivery.coordinator_generation,
                delivery.dag_snapshot_digest,
                delivery.role_binding_snapshot_digest,
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
        for binding in bindings {
            tx.execute(
                "INSERT INTO orchestration_artifact_bindings(
                   binding_id, run_id, delivery_id, edge_port_id,
                   source_output_port_id, target_input_port_id,
                   object_ref, sha256, size,
                   content_schema_id, content_schema_version,
                   content_schema_digest, normalized_content_type,
                   normalized_content_type_policy_version,
                   content_schema_ref_json
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                          ?12, ?13, ?14, ?15)",
                params![
                    binding.binding_id,
                    delivery.run_id,
                    delivery.delivery_id,
                    binding.edge_port_id,
                    binding.source_output_port_id,
                    binding.target_input_port_id,
                    binding.object_ref,
                    binding.sha256,
                    binding.size,
                    binding.content_schema_id,
                    binding.content_schema_version,
                    binding.content_schema_digest,
                    binding.normalized_content_type,
                    binding.normalized_content_type_policy_version,
                    binding.content_schema_ref_json,
                ],
            )?;
        }
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&delivery.run_id],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            &delivery.run_id,
            "handoff_delivery_recorded",
            "handoff_delivery",
            &delivery.delivery_id,
            "{\"status\":\"journaled\"}",
            &format!("handoff_delivery:{}", delivery.delivery_id),
            generation,
        )?;
        Self::maybe_complete_attempt_sealing(&tx, &delivery)?;
        tx.commit()?;
        Ok(false)
    }

    fn maybe_complete_attempt_sealing(
        tx: &rusqlite::Transaction<'_>,
        delivery: &HandoffDeliveryRecord,
    ) -> Result<(), StorageError> {
        let attempt = tx
            .query_row(
                "SELECT node_id, status FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&delivery.attempt_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            })?;
        if attempt.1 != "sealing" {
            return Ok(());
        }
        let required_edges: Vec<String> = tx
            .prepare(
                "SELECT edge_id FROM orchestration_edges WHERE run_id = ?1 AND from_node_id = ?2",
            )?
            .query_map(params![delivery.run_id, attempt.0], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        if required_edges.is_empty() {
            return Ok(());
        }
        let mut pending_edges = Vec::new();
        for edge_id in &required_edges {
            let delivered: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM orchestration_handoff_deliveries
                     WHERE attempt_id = ?1 AND edge_id = ?2 AND lease_epoch = ?3",
                    params![delivery.attempt_id, edge_id, delivery.lease_epoch],
                    |row| row.get(0),
                )
                .optional()?;
            if delivered.is_none() {
                pending_edges.push(edge_id.clone());
            }
        }
        if !pending_edges.is_empty() {
            return Ok(());
        }
        tx.execute(
            "UPDATE orchestration_task_attempts SET status = 'completed' WHERE attempt_id = ?1",
            [&delivery.attempt_id],
        )?;
        tx.execute(
            "UPDATE orchestration_task_nodes SET status = 'completed', version = version + 1 WHERE node_id = ?1",
            [&attempt.0],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&delivery.run_id],
            |row| row.get(0),
        )?;
        append_audit_event(
            tx,
            &delivery.run_id,
            "task_attempt_completed",
            "task_attempt",
            &delivery.attempt_id,
            "{\"status\":\"completed\"}",
            &format!("task_attempt_completed:{}", delivery.attempt_id),
            generation,
        )?;
        append_audit_event(
            tx,
            &delivery.run_id,
            "task_node_completed",
            "task_node",
            &attempt.0,
            "{\"status\":\"completed\"}",
            &format!("task_node_completed:{}", attempt.0),
            generation,
        )?;
        Ok(())
    }

    fn verify_cas_before_journal(
        cas: &dyn CasVerifier,
        delivery: &HandoffDeliveryRecord,
        bindings: &[ArtifactBindingInput],
    ) -> Result<(), StorageError> {
        verify_cas_object(
            cas,
            &delivery.envelope_object_ref,
            &delivery.envelope_raw_sha256,
        )?;
        verify_cas_object(
            cas,
            &delivery.acceptance_contract_ref,
            &delivery.acceptance_contract_digest,
        )?;
        verify_cas_object(
            cas,
            &delivery.acceptance_evidence_ref,
            &delivery.acceptance_evidence_digest,
        )?;
        for binding in bindings {
            verify_cas_object(cas, &binding.object_ref, &binding.sha256)?;
        }
        Ok(())
    }

    fn validate_handoff_authority(
        tx: &rusqlite::Transaction<'_>,
        delivery: &HandoffDeliveryRecord,
        bindings: &[ArtifactBindingInput],
    ) -> Result<(), StorageError> {
        use agenttalk_orchestration_contracts::handoff;
        use serde_json::{Map, Value};

        let attempt = tx
            .query_row(
                "SELECT run_id, node_id, from_execution_run_id, status
             FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&delivery.attempt_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            })?;
        if attempt.0 != delivery.run_id
            || attempt.1 != delivery.from_task_node_id
            || attempt.2.as_deref() != Some(delivery.from_execution_run_id.as_str())
            || attempt.3 != "sealing"
        {
            return Err(StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            });
        }

        let edge = tx
            .query_row(
                "SELECT run_id, from_node_id, to_node_id, dag_snapshot_digest
             FROM orchestration_edges WHERE edge_id = ?1",
                [&delivery.edge_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            })?;
        if edge.0 != delivery.run_id
            || edge.1 != attempt.1
            || edge.1 != delivery.from_task_node_id
            || edge.2 != delivery.to_task_node_id
            || edge.3 != delivery.dag_snapshot_digest
        {
            return Err(StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            });
        }
        let run_authority = tx.query_row(
            "SELECT dag_snapshot_digest, role_binding_snapshot_digest
                 FROM orchestration_runs WHERE run_id = ?1",
            [&delivery.run_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        if run_authority.0 != delivery.dag_snapshot_digest
            || run_authority.1 != delivery.role_binding_snapshot_digest
        {
            return Err(StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            });
        }
        let role_binding_ok: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM orchestration_role_binding_snapshots
                 WHERE run_id = ?1 AND digest = ?2 LIMIT 1",
                params![delivery.run_id, delivery.role_binding_snapshot_digest],
                |row| row.get(0),
            )
            .optional()?;
        if role_binding_ok.is_none() {
            return Err(StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            });
        }
        let context_ok: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM orchestration_context_manifest_authorities
                 WHERE run_id = ?1 AND attempt_id = ?2 AND producer_context_manifest_digest = ?3 LIMIT 1",
                params![
                    delivery.run_id,
                    delivery.attempt_id,
                    delivery.producer_context_manifest_digest
                ],
                |row| row.get(0),
            )
            .optional()?;
        if context_ok.is_none() {
            return Err(StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            });
        }

        let lease = tx
            .query_row(
                "SELECT run_id, status, deadline, lease_owner, coordinator_generation
             FROM orchestration_leases
             WHERE attempt_id = ?1 AND lease_epoch = ?2
             ORDER BY coordinator_generation DESC LIMIT 1",
                params![delivery.attempt_id, delivery.lease_epoch],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            })?;
        let current_generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&delivery.run_id],
            |row| row.get(0),
        )?;
        let now = crate::orchestration::now_unix()?;
        if lease.0 != delivery.run_id
            || lease.1 != "active"
            || lease.2 <= now
            || lease.3 != delivery.lease_owner
            || lease.4 != delivery.coordinator_generation
            || lease.4 != current_generation
        {
            return Err(StorageError::StaleLease {
                attempt_id: delivery.attempt_id.clone(),
            });
        }

        if delivery.delivery_id != delivery.envelope_handoff_id
            || !delivery
                .delivery_id
                .strip_prefix("handoff-")
                .is_some_and(is_hex64)
        {
            return Err(StorageError::HandoffDeliveryConflict {
                attempt_id: delivery.attempt_id.clone(),
                edge_id: delivery.edge_id.clone(),
                lease_epoch: delivery.lease_epoch,
            });
        }

        if !is_object_ref(&delivery.envelope_object_ref)
            || !is_hex64(&delivery.envelope_raw_sha256)
            || !is_hex64(&delivery.envelope_sha256_jcs)
            || delivery.envelope_object_ref != format!("sha256:{}", delivery.envelope_raw_sha256)
            || delivery.acceptance_contract_ref
                != format!("sha256:{}", delivery.acceptance_contract_digest)
            || delivery.acceptance_evidence_ref
                != format!("sha256:{}", delivery.acceptance_evidence_digest)
        {
            return Err(StorageError::HandoffDeliveryConflict {
                attempt_id: delivery.attempt_id.clone(),
                edge_id: delivery.edge_id.clone(),
                lease_epoch: delivery.lease_epoch,
            });
        }

        let mut seen_bindings = HashSet::new();
        for binding in bindings {
            if binding.edge_port_id.is_empty()
                || binding.content_schema_ref_json.is_empty()
                || binding.content_schema_id.is_empty()
                || binding.content_schema_digest.is_empty()
                || binding.normalized_content_type.is_empty()
                || binding.normalized_content_type_policy_version.is_empty()
                || !is_hex64(&binding.content_schema_digest)
                || binding.size < 0
                || !is_object_ref(&binding.object_ref)
                || !is_hex64(&binding.sha256)
                || binding.object_ref != format!("sha256:{}", binding.sha256)
            {
                return Err(StorageError::OrchestrationArtifactBindingInvalid {
                    reason: "sealed artifact binding fields are invalid".into(),
                });
            }
            if !seen_bindings.insert(binding.edge_port_id.clone()) {
                return Err(StorageError::OrchestrationArtifactBindingInvalid {
                    reason: format!("duplicate edge_port binding: {}", binding.edge_port_id),
                });
            }
            let port = tx
                .query_row(
                    "SELECT edge_id, source_output_port_id, target_input_port_id
                 FROM orchestration_edge_ports WHERE edge_port_id = ?1",
                    [&binding.edge_port_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| StorageError::OrchestrationArtifactBindingInvalid {
                    reason: format!("unknown edge_port: {}", binding.edge_port_id),
                })?;
            if port.0 != delivery.edge_id
                || port.1 != binding.source_output_port_id
                || port.2 != binding.target_input_port_id
            {
                return Err(StorageError::OrchestrationArtifactBindingInvalid {
                    reason: "edge port does not match sealed edge authority".into(),
                });
            }
        }

        let required_ports: HashSet<String> = tx
            .prepare("SELECT edge_port_id FROM orchestration_edge_ports WHERE edge_id = ?1")?
            .query_map([&delivery.edge_id], |row| row.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        let provided_ports: HashSet<String> = bindings
            .iter()
            .map(|binding| binding.edge_port_id.clone())
            .collect();
        if required_ports != provided_ports {
            return Err(StorageError::OrchestrationArtifactBindingInvalid {
                reason: "artifact bindings do not exactly cover the sealed edge port set".into(),
            });
        }

        let bindings_json: Vec<Value> = bindings
            .iter()
            .map(|binding| {
                Value::Object(Map::from_iter([
                    ("sourceOutput".to_owned(), {
                        Value::Object(Map::from_iter([(
                            "portId".to_owned(),
                            Value::String(binding.source_output_port_id.clone()),
                        )]))
                    }),
                    ("targetInput".to_owned(), {
                        Value::Object(Map::from_iter([(
                            "portId".to_owned(),
                            Value::String(binding.target_input_port_id.clone()),
                        )]))
                    }),
                    ("artifactRef".to_owned(), {
                        Value::Object(Map::from_iter([
                            (
                                "objectRef".to_owned(),
                                Value::String(binding.object_ref.clone()),
                            ),
                            ("sha256".to_owned(), Value::String(binding.sha256.clone())),
                            ("size".to_owned(), Value::from(binding.size)),
                            ("contentSchemaRef".to_owned(), {
                                Value::Object(Map::from_iter([
                                    (
                                        "id".to_owned(),
                                        Value::String(binding.content_schema_id.clone()),
                                    ),
                                    (
                                        "version".to_owned(),
                                        Value::String(binding.content_schema_version.clone()),
                                    ),
                                    (
                                        "digest".to_owned(),
                                        Value::String(binding.content_schema_digest.clone()),
                                    ),
                                ]))
                            }),
                            (
                                "normalizedContentType".to_owned(),
                                Value::String(binding.normalized_content_type.clone()),
                            ),
                            (
                                "normalizedContentTypePolicyVersion".to_owned(),
                                Value::String(
                                    binding.normalized_content_type_policy_version.clone(),
                                ),
                            ),
                        ]))
                    }),
                ]))
            })
            .collect();

        let envelope = Value::Object(Map::from_iter([
            (
                "schemaVersion".to_owned(),
                Value::String("agenttalk.handoff.envelope.v1".to_owned()),
            ),
            (
                "handoffId".to_owned(),
                Value::String(delivery.envelope_handoff_id.clone()),
            ),
            (
                "projectRunId".to_owned(),
                Value::String(delivery.run_id.clone()),
            ),
            ("edgeId".to_owned(), Value::String(delivery.edge_id.clone())),
            ("from".to_owned(), {
                Value::Object(Map::from_iter([
                    (
                        "taskNodeId".to_owned(),
                        Value::String(delivery.from_task_node_id.clone()),
                    ),
                    (
                        "attemptId".to_owned(),
                        Value::String(delivery.attempt_id.clone()),
                    ),
                    (
                        "executionRunId".to_owned(),
                        Value::String(delivery.from_execution_run_id.clone()),
                    ),
                ]))
            }),
            ("to".to_owned(), {
                Value::Object(Map::from_iter([(
                    "taskNodeId".to_owned(),
                    Value::String(delivery.to_task_node_id.clone()),
                )]))
            }),
            ("leaseEpoch".to_owned(), Value::from(delivery.lease_epoch)),
            ("artifactBindings".to_owned(), Value::Array(bindings_json)),
        ]));

        let computed_idempotency = handoff::idempotency_key_hex(&envelope).map_err(|_| {
            StorageError::HandoffDeliveryConflict {
                attempt_id: delivery.attempt_id.clone(),
                edge_id: delivery.edge_id.clone(),
                lease_epoch: delivery.lease_epoch,
            }
        })?;
        if computed_idempotency != delivery.idempotency_key {
            return Err(StorageError::HandoffDeliveryConflict {
                attempt_id: delivery.attempt_id.clone(),
                edge_id: delivery.edge_id.clone(),
                lease_epoch: delivery.lease_epoch,
            });
        }

        let computed_payload = handoff::delivery_payload_digest_hex(
            &delivery.declaration_digest,
            &delivery.artifact_transfer_set_digest,
            &delivery.acceptance_contract_digest,
            &delivery.acceptance_evidence_digest,
            &delivery.producer_context_manifest_digest,
            &delivery.dag_snapshot_digest,
            &delivery.role_binding_snapshot_digest,
        )
        .map_err(|_| StorageError::HandoffDeliveryConflict {
            attempt_id: delivery.attempt_id.clone(),
            edge_id: delivery.edge_id.clone(),
            lease_epoch: delivery.lease_epoch,
        })?;
        if computed_payload != delivery.delivery_payload_digest {
            return Err(StorageError::HandoffDeliveryConflict {
                attempt_id: delivery.attempt_id.clone(),
                edge_id: delivery.edge_id.clone(),
                lease_epoch: delivery.lease_epoch,
            });
        }

        let computed_transfer =
            handoff::artifact_transfer_set_digest_hex(&envelope).map_err(|_| {
                StorageError::HandoffDeliveryConflict {
                    attempt_id: delivery.attempt_id.clone(),
                    edge_id: delivery.edge_id.clone(),
                    lease_epoch: delivery.lease_epoch,
                }
            })?;
        if computed_transfer != delivery.artifact_transfer_set_digest {
            return Err(StorageError::HandoffDeliveryConflict {
                attempt_id: delivery.attempt_id.clone(),
                edge_id: delivery.edge_id.clone(),
                lease_epoch: delivery.lease_epoch,
            });
        }

        for digest in [
            &delivery.declaration_digest,
            &delivery.artifact_transfer_set_digest,
            &delivery.acceptance_contract_digest,
            &delivery.acceptance_evidence_digest,
            &delivery.producer_context_manifest_digest,
            &delivery.dag_snapshot_digest,
            &delivery.role_binding_snapshot_digest,
        ] {
            if !is_hex64(digest) {
                return Err(StorageError::HandoffDeliveryConflict {
                    attempt_id: delivery.attempt_id.clone(),
                    edge_id: delivery.edge_id.clone(),
                    lease_epoch: delivery.lease_epoch,
                });
            }
        }

        Ok(())
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
             ) VALUES(?1, ?2, ?3, 1, 'pending', 1, 0, 1)
             ON CONFLICT(node_id) DO NOTHING",
            params![node_id, run_id, node_key],
        )?;
        Ok(())
    }

    pub fn mark_orchestration_task_ready(
        &mut self,
        node_id: &str,
        input_artifact_set_digest: &str,
        role_id: &str,
        acceptance_contract_ref: &str,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let node = tx
            .query_row(
                "SELECT run_id, status FROM orchestration_task_nodes WHERE node_id = ?1",
                [node_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationTaskNotFound {
                node_id: node_id.to_owned(),
            })?;
        if node.1 != "pending" {
            return Err(StorageError::OrchestrationTaskNotReady {
                node_id: node_id.to_owned(),
                status: node.1,
            });
        }
        tx.execute(
            "UPDATE orchestration_task_nodes
             SET status = 'ready', input_artifact_set_digest = ?2,
                 role_id = ?3, acceptance_contract_ref = ?4,
                 version = version + 1
             WHERE node_id = ?1",
            params![
                node_id,
                input_artifact_set_digest,
                role_id,
                acceptance_contract_ref
            ],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&node.0],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            &node.0,
            "task_node_ready",
            "task_node",
            node_id,
            "{\"status\":\"ready\"}",
            &format!("task_node_ready:{node_id}"),
            generation,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn transition_task_ready_to_running(
        &mut self,
        node_id: &str,
        from_execution_run_id: &str,
        lease_owner: &str,
    ) -> Result<TaskReadyToRunningOutcome, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let node = tx
            .query_row(
                "SELECT run_id, status, attempt_count, max_attempts
                 FROM orchestration_task_nodes WHERE node_id = ?1",
                [node_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationTaskNotFound {
                node_id: node_id.to_owned(),
            })?;
        if node.1 != "ready" {
            return Err(StorageError::OrchestrationTaskNotReady {
                node_id: node_id.to_owned(),
                status: node.1,
            });
        }
        if node.2 >= node.3 {
            return Err(StorageError::OrchestrationTaskTerminal {
                node_id: node_id.to_owned(),
            });
        }
        let coordinator_generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&node.0],
            |row| row.get(0),
        )?;
        let attempt_no = node.2 + 1;
        let lease_epoch = attempt_no;
        let attempt_id = format!("{node_id}:attempt:{attempt_no}");
        let now = crate::orchestration::now_unix()?;
        // Attempt is created as leased before it may transition to running.
        tx.execute(
            "INSERT INTO orchestration_task_attempts(
               attempt_id, run_id, node_id, attempt_no, from_execution_run_id,
               status, lease_epoch
             ) VALUES(?1, ?2, ?3, ?4, ?5, 'leased', ?6)",
            params![
                attempt_id,
                node.0,
                node_id,
                attempt_no,
                from_execution_run_id,
                lease_epoch,
            ],
        )?;
        tx.execute(
            "INSERT INTO orchestration_leases(
               attempt_id, run_id, node_id, lease_epoch, lease_owner,
               heartbeat_at, deadline, coordinator_generation, status
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?6 + 30000, ?7, 'active')",
            params![
                attempt_id,
                node.0,
                node_id,
                lease_epoch,
                lease_owner,
                now,
                coordinator_generation,
            ],
        )?;
        append_audit_event(
            &tx,
            &node.0,
            "task_attempt_leased",
            "task_attempt",
            &attempt_id,
            "{\"lease_epoch\":1}",
            &format!("task_attempt_leased:{attempt_id}:{lease_epoch}"),
            coordinator_generation,
        )?;
        // Node projection is written directly as running; leased is only an
        // Attempt state.
        tx.execute(
            "UPDATE orchestration_task_nodes
             SET status = 'running', active_attempt_id = ?2,
                 attempt_count = ?3, version = version + 1
             WHERE node_id = ?1",
            params![node_id, attempt_id, attempt_no],
        )?;
        tx.execute(
            "UPDATE orchestration_task_attempts SET status = 'running' WHERE attempt_id = ?1",
            [&attempt_id],
        )?;
        append_audit_event(
            &tx,
            &node.0,
            "task_attempt_running",
            "task_attempt",
            &attempt_id,
            "{\"status\":\"running\"}",
            &format!("task_attempt_running:{attempt_id}"),
            coordinator_generation,
        )?;
        tx.commit()?;
        Ok(TaskReadyToRunningOutcome {
            node_id: node_id.to_owned(),
            attempt_id,
            attempt_no,
            lease_epoch,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn set_task_max_attempts(
        &mut self,
        node_id: &str,
        max_attempts: i64,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_id: String = tx
            .query_row(
                "SELECT run_id FROM orchestration_task_nodes WHERE node_id = ?1",
                [node_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationTaskNotFound {
                node_id: node_id.to_owned(),
            })?;
        tx.execute(
            "UPDATE orchestration_task_nodes SET max_attempts = ?2 WHERE node_id = ?1",
            params![node_id, max_attempts],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&run_id],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            &run_id,
            "task_node_budget_changed",
            "task_node",
            node_id,
            &format!("{{\"max_attempts\":{max_attempts}}}"),
            &format!("task_node_budget_changed:{node_id}:{max_attempts}"),
            generation,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn transition_attempt_to_sealing(&mut self, node_id: &str) -> Result<String, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let node = tx
            .query_row(
                "SELECT run_id, status, active_attempt_id FROM orchestration_task_nodes WHERE node_id = ?1",
                [node_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationTaskNotFound {
                node_id: node_id.to_owned(),
            })?;
        if node.1 != "running" {
            return Err(StorageError::OrchestrationTaskNotReady {
                node_id: node_id.to_owned(),
                status: node.1,
            });
        }
        let attempt_id = node
            .2
            .ok_or_else(|| StorageError::OrchestrationTaskNotReady {
                node_id: node_id.to_owned(),
                status: node.1,
            })?;
        tx.execute(
            "UPDATE orchestration_task_attempts SET status = 'sealing' WHERE attempt_id = ?1 AND status = 'running'",
            [&attempt_id],
        )?;
        tx.execute(
            "UPDATE orchestration_task_nodes SET status = 'sealing' WHERE node_id = ?1",
            [node_id],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&node.0],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            &node.0,
            "task_attempt_sealing",
            "task_attempt",
            &attempt_id,
            "{\"status\":\"sealing\"}",
            &format!("task_attempt_sealing:{attempt_id}"),
            generation,
        )?;
        tx.commit()?;
        Ok(attempt_id)
    }

    pub fn recover_active_attempt_interrupted(
        &mut self,
        node_id: &str,
    ) -> Result<String, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let node = tx
            .query_row(
                "SELECT run_id, active_attempt_id, attempt_count, max_attempts
                 FROM orchestration_task_nodes WHERE node_id = ?1",
                [node_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationTaskNotFound {
                node_id: node_id.to_owned(),
            })?;
        let attempt_id = node
            .1
            .ok_or_else(|| StorageError::OrchestrationTaskNotReady {
                node_id: node_id.to_owned(),
                status: "no active attempt".into(),
            })?;
        tx.execute(
            "UPDATE orchestration_task_attempts
             SET status = 'interrupted'
             WHERE attempt_id = ?1 AND status IN ('leased','running','sealing')",
            [&attempt_id],
        )?;
        let next_node_status = if node.2 < node.3 { "ready" } else { "failed" };
        tx.execute(
            "UPDATE orchestration_task_nodes
             SET status = ?2, active_attempt_id = NULL, version = version + 1
             WHERE node_id = ?1",
            params![node_id, next_node_status],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [&node.0],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            &node.0,
            "task_attempt_interrupted",
            "task_attempt",
            &attempt_id,
            "{\"status\":\"interrupted\"}",
            &format!("task_attempt_interrupted:{attempt_id}"),
            generation,
        )?;
        append_audit_event(
            &tx,
            &node.0,
            "task_node_recovered",
            "task_node",
            node_id,
            &format!("{{\"status\":\"{next_node_status}\"}}"),
            &format!("task_node_recovered:{node_id}:{next_node_status}"),
            generation,
        )?;
        tx.commit()?;
        Ok(attempt_id)
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
        append_audit_event(
            &tx,
            run_id,
            "coordinator_generation_bumped",
            "run",
            run_id,
            &format!("{{\"coordinator_generation\":{next}}}"),
            &format!("coordinator_generation_bumped:{run_id}:{next}"),
            next,
        )?;
        tx.commit()?;
        Ok(next)
    }

    pub fn assert_lease_epoch_current(
        &self,
        attempt_id: &str,
        requested_epoch: i64,
        requested_generation: i64,
        expected_owner: &str,
    ) -> Result<(), StorageError> {
        let now = crate::orchestration::now_unix()?;
        let current = self
            .connection
            .query_row(
                "SELECT l.status, l.deadline, l.lease_owner, l.coordinator_generation,
                        r.coordinator_generation
                 FROM orchestration_leases l
                 JOIN orchestration_runs r ON r.run_id = l.run_id
                 WHERE l.attempt_id = ?1 AND l.lease_epoch = ?2
                 ORDER BY l.coordinator_generation DESC LIMIT 1",
                params![attempt_id, requested_epoch],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StorageError::StaleLease {
                attempt_id: attempt_id.to_owned(),
            })?;
        if current.0 != "active"
            || current.1 <= now
            || current.2 != expected_owner
            || current.3 != requested_generation
            || current.3 != current.4
        {
            return Err(StorageError::StaleLease {
                attempt_id: attempt_id.to_owned(),
            });
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn transition_run_to_awaiting_approval(
        &mut self,
        run_id: &str,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let status: String = tx
            .query_row(
                "SELECT status FROM orchestration_runs WHERE run_id = ?1",
                [run_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StorageError::OrchestrationRunNotFound {
                run_id: run_id.to_owned(),
            })?;
        if status != "pending" {
            return Err(StorageError::OrchestrationRunStatusInvalid {
                run_id: run_id.to_owned(),
                status,
            });
        }
        tx.execute(
            "UPDATE orchestration_runs SET status = 'awaiting_approval', version = version + 1 WHERE run_id = ?1",
            [run_id],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            run_id,
            "run_state_changed",
            "run",
            run_id,
            "{\"status\":\"awaiting_approval\"}",
            &format!("run_state_changed:{run_id}:awaiting_approval"),
            generation,
        )?;
        tx.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn record_role_binding_snapshot(
        &mut self,
        run_id: &str,
        snapshot_id: &str,
        digest: &str,
        role_id: &str,
        agent_id: &str,
        workspace_access: &str,
    ) -> Result<(), StorageError> {
        self.orchestration_run(run_id)?;
        if !is_hex64(digest) {
            return Err(StorageError::OrchestrationArtifactBindingInvalid {
                reason: "role binding digest must be lowercase hex64".into(),
            });
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM orchestration_role_binding_snapshots
                 WHERE run_id = ?1 AND role_id = ?2 AND agent_id = ?3",
                params![run_id, role_id, agent_id],
                |row| row.get(0),
            )
            .optional()?;
        if existing.is_some() {
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "INSERT INTO orchestration_role_binding_snapshots(
               role_binding_snapshot_id, run_id, digest, sealed_at,
               role_id, agent_id, workspace_access
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                snapshot_id,
                run_id,
                digest,
                crate::orchestration::now_unix()?,
                role_id,
                agent_id,
                workspace_access,
            ],
        )?;
        let generation: i64 = tx.query_row(
            "SELECT coordinator_generation FROM orchestration_runs WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )?;
        append_audit_event(
            &tx,
            run_id,
            "role_binding_snapshot_recorded",
            "role_binding_snapshot",
            snapshot_id,
            "{\"status\":\"sealed\"}",
            &format!("role_binding_snapshot:{snapshot_id}"),
            generation,
        )?;
        tx.commit()?;
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
        hex_digest(crate::MIGRATION_V16_SQL.as_bytes())
    }

    pub fn orchestration_schema_version(&self) -> i64 {
        SCHEMA_VERSION
    }

    pub fn legacy_versions_unchanged(&self) -> Result<Vec<i64>, StorageError> {
        let mut statement = self
            .connection
            .prepare("SELECT version FROM schema_migrations WHERE version < ?1 ORDER BY version")?;
        let rows = statement
            .query_map([V16_SCHEMA_VERSION], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

fn verify_cas_object(
    cas: &dyn CasVerifier,
    object_ref: &str,
    expected_sha256: &str,
) -> Result<(), StorageError> {
    let expected_hex = object_ref.strip_prefix("sha256:").ok_or_else(|| {
        StorageError::OrchestrationArtifactBindingInvalid {
            reason: "object ref must be sha256:<hex>".into(),
        }
    })?;
    if expected_hex != expected_sha256 || !is_hex64(expected_sha256) {
        return Err(StorageError::OrchestrationArtifactBindingInvalid {
            reason: "object_ref does not match expected sha256".into(),
        });
    }
    let bytes = cas.verify_object(object_ref)?;
    let actual = hex_digest(&bytes);
    if actual != expected_sha256 {
        return Err(StorageError::OrchestrationArtifactBindingInvalid {
            reason: "CAS object content digest mismatch".into(),
        });
    }
    Ok(())
}

fn canonicalize_audit_payload(payload: &str) -> Result<String, StorageError> {
    let value = agenttalk_orchestration_contracts::json::parse_duplicate_safe_str(payload)
        .map_err(|error| StorageError::AuditPayloadCanonicalization {
            reason: error.to_string(),
        })?;
    // The frozen contract rule set rejects duplicate keys and non-NFC
    // strings. Audit payloads add one stricter rule: every number must be a
    // literal safe integer, not a floating-point representation such as 2.0.
    reject_non_integer_numbers(&value, "$")?;
    let canonical =
        agenttalk_orchestration_contracts::json::canonicalize(&value).map_err(|error| {
            StorageError::AuditPayloadCanonicalization {
                reason: error.to_string(),
            }
        })?;
    Ok(String::from_utf8(canonical).expect("RFC 8785 canonical JSON is valid UTF-8"))
}

fn reject_non_integer_numbers(value: &serde_json::Value, path: &str) -> Result<(), StorageError> {
    match value {
        serde_json::Value::Number(number) => {
            if number.as_i64().is_none() && number.as_u64().is_none() {
                return Err(StorageError::AuditPayloadCanonicalization {
                    reason: format!("non-integer number at {path}"),
                });
            }
            Ok(())
        }
        serde_json::Value::Array(values) => {
            for (index, item) in values.iter().enumerate() {
                reject_non_integer_numbers(item, &format!("{path}[{index}]"))?;
            }
            Ok(())
        }
        serde_json::Value::Object(object) => {
            for (key, item) in object {
                reject_non_integer_numbers(item, &format!("{path}.{key}"))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn append_audit_event(
    tx: &rusqlite::Transaction<'_>,
    run_id: &str,
    event_type: &str,
    subject_kind: &str,
    subject_id: &str,
    payload_json: &str,
    idempotency_key: &str,
    coordinator_generation: i64,
) -> Result<(), StorageError> {
    let sequence: i64 = tx.query_row(
        "SELECT COALESCE(MAX(sequence), 0) + 1 FROM orchestration_audit_events WHERE run_id = ?1",
        [run_id],
        |row| row.get(0),
    )?;
    let canonical_payload = canonicalize_audit_payload(payload_json)?;
    let event_id = format!("{run_id}:{sequence}:{event_type}");
    let payload_sha256 = hex_digest(canonical_payload.as_bytes());
    let core_timestamp = crate::orchestration::now_unix()?;
    tx.execute(
        "INSERT INTO orchestration_audit_events(
           event_id, run_id, sequence, event_type, schema_version,
           subject_kind, subject_id, payload_json, payload_sha256,
           idempotency_key, coordinator_generation, core_timestamp
         ) VALUES(?1, ?2, ?3, ?4, 'v1', ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            event_id,
            run_id,
            sequence,
            event_type,
            subject_kind,
            subject_id,
            canonical_payload,
            payload_sha256,
            idempotency_key,
            coordinator_generation,
            core_timestamp,
        ],
    )?;
    Ok(())
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
struct TestCas;

#[cfg(test)]
impl CasVerifier for TestCas {
    fn verify_object(&self, _object_ref: &str) -> Result<Vec<u8>, StorageError> {
        Ok(vec![0u8; 32])
    }
}

#[cfg(test)]
struct MissingCas;

#[cfg(test)]
impl CasVerifier for MissingCas {
    fn verify_object(&self, _object_ref: &str) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::ArtifactBodyNotFound {
            id: "missing".into(),
        })
    }
}

#[cfg(test)]
struct TamperedCas;

#[cfg(test)]
impl CasVerifier for TamperedCas {
    fn verify_object(&self, _object_ref: &str) -> Result<Vec<u8>, StorageError> {
        Ok(vec![1u8; 32])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqliteStore;

    fn prepare_sealed_delivery(
        store: &mut SqliteStore,
    ) -> (HandoffDeliveryRecord, TaskReadyToRunningOutcome) {
        use agenttalk_orchestration_contracts::handoff;
        use serde_json::{Map, Value};

        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        let outcome = store
            .transition_task_ready_to_running("node-1", "exec-run-1", "worker-a")
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO orchestration_edges(
                   edge_id, run_id, from_node_id, to_node_id,
                   dag_snapshot_digest, allowed_consumer_json
                 ) VALUES('edge-1', 'run-1', 'node-1', 'node-1', ?1, '[]')",
                [hex64('b')],
            )
            .unwrap();
        store.transition_attempt_to_sealing("node-1").unwrap();
        let dag = hex64('b');
        let role = hex64('c');
        store
            .connection
            .execute(
                "UPDATE orchestration_runs SET dag_snapshot_digest = ?1, role_binding_snapshot_digest = ?2 WHERE run_id = 'run-1'",
                params![dag, role],
            )
            .unwrap();
        store
            .record_role_binding_snapshot(
                "run-1",
                "snapshot-1",
                &role,
                "role-a",
                "agent-a",
                "read-write",
            )
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO orchestration_context_manifest_authorities(
                   context_manifest_ref_id, run_id, attempt_id,
                   producer_context_manifest_digest, sealed_at
                 ) VALUES('ctx-1','run-1',?1,?2,1)",
                params![outcome.attempt_id, hex64('a')],
            )
            .unwrap();
        let envelope = Value::Object(Map::from_iter([
            (
                "schemaVersion".to_owned(),
                Value::String("agenttalk.handoff.envelope.v1".to_owned()),
            ),
            (
                "handoffId".to_owned(),
                Value::String(format!("handoff-{}", hex64('0'))),
            ),
            ("projectRunId".to_owned(), Value::String("run-1".to_owned())),
            ("edgeId".to_owned(), Value::String("edge-1".to_owned())),
            ("from".to_owned(), {
                Value::Object(Map::from_iter([
                    ("taskNodeId".to_owned(), Value::String("node-1".to_owned())),
                    (
                        "attemptId".to_owned(),
                        Value::String(outcome.attempt_id.clone()),
                    ),
                    (
                        "executionRunId".to_owned(),
                        Value::String("exec-run-1".to_owned()),
                    ),
                ]))
            }),
            ("to".to_owned(), {
                Value::Object(Map::from_iter([(
                    "taskNodeId".to_owned(),
                    Value::String("node-1".to_owned()),
                )]))
            }),
            ("leaseEpoch".to_owned(), Value::from(outcome.lease_epoch)),
            ("artifactBindings".to_owned(), Value::Array(vec![])),
        ]));
        let computed_idempotency = handoff::idempotency_key_hex(&envelope).unwrap();
        let computed_transfer = handoff::artifact_transfer_set_digest_hex(&envelope).unwrap();
        let cas_sha = hex_digest(&[0u8; 32]);
        let declaration = hex64('f');
        let contract = cas_sha.clone();
        let evidence = cas_sha.clone();
        let producer_context = hex64('a');
        let computed_payload = handoff::delivery_payload_digest_hex(
            &declaration,
            &computed_transfer,
            &contract,
            &evidence,
            &producer_context,
            &dag,
            &role,
        )
        .unwrap();
        let delivery = HandoffDeliveryRecord {
            delivery_id: format!("handoff-{}", hex64('0')),
            run_id: "run-1".into(),
            attempt_id: outcome.attempt_id.clone(),
            edge_id: "edge-1".into(),
            lease_epoch: outcome.lease_epoch,
            lease_owner: "worker-a".into(),
            coordinator_generation: 1,
            envelope_handoff_id: format!("handoff-{}", hex64('0')),
            from_task_node_id: "node-1".into(),
            from_execution_run_id: "exec-run-1".into(),
            to_task_node_id: "node-1".into(),
            dag_snapshot_digest: dag,
            role_binding_snapshot_digest: role,
            declaration_digest: declaration,
            artifact_transfer_set_digest: computed_transfer,
            idempotency_key: computed_idempotency,
            delivery_payload_digest: computed_payload,
            envelope_object_ref: format!("sha256:{cas_sha}"),
            envelope_raw_sha256: cas_sha.clone(),
            envelope_sha256_jcs: cas_sha,
            acceptance_contract_ref: format!("sha256:{contract}"),
            acceptance_contract_digest: contract,
            acceptance_evidence_ref: format!("sha256:{evidence}"),
            acceptance_evidence_digest: evidence,
            producer_context_manifest_digest: producer_context,
            replay_receipt_json: None,
        };
        (delivery, outcome)
    }

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

    fn hex64(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn receipt(
        receipt_id: &str,
        milestone_id: &str,
        run_id: &str,
        decision: &str,
    ) -> HumanReceiptRecord {
        HumanReceiptRecord {
            receipt_id: receipt_id.into(),
            run_id: run_id.into(),
            milestone_id: milestone_id.into(),
            request_id: "request-1".into(),
            semantic_payload_hash: "payload-hash-1".into(),
            decision: decision.into(),
            expected_version: 1,
            brief_tree_digest: hex64('a'),
            presented_artifact_set_digest: format!("sha256:{}", hex64('b')),
            acceptance_evidence_digest: format!("sha256:{}", hex64('c')),
            authenticated_principal: "human-a".into(),
            core_timestamp: 1000,
        }
    }

    fn minimal_delivery() -> HandoffDeliveryRecord {
        let sha = hex64('a');
        HandoffDeliveryRecord {
            delivery_id: format!("handoff-{}", hex64('b')),
            run_id: "run-1".into(),
            attempt_id: "attempt-1".into(),
            edge_id: "edge-1".into(),
            lease_epoch: 1,
            lease_owner: "worker-a".into(),
            coordinator_generation: 1,
            envelope_handoff_id: format!("handoff-{}", hex64('b')),
            from_task_node_id: "node-1".into(),
            from_execution_run_id: "exec-1".into(),
            to_task_node_id: "node-2".into(),
            dag_snapshot_digest: sha.clone(),
            role_binding_snapshot_digest: sha.clone(),
            declaration_digest: sha.clone(),
            artifact_transfer_set_digest: sha.clone(),
            idempotency_key: sha.clone(),
            delivery_payload_digest: sha.clone(),
            envelope_object_ref: format!("sha256:{sha}"),
            envelope_raw_sha256: sha.clone(),
            envelope_sha256_jcs: sha.clone(),
            acceptance_contract_ref: format!("sha256:{sha}"),
            acceptance_contract_digest: sha.clone(),
            acceptance_evidence_ref: format!("sha256:{sha}"),
            acceptance_evidence_digest: sha.clone(),
            producer_context_manifest_digest: sha.clone(),
            replay_receipt_json: None,
        }
    }

    #[test]
    fn cas_verifier_missing_and_tampered_fail_closed() {
        let delivery = minimal_delivery();
        let binding = ArtifactBindingInput {
            binding_id: "binding-1".into(),
            edge_port_id: "edge-port-1".into(),
            source_output_port_id: "out".into(),
            target_input_port_id: "in".into(),
            object_ref: format!("sha256:{}", hex64('a')),
            sha256: hex64('a'),
            size: 0,
            content_schema_id: "schema".into(),
            content_schema_version: "1".into(),
            content_schema_digest: hex64('a'),
            normalized_content_type: "text/plain".into(),
            normalized_content_type_policy_version: "1".into(),
            content_schema_ref_json: "{}".into(),
        };
        assert!(matches!(
            SqliteStore::verify_cas_before_journal(
                &MissingCas,
                &delivery,
                std::slice::from_ref(&binding)
            ),
            Err(StorageError::ArtifactBodyNotFound { .. })
        ));
        assert!(matches!(
            SqliteStore::verify_cas_before_journal(
                &TamperedCas,
                &delivery,
                std::slice::from_ref(&binding)
            ),
            Err(StorageError::OrchestrationArtifactBindingInvalid { .. })
        ));
    }

    #[test]
    fn core_cas_verifier_round_trips_real_objects() {
        let root = std::env::temp_dir().join(format!(
            "agenttalk-c4a-corecas-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let cas = agenttalk_brief_sealer::CoreCas::new(&root);
        let bytes = b"hello sealed object";
        let object = cas.publish(bytes).unwrap();
        let verifier = CoreCasVerifier { cas: &cas };
        assert_eq!(verifier.verify_object(&object.object_ref).unwrap(), bytes);
        assert!(verifier
            .verify_object(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )
            .is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn v16_migration_checksum_version_and_legacy_tables_are_recorded() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.orchestration_schema_version(), 16);
        assert_eq!(store.orchestration_migration_checksum().len(), 64);
        assert_eq!(
            store.legacy_versions_unchanged().unwrap(),
            vec![11, 12, 13, 14, 15]
        );
        let runtime = store.orchestration_migration_checksum();
        assert_eq!(runtime, store.migration_checksum());
        assert_eq!(runtime, hex_digest(crate::MIGRATION_V16_SQL.as_bytes()));
        let db_checksum: String = store
            .connection
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = 16",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(db_checksum, runtime);
        let table_count: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE 'orchestration!_%' ESCAPE '!'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_count, 13);
    }

    #[test]
    fn run_creation_allows_multiple_runs_for_same_brief_and_rejects_different_binding() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store.create_orchestration_run(seed("run-2")).unwrap();
        let record = store.orchestration_run("run-1").unwrap();
        assert_eq!(record.status, "pending");
        assert_eq!(record.coordinator_generation, 1);
        let mut conflict = seed("run-1");
        conflict.brief_tree_digest = "1".repeat(64);
        assert!(matches!(
            store.create_orchestration_run(conflict),
            Err(StorageError::OrchestrationRunConflict { .. })
        ));
    }

    #[test]
    fn human_receipt_approve_and_reject_close_run_and_milestone_states() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store.transition_run_to_awaiting_approval("run-1").unwrap();
        store
            .ensure_orchestration_milestone(
                "run-1",
                "milestone-1",
                "m1",
                &hex64('a'),
                &format!("sha256:{}", hex64('b')),
                &format!("sha256:{}", hex64('c')),
            )
            .unwrap();
        let r = receipt("receipt-1", "milestone-1", "run-1", "approve");
        assert!(!store.record_human_receipt(r.clone()).unwrap());
        assert!(store.record_human_receipt(r).unwrap());
        let run = store.orchestration_run("run-1").unwrap();
        assert_eq!(run.status, "completed");
        let milestone_status: String = store
            .connection
            .query_row(
                "SELECT status FROM orchestration_milestones WHERE milestone_id = 'milestone-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(milestone_status, "approved");

        store.create_orchestration_run(seed("run-2")).unwrap();
        store.transition_run_to_awaiting_approval("run-2").unwrap();
        store
            .ensure_orchestration_milestone(
                "run-2",
                "milestone-2a",
                "m2a",
                &hex64('a'),
                &format!("sha256:{}", hex64('b')),
                &format!("sha256:{}", hex64('c')),
            )
            .unwrap();
        store
            .ensure_orchestration_milestone(
                "run-2",
                "milestone-2b",
                "m2b",
                &hex64('a'),
                &format!("sha256:{}", hex64('b')),
                &format!("sha256:{}", hex64('c')),
            )
            .unwrap();
        let r = receipt("receipt-2a", "milestone-2a", "run-2", "approve");
        assert!(!store.record_human_receipt(r).unwrap());
        assert_eq!(store.orchestration_run("run-2").unwrap().status, "running");

        store.create_orchestration_run(seed("run-3")).unwrap();
        store.transition_run_to_awaiting_approval("run-3").unwrap();
        store
            .ensure_orchestration_milestone(
                "run-3",
                "milestone-3",
                "m3",
                &hex64('a'),
                &format!("sha256:{}", hex64('b')),
                &format!("sha256:{}", hex64('c')),
            )
            .unwrap();
        let r = receipt("receipt-3", "milestone-3", "run-3", "reject");
        assert!(!store.record_human_receipt(r).unwrap());
        let run = store.orchestration_run("run-3").unwrap();
        assert_eq!(run.status, "failed");
    }

    #[test]
    fn human_receipt_rejects_wrong_state_digests_and_active_attempt() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .ensure_orchestration_milestone(
                "run-1",
                "milestone-1",
                "m1",
                &hex64('a'),
                &format!("sha256:{}", hex64('b')),
                &format!("sha256:{}", hex64('c')),
            )
            .unwrap();
        assert!(matches!(
            store.record_human_receipt(receipt("receipt-1", "milestone-1", "run-1", "approve")),
            Err(StorageError::OrchestrationRunStatusInvalid { .. })
        ));
        store.transition_run_to_awaiting_approval("run-1").unwrap();
        let mut bad = receipt("receipt-1", "milestone-1", "run-1", "approve");
        bad.expected_version = 9;
        assert!(matches!(
            store.record_human_receipt(bad),
            Err(StorageError::OrchestrationMilestoneStateInvalid { .. })
        ));
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        store
            .transition_task_ready_to_running("node-1", "exec-run-1", "worker-a")
            .unwrap();
        assert!(matches!(
            store.record_human_receipt(receipt("receipt-1", "milestone-1", "run-1", "approve")),
            Err(StorageError::OrchestrationActiveAttemptExists { .. })
        ));
    }

    #[test]
    fn ready_only_transition_and_lease_fencing() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        assert!(matches!(
            store.transition_task_ready_to_running("node-1", "exec-run-1", "worker-a"),
            Err(StorageError::OrchestrationTaskNotReady { .. })
        ));
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        let outcome = store
            .transition_task_ready_to_running("node-1", "exec-run-1", "worker-a")
            .unwrap();
        assert_eq!(outcome.attempt_no, 1);
        assert_eq!(outcome.lease_epoch, 1);
        let attempt_status: String = store
            .connection
            .query_row(
                "SELECT status FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&outcome.attempt_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempt_status, "running");
        store
            .assert_lease_epoch_current(&outcome.attempt_id, 1, 1, "worker-a")
            .unwrap();
        assert!(matches!(
            store.assert_lease_epoch_current(&outcome.attempt_id, 0, 1, "worker-a"),
            Err(StorageError::StaleLease { .. })
        ));
        assert!(matches!(
            store.assert_lease_epoch_current(&outcome.attempt_id, 1, 2, "worker-a"),
            Err(StorageError::StaleLease { .. })
        ));
        assert!(matches!(
            store.assert_lease_epoch_current(&outcome.attempt_id, 1, 1, "worker-b"),
            Err(StorageError::StaleLease { .. })
        ));
        store.bump_coordinator_generation("run-1").unwrap();
        assert!(matches!(
            store.assert_lease_epoch_current(&outcome.attempt_id, 1, 1, "worker-a"),
            Err(StorageError::StaleLease { .. })
        ));
        assert!(matches!(
            store.transition_task_ready_to_running("node-1", "exec-run-2", "worker-b"),
            Err(StorageError::OrchestrationTaskNotReady { .. })
        ));
    }

    #[test]
    fn role_snapshot_allows_same_digest_for_multiple_roles_and_artifact_binding_is_guarded() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .record_role_binding_snapshot(
                "run-1",
                "snapshot-1",
                &hex64('a'),
                "role-a",
                "agent-a",
                "read-write",
            )
            .unwrap();
        store
            .record_role_binding_snapshot(
                "run-1",
                "snapshot-2",
                &hex64('a'),
                "role-b",
                "agent-b",
                "read",
            )
            .unwrap();
        let role_count: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM orchestration_role_binding_snapshots WHERE digest = ?1",
                [hex64('a')],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(role_count, 2);
    }

    #[test]
    fn audit_events_are_append_only_and_transactions_emit_them() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        let count: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM orchestration_audit_events WHERE run_id = 'run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count >= 1);

        let update_rejected = store
            .connection
            .execute(
                "UPDATE orchestration_audit_events SET event_type = 'changed' WHERE run_id = 'run-1'",
                [],
            )
            .is_err();
        assert!(update_rejected);
        let delete_rejected = store
            .connection
            .execute(
                "DELETE FROM orchestration_audit_events WHERE run_id = 'run-1'",
                [],
            )
            .is_err();
        assert!(delete_rejected);
    }

    #[test]
    fn failed_receipt_rolls_back_audit_events() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .ensure_orchestration_milestone(
                "run-1",
                "milestone-1",
                "m1",
                &hex64('a'),
                &format!("sha256:{}", hex64('b')),
                &format!("sha256:{}", hex64('c')),
            )
            .unwrap();
        let result =
            store.record_human_receipt(receipt("receipt-1", "milestone-1", "run-1", "approve"));
        assert!(matches!(
            result,
            Err(StorageError::OrchestrationRunStatusInvalid { .. })
        ));
        let count: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM orchestration_audit_events WHERE run_id = 'run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn node_never_persists_leased_and_recovery_interrupts_attempt() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store.set_task_max_attempts("node-1", 2).unwrap();
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        let first = store
            .transition_task_ready_to_running("node-1", "exec-run-1", "worker-a")
            .unwrap();
        let node_status: String = store
            .connection
            .query_row(
                "SELECT status FROM orchestration_task_nodes WHERE node_id = 'node-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(node_status, "running");
        let sealed_attempt = store.transition_attempt_to_sealing("node-1").unwrap();
        assert_eq!(sealed_attempt, first.attempt_id);
        let attempt_status: String = store
            .connection
            .query_row(
                "SELECT status FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&sealed_attempt],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempt_status, "sealing");
        let recovered = store.recover_active_attempt_interrupted("node-1").unwrap();
        assert_eq!(recovered, sealed_attempt);
        let attempt_status: String = store
            .connection
            .query_row(
                "SELECT status FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&recovered],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempt_status, "interrupted");
        let node_status: String = store
            .connection
            .query_row(
                "SELECT status FROM orchestration_task_nodes WHERE node_id = 'node-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(node_status, "ready");
        let leased_node_count: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM orchestration_task_nodes WHERE status = 'leased'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leased_node_count, 0);
        // Retry succeeds after recovery.
        let second = store
            .transition_task_ready_to_running("node-1", "exec-run-2", "worker-a")
            .unwrap();
        assert_eq!(second.attempt_no, 2);
    }

    #[test]
    fn legacy_event_store_is_not_written() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        store
            .transition_task_ready_to_running("node-1", "exec-run-1", "worker-a")
            .unwrap();
        let legacy_count: i64 = store
            .connection
            .query_row("SELECT count(*) FROM event_store", [], |row| row.get(0))
            .unwrap();
        assert_eq!(legacy_count, 0);
    }

    #[test]
    fn canonical_audit_payload_is_deterministic_and_duplicate_safe() {
        let canonical =
            canonicalize_audit_payload(" { \"b\": 1, \"a\": [true, null, 2] } ").unwrap();
        assert_eq!(canonical, "{\"a\":[true,null,2],\"b\":1}");
        assert!(canonicalize_audit_payload("{\"a\":1,\"a\":2}").is_err());
        assert!(canonicalize_audit_payload("{not json}").is_err());
        assert!(canonicalize_audit_payload("{\"a\":2.0}").is_err());
        assert!(canonicalize_audit_payload("{\"a\":9007199254740992}").is_err());
    }

    #[test]
    fn v16_preflight_rejects_bad_v15_rows_before_any_rebuild() {
        let mut connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE orchestration_task_nodes(
                   node_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_key TEXT NOT NULL,
                   required INTEGER NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL,
                   active_attempt_id TEXT, attempt_count INTEGER NOT NULL DEFAULT 0,
                   max_attempts INTEGER NOT NULL DEFAULT 1, input_artifact_set_digest TEXT,
                   role_id TEXT, acceptance_contract_ref TEXT, terminal_reason TEXT);
                 CREATE TABLE orchestration_task_attempts(
                   attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL,
                   attempt_no INTEGER NOT NULL, from_execution_run_id TEXT, status TEXT NOT NULL,
                   lease_epoch INTEGER NOT NULL DEFAULT 0, artifact_set_digest TEXT,
                   acceptance_evidence_digest TEXT, terminal_reason TEXT,
                   terminal_identity_json TEXT);
                 CREATE TABLE orchestration_milestones(
                   milestone_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, milestone_key TEXT NOT NULL,
                   required INTEGER NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL,
                   brief_tree_digest TEXT, presented_artifact_set_digest TEXT,
                   acceptance_evidence_digest TEXT, terminal_reason TEXT);
                 INSERT INTO orchestration_task_nodes(node_id, run_id, node_key, required, status, version)
                 VALUES('n1','run-1','k1',1,'leased',1);",
            )
            .unwrap();
        let tx = connection.transaction().unwrap();
        assert!(matches!(
            SqliteStore::validate_v15_to_v16_state(&tx),
            Err(StorageError::MigrationInvalidV15State { .. })
        ));
        tx.rollback().unwrap();
        let audit_exists: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='orchestration_audit_events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit_exists, 0);
        let original_exists: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='orchestration_task_nodes'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(original_exists, 1);
    }

    #[test]
    fn v16_rejects_unmappable_v15_states_and_null_sealed_digests() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE orchestration_task_nodes_v15(
                   node_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_key TEXT NOT NULL,
                   required INTEGER NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL,
                   active_attempt_id TEXT, attempt_count INTEGER NOT NULL DEFAULT 0,
                   max_attempts INTEGER NOT NULL DEFAULT 1, input_artifact_set_digest TEXT,
                   role_id TEXT, acceptance_contract_ref TEXT, terminal_reason TEXT);
                 CREATE TABLE orchestration_task_attempts_v15(
                   attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL,
                   attempt_no INTEGER NOT NULL, from_execution_run_id TEXT, status TEXT NOT NULL,
                   lease_epoch INTEGER NOT NULL DEFAULT 0, artifact_set_digest TEXT,
                   acceptance_evidence_digest TEXT, terminal_reason TEXT,
                   terminal_identity_json TEXT);
                 CREATE TABLE orchestration_milestones_v15(
                   milestone_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, milestone_key TEXT NOT NULL,
                   required INTEGER NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL,
                   brief_tree_digest TEXT, presented_artifact_set_digest TEXT,
                   acceptance_evidence_digest TEXT, terminal_reason TEXT);
                 INSERT INTO orchestration_task_nodes_v15(
                   node_id, run_id, node_key, required, status, version)
                 VALUES('n1','run-1','k1',1,'leased',1);
                 INSERT INTO orchestration_task_attempts_v15(
                   attempt_id, run_id, node_id, attempt_no, status, lease_epoch)
                 VALUES('a1','run-1','n1',1,'leased',1);
                 INSERT INTO orchestration_milestones_v15(
                   milestone_id, run_id, milestone_key, required, status, version)
                 VALUES('m1','run-1','mk1',1,'awaiting_approval',1);",
            )
            .unwrap();
        assert!(connection.execute_batch(crate::MIGRATION_V16_SQL).is_err());

        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE orchestration_task_nodes_v15(
                   node_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_key TEXT NOT NULL,
                   required INTEGER NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL,
                   active_attempt_id TEXT, attempt_count INTEGER NOT NULL DEFAULT 0,
                   max_attempts INTEGER NOT NULL DEFAULT 1, input_artifact_set_digest TEXT,
                   role_id TEXT, acceptance_contract_ref TEXT, terminal_reason TEXT);
                 CREATE TABLE orchestration_task_attempts_v15(
                   attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, node_id TEXT NOT NULL,
                   attempt_no INTEGER NOT NULL, from_execution_run_id TEXT, status TEXT NOT NULL,
                   lease_epoch INTEGER NOT NULL DEFAULT 0, artifact_set_digest TEXT,
                   acceptance_evidence_digest TEXT, terminal_reason TEXT,
                   terminal_identity_json TEXT);
                 CREATE TABLE orchestration_milestones_v15(
                   milestone_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, milestone_key TEXT NOT NULL,
                   required INTEGER NOT NULL, status TEXT NOT NULL, version INTEGER NOT NULL,
                   brief_tree_digest TEXT, presented_artifact_set_digest TEXT,
                   acceptance_evidence_digest TEXT, terminal_reason TEXT);
                 INSERT INTO orchestration_task_nodes_v15(
                   node_id, run_id, node_key, required, status, version)
                 VALUES('n1','run-1','k1',1,'ready',1);
                 INSERT INTO orchestration_task_attempts_v15(
                   attempt_id, run_id, node_id, attempt_no, status, lease_epoch)
                 VALUES('a1','run-1','n1',1,'running',1);
                 INSERT INTO orchestration_milestones_v15(
                   milestone_id, run_id, milestone_key, required, status, version,
                   brief_tree_digest, presented_artifact_set_digest, acceptance_evidence_digest)
                 VALUES('m1','run-1','mk1',1,'awaiting_approval',1,NULL,NULL,NULL);",
            )
            .unwrap();
        assert!(connection.execute_batch(crate::MIGRATION_V16_SQL).is_err());
    }

    #[test]
    fn restart_after_delivery_reopens_journal_and_audit_facts() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agenttalk-c4a-delivery-restart-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let mut store = SqliteStore::open(&path).unwrap();
            let (delivery, _outcome) = prepare_sealed_delivery(&mut store);
            assert!(!store
                .record_handoff_delivery(delivery.clone(), &[], &TestCas)
                .unwrap());
        }
        {
            let store = SqliteStore::open(&path).unwrap();
            let count: i64 = store
                .connection
                .query_row(
                    "SELECT count(*) FROM orchestration_handoff_deliveries WHERE delivery_id = ?1",
                    ["handoff-0000000000000000000000000000000000000000000000000000000000000000"],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);
            let audit_count: i64 = store
                .connection
                .query_row(
                    "SELECT count(*) FROM orchestration_audit_events WHERE run_id = 'run-1'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(audit_count >= 1);
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }

    #[test]
    fn attempt_remains_sealing_until_all_required_edges_are_delivered() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        let outcome = store
            .transition_task_ready_to_running("node-1", "exec-run-1", "worker-a")
            .unwrap();
        store.transition_attempt_to_sealing("node-1").unwrap();
        store
            .connection
            .execute_batch(
                "INSERT INTO orchestration_edges(edge_id, run_id, from_node_id, to_node_id, dag_snapshot_digest, allowed_consumer_json)
                 VALUES('edge-1','run-1','node-1','node-2','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','[]');
                 INSERT INTO orchestration_edges(edge_id, run_id, from_node_id, to_node_id, dag_snapshot_digest, allowed_consumer_json)
                 VALUES('edge-2','run-1','node-1','node-3','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','[]');",
            )
            .unwrap();
        let delivery = HandoffDeliveryRecord {
            delivery_id: "handoff-0000000000000000000000000000000000000000000000000000000000000000"
                .into(),
            run_id: "run-1".into(),
            attempt_id: outcome.attempt_id.clone(),
            edge_id: "edge-1".into(),
            lease_epoch: outcome.lease_epoch,
            lease_owner: "worker-a".into(),
            coordinator_generation: 1,
            envelope_handoff_id:
                "handoff-0000000000000000000000000000000000000000000000000000000000000000".into(),
            from_task_node_id: "node-1".into(),
            from_execution_run_id: "exec-run-1".into(),
            to_task_node_id: "node-2".into(),
            dag_snapshot_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            role_binding_snapshot_digest:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            declaration_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            artifact_transfer_set_digest:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            idempotency_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            delivery_payload_digest:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            envelope_object_ref:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            envelope_raw_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            envelope_sha256_jcs: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            acceptance_contract_ref:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            acceptance_contract_digest:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            acceptance_evidence_ref:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            acceptance_evidence_digest:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            producer_context_manifest_digest:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            replay_receipt_json: None,
        };
        let tx = store.connection.transaction().unwrap();
        SqliteStore::maybe_complete_attempt_sealing(&tx, &delivery).unwrap();
        let status: String = tx
            .query_row(
                "SELECT status FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&outcome.attempt_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "sealing");
        let sql = "INSERT INTO orchestration_handoff_deliveries(delivery_id, run_id, attempt_id, edge_id, lease_epoch, envelope_handoff_id, from_task_node_id, from_execution_run_id, to_task_node_id, lease_owner, coordinator_generation, dag_snapshot_digest, role_binding_snapshot_digest, declaration_digest, artifact_transfer_set_digest, idempotency_key, delivery_payload_digest, envelope_object_ref, envelope_raw_sha256, envelope_sha256_jcs, acceptance_contract_ref, acceptance_contract_digest, acceptance_evidence_ref, acceptance_evidence_digest, producer_context_manifest_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25)";
        let a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        tx.execute(
            sql,
            params![
                "d1",
                "run-1",
                outcome.attempt_id,
                "edge-1",
                outcome.lease_epoch,
                "h",
                "node-1",
                "exec-run-1",
                "node-2",
                "worker-a",
                1,
                a,
                a,
                a,
                a,
                a,
                a,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                a,
                a,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                a,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                a,
                a
            ],
        )
        .unwrap();
        SqliteStore::maybe_complete_attempt_sealing(&tx, &delivery).unwrap();
        let status: String = tx
            .query_row(
                "SELECT status FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&outcome.attempt_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "sealing");
        tx.execute(
            sql,
            params![
                "d2",
                "run-1",
                outcome.attempt_id,
                "edge-2",
                outcome.lease_epoch,
                "h2",
                "node-1",
                "exec-run-1",
                "node-3",
                "worker-a",
                1,
                a,
                a,
                a,
                a,
                a,
                a,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                a,
                a,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                a,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                a,
                a
            ],
        )
        .unwrap();
        SqliteStore::maybe_complete_attempt_sealing(&tx, &delivery).unwrap();
        let status: String = tx
            .query_row(
                "SELECT status FROM orchestration_task_attempts WHERE attempt_id = ?1",
                [&outcome.attempt_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
        tx.commit().unwrap();
    }

    #[test]
    fn recovery_matrix_and_cas_before_journal_ordering_are_explicit() {
        let mut store = SqliteStore::open_in_memory().unwrap();
        store.create_orchestration_run(seed("run-1")).unwrap();
        store
            .insert_orchestration_task_node("run-1", "node-1", "key-1")
            .unwrap();
        store
            .mark_orchestration_task_ready("node-1", "input-1", "role-1", "contract-1")
            .unwrap();
        store
            .transition_task_ready_to_running("node-1", "exec-run-1", "worker-a")
            .unwrap();
        let matrix = store.orchestration_recovery_state("run-1").unwrap();
        assert_eq!(matrix[0].0, "node-1");
        assert_eq!(matrix[0].1, "running");
        let record = store.orchestration_run("run-1").unwrap();
        assert!(is_object_ref(&record.brief_snapshot_id));
        assert_eq!(record.brief_tree_digest.len(), 64);
    }
}
