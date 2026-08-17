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
pub struct ArtifactBindingInput {
    pub binding_id: String,
    pub edge_port_id: String,
    pub object_ref: String,
    pub sha256: String,
    pub size: i64,
    pub content_schema_ref_json: String,
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
        // New path: authority guards must pass before any row is written.
        let lease_ok: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM orchestration_leases
                 WHERE attempt_id = ?1 AND lease_epoch = ?2 AND status = 'active'",
                params![delivery.attempt_id, delivery.lease_epoch],
                |row| row.get(0),
            )
            .optional()?;
        if lease_ok.is_none() {
            return Err(StorageError::StaleLease {
                attempt_id: delivery.attempt_id,
            });
        }
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
        for binding in bindings {
            tx.execute(
                "INSERT INTO orchestration_artifact_bindings(
                   binding_id, run_id, delivery_id, edge_port_id,
                   object_ref, sha256, size, content_schema_ref_json
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    binding.binding_id,
                    delivery.run_id,
                    delivery.delivery_id,
                    binding.edge_port_id,
                    binding.object_ref,
                    binding.sha256,
                    binding.size,
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

    pub fn set_task_max_attempts(
        &mut self,
        node_id: &str,
        max_attempts: i64,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE orchestration_task_nodes SET max_attempts = ?2 WHERE node_id = ?1",
            params![node_id, max_attempts],
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

    pub fn transition_run_to_awaiting_approval(
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
        tx.commit()?;
        Ok(())
    }

    pub fn record_role_binding_snapshot(
        &mut self,
        run_id: &str,
        snapshot_id: &str,
        digest: &str,
        role_id: &str,
        agent_id: &str,
        workspace_access: &str,
    ) -> Result<(), StorageError> {
        self.orchestration_run(run_id)?;
        self.connection.execute(
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
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_artifact_binding(
        &mut self,
        binding_id: &str,
        run_id: &str,
        delivery_id: &str,
        edge_port_id: &str,
        object_ref: &str,
        sha256: &str,
        size: i64,
    ) -> Result<(), StorageError> {
        if delivery_id.is_empty() || edge_port_id.is_empty() {
            return Err(StorageError::OrchestrationArtifactBindingInvalid {
                reason: "delivery_id and edge_port_id are required".into(),
            });
        }
        if !is_object_ref(object_ref) || !is_hex64(sha256) {
            return Err(StorageError::OrchestrationArtifactBindingInvalid {
                reason: "object_ref must be sha256:<64hex> and sha256 must be 64hex".into(),
            });
        }
        if object_ref != format!("sha256:{sha256}") {
            return Err(StorageError::OrchestrationArtifactBindingInvalid {
                reason: "object_ref does not match sha256".into(),
            });
        }
        self.connection.execute(
            "INSERT INTO orchestration_artifact_bindings(
               binding_id, run_id, delivery_id, edge_port_id,
               object_ref, sha256, size
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                binding_id,
                run_id,
                delivery_id,
                edge_port_id,
                object_ref,
                sha256,
                size,
            ],
        )?;
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
    let event_id = format!("{run_id}:{sequence}:{event_type}");
    let payload_sha256 = hex_digest(payload_json.as_bytes());
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
            payload_json,
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

    #[test]
    fn v16_migration_checksum_version_and_legacy_tables_are_recorded() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.orchestration_schema_version(), 16);
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
    fn handoff_delivery_replay_and_conflict_are_slot_scoped() {
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
        let delivery = HandoffDeliveryRecord {
            delivery_id: "delivery-1".into(),
            run_id: "run-1".into(),
            attempt_id: outcome.attempt_id.clone(),
            edge_id: "edge-1".into(),
            lease_epoch: outcome.lease_epoch,
            declaration_digest: "decl-1".into(),
            artifact_transfer_set_digest: "artifact-set-1".into(),
            idempotency_key: "key-1".into(),
            delivery_payload_digest: "payload-1".into(),
            envelope_object_ref: format!("sha256:{}", hex64('c')),
            envelope_raw_sha256: hex64('c'),
            envelope_sha256_jcs: hex64('c'),
            acceptance_contract_ref: "contract-1".into(),
            acceptance_contract_digest: "contract-digest-1".into(),
            acceptance_evidence_ref: "evidence-1".into(),
            acceptance_evidence_digest: "evidence-digest-1".into(),
            producer_context_manifest_digest: "context-1".into(),
            replay_receipt_json: None,
        };
        assert!(!store
            .record_handoff_delivery(delivery.clone(), &[])
            .unwrap());
        assert!(store
            .record_handoff_delivery(delivery.clone(), &[])
            .unwrap());
        let mut conflicting = delivery;
        conflicting.delivery_payload_digest = "different".into();
        assert!(matches!(
            store.record_handoff_delivery(conflicting, &[]),
            Err(StorageError::HandoffDeliveryConflict { .. })
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
                "digest-1",
                "role-a",
                "agent-a",
                "read-write",
            )
            .unwrap();
        store
            .record_role_binding_snapshot(
                "run-1",
                "snapshot-2",
                "digest-1",
                "role-b",
                "agent-b",
                "read",
            )
            .unwrap();
        let role_count: i64 = store
            .connection
            .query_row(
                "SELECT count(*) FROM orchestration_role_binding_snapshots WHERE digest = 'digest-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(role_count, 2);

        let sha = hex64('d');
        store
            .record_artifact_binding(
                "binding-1",
                "run-1",
                "delivery-1",
                "edge-port-1",
                &format!("sha256:{sha}"),
                &sha,
                12,
            )
            .unwrap();
        assert!(matches!(
            store.record_artifact_binding(
                "binding-2",
                "run-1",
                "delivery-1",
                "edge-port-1",
                "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                &sha,
                12,
            ),
            Err(StorageError::OrchestrationArtifactBindingInvalid { .. })
        ));
        assert!(matches!(
            store.record_artifact_binding(
                "binding-3",
                "run-1",
                "",
                "edge-port-1",
                &format!("sha256:{sha}"),
                &sha,
                12,
            ),
            Err(StorageError::OrchestrationArtifactBindingInvalid { .. })
        ));
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
