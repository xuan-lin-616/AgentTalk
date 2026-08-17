use agenttalk_brief_sealer::{BriefSealer, CoreCas};
use agenttalk_orchestration_contracts::brief::{InMemoryBriefBytesMap, ParsedManifest};
use agenttalk_orchestration_contracts::json;
use agenttalk_orchestration_contracts::registry::InMemorySchemaRegistry;
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct TestProject {
    root: PathBuf,
}

impl TestProject {
    fn new() -> Self {
        let unique = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "agenttalk-brief-sealer-test-{}-{unique}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
        fs::create_dir_all(root.join("plan")).unwrap();
        Self { root }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    fn write(&self, relative: &str, bytes: &[u8]) {
        let path = self.path(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, bytes).unwrap();
    }
}

impl Drop for TestProject {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn valid_manifest(roadmap: &[u8], env_example: &[u8]) -> serde_json::Value {
    json!({
        "schemaVersion": "agenttalk.brief.manifest.v1",
        "projectId": "sealer-test",
        "title": "Sealer Test",
        "roles": [
            {"roleId": "architect", "displayName": "Architect"},
            {"roleId": "pm", "displayName": "PM"}
        ],
        "files": [
            {
                "path": "plan/roadmap.md",
                "kind": "plan",
                "format": "markdown",
                "contentSchemaRef": null,
                "required": true,
                "sha256": agenttalk_brief_sealer::cas::sha256_hex(roadmap),
                "size": roadmap.len(),
                "context": {"layer": "shared", "roleIds": ["architect", "pm"], "retention": "run", "workspaceAccess": "read_only"},
                "declaredOwnerRoleId": "pm"
            },
            {
                "path": "plan/.env.example",
                "kind": "plan",
                "format": "text",
                "contentSchemaRef": null,
                "required": false,
                "sha256": agenttalk_brief_sealer::cas::sha256_hex(env_example),
                "size": env_example.len(),
                "context": {"layer": "role", "roleIds": ["pm"], "retention": "project", "workspaceAccess": "none"},
                "declaredOwnerRoleId": "pm"
            }
        ]
    })
}

fn write_valid_project(project: &TestProject, roadmap: &[u8], env_example: &[u8]) {
    let manifest = valid_manifest(roadmap, env_example);
    project.write(
        "agenttalk-brief.json",
        &serde_json::to_vec(&manifest).unwrap(),
    );
    project.write("plan/roadmap.md", roadmap);
    project.write("plan/.env.example", env_example);
}

#[test]
fn seals_valid_brief_and_digest_is_stable() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap\n\nsealer fixture\n";
    let env_example = b"EXAMPLE_TOKEN=replace-me\n";
    write_valid_project(&project, roadmap, env_example);
    let registry = InMemorySchemaRegistry::new();
    let sealer = BriefSealer::new(project.root.clone());

    let first = sealer.seal(&registry).unwrap();
    let second = sealer.seal(&registry).unwrap();

    assert_eq!(first.brief_snapshot_id(), second.brief_snapshot_id());
    assert_eq!(first.brief_tree_digest(), second.brief_tree_digest());
    assert_eq!(first.files().len(), 2);
    assert!(first.brief_snapshot_id().starts_with("sha256:"));

    // Cross-check the tree digest with the contracts crate directly.
    let manifest_bytes = fs::read(project.path("agenttalk-brief.json")).unwrap();
    let mut bytes_map = InMemoryBriefBytesMap::new();
    bytes_map.insert("plan/roadmap.md", roadmap.to_vec());
    bytes_map.insert("plan/.env.example", env_example.to_vec());
    let content = ParsedManifest::parse(&manifest_bytes)
        .unwrap()
        .validate_shape()
        .unwrap()
        .validate_content(&InMemorySchemaRegistry::new(), &bytes_map)
        .unwrap();
    assert_eq!(first.brief_tree_digest(), content.brief_tree_digest());

    // CAS idempotency: publishing the same bytes again must not create a
    // second object.
    let cas = CoreCas::new(project.root.clone());
    let object = cas.publish(roadmap).unwrap();
    assert_eq!(
        object.sha256,
        agenttalk_brief_sealer::cas::sha256_hex(roadmap)
    );
    assert_eq!(cas.publish(roadmap).unwrap().object_ref, object.object_ref);
    assert!(cas.object_path(&object.object_ref).exists());
    assert_eq!(cas.read(&object.object_ref).unwrap(), roadmap);
}

#[test]
fn source_change_produces_new_digest() {
    let project = TestProject::new();
    let roadmap_a = b"# Roadmap A\n";
    let env_example = b"EXAMPLE_TOKEN=replace-me\n";
    write_valid_project(&project, roadmap_a, env_example);
    let registry = InMemorySchemaRegistry::new();
    let sealer = BriefSealer::new(project.root.clone());
    let first = sealer.seal(&registry).unwrap();

    let roadmap_b = b"# Roadmap B changed\n";
    write_valid_project(&project, roadmap_b, env_example);
    let second = sealer.seal(&registry).unwrap();

    assert_ne!(first.brief_tree_digest(), second.brief_tree_digest());
    assert_ne!(first.brief_snapshot_id(), second.brief_snapshot_id());
}

#[test]
fn cas_read_verifies_digest_and_target_conflict_fails_closed() {
    let project = TestProject::new();
    let cas = CoreCas::new(project.root.clone());
    let object = cas.publish(b"hello cas").unwrap();
    assert_eq!(cas.read(&object.object_ref).unwrap(), b"hello cas");

    let path = cas.object_path(&object.object_ref);
    fs::write(&path, b"tampered").unwrap();
    assert!(matches!(
        cas.read(&object.object_ref),
        Err(agenttalk_brief_sealer::cas::CasError::HashMismatch { .. })
    ));

    let expected_sha = agenttalk_brief_sealer::cas::sha256_hex(b"expected content");
    let other_path = cas.object_path(&format!("sha256:{expected_sha}"));
    fs::create_dir_all(other_path.parent().unwrap()).unwrap();
    fs::write(&other_path, b"wrong content").unwrap();
    assert!(matches!(
        cas.publish(b"expected content"),
        Err(agenttalk_brief_sealer::cas::CasError::ObjectConflict { .. })
    ));
}

#[test]
fn failed_seal_leaves_only_orphan_candidate_objects() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap\n";
    let env_example = b"EXAMPLE_TOKEN=replace-me\n";
    let mut manifest = valid_manifest(roadmap, env_example);
    // Break the declared hash for the first file. The source will still be
    // published to the CAS before content verification fails; no snapshot may
    // be returned.
    manifest["files"][0]["sha256"] = json!("00".repeat(32));
    project.write(
        "agenttalk-brief.json",
        &serde_json::to_vec(&manifest).unwrap(),
    );
    project.write("plan/roadmap.md", roadmap);
    project.write("plan/.env.example", env_example);

    let sealer = BriefSealer::new(project.root.clone());
    let error = sealer.seal(&InMemorySchemaRegistry::new()).unwrap_err();
    assert_eq!(error.code_str(), "BRIEF_HASH_MISMATCH");

    let cas = CoreCas::new(project.root.clone());
    assert!(cas
        .object_path(&format!(
            "sha256:{}",
            agenttalk_brief_sealer::cas::sha256_hex(roadmap)
        ))
        .exists());
}

#[test]
fn path_lexical_cas_and_sensitive_rules_are_rejected() {
    let cases = [
        ("/plan/roadmap.md", "BRIEF_PATH_LEXICAL_INVALID"),
        ("plan/../roadmap.md", "BRIEF_PATH_LEXICAL_INVALID"),
        (".agenttalk/objects/x.md", "BRIEF_CAS_REFERENCE"),
        ("plan/.env", "BRIEF_SENSITIVE_SOURCE_FORBIDDEN"),
    ];
    for (bad_path, code) in cases {
        let project = TestProject::new();
        let roadmap = b"# Roadmap\n";
        let mut manifest = valid_manifest(roadmap, b"x\n");
        manifest["files"][0]["path"] = json!(bad_path);
        project.write(
            "agenttalk-brief.json",
            &serde_json::to_vec(&manifest).unwrap(),
        );
        project.write("plan/roadmap.md", roadmap);

        let error = BriefSealer::new(project.root.clone())
            .seal(&InMemorySchemaRegistry::new())
            .unwrap_err();
        assert_eq!(error.code_str(), code, "path {bad_path}");
    }
}

#[test]
fn physical_hard_link_alias_is_rejected() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap\n";
    let env_example = b"x\n";
    let mut manifest = valid_manifest(roadmap, env_example);
    // Declare a second path that is a hard link to the first physical file.
    manifest["files"].as_array_mut().unwrap().push(json!({
        "path": "plan/roadmap-alias.md",
        "kind": "plan",
        "format": "markdown",
        "contentSchemaRef": null,
        "required": false,
        "sha256": agenttalk_brief_sealer::cas::sha256_hex(roadmap),
        "size": roadmap.len(),
        "context": {"layer": "shared", "roleIds": ["pm"], "retention": "run", "workspaceAccess": "read_only"},
        "declaredOwnerRoleId": "pm"
    }));
    project.write(
        "agenttalk-brief.json",
        &serde_json::to_vec(&manifest).unwrap(),
    );
    project.write("plan/roadmap.md", roadmap);
    project.write(
        "plan/.env.example",
        b"x
",
    );
    fs::hard_link(
        project.path("plan/roadmap.md"),
        project.path("plan/roadmap-alias.md"),
    )
    .unwrap();

    let error = BriefSealer::new(project.root.clone())
        .seal(&InMemorySchemaRegistry::new())
        .unwrap_err();
    assert_eq!(error.code_str(), "BRIEF_PATH_ALIAS");
}

#[test]
fn cas_objects_do_not_affect_brief_tree_digest() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap\n";
    write_valid_project(&project, roadmap, b"EXAMPLE_TOKEN=replace-me\n");
    let registry = InMemorySchemaRegistry::new();
    let sealer = BriefSealer::new(project.root.clone());
    let before = sealer.seal(&registry).unwrap();

    // Add an unrelated CAS object after sealing. Re-sealing the same manifest
    // must produce the same briefTreeDigest.
    let cas = CoreCas::new(project.root.clone());
    cas.publish(b"unrelated blob").unwrap();
    let after = sealer.seal(&registry).unwrap();
    assert_eq!(before.brief_tree_digest(), after.brief_tree_digest());
}

#[test]
fn schema_ref_unresolved_fails_closed() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap\n";
    let mut manifest = valid_manifest(roadmap, b"x\n");
    manifest["files"][0]["contentSchemaRef"] = json!({
        "id": "agenttalk.unknown.v1",
        "version": "1",
        "digest": "00".repeat(32)
    });
    project.write(
        "agenttalk-brief.json",
        &serde_json::to_vec(&manifest).unwrap(),
    );
    project.write("plan/roadmap.md", roadmap);
    project.write(
        "plan/.env.example",
        b"x
",
    );

    let error = BriefSealer::new(project.root.clone())
        .seal(&InMemorySchemaRegistry::new())
        .unwrap_err();
    assert_eq!(error.code_str(), "BRIEF_SCHEMA_REF_UNRESOLVED");
}

// Environment-only smoke test. It is not part of the required security
// gate; deterministic traversal/reparse coverage lives in fs_guard unit
// tests. Run manually with `cargo test -- --ignored`.
#[ignore]
#[test]
fn reparse_point_is_rejected_when_creatable() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap\n";
    let env_example = b"x\n";
    write_valid_project(&project, roadmap, env_example);

    // Try to create a junction at plan/linked. This may fail in restricted CI
    // environments; if it fails, the test simply cannot exercise the reparse
    // branch here and returns without failing.
    let link = project.path("plan/linked");
    let target = project.path("plan");
    let result = std::process::Command::new("cmd")
        .args(["/c", "mklink", "/J"])
        .arg(&link)
        .arg(&target)
        .output();
    let Ok(output) = result else {
        eprintln!("junction creation unavailable; skipping reparse assertion");
        return;
    };
    if !output.status.success() {
        eprintln!("junction creation unavailable; skipping reparse assertion");
        return;
    }

    let mut manifest = valid_manifest(roadmap, env_example);
    manifest["files"][0]["path"] = json!("plan/linked/roadmap.md");
    project.write(
        "agenttalk-brief.json",
        &serde_json::to_vec(&manifest).unwrap(),
    );

    let error = BriefSealer::new(project.root.clone())
        .seal(&InMemorySchemaRegistry::new())
        .unwrap_err();
    assert_eq!(error.code_str(), "BRIEF_REPARSE_POINT");
}

// Ensure the sealer product is not accidentally an orchestration run type.
#[test]
fn prepared_seal_is_an_immutable_snapshot_not_a_run() {
    let project = TestProject::new();
    write_valid_project(&project, b"# Roadmap\n", b"x\n");
    let seal = BriefSealer::new(project.root.clone())
        .seal(&InMemorySchemaRegistry::new())
        .unwrap();
    let _ = seal.brief_snapshot_id();
    let _ = seal.canonical_tree_record();
    // There is no journal write API in this crate; C3-A stops here.
    let _ = json::parse_duplicate_safe_str("{}").unwrap();
}

#[test]
fn snapshot_descriptor_reopens_after_authoring_tree_mutation() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap
";
    let env_example = b"EXAMPLE_TOKEN=replace-me
";
    write_valid_project(&project, roadmap, env_example);
    let sealer = BriefSealer::new(project.root.clone());
    let seal = sealer.seal(&InMemorySchemaRegistry::new()).unwrap();

    fs::remove_file(project.path("agenttalk-brief.json")).unwrap();
    fs::remove_file(project.path("plan/roadmap.md")).unwrap();
    fs::write(
        project.path("plan/.env.example"),
        b"mutated
",
    )
    .unwrap();

    let descriptor = sealer
        .read_snapshot_descriptor(seal.brief_snapshot_id())
        .unwrap();
    assert_eq!(descriptor.brief_tree_digest(), seal.brief_tree_digest());
    let manifest_bytes = sealer.cas().read(descriptor.manifest_object_ref()).unwrap();
    let parsed = ParsedManifest::parse(&manifest_bytes).unwrap();
    let shape = parsed.validate_shape().unwrap();
    assert_eq!(
        shape
            .as_value()
            .get("title")
            .and_then(serde_json::Value::as_str),
        Some("Sealer Test")
    );

    let mut re_read = Vec::new();
    for file in descriptor.files() {
        re_read.push((
            file.path().to_owned(),
            sealer.cas().read(file.object_ref()).unwrap(),
        ));
    }
    assert!(re_read
        .iter()
        .any(|(path, bytes)| path == "plan/roadmap.md" && bytes == roadmap));
    assert!(re_read
        .iter()
        .any(|(path, bytes)| path == "plan/.env.example" && bytes == env_example));
}

#[test]
fn same_tree_digest_different_raw_manifest_produces_distinct_reopenable_snapshots() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap
";
    let env_example = b"EXAMPLE_TOKEN=replace-me
";
    let manifest = valid_manifest(roadmap, env_example);
    let compact = serde_json::to_vec(&manifest).unwrap();
    let pretty = serde_json::to_vec_pretty(&manifest).unwrap();
    project.write("agenttalk-brief.json", &compact);
    project.write("plan/roadmap.md", roadmap);
    project.write("plan/.env.example", env_example);

    let sealer = BriefSealer::new(project.root.clone());
    let first = sealer.seal(&InMemorySchemaRegistry::new()).unwrap();
    project.write("agenttalk-brief.json", &pretty);
    let second = sealer.seal(&InMemorySchemaRegistry::new()).unwrap();

    assert_eq!(first.brief_tree_digest(), second.brief_tree_digest());
    assert_ne!(first.brief_snapshot_id(), second.brief_snapshot_id());
    let first_descriptor = sealer
        .read_snapshot_descriptor(first.brief_snapshot_id())
        .unwrap();
    let second_descriptor = sealer
        .read_snapshot_descriptor(second.brief_snapshot_id())
        .unwrap();
    assert_ne!(
        first_descriptor.manifest_object_ref(),
        second_descriptor.manifest_object_ref()
    );
    let first_raw = sealer
        .cas()
        .read(first_descriptor.manifest_object_ref())
        .unwrap();
    let second_raw = sealer
        .cas()
        .read(second_descriptor.manifest_object_ref())
        .unwrap();
    assert_eq!(first_raw, compact);
    assert_eq!(second_raw, pretty);
}

#[test]
fn schema_registry_canonical_bytes_are_sealed_in_descriptor() {
    let project = TestProject::new();
    let roadmap = b"# Roadmap
";
    let schema_value = json::parse_duplicate_safe_str(
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false}"#,
    )
    .unwrap();
    let canonical_schema_bytes = json::canonicalize(&schema_value).unwrap();
    let mut registry = InMemorySchemaRegistry::new();
    let reference = registry
        .register_value("agenttalk.plan.schema.v1", "1", schema_value)
        .unwrap();

    let json_body = b"{\"ok\":true}
";
    let mut manifest = valid_manifest(
        roadmap, b"x
",
    );
    manifest["files"][0]["format"] = json!("json");
    manifest["files"][0]["contentSchemaRef"] = json!({
        "id": reference.id,
        "version": reference.version,
        "digest": reference.digest,
    });
    manifest["files"][0]["sha256"] = json!(agenttalk_brief_sealer::cas::sha256_hex(json_body));
    manifest["files"][0]["size"] = json!(json_body.len());
    project.write(
        "agenttalk-brief.json",
        &serde_json::to_vec(&manifest).unwrap(),
    );
    project.write("plan/roadmap.md", json_body);
    project.write(
        "plan/.env.example",
        b"x
",
    );

    let sealer = BriefSealer::new(project.root.clone());
    let seal = sealer.seal(&registry).unwrap();
    assert_eq!(seal.schemas().len(), 1);
    let schema_ref = &seal.schemas()[0];
    assert_eq!(schema_ref.digest(), reference.digest);
    assert_eq!(
        sealer
            .cas()
            .read(schema_ref.canonical_schema_object_ref())
            .unwrap(),
        canonical_schema_bytes
    );

    let descriptor = sealer
        .read_snapshot_descriptor(seal.brief_snapshot_id())
        .unwrap();
    assert_eq!(descriptor.schemas().len(), 1);
    assert_eq!(
        sealer
            .cas()
            .read(descriptor.schemas()[0].canonical_schema_object_ref())
            .unwrap(),
        canonical_schema_bytes
    );
}

#[test]
fn cas_read_rejects_reparse_object_path() {
    let project = TestProject::new();
    let cas = CoreCas::new(project.root.clone());
    let object = cas.publish(b"hello cas").unwrap();
    let object_path = cas.object_path(&object.object_ref);
    fs::remove_file(&object_path).unwrap();

    let target = project.path("plan");
    let result = std::process::Command::new("cmd")
        .args(["/c", "mklink", "/J"])
        .arg(&object_path)
        .arg(&target)
        .output();
    let Ok(output) = result else {
        eprintln!("junction creation unavailable; skipping CAS reparse read assertion");
        return;
    };
    if !output.status.success() {
        eprintln!("junction creation unavailable; skipping CAS reparse read assertion");
        return;
    }

    let error = cas.read(&object.object_ref).unwrap_err();
    assert_eq!(error.code_str(), "BRIEF_REPARSE_POINT");
}

#[test]
fn delete_file_by_handle_removes_temp_and_publish_leaves_no_tmp() {
    let project = TestProject::new();
    let cas = CoreCas::new(project.root.clone());
    cas.publish(b"abc").unwrap();
    let objects_dir = cas.objects_root();
    let mut entries = std::fs::read_dir(&objects_dir).unwrap();
    assert!(entries.all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));

    // AlreadyExists idempotent path must also leave no temp.
    cas.publish(b"abc").unwrap();
    let mut entries = std::fs::read_dir(&objects_dir).unwrap();
    assert!(entries.all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
}
