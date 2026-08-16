#![allow(dead_code)] // Shared fixture helpers are used by different integration binaries.

use agenttalk_orchestration_contracts::brief::InMemoryBriefBytesMap;
use agenttalk_orchestration_contracts::handoff::{
    DeliveryDeclaration, HandoffVerificationContext, InMemoryObjectStore,
};
use agenttalk_orchestration_contracts::registry::InMemorySchemaRegistry;
use std::fs;
use std::path::{Path, PathBuf};

pub fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("golden")
}

pub fn read_bytes(path: impl AsRef<Path>) -> Vec<u8> {
    fs::read(golden_dir().join(&path)).unwrap_or_else(|error| {
        panic!(
            "failed to read fixture {}: {error}",
            path.as_ref().display()
        )
    })
}

pub fn read_text(path: impl AsRef<Path>) -> String {
    fs::read_to_string(golden_dir().join(&path)).unwrap_or_else(|error| {
        panic!(
            "failed to read fixture {}: {error}",
            path.as_ref().display()
        )
    })
}

pub fn trimmed(path: impl AsRef<Path>) -> String {
    read_text(path).trim().to_owned()
}

pub fn load_bytes_map(case: impl AsRef<Path>) -> InMemoryBriefBytesMap {
    let mut map = InMemoryBriefBytesMap::new();
    let bytes_dir = golden_dir().join(case).join("bytes");
    if !bytes_dir.exists() {
        return map;
    }
    collect_files(&bytes_dir, &bytes_dir, &mut |relative, bytes| {
        map.insert(relative.replace('\\', "/"), bytes);
    });
    map
}

fn collect_files(root: &Path, directory: &Path, f: &mut dyn FnMut(String, Vec<u8>)) {
    for entry in fs::read_dir(directory).expect("bytes directory must be readable") {
        let entry = entry.expect("directory entry must be readable");
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, f);
        } else {
            let relative = path
                .strip_prefix(root)
                .expect("bytes path is below root")
                .to_string_lossy()
                .to_string();
            f(
                relative,
                fs::read(&path).expect("fixture bytes must be readable"),
            );
        }
    }
}

pub fn load_brief_registry(case: &str) -> InMemorySchemaRegistry {
    let mut registry = InMemorySchemaRegistry::new();
    if case == "brief/valid-schema-registry" {
        register_schema(
            &mut registry,
            case,
            "registry/design-spec.json",
            "registry/design-spec.digest.txt",
            "agenttalk.design.spec.v1",
            "1",
        );
        register_schema(
            &mut registry,
            case,
            "registry/acceptance.json",
            "registry/acceptance.digest.txt",
            "agenttalk.acceptance.v1",
            "1",
        );
    }
    registry
}

fn register_schema(
    registry: &mut InMemorySchemaRegistry,
    case: &str,
    schema_file: &str,
    digest_file: &str,
    id: &str,
    version: &str,
) {
    let path = Path::new(case).join(schema_file);
    let reference = registry
        .register_json(id, version, &read_bytes(path))
        .expect("registry fixture must register");
    let expected_digest = trimmed(Path::new(case).join(digest_file));
    assert_eq!(reference.digest, expected_digest, "literal schema digest");
}

pub struct HandoffFixtureContext {
    pub cas: InMemoryObjectStore,
    pub registry: InMemorySchemaRegistry,
    pub declaration: DeliveryDeclaration,
}

pub fn load_handoff_context(case: &str) -> HandoffFixtureContext {
    let mut cas = InMemoryObjectStore::new();
    let cas_dir = golden_dir().join(case).join("cas");
    if cas_dir.exists() {
        for entry in fs::read_dir(cas_dir).expect("cas dir must be readable") {
            let entry = entry.expect("cas entry must be readable");
            let path = entry.path();
            let file_name = path
                .file_stem()
                .expect("cas blob must have stem")
                .to_string_lossy();
            let object_ref = format!("sha256:{file_name}");
            cas.insert(
                object_ref,
                fs::read(&path).expect("cas blob must be readable"),
            );
        }
    }

    let mut registry = InMemorySchemaRegistry::new();
    let registry_dir = golden_dir().join(case).join("registry");
    if registry_dir.exists() {
        register_schema(
            &mut registry,
            case,
            "registry/spec-schema.json",
            "registry/spec-schema.digest.txt",
            "agenttalk.design.spec.v1",
            "1",
        );
        register_schema(
            &mut registry,
            case,
            "registry/notes-schema.json",
            "registry/notes-schema.digest.txt",
            "agenttalk.notes.v1",
            "1",
        );
    }

    let declaration_path = Path::new(case).join("declaration.input.json");
    let declaration = DeliveryDeclaration::parse(&read_bytes(declaration_path))
        .expect("declaration fixture must parse");

    HandoffFixtureContext {
        cas,
        registry,
        declaration,
    }
}

pub fn verification_context<'a>(
    fixture: &'a HandoffFixtureContext,
) -> HandoffVerificationContext<'a> {
    HandoffVerificationContext::new(&fixture.cas, &fixture.registry, &fixture.declaration)
}

pub fn list_dirs(case: &str) -> Vec<String> {
    let root = golden_dir().join(case);
    let mut names = fs::read_dir(root)
        .expect("fixture root must exist")
        .map(|entry| {
            entry
                .expect("entry must be readable")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect::<Vec<String>>();
    names.sort();
    names
}
