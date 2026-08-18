//! One-off offline generator for the bundled `production_catalog.json`.
//!
//! Filters out agents whose Windows distribution is a subdirectory path or a
//! `.cmd` batch launcher (unsupported by the flat, no-shell binary model), then
//! converts the remaining registry through `convert_acp_registry_bytes` and
//! emits a self-referential-digest `CatalogCache` document suitable for
//! `include_bytes!("production_catalog.json")`.
//!
//! Usage:
//!   cargo run -p agenttalk-runtime-host --example generate-production-catalog -- \
//!     <registry.json> <output.json> [revision]
use std::fs;

use agenttalk_runtime_host::{convert_acp_registry_bytes, normalized_catalog_digest, CatalogCache};
use serde_json::Value;

/// Agents whose Windows binary lives in an archive subdirectory or is a `.cmd`
/// batch file. The current converter models a flat, directly-executable `.exe`
/// only; supporting these needs a separate converter/provider change.
const SKIP_IDS: &[&str] = &["cortex-code", "cursor", "devin", "goose", "junie"];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: generate-production-catalog <registry.json> <output.json> [revision]");
        std::process::exit(2);
    }
    let input = &args[1];
    let output = &args[2];
    let revision = args.get(3).map(String::as_str).unwrap_or("agenttalk-v1");

    let bytes = fs::read(input).expect("read registry json");

    let mut value: Value = serde_json::from_slice(&bytes).expect("registry is valid json");
    let mut skipped = 0usize;
    if let Some(agents) = value.get_mut("agents").and_then(Value::as_array_mut) {
        let before = agents.len();
        agents.retain(|agent| {
            let keep = !agent
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| SKIP_IDS.contains(&id));
            if !keep {
                skipped += 1;
            }
            keep
        });
        eprintln!(
            "filtered {}/{} agents (skipped {skipped})",
            agents.len(),
            before
        );
    }
    let filtered = serde_json::to_vec(&value).expect("re-serialize filtered registry");

    let converted = convert_acp_registry_bytes(&filtered, "windows", "x86_64", revision)
        .expect("convert filtered registry");
    let mut manifests = converted
        .into_iter()
        .map(|entry| entry.manifest)
        .collect::<Vec<_>>();

    // Placeholder so the digest-bearing fields are PRESENT in serialization
    // (normalized_catalog_digest zeroes them; absent fields would change the
    // digest between the placeholder and final serializations).
    let zero = "0".repeat(64);
    for manifest in &mut manifests {
        let source = manifest
            .source
            .as_mut()
            .expect("converted manifest must carry a source");
        source.catalog_sha256 = Some(zero.clone());
    }

    let mut cache = CatalogCache {
        version: 1,
        generation: 1,
        revision: revision.to_owned(),
        created_at_ms: 0,
        registry_sha256: zero,
        manifests,
    };

    let placeholder = serde_json::to_vec(&cache).expect("serialize placeholder");
    let digest = normalized_catalog_digest(&placeholder).expect("compute self-referential digest");

    cache.registry_sha256 = digest.clone();
    for manifest in &mut cache.manifests {
        if let Some(source) = &mut manifest.source {
            source.catalog_sha256 = Some(digest.clone());
        }
    }

    let final_json = serde_json::to_string_pretty(&cache).expect("serialize final catalog");
    fs::write(output, final_json).expect("write catalog");
    eprintln!(
        "wrote {} manifests to {} (revision={revision}, digest={digest})",
        cache.manifests.len(),
        output
    );
}
