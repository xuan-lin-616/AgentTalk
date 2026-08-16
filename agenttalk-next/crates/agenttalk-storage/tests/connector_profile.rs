use agenttalk_domain::{ConnectorProfile, CONNECTOR_PROFILE_SCOPE};
use agenttalk_storage::{SqliteStore, StorageError, CONNECTOR_PROFILE_QUERY_LIMIT_MAX};
use serde_json::json;
use std::path::PathBuf;

fn database_path(label: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agenttalk-{label}-{}-{nonce}.sqlite3",
        std::process::id()
    ))
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

fn profile(display_name: &str) -> ConnectorProfile {
    ConnectorProfile {
        scope_id: CONNECTOR_PROFILE_SCOPE.into(),
        connector_id: "fixture.connector".into(),
        display_name: display_name.into(),
        provider_type: "openai-compatible".into(),
        runtime_type: "local_gateway".into(),
        enabled: true,
        auth_env_key: Some("AGENTTALK_AUTH_KEY".into()),
    }
}

#[test]
fn connector_profile_lifecycle_is_scope_safe_idempotent_and_reopenable() {
    let path = database_path("connector-profile");
    let mut store = SqliteStore::open(&path).expect("open store");
    assert!(
        store.projection_snapshot().expect("initial projection")["connectorProfiles"]
            .as_array()
            .expect("connector profile projection array")
            .is_empty()
    );

    let original = profile("Fixture connector");
    assert!(store
        .create_connector_profile(&original)
        .expect("create profile"));
    assert!(!store
        .create_connector_profile(&original)
        .expect("idempotent create"));

    let mut conflicting = original.clone();
    conflicting.display_name = "Different metadata".into();
    assert!(matches!(
        store.create_connector_profile(&conflicting),
        Err(StorageError::ConnectorProfileConflict { .. })
    ));

    assert_eq!(
        store
            .query_connector_profiles(CONNECTOR_PROFILE_SCOPE, None, 10)
            .expect("query profile"),
        vec![original.clone()]
    );
    assert!(!store
        .update_connector_profile(&original)
        .expect("idempotent update"));

    let mut updated = original.clone();
    updated.enabled = false;
    updated.runtime_type = "fixture_runtime".into();
    assert!(store
        .update_connector_profile(&updated)
        .expect("update profile"));
    assert!(matches!(
        store.update_connector_profile(&original),
        Ok(true)
    ));

    let projection = store
        .projection_snapshot()
        .expect("projection after update");
    assert_eq!(
        projection["connectorProfiles"][0]["connectorId"],
        "fixture.connector"
    );
    assert_eq!(
        projection["connectorProfiles"][0]["authEnvKey"],
        "AGENTTALK_AUTH_KEY"
    );
    let encoded =
        serde_json::to_string(&projection["connectorProfiles"]).expect("encode projection");
    assert!(!encoded.contains("Authorization"));

    drop(store);
    let mut reopened = SqliteStore::open(&path).expect("reopen store");
    assert!(!reopened.migration_checksum().is_empty());
    assert_eq!(
        reopened
            .query_connector_profiles(CONNECTOR_PROFILE_SCOPE, Some("fixture.connector"), 1)
            .expect("query after reopen")
            .len(),
        1
    );
    assert!(reopened
        .remove_connector_profile(CONNECTOR_PROFILE_SCOPE, "fixture.connector")
        .expect("remove profile"));
    assert!(!reopened
        .remove_connector_profile(CONNECTOR_PROFILE_SCOPE, "fixture.connector")
        .expect("idempotent remove"));
    assert!(reopened
        .query_connector_profiles(
            CONNECTOR_PROFILE_SCOPE,
            None,
            CONNECTOR_PROFILE_QUERY_LIMIT_MAX
        )
        .expect("query after remove")
        .is_empty());
    drop(reopened);
    cleanup(&path);
}

#[test]
fn connector_profile_rejects_invalid_scope_auth_name_and_unknown_fields() {
    let path = database_path("connector-profile-validation");
    let mut store = SqliteStore::open(&path).expect("open store");

    let mut wrong_scope = profile("Wrong scope");
    wrong_scope.scope_id = "project-1".into();
    assert!(matches!(
        store.create_connector_profile(&wrong_scope),
        Err(StorageError::ConnectorProfileScopeInvalid { .. })
    ));

    let mut wrong_auth_name = profile("Wrong auth name");
    wrong_auth_name.auth_env_key = Some("not an environment value".into());
    assert!(matches!(
        store.create_connector_profile(&wrong_auth_name),
        Err(StorageError::ConnectorProfileInvalid { field, .. }) if field == "authEnvKey"
    ));

    let unknown = serde_json::from_value::<ConnectorProfile>(json!({
        "scopeId": "desktop",
        "connectorId": "fixture.connector",
        "displayName": "Unknown field",
        "providerType": "openai-compatible",
        "runtimeType": "local_gateway",
        "enabled": true,
        "authEnvKey": null,
        "unexpected": "rejected"
    }));
    assert!(unknown.is_err());

    drop(store);
    cleanup(&path);
}
