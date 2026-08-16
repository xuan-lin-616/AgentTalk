#![cfg(windows)]

use agenttalk_core::PersistentCore;

#[test]
fn production_core_default_is_unconfigured_and_fail_closed() {
    let core = PersistentCore::open(":memory:").expect("default Core should open metadata storage");
    let health = core.runtime_health();
    assert_eq!(health["runtimeId"], "unconfigured");
    assert_eq!(health["availability"], "unavailable");
    assert_eq!(health["connectors"][0]["verified"], false);
    assert!(core.runtime_models()["models"]
        .as_array()
        .unwrap()
        .is_empty());
}
