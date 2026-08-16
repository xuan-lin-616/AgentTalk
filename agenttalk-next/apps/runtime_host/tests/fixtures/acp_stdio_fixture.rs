use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

fn marker_path(name: &str) -> PathBuf {
    std::env::current_exe()
        .expect("fixture executable path")
        .parent()
        .expect("fixture executable parent")
        .join(name)
}

fn spawn_owned_child() {
    if Command::new(std::env::current_exe().expect("fixture executable"))
        .arg("child-loop")
        .spawn()
        .is_err()
    {
        std::process::exit(14);
    }
    let marker = marker_path("descendant.pid");
    for _ in 0..100 {
        if marker.is_file() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    std::process::exit(15);
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "success".into());
    if mode == "child-loop" {
        let _ = std::fs::write(marker_path("descendant.pid"), std::process::id().to_string());
        loop {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    let _ = std::fs::write(marker_path("root.pid"), std::process::id().to_string());
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(marker_path("initialize.invocations"))
        .and_then(|mut file| writeln!(file, "{}", std::process::id()));

    let mut request = String::new();
    if BufReader::new(std::io::stdin())
        .read_line(&mut request)
        .is_err()
    {
        std::process::exit(11);
    }
    let initialize_count = request.matches("\"method\":\"initialize\"").count();
    let forbidden_request =
        request.contains("session/") || request.contains("prompt") || request.contains("tool/");
    if initialize_count != 1 || forbidden_request {
        std::process::exit(12);
    }

    if mode == "timeout" {
        loop {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    if mode == "spawn-child-timeout" {
        spawn_owned_child();
        loop {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    if mode == "crash" {
        std::process::exit(13);
    }
    if mode == "spawn-child-crash" {
        spawn_owned_child();
        let release = marker_path("allow-crash");
        for _ in 0..400 {
            if release.is_file() {
                std::process::exit(13);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        std::process::exit(13);
    }
    if mode == "truncated" {
        let _ = std::io::stdout().write_all(&[0xff, b'\n']);
        let _ = std::io::stdout().flush();
        return;
    }
    if mode == "oversized" {
        let _ = std::io::stdout().write_all(&vec![b'x'; 70 * 1024]);
        let _ = std::io::stdout().write_all(b"\n");
        let _ = std::io::stdout().flush();
        return;
    }
    if mode == "stdout-pollution" {
        println!("not-json");
    }
    if mode == "stderr" {
        eprintln!("fixture diagnostic");
    }
    if mode == "whitespace-stderr" {
        eprint!(" \t\r\n");
    }

    let mut version = if std::env::var_os("AGENTTALK_W4_UNRELATED_CREDENTIAL").is_none() {
        "init1-other0-envabsent".to_owned()
    } else {
        "init1-other0-envpresent".to_owned()
    };
    if mode == "spawn-child" {
        spawn_owned_child();
        version.push_str("-child");
    }
    if mode == "spawn-child-keepalive" {
        spawn_owned_child();
        version.push_str("-child-keepalive");
    }
    if mode == "environment-allowlist" {
        version.push_str(if std::env::var_os("AGENTTALK_W4_SAFE_ALLOWED").as_deref()
            == Some(std::ffi::OsStr::new("allowed"))
        {
            "-allowed"
        } else {
            "-allowlist-missing"
        });
    }
    let protocol_version = if mode == "unsupported-major" { 2 } else { 1 };
    let auth_methods = if mode == "official-capabilities" {
        r#"[{"_meta":{"ignore":"metadata"},"id":"agent-login","name":"Agent login","description":"Agent-managed sign in"}]"#
    } else if mode == "auth-required" {
        r#"[{"id":"agent-login","name":"Agent login"}]"#
    } else if mode == "invalid-auth-method" {
        r#"[{"id":"agent-login","name":"Agent login","type":"oauth"}]"#
    } else {
        "[]"
    };
    let agent_info = if mode == "invalid-agent-info" {
        r#"{"name":"https://private.invalid","title":"Fixture Agent","version":"1"}"#.to_owned()
    } else {
        format!(
            r#"{{"name":"fixture-agent","title":"Fixture Agent","version":"{version}"}}"#
        )
    };
    let capabilities = if mode == "invalid-capabilities" {
        r#"{"unexpected":true}"#
    } else if mode == "official-capabilities" {
        r#"{"_meta":{"ignore":"metadata"},"loadSession":true,"promptCapabilities":{"_meta":{"ignore":"metadata"},"image":true,"audio":true,"embeddedContext":true},"mcpCapabilities":{"_meta":{"ignore":"metadata"},"http":true,"sse":true},"sessionCapabilities":{"_meta":{"ignore":"metadata"},"additionalDirectories":{"_meta":{"ignore":"metadata"}},"close":{"_meta":{"ignore":"metadata"}},"delete":{"_meta":{"ignore":"metadata"}},"list":{"_meta":{"ignore":"metadata"}},"resume":{"_meta":{"ignore":"metadata"}}},"auth":{"_meta":{"ignore":"metadata"},"logout":{"_meta":{"ignore":"metadata"}}}}"#
    } else {
        "{}"
    };
    let response_meta = if mode == "official-capabilities" {
        r#","_meta":{"ignore":"metadata"}"#
    } else {
        ""
    };
    let result_meta = if mode == "official-capabilities" {
        r#""_meta":{"ignore":"metadata"},"#
    } else {
        ""
    };
    let response = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":0{response_meta},\"result\":{{\
         {result_meta}\
         \"protocolVersion\":{protocol_version},\
         \"agentCapabilities\":{capabilities},\
         \"agentInfo\":{agent_info},\
         \"authMethods\":{auth_methods}\
         }}}}"
    );
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(response.as_bytes());
    let _ = stdout.write_all(b"\n");
    if mode == "duplicate-response" {
        let _ = stdout.write_all(response.as_bytes());
        let _ = stdout.write_all(b"\n");
    }
    if mode == "trailing-frame" {
        let _ = stdout.write_all(b"\n");
    }
    if mode == "empty-frame" {
        let _ = stdout.write_all(b"\n");
    }
    let _ = stdout.flush();
    if mode == "response-then-crash" {
        std::process::exit(13);
    }
    if mode == "spawn-child-keepalive" {
        loop {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
