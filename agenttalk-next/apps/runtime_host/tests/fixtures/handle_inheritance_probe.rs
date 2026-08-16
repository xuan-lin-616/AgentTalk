use std::io::{BufRead, BufReader, Read, Write};

const WORKER_FRAME_MAGIC: &str = "AGENTTALK_LOCAL_DISCOVERY_WORKER_V1";
const WORKER_PROTOCOL_ID: &str = "agenttalk.local-discovery-worker.v1";
const WORKER_BUILD_ID: &str = "agenttalk-runtime-host:0.1.0:local-discovery-worker";
const MAX_WORKER_REQUEST_BYTES: usize = 64 * 1024;

fn main() {
    std::process::exit(match run() {
        Ok(()) => 0,
        Err(()) => 1,
    });
}

fn run() -> Result<(), ()> {
    #[cfg(windows)]
    probe_inherited_handle()?;
    let _request = read_worker_request_frame(std::io::stdin().lock())?;
    let response = format!(
        r#"{{"version":1,"protocolIdentity":"{WORKER_PROTOCOL_ID}","buildIdentity":"{WORKER_BUILD_ID}","observations":[],"diagnostics":[]}}"#
    );
    write_worker_frame(std::io::stdout().lock(), response.as_bytes())
}

fn read_worker_request_frame<R: Read>(reader: R) -> Result<Vec<u8>, ()> {
    let mut reader = BufReader::new(reader);
    let mut header = String::new();
    let read = reader.read_line(&mut header).map_err(|_| ())?;
    if read == 0 || header.len() > 128 {
        return Err(());
    }
    let header = header.trim_end_matches(['\r', '\n']);
    let Some(length_text) = header
        .strip_prefix(WORKER_FRAME_MAGIC)
        .and_then(|tail| tail.strip_prefix(' '))
    else {
        return Err(());
    };
    let length = length_text.parse::<usize>().map_err(|_| ())?;
    if length > MAX_WORKER_REQUEST_BYTES {
        return Err(());
    }
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload).map_err(|_| ())?;
    Ok(payload)
}

fn write_worker_frame<W: Write>(mut writer: W, payload: &[u8]) -> Result<(), ()> {
    writer
        .write_all(format!("{WORKER_FRAME_MAGIC} {}\n", payload.len()).as_bytes())
        .map_err(|_| ())?;
    writer.write_all(payload).map_err(|_| ())?;
    writer.flush().map_err(|_| ())
}

#[cfg(windows)]
fn probe_inherited_handle() -> Result<(), ()> {
    use std::os::windows::io::FromRawHandle;

    let handle_value = std::env::args()
        .nth(1)
        .ok_or(())?
        .parse::<usize>()
        .map_err(|_| ())?;
    let handle = handle_value as std::os::windows::io::RawHandle;
    if handle.is_null() {
        return Err(());
    }
    let leaked = unsafe {
        let mut file = std::fs::File::from_raw_handle(handle);
        let write = file.write_all(b"x").is_ok();
        std::mem::forget(file);
        write
    };
    if leaked {
        Err(())
    } else {
        Ok(())
    }
}
