use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const PROTOCOL_MAJOR: u16 = 1;
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamCursor {
    pub stream_id: String,
    pub sequence: u64,
    pub epoch: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolHandshake {
    pub kind: String,
    pub protocol: ProtocolVersion,
    pub client_id: String,
    pub session_id: String,
    pub session_credential: String,
    pub max_message_bytes: usize,
    pub last_seen: Option<StreamCursor>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandEnvelope {
    pub kind: String,
    pub protocol: ProtocolVersion,
    pub request_id: String,
    pub session_id: String,
    pub command: String,
    pub payload: Value,
    pub deadline_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryEnvelope {
    pub kind: String,
    pub protocol: ProtocolVersion,
    pub request_id: String,
    pub session_id: String,
    pub query: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope {
    pub kind: String,
    pub protocol: ProtocolVersion,
    pub event_id: String,
    pub session_id: String,
    pub cursor: StreamCursor,
    pub subscription_id: Option<String>,
    pub execution_run_id: Option<String>,
    pub event: String,
    pub occurred_at: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorEnvelope {
    pub kind: String,
    pub protocol: ProtocolVersion,
    pub request_id: String,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub details: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseEnvelope {
    pub kind: String,
    pub protocol: ProtocolVersion,
    pub request_id: String,
    pub ok: bool,
    pub payload: Value,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum FrameError {
    #[error("frame is shorter than the 4-byte length prefix")]
    TooShort,
    #[error("frame length {actual} exceeds maximum {maximum}")]
    TooLarge { actual: usize, maximum: usize },
    #[error("frame length prefix {declared} does not match payload length {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("invalid JSON payload: {0}")]
    Json(String),
}

pub fn encode_frame<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>, FrameError> {
    let payload = serde_json::to_vec(value).map_err(|error| FrameError::Json(error.to_string()))?;
    if payload.len() > maximum {
        return Err(FrameError::TooLarge {
            actual: payload.len(),
            maximum,
        });
    }
    let length = (payload.len() as u32).to_be_bytes();
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&length);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn decode_frame(frame: &[u8], maximum: usize) -> Result<Vec<u8>, FrameError> {
    if frame.len() < 4 {
        return Err(FrameError::TooShort);
    }
    let declared = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    if declared > maximum {
        return Err(FrameError::TooLarge {
            actual: declared,
            maximum,
        });
    }
    let actual = frame.len() - 4;
    if declared != actual {
        return Err(FrameError::LengthMismatch { declared, actual });
    }
    Ok(frame[4..].to_vec())
}

pub fn validate_protocol(version: &ProtocolVersion) -> Result<(), FrameError> {
    if version.major != PROTOCOL_MAJOR {
        return Err(FrameError::Json(format!(
            "unsupported protocol major {}",
            version.major
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn length_prefixed_frame_round_trips() {
        let command = CommandEnvelope {
            kind: "command".into(),
            protocol: ProtocolVersion { major: 1, minor: 0 },
            request_id: "req-1".into(),
            session_id: "session-123456789".into(),
            command: "execution.start".into(),
            payload: json!({"projectId":"p"}),
            deadline_ms: Some(1000),
        };
        let frame = encode_frame(&command, DEFAULT_MAX_MESSAGE_BYTES).unwrap();
        let bytes = decode_frame(&frame, DEFAULT_MAX_MESSAGE_BYTES).unwrap();
        let decoded: CommandEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, command);
    }

    #[test]
    fn framing_fails_closed_on_oversized_or_malformed_input() {
        assert!(matches!(
            encode_frame(&json!({"large":"123456789"}), 4),
            Err(FrameError::TooLarge { .. })
        ));
        assert!(matches!(
            decode_frame(&[0, 0, 0, 8, 1], 16),
            Err(FrameError::LengthMismatch { .. })
        ));
        assert!(matches!(
            decode_frame(&[0, 0, 0], 16),
            Err(FrameError::TooShort)
        ));
    }

    #[test]
    fn checked_fixtures_use_the_versioned_schema_shape() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let handshake: ProtocolHandshake = serde_json::from_str(
            &std::fs::read_to_string(root.join("../../fixtures/ipc/handshake.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(handshake.kind, "handshake");
        assert_eq!(handshake.protocol.major, PROTOCOL_MAJOR);
        let command: CommandEnvelope = serde_json::from_str(
            &std::fs::read_to_string(root.join("../../fixtures/ipc/execution-start.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(command.command, "execution.start");
        let event: EventEnvelope = serde_json::from_str(
            &std::fs::read_to_string(root.join("../../fixtures/ipc/output-delta.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(event.event, "output.delta");
        assert_eq!(event.cursor.sequence, 1);
    }
}
