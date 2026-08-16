use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpCommand {
    pub execute: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpResponse {
    #[serde(rename = "return")]
    pub data: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpErrorData {
    pub class: String,
    pub desc: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpError {
    pub error: QmpErrorData,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpOobCommand {
    #[serde(rename = "exec-oob")]
    pub execute: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpTimeStamp {
    pub seconds: isize,
    pub microseconds: isize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpEvent {
    pub event: String,
    pub data: Option<Value>,
    pub timestamp: QmpTimeStamp,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QmpPayload {
    Command(QmpCommand),
    Oob(QmpOobCommand),
    Response(QmpResponse),
    Error(QmpError),
    Event(QmpEvent),
}

impl From<QmpCommand> for QmpPayload {
    fn from(cmd: QmpCommand) -> Self {
        Self::Command(cmd)
    }
}

impl From<QmpOobCommand> for QmpPayload {
    fn from(cmd: QmpOobCommand) -> Self {
        Self::Oob(cmd)
    }
}

impl From<QmpResponse> for QmpPayload {
    fn from(cmd: QmpResponse) -> Self {
        Self::Response(cmd)
    }
}

impl From<QmpError> for QmpPayload {
    fn from(cmd: QmpError) -> Self {
        Self::Error(cmd)
    }
}

impl From<QmpEvent> for QmpPayload {
    fn from(cmd: QmpEvent) -> Self {
        Self::Event(cmd)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QmpMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(flatten)]
    pub payload: QmpPayload,
}

impl From<QmpCommand> for QmpMessage {
    fn from(cmd: QmpCommand) -> Self {
        Self {
            id: None,
            payload: cmd.into(),
        }
    }
}

impl From<QmpOobCommand> for QmpMessage {
    fn from(cmd: QmpOobCommand) -> Self {
        Self {
            id: None,
            payload: cmd.into(),
        }
    }
}

impl From<QmpResponse> for QmpMessage {
    fn from(cmd: QmpResponse) -> Self {
        Self {
            id: None,
            payload: cmd.into(),
        }
    }
}

impl From<QmpError> for QmpMessage {
    fn from(cmd: QmpError) -> Self {
        Self {
            id: None,
            payload: cmd.into(),
        }
    }
}

impl From<QmpEvent> for QmpMessage {
    fn from(cmd: QmpEvent) -> Self {
        Self {
            id: None,
            payload: cmd.into(),
        }
    }
}

impl From<QmpPayload> for QmpMessage {
    fn from(payload: QmpPayload) -> Self {
        QmpMessage { id: None, payload }
    }
}

impl QmpMessage {
    pub fn new(id: Option<Value>, payload: QmpPayload) -> Self {
        Self { id, payload }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(msg: &str) -> QmpMessage {
        serde_json::from_str(msg).unwrap()
    }

    #[test]
    fn command_serializes_without_id() {
        let msg = QmpMessage::from(QmpCommand {
            execute: "qmp_capabilities".to_string(),
            arguments: None,
        });
        assert_eq!(
            serde_json::to_value(msg).unwrap(),
            json!({"execute": "qmp_capabilities"})
        );
    }

    #[test]
    fn command_serializes_with_numeric_id() {
        let msg = QmpMessage::new(
            Some(Value::Number(7.into())),
            QmpCommand {
                execute: "query-version".to_string(),
                arguments: None,
            }
            .into(),
        );
        assert_eq!(
            serde_json::to_value(msg).unwrap(),
            json!({"execute": "query-version", "id": 7})
        );
    }

    #[test]
    fn arbitrary_json_id_round_trips() {
        let original = r#"{"execute":"query-version","id":{"client":1}}"#;
        let msg: QmpMessage = serde_json::from_str(original).unwrap();
        assert_eq!(msg.id, Some(json!({"client": 1})));
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            serde_json::from_str::<Value>(original).unwrap()
        );
    }

    #[test]
    fn deserialize_response() {
        let msg = parse(r#"{"return":{"a":1},"id":1}"#);
        assert_eq!(msg.id, Some(Value::Number(1.into())));
        match msg.payload {
            QmpPayload::Response(resp) => assert_eq!(resp.data, json!({"a": 1})),
            other => panic!("expected Response, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_error() {
        let msg = parse(r#"{"error":{"class":"CommandNotFound","desc":"nope"},"id":5}"#);
        assert_eq!(msg.id, Some(Value::Number(5.into())));
        match msg.payload {
            QmpPayload::Error(err) => {
                assert_eq!(err.error.class, "CommandNotFound");
                assert_eq!(err.error.desc, "nope");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_event() {
        let msg =
            parse(r#"{"event":"RESET","data":{"x":1},"timestamp":{"seconds":1,"microseconds":2}}"#);
        assert_eq!(msg.id, None);
        match msg.payload {
            QmpPayload::Event(evt) => {
                assert_eq!(evt.event, "RESET");
                assert_eq!(evt.data, Some(json!({"x": 1})));
                assert_eq!((evt.timestamp.seconds, evt.timestamp.microseconds), (1, 2));
            }
            other => panic!("expected Event, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_oob_command() {
        let msg = parse(r#"{"exec-oob":"query-version","arguments":{}}"#);
        assert!(matches!(msg.payload, QmpPayload::Oob(_)));
    }

    #[test]
    fn event_with_null_data() {
        let msg = parse(r#"{"event":"POWERDOWN","timestamp":{"seconds":0,"microseconds":0}}"#);
        match msg.payload {
            QmpPayload::Event(evt) => assert_eq!(evt.data, None),
            other => panic!("expected Event, got {other:?}"),
        }
    }
}
