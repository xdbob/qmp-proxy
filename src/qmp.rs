use std::matches;

use log::{debug, warn};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::protocol::{QmpCommand, QmpError, QmpErrorData, QmpMessage, QmpPayload, QmpResponse};
use crate::qapi::control::ServerGreeting;
use crate::qmp_decoder::{QmpDecoder, QmpDecoderError};

#[derive(Debug, Error)]
pub enum QmpClientError {
    #[error("Failed to decode incoming message: {0}")]
    Decode(#[from] QmpDecoderError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Json decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Json encode error: {0}")]
    Encode(serde_json::Error),
    #[error("Invalid QMP response: {0:?}")]
    FailedHandcheck(QmpMessage),
}

type Result<T> = std::result::Result<T, QmpClientError>;

pub struct QmpReader<R>
where
    R: AsyncReadExt + Unpin,
{
    reader: FramedRead<R, QmpDecoder>,
}

impl<R> QmpReader<R>
where
    R: AsyncReadExt + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: FramedRead::new(reader, QmpDecoder::new()),
        }
    }

    pub fn new_bounded(reader: R, max_size: usize) -> Self {
        Self {
            reader: FramedRead::new(reader, QmpDecoder::new_bounded(max_size)),
        }
    }

    async fn recv_raw_msg(&mut self) -> Result<Value> {
        Ok(self.reader.next().await.ok_or_else(|| {
            let eof = std::io::ErrorKind::UnexpectedEof;
            std::io::Error::from(eof)
        })??)
    }

    pub async fn recv_msg(&mut self) -> Result<QmpMessage> {
        Ok(serde_json::from_value(self.recv_raw_msg().await?)?)
    }
}

pub struct QmpWriter<W>
where
    W: AsyncWriteExt + Unpin,
{
    writer: W,
}

impl<W> QmpWriter<W>
where
    W: AsyncWriteExt + Unpin,
{
    #[cfg(test)]
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    async fn send_raw_msg<M: Serialize>(&mut self, msg: &M) -> Result<()> {
        let mut raw_data = serde_json::to_vec(msg).map_err(QmpClientError::Encode)?;
        raw_data.extend(b"\r\n");
        Ok(self.writer.write_all(&raw_data).await?)
    }

    pub async fn send_msg(&mut self, msg: &QmpMessage) -> Result<()> {
        self.send_raw_msg(msg).await
    }
}

pub async fn qmp_connect<R, W>(
    reader: R,
    writer: W,
    max_msg_size: Option<usize>,
) -> Result<(QmpReader<R>, QmpWriter<W>, ServerGreeting)>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut reader = if let Some(max_size) = max_msg_size {
        QmpReader::new_bounded(reader, max_size)
    } else {
        QmpReader::new(reader)
    };
    debug!("Awaiting server greeting");
    let greeting: ServerGreeting = serde_json::from_value(reader.recv_raw_msg().await?)?;

    let mut writer = QmpWriter { writer };

    writer
        .send_msg(
            &QmpCommand {
                execute: "qmp_capabilities".to_string(),
                arguments: Some(serde_json::json!({
                    "enable": greeting.QMP.capabilities,
                })),
            }
            .into(),
        )
        .await?;

    let cap_ret = reader.recv_msg().await?;
    if !matches!(cap_ret.payload, QmpPayload::Response(ref resp) if matches!(&resp.data, Value::Object(ret) if ret.is_empty()))
    {
        return Err(QmpClientError::FailedHandcheck(cap_ret));
    }
    debug!("Server greeted us with: {:?}", greeting);

    Ok((reader, writer, greeting))
}

pub async fn qmp_serve<R, W>(
    reader: R,
    writer: W,
    greeting: &ServerGreeting,
    max_msg_size: Option<usize>,
) -> Result<(QmpReader<R>, QmpWriter<W>)>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut reader = if let Some(max_size) = max_msg_size {
        QmpReader::new_bounded(reader, max_size)
    } else {
        QmpReader::new(reader)
    };
    let mut writer = QmpWriter { writer };

    writer.send_raw_msg(&greeting).await?;

    // Poor man's command handling
    loop {
        debug!("Awaiting client capabilities");
        let client_msg = reader.recv_msg().await?;
        match client_msg.payload {
            QmpPayload::Command(cmd) if cmd.execute == "qmp_capabilities" => {
                debug!("Client negociated capabilites: {cmd:?}");
                let resp = QmpResponse {
                    data: serde_json::Value::Object(serde_json::Map::new()),
                };
                writer
                    .send_msg(&QmpMessage::new(client_msg.id, resp.into()))
                    .await?;
                break;
            }
            _ => {
                warn!("Client did not negociate capabilities with: {client_msg:?}");
                let err = QmpError {
                    error: QmpErrorData {
                        class: "CommandNotFound".to_string(),
                        desc: "Only `qmp_capabilities` is available for now".to_string(),
                    },
                };
                writer
                    .send_msg(&QmpMessage::new(client_msg.id, err.into()))
                    .await?;
            }
        }
    }

    Ok((reader, writer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::net::UnixStream;

    use crate::qapi::control::QMPCapability;
    use crate::test_util::{recv_value, send_value, test_greeting};

    fn greeting_payload() -> Value {
        json!({
            "QMP": {
                "version": {
                    "qemu": {"major": 9, "minor": 0, "micro": 0},
                    "package": "fake"
                },
                "capabilities": ["oob"]
            }
        })
    }

    #[tokio::test]
    async fn qmp_connect_handshake_success() {
        let (proxy_sock, fake_sock) = UnixStream::pair().unwrap();
        let (proxy_rd, proxy_wr) = proxy_sock.into_split();
        let fake = tokio::spawn(async move {
            let mut fake = fake_sock;
            send_value(&mut fake, &greeting_payload()).await;
            let req = recv_value(&mut fake).await;
            assert_eq!(req["execute"], "qmp_capabilities");
            assert_eq!(req["arguments"]["enable"], json!(["oob"]));
            send_value(&mut fake, &json!({"return": {}})).await;
        });
        let (reader, writer, greeting) = qmp_connect(proxy_rd, proxy_wr, None).await.unwrap();
        assert_eq!(greeting.QMP.version.qemu.major, 9);
        assert_eq!(greeting.QMP.capabilities, vec![QMPCapability::oob]);
        let _ = (reader, writer);
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn qmp_connect_rejects_bad_return() {
        let (proxy_sock, fake_sock) = UnixStream::pair().unwrap();
        let (proxy_rd, proxy_wr) = proxy_sock.into_split();
        let fake = tokio::spawn(async move {
            let mut fake = fake_sock;
            send_value(&mut fake, &greeting_payload()).await;
            let _req = recv_value(&mut fake).await;
            send_value(
                &mut fake,
                &json!({"error": {"class": "CommandNotFound", "desc": "nope"}}),
            )
            .await;
        });
        match qmp_connect(proxy_rd, proxy_wr, None).await {
            Err(QmpClientError::FailedHandcheck(_)) => {}
            Err(other) => panic!("unexpected error: {other}"),
            Ok(_) => panic!("expected the capabilities handcheck to fail"),
        }
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn qmp_serve_handshakes_and_echoes_greeting() {
        let (server_sock, client_sock) = UnixStream::pair().unwrap();
        let (s_rd, s_wr) = server_sock.into_split();
        let serve = tokio::spawn(async move { qmp_serve(s_rd, s_wr, &test_greeting(), None).await });
        let mut client = client_sock;
        let greeting = recv_value(&mut client).await;
        assert_eq!(greeting["QMP"]["capabilities"], json!(["oob"]));
        send_value(&mut client, &json!({"execute": "qmp_capabilities"})).await;
        assert_eq!(recv_value(&mut client).await, json!({"return": {}}));
        serve.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn qmp_serve_rejects_other_commands_during_handshake() {
        let (server_sock, client_sock) = UnixStream::pair().unwrap();
        let (s_rd, s_wr) = server_sock.into_split();
        let serve = tokio::spawn(async move { qmp_serve(s_rd, s_wr, &test_greeting(), None).await });
        let mut client = client_sock;
        let _greeting = recv_value(&mut client).await;
        send_value(&mut client, &json!({"execute": "query-version"})).await;
        let err = recv_value(&mut client).await;
        assert_eq!(err["error"]["class"], "CommandNotFound");
        // A proper handshake still succeeds afterwards
        send_value(&mut client, &json!({"execute": "qmp_capabilities"})).await;
        assert_eq!(recv_value(&mut client).await, json!({"return": {}}));
        serve.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reader_writer_round_trip() {
        let (a, b) = UnixStream::pair().unwrap();
        let (a_rd, a_wr) = a.into_split();
        let (b_rd, b_wr) = b.into_split();
        let send = tokio::spawn(async move {
            let mut writer = QmpWriter::new(a_wr);
            writer
                .send_msg(&QmpMessage::new(
                    Some(Value::Number(7.into())),
                    QmpResponse {
                        data: json!({"ok": true}),
                    }
                    .into(),
                ))
                .await
                .unwrap();
        });
        let mut reader = QmpReader::new(b_rd);
        let msg = reader.recv_msg().await.unwrap();
        assert_eq!(msg.id, Some(Value::Number(7.into())));
        assert!(matches!(msg.payload, QmpPayload::Response(_)));
        let _ = (a_rd, b_wr);
        send.await.unwrap();
    }
}
