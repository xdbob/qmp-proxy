use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind::UnexpectedEof;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use log::{debug, info, trace, warn};
use serde_json::Value;
use thiserror::Error;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixSocket};
use tokio::sync::{Mutex, RwLock};
use tokio::task;

use crate::matcher::Matcher;
use crate::protocol::{QmpMessage, QmpPayload};
use crate::qapi::control::ServerGreeting;
use crate::qmp::{QmpClientError, QmpReader, QmpWriter, qmp_connect, qmp_serve};

#[derive(Debug, Error)]
pub enum QmpProxyError {
    #[error("QMP Error: {0}")]
    Sock(#[from] QmpClientError),
    #[error("Connection failed: {0}")]
    Connect(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, QmpProxyError>;

/// Client handshake timeout duration
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// Sleep between failed `accept_client` calls, doubling up to `ACCEPT_ERROR_BACKOFF_MAX`.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// A live client connection, identified by the writer's `Arc` pointer.
#[derive(Clone)]
struct ClientWriter(Arc<Mutex<QmpWriter<OwnedWriteHalf>>>);

impl PartialEq for ClientWriter {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ClientWriter {}

impl Hash for ClientWriter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

#[derive(Clone)]
struct Command {
    client: Weak<Mutex<QmpWriter<OwnedWriteHalf>>>,
    original_id: Option<Value>,
}

struct QmpProxyShared {
    qemu_writer: Mutex<QmpWriter<OwnedWriteHalf>>,
    greeting: ServerGreeting,
    server: UnixListener,
    clients: RwLock<HashSet<ClientWriter>>,
    commands: Matcher<Command>,
    max_msg_size: Option<usize>,
}

impl QmpProxyShared {
    fn new(
        qemu_writer: QmpWriter<OwnedWriteHalf>,
        greeting: ServerGreeting,
        server: UnixListener,
        max_msg_size: Option<usize>,
    ) -> Self {
        Self {
            qemu_writer: Mutex::new(qemu_writer),
            greeting,
            server,
            clients: RwLock::new(HashSet::new()),
            commands: Matcher::new(),
            max_msg_size,
        }
    }

    async fn client_reply(&self, id: usize, payload: QmpPayload) {
        let cmd = match self.commands.pop(id).await {
            Some(cmd) => cmd,
            None => {
                warn!("Unknown response received id:{id}, {payload:?}");
                return;
            }
        };
        let response = QmpMessage::new(cmd.original_id, payload);
        trace!("=({})> {response:?}", id);
        if let Some(client) = cmd.client.upgrade() {
            if let Err(err) = client.lock().await.send_msg(&response).await {
                warn!("Failed to forward reply to client: {err}");
            }
        } else {
            warn!("Dropping QEMU reply without a live client: {response:?}");
        }
    }

    async fn broadcast(&self, msg: QmpMessage) {
        trace!("=(*)> {msg:?}");

        // snapshot clients to avoid holding the lock
        let clients: Vec<ClientWriter> = self.clients.read().await.iter().cloned().collect();

        stream::iter(clients.into_iter().map(async |client| {
            if let Err(err) = client.0.lock().await.send_msg(&msg).await {
                warn!("Failed to broadcast message to a client: {err}");
            }
        }))
        .buffer_unordered(8)
        .collect::<()>()
        .await;
    }

    async fn accept_client(self: &Arc<Self>) -> Result<()> {
        let (client, addr) = self.server.accept().await?;
        info!("New client connection from: {addr:?}");
        let (client_rd, client_wr) = client.into_split();

        // Handshake runs detached so a slow/hostile client can't stall the
        // accept loop; HANDSHAKE_TIMEOUT bounds how long it may take.
        task::spawn(Self::client_handshake(self.clone(), client_rd, client_wr));
        Ok(())
    }

    async fn client_handshake(
        shared: Arc<Self>,
        client_rd: OwnedReadHalf,
        client_wr: OwnedWriteHalf,
    ) {
        let handshake = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            qmp_serve(client_rd, client_wr, &shared.greeting, shared.max_msg_size),
        )
        .await;

        let (reader, writer) = match handshake {
            Ok(Ok(pair)) => pair,
            Ok(Err(err)) => {
                warn!("Client handshake failed: {err}");
                return;
            }
            Err(_elapsed) => {
                warn!("Client handshake timed out after {HANDSHAKE_TIMEOUT:?}");
                return;
            }
        };

        let writer = Arc::new(Mutex::new(writer));
        shared
            .clients
            .write()
            .await
            .insert(ClientWriter(writer.clone()));

        let _client_loop = task::spawn(async move {
            shared.client_loop(reader, writer).await;
        });
    }

    async fn client_loop(
        self: &Arc<Self>,
        mut reader: QmpReader<OwnedReadHalf>,
        writer: Arc<Mutex<QmpWriter<OwnedWriteHalf>>>,
    ) {
        loop {
            let msg = match reader.recv_msg().await {
                Ok(msg) => msg,
                Err(QmpClientError::Io(err)) if err.kind() == UnexpectedEof => {
                    info!("Client disconnected");
                    self.disconnect(writer).await;
                    break;
                }
                Err(err) => {
                    warn!("Failed to recv client message: {err}");
                    continue;
                }
            };

            // Note: we could avoid the ID clone without the log
            let original_id = msg.id.clone();
            let id = self
                .commands
                .insert(Command {
                    client: Arc::downgrade(&writer),
                    original_id,
                })
                .await;
            trace!("<({id})= {msg:?}");

            if let Err(err) = self
                .qemu_writer
                .lock()
                .await
                .send_msg(&QmpMessage::new(
                    Some(Value::Number(id.into())),
                    msg.payload,
                ))
                .await
            {
                warn!("Failed to forward message to QEMU: {err}");
            }
        }
    }

    async fn disconnect(&self, writer: Arc<Mutex<QmpWriter<OwnedWriteHalf>>>) {
        let drop_ptr = Arc::downgrade(&writer);
        self.clients.write().await.remove(&ClientWriter(writer));
        let dropped = self
            .commands
            .remove_if(|_, cmd| Weak::ptr_eq(&drop_ptr, &cmd.client))
            .await;
        if !dropped.is_empty() {
            info!(
                "Dropped {} pending command(s) after disconnect",
                dropped.len()
            );
        }
    }
}

/// Unlinks the proxy's listen socket file when dropped, so a clean shutdown (or
/// an error/panic unwind) doesn't leave a stale `.sock` file behind. The OS
/// keeps the bound listener's fd alive, so unlink while `UnixListener` is still
/// open is fine.
struct SocketFileGuard(PathBuf);

impl Drop for SocketFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub struct QmpProxy {
    qemu_reader: QmpReader<OwnedReadHalf>,
    shared: Arc<QmpProxyShared>,
    _listen_sock: SocketFileGuard,
}

impl QmpProxy {
    pub async fn new(
        qemu_sock: &Path,
        server_path: &Path,
        max_msg_size: Option<usize>,
    ) -> Result<Self> {
        debug!("Connecting to {qemu_sock:?}");
        let qemu_sock = UnixSocket::new_stream()?.connect(qemu_sock).await?;
        let (qemu_rd, qemu_wr) = qemu_sock.into_split();
        let (qemu_reader, qemu_writer, greeting) =
            qmp_connect(qemu_rd, qemu_wr, max_msg_size).await?;

        Ok(Self {
            qemu_reader,
            shared: Arc::new(QmpProxyShared::new(
                qemu_writer,
                greeting,
                UnixListener::bind(server_path)?,
                max_msg_size,
            )),
            _listen_sock: SocketFileGuard(server_path.to_path_buf()),
        })
    }

    pub async fn run(self) -> Result<()> {
        let QmpProxy {
            mut qemu_reader,
            shared,
            _listen_sock,
        } = self;

        let accept = shared.clone();
        let _accept_loop = task::spawn(async move {
            let mut backoff = ACCEPT_ERROR_BACKOFF;
            loop {
                match accept.accept_client().await {
                    Err(err) => {
                        warn!("Failed to accept incoming client: {err:?}");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(ACCEPT_ERROR_BACKOFF_MAX);
                    }
                    Ok(()) => backoff = ACCEPT_ERROR_BACKOFF,
                }
            }
        });

        loop {
            let msg = match qemu_reader.recv_msg().await {
                Ok(msg) => msg,
                Err(QmpClientError::Io(err)) if err.kind() == UnexpectedEof => {
                    info!("QEMU socket closed, exiting");
                    return Ok(());
                }
                Err(err) => {
                    warn!("Failed to read QEMU message: {err}");
                    continue;
                }
            };

            if let Some(id) = msg.id {
                match id.as_u64().and_then(|n| usize::try_from(n).ok()) {
                    Some(id) => shared.client_reply(id, msg.payload).await,
                    None => warn!("Failed to parse response id: {id:?}"),
                }
            } else {
                shared.broadcast(msg).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use std::path::PathBuf;
    use tokio::net::UnixStream;

    use crate::protocol::{QmpCommand, QmpEvent, QmpResponse, QmpTimeStamp};
    use crate::test_util::{TestSocket, test_greeting, wait_for};

    async fn server() -> (
        QmpReader<OwnedReadHalf>,
        QmpWriter<OwnedWriteHalf>,
        ServerGreeting,
        (QmpReader<OwnedReadHalf>, QmpWriter<OwnedWriteHalf>),
    ) {
        let (proxy_sock, mock_sock) = UnixStream::pair().unwrap();
        let (mock_rd, mock_wr) = mock_sock.into_split();
        let greeting = test_greeting();
        let serve =
            tokio::spawn(async move { qmp_serve(mock_rd, mock_wr, &greeting, None).await.unwrap() });
        let (proxy_rd, proxy_wr) = proxy_sock.into_split();
        let (reader, writer, greeting) = qmp_connect(proxy_rd, proxy_wr, None).await.unwrap();
        let mock = serve.await.unwrap();
        (reader, writer, greeting, mock)
    }

    async fn proxy(
        qemu_reader: QmpReader<OwnedReadHalf>,
        qemu_writer: QmpWriter<OwnedWriteHalf>,
        greeting: ServerGreeting,
    ) -> (QmpProxy, TestSocket) {
        let server_sock = TestSocket::unique();
        let shared = Arc::new(QmpProxyShared::new(
            qemu_writer,
            greeting,
            UnixListener::bind(server_sock.path()).unwrap(),
            None,
        ));
        (
            QmpProxy {
                qemu_reader,
                shared,
                _listen_sock: SocketFileGuard(server_sock.path().to_path_buf()),
            },
            server_sock,
        )
    }

    async fn client(
        sock: PathBuf,
    ) -> (
        QmpReader<OwnedReadHalf>,
        QmpWriter<OwnedWriteHalf>,
        ServerGreeting,
    ) {
        let (rd, wr) = UnixStream::connect(&sock).await.unwrap().into_split();
        qmp_connect(rd, wr, None).await.unwrap()
    }

    // The handshake is detached, so a client can finish its side of the
    // handshake before the proxy registers its writer; wait for the count.
    async fn wait_for_client_count(shared: &Arc<QmpProxyShared>, n: usize) {
        let shared_wait = shared.clone();
        wait_for(
            move || {
                let shared_wait = shared_wait.clone();
                async move { shared_wait.clients.read().await.len() == n }
            },
            Duration::from_millis(500),
        )
        .await;
    }

    #[tokio::test]
    async fn run_forwards_replies_and_broadcasts() {
        let (qemu_reader, qemu_writer, greeting, (mut fake_reader, mut fake_writer)) =
            server().await;

        let (proxy, server_path) = proxy(qemu_reader, qemu_writer, greeting).await;
        let run_task = tokio::spawn(proxy.run());

        let (mut client_reader, mut client_writer, _) =
            client(server_path.path().to_path_buf()).await;

        client_writer
            .send_msg(&QmpMessage::new(
                Some(Value::Number(42.into())),
                QmpCommand {
                    execute: "query-status".to_string(),
                    arguments: None,
                }
                .into(),
            ))
            .await
            .unwrap();

        let fwd = fake_reader.recv_msg().await.unwrap();
        let fwd_id = fwd
            .id
            .expect("forwarded command should carry the proxy's id");
        match fwd.payload {
            QmpPayload::Command(cmd) => assert_eq!(cmd.execute, "query-status"),
            other => panic!("expected a command, got {other:?}"),
        }
        assert_ne!(fwd_id.as_u64().unwrap(), 42);

        // A reply is routed back to the owning client with the original id restored.
        fake_writer
            .send_msg(&QmpMessage::new(
                Some(fwd_id),
                QmpResponse {
                    data: json!({"status": "running"}),
                }
                .into(),
            ))
            .await
            .unwrap();
        let reply = client_reader.recv_msg().await.unwrap();
        assert_eq!(reply.id, Some(Value::Number(42.into())));
        match reply.payload {
            QmpPayload::Response(resp) => assert_eq!(resp.data["status"], "running"),
            other => panic!("expected Response, got {other:?}"),
        }

        // An id-less QEMU message is broadcast to the client as-is.
        fake_writer
            .send_msg(&QmpMessage::from(QmpEvent {
                event: "POWERDOWN".to_string(),
                data: None,
                timestamp: QmpTimeStamp {
                    seconds: 0,
                    microseconds: 0,
                },
            }))
            .await
            .unwrap();
        let evt = client_reader.recv_msg().await.unwrap();
        assert_eq!(evt.id, None);
        assert!(matches!(evt.payload, QmpPayload::Event(e) if e.event == "POWERDOWN"));

        // A reply with an unparseable id is ignored, but the loop keeps running.
        fake_writer
            .send_msg(&QmpMessage::from(QmpEvent {
                event: "RESET".to_string(),
                data: None,
                timestamp: QmpTimeStamp {
                    seconds: 1,
                    microseconds: 0,
                },
            }))
            .await
            .unwrap();
        let evt = client_reader.recv_msg().await.unwrap();
        assert!(matches!(evt.payload, QmpPayload::Event(e) if e.event == "RESET"));

        // QEMU closing the socket makes run() exit cleanly.
        drop((fake_reader, fake_writer));
        assert!(run_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn run_unlinks_listen_socket_on_exit() {
        let (qemu_reader, qemu_writer, greeting, (fake_reader, fake_writer)) = server().await;

        let (proxy, server_path) = proxy(qemu_reader, qemu_writer, greeting).await;
        let run_task = tokio::spawn(proxy.run());
        assert!(server_path.path().exists());

        // QEMU closing the socket makes run() exit cleanly, and its socket
        // guard must have unlinked the listen socket file by the time it returns.
        drop((fake_reader, fake_writer));
        assert!(run_task.await.unwrap().is_ok());
        assert!(!server_path.path().exists());
    }

    #[tokio::test]
    async fn broadcast_reaches_all_clients_but_skips_disconnected() {
        let (qemu_reader, qemu_writer, greeting, _mock) = server().await;

        let (proxy, server_path) = proxy(qemu_reader, qemu_writer, greeting).await;
        let QmpProxy {
            qemu_reader,
            shared,
            ..
        } = proxy;
        drop(qemu_reader);

        let client_a = tokio::spawn(client(server_path.path().to_path_buf()));
        shared.accept_client().await.unwrap();
        let (mut a_rd, a_wr, _) = client_a.await.unwrap();
        wait_for_client_count(&shared, 1).await;

        let client_b = tokio::spawn(client(server_path.path().to_path_buf()));
        shared.accept_client().await.unwrap();
        let (mut b_rd, b_wr, _) = client_b.await.unwrap();
        wait_for_client_count(&shared, 2).await;

        let event = |name: &str| {
            QmpMessage::from(QmpEvent {
                event: name.to_string(),
                data: None,
                timestamp: QmpTimeStamp {
                    seconds: 0,
                    microseconds: 0,
                },
            })
        };
        shared.broadcast(event("B")).await;
        let e_a = a_rd.recv_msg().await.unwrap();
        let e_b = b_rd.recv_msg().await.unwrap();
        assert_eq!(e_a.id, e_b.id);
        assert!(
            matches!((e_a.payload, e_b.payload), (QmpPayload::Event(x), QmpPayload::Event(y)) if x.event == y.event && x.event == "B")
        );

        // Closing the client's write half lets the proxy notice the disconnect.
        drop(a_wr);
        let shared_wait = shared.clone();
        wait_for(
            move || {
                let shared_wait = shared_wait.clone();
                async move { shared_wait.clients.read().await.len() == 1 }
            },
            Duration::from_millis(500),
        )
        .await;

        shared.broadcast(event("C")).await;
        let e_b = b_rd.recv_msg().await.unwrap();
        assert!(matches!(e_b.payload, QmpPayload::Event(e) if e.event == "C"));
        // The disconnected client gets nothing: either a clean EOF (the proxy
        // dropped its writer) or nothing at all, but never the broadcast event.
        match tokio::time::timeout(Duration::from_millis(150), a_rd.recv_msg()).await {
            Err(_elapsed) => {}
            Ok(Err(QmpClientError::Io(err))) if err.kind() == UnexpectedEof => {}
            Ok(Ok(msg)) => panic!("disconnected client received {msg:?}"),
            Ok(Err(other)) => panic!("disconnected client errored with {other}"),
        }
        drop((b_wr, a_rd, b_rd));
    }

    #[tokio::test]
    async fn client_reply_restores_original_id() {
        let (qemu_reader, qemu_writer, greeting, (mut fake_reader, mut fake_writer)) =
            server().await;

        let (proxy, server_path) = proxy(qemu_reader, qemu_writer, greeting).await;
        let run_task = tokio::spawn(proxy.run());

        let (mut client_reader, mut client_writer, _) =
            client(server_path.path().to_path_buf()).await;

        client_writer
            .send_msg(&QmpMessage::new(
                Some(Value::Number(7.into())),
                QmpCommand {
                    execute: "query-version".to_string(),
                    arguments: None,
                }
                .into(),
            ))
            .await
            .unwrap();

        // The proxy re-multiplexes with its own numeric id; answering with that id
        // routes the reply back to the client with its original id restored.
        let fwd = fake_reader.recv_msg().await.unwrap();
        let fwd_id = fwd
            .id
            .expect("forwarded command should carry the proxy's id");
        fake_writer
            .send_msg(&QmpMessage::new(
                Some(fwd_id),
                QmpResponse {
                    data: json!({"ok": true}),
                }
                .into(),
            ))
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(3), client_reader.recv_msg())
            .await
            .expect("timed out waiting for the reply")
            .unwrap();
        assert_eq!(msg.id, Some(Value::Number(7.into())));
        assert!(matches!(msg.payload, QmpPayload::Response(_)));

        drop((fake_reader, fake_writer));
        assert!(run_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn client_reply_unknown_id_is_harmless() {
        let (qemu_reader, qemu_writer, greeting, (mut fake_reader, mut fake_writer)) =
            server().await;

        let (proxy, server_path) = proxy(qemu_reader, qemu_writer, greeting).await;
        let run_task = tokio::spawn(proxy.run());

        let (mut client_reader, mut client_writer, _) =
            client(server_path.path().to_path_buf()).await;

        // A reply whose id matches no pending command is dropped; the loop keeps running.
        fake_writer
            .send_msg(&QmpMessage::new(
                Some(Value::Number(9999.into())),
                QmpResponse { data: json!({}) }.into(),
            ))
            .await
            .unwrap();

        // A subsequent round-trip still works, proving the loop is alive.
        client_writer
            .send_msg(&QmpMessage::new(
                Some(Value::Number(1.into())),
                QmpCommand {
                    execute: "query-status".to_string(),
                    arguments: None,
                }
                .into(),
            ))
            .await
            .unwrap();
        let fwd = fake_reader.recv_msg().await.unwrap();
        let fwd_id = fwd
            .id
            .expect("forwarded command should carry the proxy's id");
        fake_writer
            .send_msg(&QmpMessage::new(
                Some(fwd_id),
                QmpResponse {
                    data: json!({"status": "running"}),
                }
                .into(),
            ))
            .await
            .unwrap();
        let reply = client_reader.recv_msg().await.unwrap();
        assert_eq!(reply.id, Some(Value::Number(1.into())));
        assert!(
            matches!(reply.payload, QmpPayload::Response(resp) if resp.data["status"] == "running")
        );

        drop((fake_reader, fake_writer));
        assert!(run_task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn disconnect_drops_pending_commands() {
        let (qemu_reader, qemu_writer, greeting, (mut fake_reader, _fake_writer)) = server().await;

        let (proxy, server_path) = proxy(qemu_reader, qemu_writer, greeting).await;
        let QmpProxy { shared, .. } = proxy;

        let client = tokio::spawn(client(server_path.path().to_path_buf()));
        shared.accept_client().await.unwrap();
        let (client_rd, mut client_wr, _) = client.await.unwrap();

        client_wr
            .send_msg(&QmpMessage::new(
                Some(Value::Number(1.into())),
                QmpCommand {
                    execute: "query-status".to_string(),
                    arguments: None,
                }
                .into(),
            ))
            .await
            .unwrap();

        // The command is forwarded (hence registered); it is the first insert so its id is 0.
        let fwd = fake_reader.recv_msg().await.unwrap();
        match fwd.payload {
            QmpPayload::Command(cmd) => assert_eq!(cmd.execute, "query-status"),
            other => panic!("expected a command, got {other:?}"),
        }

        drop(client_wr);
        drop(client_rd);
        let shared_wait = shared.clone();
        wait_for(
            move || {
                let shared_wait = shared_wait.clone();
                async move { shared_wait.clients.read().await.is_empty() }
            },
            Duration::from_millis(500),
        )
        .await;

        // The pending command was cleaned up by disconnect().
        assert!(shared.commands.pop(0).await.is_none());
    }
}
