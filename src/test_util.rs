// SPDX-License-Identifier: MIT

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

use crate::qapi::control::ServerGreeting;

/// A realistic QMP greeting, useful as a stand-in for QEMU's own.
pub fn test_greeting() -> ServerGreeting {
    serde_json::from_value(json!({
        "QMP": {
            "version": {
                "qemu": {"major": 9, "minor": 0, "micro": 0},
                "package": "qmp-proxy-test"
            },
            "capabilities": ["oob"]
        }
    }))
    .unwrap()
}

/// Read a `\r\n`-terminated QMP message from a raw duplex stream as a `Value`.
pub async fn recv_value<R: AsyncReadExt + Unpin>(r: &mut R) -> Value {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        r.read_exact(&mut byte).await.unwrap();
        match byte[0] {
            b'\r' => {}
            b'\n' => break,
            b => buf.push(b),
        }
    }
    serde_json::from_slice(&buf).unwrap()
}

/// Write a QMP message as `\r\n`-terminated JSON to a raw duplex stream.
pub async fn send_value<W: AsyncWriteExt + Unpin>(w: &mut W, v: &Value) {
    let mut data = serde_json::to_vec(v).unwrap();
    data.extend_from_slice(b"\r\n");
    w.write_all(&data).await.unwrap();
}

static SOCK_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A unix socket path in the OS temp dir whose file is unlinked on drop, so
/// tests don't leave dangling `.sock` files behind (even on assertion panics).
pub struct TestSocket {
    path: PathBuf,
}

impl TestSocket {
    /// A fresh, unique unix socket path in the OS temp dir.
    pub fn unique() -> Self {
        let id = SOCK_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            path: std::env::temp_dir().join(format!(
                "qmp-proxy-test-{}-{id}.sock",
                std::process::id()
            )),
        }
    }

    /// The socket file path, for `UnixListener::bind` and `UnixStream::connect`.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Poll a condition until it holds, failing the test after a deadline.
pub async fn wait_for<F, Fut>(mut cond: F, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if tokio::time::timeout(timeout.saturating_sub(start.elapsed()), cond())
            .await
            .expect("Timeout waiting for condition")
        {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "condition not met within deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
