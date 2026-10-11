// SPDX-License-Identifier: MIT

mod matcher;
mod protocol;
mod proxy;
mod qapi;
mod qmp;
mod qmp_decoder;
#[cfg(test)]
mod test_util;

use std::path::{Path, PathBuf};

use clap::Parser;
use log::warn;

use crate::proxy::QmpProxy;

#[derive(Debug, Parser)]
#[command(
    name = "qmp-proxy",
    about = "Multiplex many QMP clients over a single QEMU QMP connection."
)]
struct Cli {
    /// QEMU QMP socket to connect to.
    qemu_socket: PathBuf,

    /// Unix socket where the proxy listens for its clients. Defaults to
    /// `proxy.<qemu-socket>` in the same directory.
    #[arg(long, value_name = "PATH")]
    proxy_socket: Option<PathBuf>,

    /// Maximum inbound QMP message size in bytes, unlimited when unspecified.
    #[arg(long, value_name = "BYTES")]
    max_msg_size: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();

    let proxy_socket = cli
        .proxy_socket
        .unwrap_or_else(|| default_proxy_socket(&cli.qemu_socket));
    if proxy_socket.exists() {
        warn!("{proxy_socket:?} already exists, removing");
        std::fs::remove_file(&proxy_socket)?;
    }

    let proxy = QmpProxy::new(&cli.qemu_socket, &proxy_socket, cli.max_msg_size).await?;
    proxy.run().await?;

    Ok(())
}

/// Derive the proxy's default listen socket from the QEMU socket: same
/// directory, file name prefixed with `proxy.`. For example
/// `/tmp/qmp.sock` becomes `/tmp/proxy.qmp.sock`.
fn default_proxy_socket(qemu_socket: &Path) -> PathBuf {
    let name = qemu_socket
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    qemu_socket
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(format!("proxy.{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_socket_defaults_next_to_qemu_socket() {
        assert_eq!(
            default_proxy_socket(Path::new("/tmp/qmp.sock")),
            PathBuf::from("/tmp/proxy.qmp.sock")
        );
        assert_eq!(
            default_proxy_socket(Path::new("qmp.sock")),
            PathBuf::from("proxy.qmp.sock")
        );
        assert_eq!(
            default_proxy_socket(Path::new("/var/run/qemu/abc")),
            PathBuf::from("/var/run/qemu/proxy.abc")
        );
    }
}
