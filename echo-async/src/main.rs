use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    signal,
    sync::broadcast::Receiver,
    task::JoinSet,
};

async fn handle_conn(id: u32, mut stream: TcpStream, peer: SocketAddr, mut shutdown: Receiver<()>) {
    eprintln!("accepted conn #{} from {}", id, peer);
    let mut buf = [0u8; 4096];
    let mut total: u64 = 0;
    loop {
        tokio::select! {
        biased;
        _ = shutdown.recv() => {
            eprintln!("conn #{} closed on shutdown after {} bytes", id, total);
            return;
        }
        read = stream.read(&mut buf) => {
        match read {
            Ok(0) => {
                eprintln!("conn #{} closed by peer after {} bytes", id, total);
                return;
            }
            Ok(n) => {
                if let Err(e) = stream.write_all(&buf[..n]).await {
                    eprintln!("conn #{} error: {}", id, e);
                    return;
                }
                total += n as u64;
                eprintln!("conn #{} echoed {} bytes", id, n);
            }
            Err(e) => {
                eprintln!("conn #{} error: {}", id, e);
                return;
            }
            }
        }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878")
        .await
        .context("bind failed")?;
    let mut total_conns: u32 = 0;
    let mut tasks: JoinSet<()> = JoinSet::new();
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    eprintln!("listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                eprintln!("shutdown requested. draining {} active connection{}",
                    tasks.len(),
                    if tasks.len() == 1 { "" } else { "s" }
                );
                let _ = shutdown_tx.send(());
                break;
            },
            accept = listener.accept() => {
                let (stream, peer) = accept.context("accept failed")?;
                let shutdown_rx = shutdown_tx.subscribe();
                total_conns += 1;
                let id = total_conns;
                tasks.spawn(handle_conn(id, stream, peer, shutdown_rx));
            }
        }
    }

    // drain
    let drain = tokio::time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    });
    let _ = drain.await; // ignore timeout; we exit either way
    eprintln!("shutdown complete");

    Ok(())
}
