use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast::{Receiver, Sender},
    task::JoinSet,
};

const STATS_LINE_INTERVAL: u64 = 5;

/// Handles an inbound connection, echoing data received until shutdown occurs or client disconnects.
///
/// # Errors
/// - Read error - failed to read into the input buffer from the stream
async fn handle_conn(id: u32, mut stream: TcpStream, mut shutdown: Receiver<()>) -> Result<()> {
    let mut buf = [0u8; 4096];
    let mut total: u64 = 0;
    loop {
        tokio::select! {
        biased;
        _ = shutdown.recv() => {
            eprintln!("conn #{} closed on shutdown after {} bytes", id, total);
            return Ok(());
        }
        read = stream.read(&mut buf) => {
            match read {
                Ok(0) => {
                    eprintln!("conn #{} closed by peer after {} bytes", id, total);
                    return Ok(());
                }
                Ok(n) => {
                    stream.write_all(&buf[..n]).await?;
                    total += n as u64;
                    eprintln!("conn #{} echoed {} bytes", id, n);
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("conn #{} read failed", id));
                }
                }
            }
        }
    }
}

/// Cleans up the remaining tasks after a server shutdown. Waits up to 5 seconds for connections
/// to finish before returning.
async fn clean_conns(mut tasks: JoinSet<Result<()>>) {
    let drain = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(())) => {} // clean cleanup
                Ok(Err(e)) => {
                    eprintln!("conn error: {}", e)
                } // handler returned Err
                Err(e) if e.is_panic() => eprintln!("conn panicked: {}", e),
                Err(_) => {} // task was aborted
            }
        }
    });
    // ignore timeout
    let _ = drain.await;
}

/// Starts async server using tokio TcpListener. Takes a Sender to notify
/// tasks to shutdown.
///
/// # Arguments
/// * `listener` - tokio TcpListener
/// * `sender` - tokio broadcast Sender
/// * `stats_enabled` - feature flag to display # of conns every N seconds (defined by STATS_LINE_INTERVAL)
///
/// # Errors
/// - accept failure -- listener fails to accept a connection
pub async fn start_server(
    listener: TcpListener,
    shutdown_tx: Sender<()>,
    stats_enabled: bool,
    max_conns: u32,
) -> Result<()> {
    let mut total_conns: u32 = 0;
    let active_conns = Arc::new(AtomicU32::new(0));
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();
    let mut stats_interval = tokio::time::interval(Duration::from_secs(STATS_LINE_INTERVAL));

    stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // skip the first tick since it's redundant
    if stats_enabled {
        stats_interval.tick().await;
    }

    let mut shutdown = shutdown_tx.subscribe();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                let n = active_conns.load(Ordering::SeqCst);
                eprintln!("shutdown requested, draining {} active connection{}",
                    n,
                    if n == 1 { "" } else { "s" }
                );
                break;
            },
            accept = listener.accept() => {
                let (mut stream, peer) = accept.context("accept failed")?;

                if max_conns == 0 || active_conns.load(Ordering::SeqCst) < max_conns {
                    let active_clone = active_conns.clone();
                    total_conns += 1;
                    eprintln!("accepted conn #{} from {}", total_conns, peer);
                    active_clone.fetch_add(1, Ordering::SeqCst);
                    let conn_shutdown = shutdown_tx.subscribe();
                    tasks.spawn(async move {
                        let result = handle_conn(total_conns, stream, conn_shutdown).await;
                        active_clone.fetch_sub(1, Ordering::SeqCst);
                        result
                    });
                } else {
                    stream.write_all(b"server is at max capacity. please try again later.\n").await?;
                    drop(stream);
                    eprintln!("rejected conn from {} at max capacity", peer);
                }

            },
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Ok(Err(e)) => eprintln!("conn error: {}", e),
                    Err(e) if e.is_panic() => eprintln!("conn panicked: {}", e),
                    _ => {}
                }
            },
            _ = stats_interval.tick(), if stats_enabled => {
                eprintln!("{} current conns", active_conns.load(Ordering::SeqCst));
            },
        }
    }

    clean_conns(tasks).await;
    Ok(())
}
