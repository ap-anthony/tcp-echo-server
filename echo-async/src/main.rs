use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    signal::{self},
    sync::broadcast::Receiver,
    task::JoinSet,
};
const STATS_LINE_INTERVAL: u64 = 5;

#[derive(Parser, Debug)]
#[command(name = "echo-async")]
#[command(about = "Asynchronous tcp echo server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    addr: String,

    #[arg(long, default_value = "7878")]
    port: u16,

    #[arg(long)]
    stats: bool,
}

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
async fn clean_conns(mut tasks: JoinSet<Result<()>>) -> Result<()> {
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
    let _ = drain.await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let listener = match TcpListener::bind(format!("{}:{}", args.addr, args.port)).await {
        Ok(l) => {
            eprintln!("listening on {}", l.local_addr()?);
            l
        }
        Err(e) => {
            eprintln!("bind failed: {e}");
            std::process::exit(2);
        }
    };
    let mut total_conns: u32 = 0;
    let active_conns = Arc::new(AtomicU32::new(0));
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    //  handle sigterm + sigint when possible
    let shutdown = async {
        #[cfg(unix)]
        {
            let mut sigterm = signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = signal::ctrl_c() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = signal::ctrl_c().await;
        }
    };
    tokio::pin!(shutdown);

    let mut stats_interval = tokio::time::interval(Duration::from_secs(STATS_LINE_INTERVAL));

    // skip the first tick since it's redundant
    stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                let n = active_conns.load(Ordering::SeqCst);
                eprintln!("shutdown requested, draining {} active connection{}",
                    n,
                    if n == 1 { "" } else { "s" }
                );
                let _ = shutdown_tx.send(());
                break;
            },
            accept = listener.accept() => {
                let (stream, peer) = accept.context("accept failed")?;
                let shutdown_rx = shutdown_tx.subscribe();
                let active_clone = active_conns.clone();
                total_conns += 1;
                eprintln!("accepted conn #{} from {}", total_conns, peer);
                active_clone.fetch_add(1, Ordering::SeqCst);
                tasks.spawn(async move {
                    let result = handle_conn(total_conns, stream, shutdown_rx).await;
                    active_clone.fetch_sub(1, Ordering::SeqCst);
                    result
                });
            },
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Ok(Err(e)) => eprintln!("conn error: {}", e),
                    Err(e) if e.is_panic() => eprintln!("conn panicked: {}", e),
                    _ => {}
                }
            },
            _ = stats_interval.tick(), if args.stats => {
                eprintln!("{} current conns", active_conns.load(Ordering::SeqCst));
            },
        }
    }

    match clean_conns(tasks).await {
        Ok(_) => eprintln!("shutdown complete"),
        Err(e) => eprintln!("error cleaning up conns {e}"),
    }

    Ok(())
}
