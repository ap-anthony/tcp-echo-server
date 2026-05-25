use anyhow::{Context, Result};
use clap::Parser;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

#[derive(Parser, Debug)]
#[command(name = "echo-sync")]
#[command(about = "Synchronous tcp echo server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    addr: String,

    #[arg(long, default_value = "7878")]
    port: u16,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut total_conns: u32 = 0;
    let active_conns = Arc::new(AtomicU32::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        ctrlc::set_handler(move || shutdown.store(true, Ordering::SeqCst))?;
    }
    let listener = TcpListener::bind(format!("{}:{}", args.addr, args.port)).context("bind failed");
    if let Err(e) = listener {
        eprintln!("bind failed: {e}");
        std::process::exit(2);
    }
    let listener = listener?;
    eprintln!("listening on {}:{}", args.addr, args.port);
    let mut threads: Vec<JoinHandle<_>> = Vec::new();
    listener.set_nonblocking(true)?;
    loop {
        if shutdown.load(Ordering::SeqCst) {
            let conns = active_conns.load(Ordering::SeqCst);
            eprintln!(
                "shutdown requested, draining {} active connection{}",
                conns,
                if conns == 1 { "" } else { "s" }
            );
            break;
        }
        match listener.accept() {
            Ok((stream, peer)) => {
                total_conns += 1;
                eprintln!(
                    "accepted conn #{} from {}:{}",
                    total_conns,
                    peer.ip(),
                    peer.port()
                );
                stream
                    .set_nonblocking(false)
                    .context("could not set stream to blocking")?;
                let client_shutdown = shutdown.clone();
                let active_conns = active_conns.clone();
                active_conns.fetch_add(1, Ordering::SeqCst);
                threads.push(std::thread::spawn(move || -> Result<()> {
                    let conn_id = total_conns;
                    let result = handle_conn(stream, conn_id, &client_shutdown);
                    active_conns.fetch_sub(1, Ordering::SeqCst);
                    if let Err(e) = &result {
                        eprintln!("conn #{conn_id} error: {e}");
                    }
                    result
                }));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => return Err(e).context("accept failed"),
        }
    }
    for t in threads {
        let _ = t.join().expect("thread panicked");
    }
    eprintln!("shutdown complete");
    Ok(())
}

fn handle_conn(mut stream: TcpStream, conn_id: u32, shutdown: &Arc<AtomicBool>) -> Result<()> {
    let mut total_bytes_read: u32 = 0;
    let mut closed_by_peer = true;
    stream.set_read_timeout(Some(Duration::new(5, 0)))?;
    loop {
        let mut buf = [0u8; 256];
        match stream.read(&mut buf) {
            Ok(r) => {
                if r == 0 {
                    break;
                }
                total_bytes_read += r as u32;
                eprintln!("conn #{} echoed {} bytes", conn_id, r);
                stream.write_all(&buf[..r])?;
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                    if shutdown.load(Ordering::SeqCst) {
                        closed_by_peer = false;
                        break;
                    }
                    // if shutdown.load isn't true, loop back through and continue
                }
                _ => {
                    return Err(e).context("read error");
                }
            },
        }
    }
    eprintln!(
        "conn #{} closed {} after {} bytes",
        conn_id,
        if closed_by_peer {
            "by peer"
        } else {
            "on shutdown"
        },
        total_bytes_read
    );
    Ok(())
}
