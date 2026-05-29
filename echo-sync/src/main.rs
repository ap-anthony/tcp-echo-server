use anyhow::{Context, Result};
use clap::Parser;
use echo_sync::server::run_server;
use std::{
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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

    run_server(listener, shutdown)?;

    eprintln!("shutdown complete");
    Ok(())
}
