use anyhow::Result;
use clap::Parser;
use echo_async::server::start_server;
use tokio::net::TcpListener;

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

    #[arg(long)]
    max_conns: Option<u32>
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut max_conns = 0; // 0 => unbounded max-conns
    match args.max_conns {
        Some(val) => {
            max_conns = val;
        },
        None => {}
    }

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

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    let signal_tx = shutdown_tx.clone();
    let signal_task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::SignalKind;

            let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
                .expect("install SIGTERM handler");
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        let _ = signal_tx.send(());
    });
    start_server(listener, shutdown_tx, args.stats, max_conns).await?;

    signal_task.abort();
    let _ = signal_task.await;

    Ok(())
}
