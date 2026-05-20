use std::{io::{BufRead, BufReader, Read}, net::{TcpListener, TcpStream}};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "echo-sync")]
#[command(about = "Synchronous tcp echo server")]
struct Args {
    #[arg(long, default_value = "localhost")]
    addr: String,

    #[arg(long, default_value = "8008")]
    port: u16,
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    println!("Listening on 127.0.0.1:7878...");

    let listener = TcpListener::bind("127.0.0.1:7878")?;
    let mut total_conns: u32 = 0;
    for _stream in listener.incoming() {
        if let Ok(stream) = _stream {
            total_conns += 1;
            println!("accepted conn #{} from {}", total_conns, stream.local_addr()?);
            handle_conn(stream, total_conns);
        }
    }

    Ok(())
}

fn handle_conn(mut stream: TcpStream, conn_id: u32) {
    let mut total_bytes_read: u32 = 0;
    loop {
        let mut buf = [0u8; 256];
        if let Ok(result) = stream.read(&mut buf) {
            let s = String::from_utf8_lossy(&buf[..result]);
            total_bytes_read += result as u32;
            println!("conn #{} echoed {} bytes", conn_id.to_string(), total_bytes_read.to_string());
            if result < 256 {
                break;
            }
        }
    }
    println!("conn #{} closed by peer after {} bytes", conn_id.to_string(), total_bytes_read.to_string());
}
