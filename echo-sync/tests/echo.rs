use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::
    io::{Read, Write}
;
use anyhow::Result;

use echo_sync::server::run_server;

fn spawn_test_server() -> Result<(SocketAddr, Arc<AtomicBool>, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    let t = thread::spawn(move || {
        run_server(listener, shutdown_clone).unwrap();
    });
    Ok((addr, shutdown, t))
}

fn echo_once(addr: SocketAddr, msg: &[u8]) {
    let mut sock = TcpStream::connect(addr).unwrap();
    echo(&mut sock, msg);
}

#[track_caller]
fn echo(sock: &mut TcpStream, msg: &[u8]) {
    let mut buf = vec![0u8; msg.len()];
    sock.write_all(msg).unwrap();
    sock.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, msg);
}

#[test]
fn echoes_a_line() {
    // bind on port 0, spawn the server task, get the actual addr back
    let (addr, shutdown, t) = spawn_test_server().unwrap();
    // simple sync client is fine inside an async test
    echo_once(addr, b"hello\n");
    shutdown.store(true, Ordering::SeqCst);
    t.join().unwrap();
}

#[test]
fn echoes_a_line_two_conns() {
    let (addr, shutdown, t) = spawn_test_server().unwrap();

    let mut sock1 = TcpStream::connect(addr).unwrap();
    let mut sock2 = TcpStream::connect(addr).unwrap();

    echo(&mut sock1, b"hello sock1\n");
    echo(&mut sock2, b"hello sock2\n");
    echo(&mut sock1, b"test\n");
    echo(&mut sock2, b"test 2\n");

    shutdown.store(true, Ordering::SeqCst);

    t.join().unwrap();
}

#[test]
fn sequential_conns() {
    let (addr, shutdown, t) = spawn_test_server().unwrap();

    echo_once(addr, b"hello\n");
    echo_once(addr, b"test\n");

    shutdown.store(true, Ordering::SeqCst);
    t.join().unwrap();
}

#[test]
fn graceful_server_drain() {
    let (addr, shutdown, t) = spawn_test_server().unwrap();

    let mut sock1 = TcpStream::connect(addr).unwrap();
    let mut sock2 = TcpStream::connect(addr).unwrap();
    let mut sock3 = TcpStream::connect(addr).unwrap();

    echo(&mut sock1, b"test1\n");
    echo(&mut sock2, b"test2\n");
    echo(&mut sock3, b"test3\n");

    shutdown.store(true, Ordering::SeqCst);

    let mut buf = [0u8; 1];
    assert_eq!(sock1.read(&mut buf).unwrap(), 0, "sock1 not closed");
    assert_eq!(sock2.read(&mut buf).unwrap(), 0, "sock2 not closed");
    assert_eq!(sock3.read(&mut buf).unwrap(), 0, "sock3 not closed");

    t.join().unwrap();
}