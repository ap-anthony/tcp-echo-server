// tests/echo.rs
use std::net::{SocketAddr};

use echo_async::server::start_server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;

async fn spawn_test_server() -> (SocketAddr, Sender<()>, JoinHandle<()>) {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let server_shutdown = shutdown_tx.clone();
    let t = tokio::spawn(async move {
        start_server(socket, server_shutdown, false).await.unwrap();
    });
    (addr, shutdown_tx, t)
}

async fn echo_once(addr: SocketAddr, msg: &[u8]) {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    echo(&mut sock, msg).await;
}

async fn echo(sock: &mut TcpStream, msg: &[u8]) {
    let mut buf = vec![0u8; msg.len()];
    sock.write_all(msg).await.unwrap();
    sock.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, msg);
}

#[tokio::test]
async fn echoes_a_line() {
    let (addr, shutdown, handle) = spawn_test_server().await;
    echo_once(addr, b"hello\n").await;
    shutdown.send(()).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn echoes_a_line_two_conns() {
    let (addr, shutdown, t) = spawn_test_server().await;

    let mut sock1 = TcpStream::connect(addr).await.unwrap();
    let mut sock2 = TcpStream::connect(addr).await.unwrap();

    echo(&mut sock1, b"hello sock1\n").await;
    echo(&mut sock2, b"hello sock2\n").await;
    echo(&mut sock1, b"test\n").await;
    echo(&mut sock2, b"test 2\n").await;

    shutdown.send(()).unwrap();
    t.await.unwrap();
}

#[tokio::test]
async fn sequential_conns() {
    let (addr, shutdown, t) = spawn_test_server().await;

    echo_once(addr, b"hello\n").await;
    echo_once(addr, b"test\n").await;

    shutdown.send(()).unwrap();
    t.await.unwrap();
}


#[tokio::test]
async fn graceful_server_drain() {
    let (addr, shutdown, t) = spawn_test_server().await;

    let mut sock1 = TcpStream::connect(addr).await.unwrap();
    let mut sock2 = TcpStream::connect(addr).await.unwrap();
    let mut sock3 = TcpStream::connect(addr).await.unwrap();

    echo(&mut sock1, b"test1\n").await;
    echo(&mut sock2, b"test2\n").await;
    echo(&mut sock3, b"test3\n").await;

    shutdown.send(()).unwrap();

    let mut buf = [0u8; 1];
    assert_eq!(sock1.read(&mut buf).await.unwrap(), 0, "sock1 not closed");
    assert_eq!(sock2.read(&mut buf).await.unwrap(), 0, "sock2 not closed");
    assert_eq!(sock3.read(&mut buf).await.unwrap(), 0, "sock3 not closed");

    t.await.unwrap();
}
