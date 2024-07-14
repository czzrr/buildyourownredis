use redis::server::{Role, Server};
use std::{env, sync::Arc};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let args: Vec<_> = env::args().collect();
    let mut port: u16 = 6379;
    if let Some(port_pos) = args.iter().position(|arg| arg == "--port") {
        port = args
            .get(port_pos + 1)
            .expect("expected: --port <port>")
            .parse()
            .expect("expected valid port number");
    }

    let server = Arc::new(Server::new(Role::Master));

    let addr = format!("127.0.0.1:{port}");
    println!("listening on {addr}");
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.expect("incoming connection");
        let server = server.clone();
        tokio::spawn(async move { server.handle_connection(stream).await });
    }
}
