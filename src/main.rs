use clap::Parser;
use redis::server::{Role, Server};
use std::{net::Ipv4Addr, sync::Arc};
use tokio::net::TcpListener;

#[derive(Parser, Debug)]
#[command()]
struct Args {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long = "replicaof", value_name = "HOST PORT")]
    replica_of: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let port = args.port.unwrap_or(6379);
    let role = match args.replica_of {
        Some(s) => {
            let (master_host, master_port) = s.split_once(' ').unwrap();
            let master_host: Ipv4Addr = if master_host == "localhost" {
                "127.0.0.1".parse().unwrap()
            } else {
                master_host
                    .parse()
                    .expect("master host should be a valid ipv4 address")
            };
            let master_port: u16 = master_port.parse().unwrap();

            Role::Slave {
                master_host,
                master_port,
            }
        }
        _ => Role::Master,
    };
    dbg!(port);
    dbg!(&role);

    let server = Arc::new(Server::new(role));

    let addr = format!("127.0.0.1:{port}");
    println!("listening on {addr}");
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.expect("incoming connection");
        let server = server.clone();
        tokio::spawn(async move { server.handle_connection(stream).await });
    }
}
