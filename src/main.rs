use anyhow::Context;
use clap::Parser;
use redis::server::{Role, Server};
use std::net::Ipv4Addr;

#[derive(Parser, Debug)]
#[command()]
struct Args {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long = "replicaof", value_name = "HOST PORT")]
    replica_of: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let port = args.port.unwrap_or(6379);
    let role = match args.replica_of {
        Some(s) => {
            let (master_host, master_port) = s.split_once(' ').context("invalid value for --replicaof")?;
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
        _ => Role::Master {
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
            replication_offset: 0,
        },
    };
    dbg!(port);
    dbg!(&role);

    let server = Server::new(role, port);
    server.start().await?;

    Ok(())
}
