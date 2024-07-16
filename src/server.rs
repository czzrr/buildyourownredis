use std::{
    collections::HashMap,
    fmt::Write,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context};
use bytes::{Bytes, BytesMut};
use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

use crate::{command::Command, frame::Frame, net::FrameStream};

pub struct Server {
    role: Role,
    db: Db,
    port: u16,
}

impl Server {
    pub fn new(role: Role, port: u16) -> Self {
        Server {
            role,
            db: Arc::new(Mutex::new(HashMap::new())),
            port,
        }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        match self.role {
            Role::Slave {
                master_host,
                master_port,
            } => {
                let conn = TcpStream::connect(format!("{}:{}", master_host, master_port))
                    .await
                    .context("failed to connect to master")?;
                let mut frame_stream = FrameStream::new(conn);

                frame_stream.write_array(vec!["PING"]).await?;
                frame_stream
                    .read_frame()
                    .await?
                    .ok_or(anyhow!("end of frame"))?;

                frame_stream
                    .write_array(vec!["REPLCONF", "listening-port", &self.port.to_string()])
                    .await?;
                frame_stream
                    .read_frame()
                    .await?
                    .ok_or(anyhow!("end of frame"))?;

                frame_stream
                    .write_array(vec!["REPLCONF", "capa", "psync2"])
                    .await?;
                frame_stream
                    .read_frame()
                    .await?
                    .ok_or(anyhow!("end of frame"))?;
            }
            Role::Master { .. } => (),
        }

        let addr = format!("127.0.0.1:{}", self.port);
        println!("listening on {addr}");
        let listener = TcpListener::bind(&addr)
            .await
            .with_context(|| anyhow!("failed to bind to {}", addr))?;
        let server = Arc::new(self);
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .context("failed to accept connection")?;
            let server = server.clone();
            tokio::spawn(async move { server.handle_connection(stream).await });
        }
    }

    pub async fn handle_connection(&self, stream: TcpStream) -> anyhow::Result<()> {
        let mut frame_stream = FrameStream::new(stream);
        loop {
            match frame_stream.read_frame().await? {
                Some(frame) => match Command::parse(frame) {
                    Ok(command) => self.handle_command(&mut frame_stream, command).await?,
                    Err(err) => {
                        frame_stream
                            .write_frame(Frame::Error(Bytes::copy_from_slice(
                                format!("{}", err).as_bytes(),
                            )))
                            .await?
                    }
                },
                _ => break,
            }
        }

        Ok(())
    }

    async fn handle_command(
        &self,
        frame_stream: &mut FrameStream,
        command: Command,
    ) -> anyhow::Result<()> {
        let response = match command {
            Command::Ping => Frame::Bulk(Bytes::from_static(b"PONG")),
            Command::Echo(bytes) => Frame::Bulk(bytes),
            Command::Get(key) => {
                let mut db = self.db.lock().unwrap();
                match db.get(&key) {
                    Some(DbValue { value, expiry }) => {
                        if let Some(expiry) = expiry {
                            if expiry < &tokio::time::Instant::now() {
                                println!("removing entry with key: {key}");
                                db.remove(&key);

                                Frame::Null
                            } else {
                                Frame::Bulk(value.clone())
                            }
                        } else {
                            Frame::Bulk(value.clone())
                        }
                    }
                    _ => Frame::Null,
                }
            }
            Command::Set { key, value, px } => {
                let expiry =
                    px.map(|millis| Instant::now() + tokio::time::Duration::from_millis(millis));
                let db_value = DbValue { value, expiry };
                self.db.lock().unwrap().insert(key.clone(), db_value);

                Frame::Bulk(Bytes::from_static(b"OK"))
            }
            Command::Info => {
                let mut buf = BytesMut::new();

                match &self.role {
                    Role::Master {
                        replication_id,
                        replication_offset,
                    } => {
                        buf.write_str("role:master\n").unwrap();
                        write!(buf, "master_replid:{}\n", replication_id).unwrap();
                        write!(buf, "master_repl_offset:{}\n", replication_offset).unwrap();
                    }
                    Role::Slave { .. } => buf.write_str("role:slave").unwrap(),
                };

                Frame::Bulk(buf.into())
            }
            Command::Replconf => Frame::Bulk(Bytes::from_static(b"OK")),
        };

        frame_stream
            .write_frame(response)
            .await
            .expect("write response");

        Ok(())
    }
}

struct DbValue {
    value: Bytes,
    expiry: Option<Instant>,
}

type Db = Arc<Mutex<HashMap<String, DbValue>>>;

#[derive(Debug)]
pub enum Role {
    Master {
        replication_id: String,
        replication_offset: u64,
    },
    Slave {
        master_host: Ipv4Addr,
        master_port: u16,
    },
}
