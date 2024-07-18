use std::{
    collections::HashMap,
    fmt::Write,
    net::Ipv4Addr,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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
                // TODO: factor out to handshake()
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
                frame_stream.write_array(vec!["PSYNC", "?", "-1"]).await?;
                frame_stream
                    .read_frame()
                    .await?
                    .ok_or(anyhow!("end of frame"))?;

                let stream = frame_stream.stream();
                let mut buf = Vec::new();
                stream.read_buf(&mut buf).await.unwrap();
                println!("rdb transfer: {}", buf.escape_ascii());
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
        let response = match command.clone() {
            Command::Ping => Some(Frame::Bulk(Bytes::from_static(b"PONG"))),
            Command::Echo(bytes) => Some(Frame::Bulk(bytes)),
            Command::Get(key) => {
                let mut db = self.db.lock().unwrap();
                let fr = match db.get(&key) {
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
                };

                Some(fr)
            }
            Command::Set { key, value, px } => {
                let expiry =
                    px.map(|millis| Instant::now() + tokio::time::Duration::from_millis(millis));
                let db_value = DbValue { value, expiry };
                self.db.lock().unwrap().insert(key.clone(), db_value);

                Some(Frame::Bulk(Bytes::from_static(b"OK")))
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

                Some(Frame::Bulk(buf.into()))
            }
            Command::Replconf => Some(Frame::Simple("OK".to_owned())),
            _ => None,
        };

        if let Some(response) = response {
            frame_stream
                .write_frame(response)
                .await
                .expect("write response");
            return Ok(());
        }

        match command {
            Command::Psync { .. } => {
                match &self.role {
                    Role::Slave { .. } => {
                        frame_stream
                            .write_frame(Frame::Error(Bytes::from_static(b"ERR not a master")))
                            .await?;
                        return Ok(());
                    }
                    Role::Master { replication_id, .. } => {
                        frame_stream
                            .write_frame(Frame::Simple(format!("FULLRESYNC {replication_id} 0")))
                            .await?;
                    }
                }
                dbg!("write empty rdb");
                let stream = frame_stream.stream();

                static EMPTY_RDB_HEX: &'static str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                let empty_rdb_bin = hex::decode(EMPTY_RDB_HEX).unwrap();
                dbg!(EMPTY_RDB_HEX.len());
                dbg!(empty_rdb_bin.len());
                let mut bytes = BytesMut::new();
                bytes.put(empty_rdb_bin.len().to_string().as_bytes());
                bytes.put(&b"\r\n"[..]);

                stream.write_all(&bytes).await?;
                stream.flush().await?;

                bytes.put(&empty_rdb_bin[..]);

                dbg!(&bytes);

                stream.write_all(&bytes).await?;
                stream.flush().await?;
            }
            _ => unreachable!(),
        }

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
