use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::{net::TcpStream, time::Instant};

use crate::{command::Command, frame::Frame, net::FrameStream};

pub struct Server {
    role: Role,
    db: Db,
}

impl Server {
    pub fn new(role: Role) -> Self {
        Server {
            role,
            db: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn handle_connection(&self, stream: TcpStream) {
        let mut frame_stream = FrameStream::new(stream);
        match frame_stream.read_frame().await.expect("read frame") {
            Some(frame) => {
                dbg!(&frame);
                match Command::parse(frame) {
                    Ok(command) => self.handle_command(&mut frame_stream, command).await,
                    Err(err) => frame_stream
                        .write_frame(Frame::Error(Bytes::copy_from_slice(
                            format!("{}", err).as_bytes(),
                        )))
                        .await
                        .expect("write error response"),
                }
            }
            _ => (),
        }
    }

    async fn handle_command(&self, frame_stream: &mut FrameStream, command: Command) {
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
            Command::Info => Frame::Bulk(Bytes::from_static(match &self.role {
                Role::Master => b"role:master",
                Role::Slave { .. } => b"role:slave",
            })),
        };

        frame_stream
            .write_frame(response)
            .await
            .expect("write response");
    }
}

struct DbValue {
    value: Bytes,
    expiry: Option<Instant>,
}

type Db = Arc<Mutex<HashMap<String, DbValue>>>;

pub enum Role {
    Master,
    Slave {
        master_host: String,
        master_port: u16,
    },
}
