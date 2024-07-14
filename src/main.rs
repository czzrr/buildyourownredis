use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use redis::{command::Command, frame::Frame, net::FrameStream};
use tokio::{net::{TcpListener, TcpStream}, time::Instant};

struct DbValue {
    value: Bytes,
    expiry: Option<Instant>
}

type Db = Arc<Mutex<HashMap<String, DbValue>>>;

#[tokio::main]
async fn main() {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.expect("incoming connection");
        let db = db.clone();
        tokio::spawn(async move { handle_connection(stream, db).await });
    }
}

async fn handle_connection(stream: TcpStream, db: Db) {
    let mut frame_stream = FrameStream::new(stream);
    match frame_stream.read_frame().await.expect("read frame") {
        Some(frame) => {
            dbg!(&frame);
            match Command::parse(frame) {
                Ok(command) => handle_command(&mut frame_stream, command, db).await,
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

async fn handle_command(frame_stream: &mut FrameStream, command: Command, db: Db) {
    let response = match command {
        Command::Ping => Frame::Bulk(Bytes::from_static(b"PONG")),
        Command::Echo(bytes) => Frame::Bulk(bytes),
        Command::Get(key) => {
            let mut db = db.lock().unwrap();
            match db.get(&key) {
                
                Some(DbValue { value, expiry}) => {
                    let frame = Frame::Bulk(value.clone());

                    if let Some(expiry ) = expiry {
                        if expiry < &tokio::time::Instant::now() {
                            db.remove(&key);
                        }
                    }
                    
                    frame
                }
                _ => Frame::Null,
            }
        }
        Command::Set { key, value, px } => {
            let expiry = px.map(|millis| Instant::now() + tokio::time::Duration::from_millis(millis));
            let db_value = DbValue {
                value, expiry
            };
            db.lock().unwrap().insert(key.clone(), db_value);
            
            Frame::Bulk(Bytes::from_static(b"OK"))
        }
    };

    frame_stream
        .write_frame(response)
        .await
        .expect("write response");
}
