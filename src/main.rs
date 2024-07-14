use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use redis::{command::Command, frame::Frame, net::FrameStream};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let database: Arc<Mutex<HashMap<String, Bytes>>> = Arc::new(Mutex::new(HashMap::new()));
    {
        database.lock().unwrap().insert("hi".to_string(), Bytes::from_static(b"hello"));
    }
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let conn = listener.accept().await.expect("incoming connection");
        let stream = conn.0;
        let client = conn.1;
        println!("accepted new connection: {}", client.ip());
        let database = database.clone();
        tokio::spawn(async move { handle_connection(stream, database).await });
    }
}

async fn handle_connection(stream: TcpStream, database: Arc<Mutex<HashMap<String, Bytes>>>) {
    let mut frame_stream = FrameStream::new(stream);
    match frame_stream
        .read_frame()
        .await
        .expect("error when reading frame")
    {
        Some(frame) => {
            dbg!(&frame);
            match Command::parse(frame) {
                Ok(command) => handle_command(&mut frame_stream, command, database).await,
                Err(_) => frame_stream
                    .write_frame(Frame::Error(Bytes::from_static(b"ERR unknown command")))
                    .await
                    .expect("write error response"),
            }
        }
        _ => (),
    }
}

async fn handle_command(
    frame_stream: &mut FrameStream,
    command: Command,
    database: Arc<Mutex<HashMap<String, Bytes>>>,
) {
    match command {
        Command::Ping => {
            frame_stream
                .write_frame(Frame::Bulk(Bytes::from_static(b"PONG")))
                .await
                .expect("write PING response");
        }
        Command::Echo(bytes) => {
            frame_stream
                .write_frame(Frame::Bulk(bytes))
                .await
                .expect("write ECHO response");
        }
        Command::Get(key) => {
            let value = {
                let database = database.lock().unwrap();
                database.get(&key).map(|v| v.clone())
            };
            match value {
                Some(value) => frame_stream
                    .write_frame(Frame::Bulk(value.clone()))
                    .await
                    .expect("write GET response"),
                _ => frame_stream
                    .write_frame(Frame::Error(Bytes::from(format!("invalid key: {}", key))))
                    .await
                    .expect("write GET response"),
            }
        }
        Command::Set { key, value } => {
            database.lock().unwrap().insert(key, value);
            frame_stream.write_frame(Frame::Bulk(Bytes::from_static(b"OK"))).await.expect("write SET response");
        }
    }
}
