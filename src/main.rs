use bytes::Bytes;
use redis::{command::Command, frame::Frame, net::FrameStream};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let conn = listener.accept().await.expect("incoming connection");
        let stream = conn.0;
        let client = conn.1;
        println!("accepted new connection: {}", client.ip());
        tokio::spawn(handle_connection(stream));
    }
}

async fn handle_connection(stream: TcpStream) {
    let mut frame_stream = FrameStream::new(stream);
    match frame_stream.read_frame().await.unwrap() {
        Some(frame) => {
            dbg!(&frame);
            match Command::parse(frame).unwrap() {
                Command::Ping => {
                    dbg!("PING");
                    frame_stream
                        .write_frame(Frame::Bulk(Bytes::from_static(b"PONG")))
                        .await
                        .expect("write PING response");
                }
                Command::Echo(bytes) => {
                    dbg!("ECHO");
                    frame_stream
                        .write_frame(Frame::Bulk(bytes))
                        .await
                        .expect("write ECHO response");
                }
            }
        }
        _ => (),
    }
}
