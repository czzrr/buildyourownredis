use bytes::Bytes;
use redis::{command::Command, frame::Frame, net::FrameStream};
use tokio::net::{TcpListener, TcpStream};

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
    match frame_stream
        .read_frame()
        .await
        .expect("error when reading frame")
    {
        Some(frame) => {
            dbg!(&frame);
            match Command::parse(frame) {
                Ok(command) => handle_command(&mut frame_stream, command).await,
                Err(_) => frame_stream
                    .write_frame(Frame::Error(Bytes::from_static(b"ERR unknown command")))
                    .await
                    .expect("write error response"),
            }
        }
        _ => (),
    }
}

async fn handle_command(frame_stream: &mut FrameStream, command: Command) {
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
    }
}
