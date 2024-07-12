use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}};

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

async fn handle_connection(mut stream: TcpStream) {
    stream.write_all(b"+PONG\r\n").await.expect("write ping response");
}