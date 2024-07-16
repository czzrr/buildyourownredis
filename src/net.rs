use std::io::Cursor;

use anyhow::anyhow;
use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::frame::{Frame, ParseError};

pub struct FrameStream {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

impl FrameStream {
    pub fn new(stream: TcpStream) -> Self {
        FrameStream {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(1024),
        }
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        loop {
            let mut cursor = Cursor::new(&self.buf[..]);
            match Frame::parse(&mut cursor) {
                Ok(frame) => {
                    let frame_len = cursor.position();
                    self.buf.advance(frame_len as usize);
                    println!("received {:?}", frame);
                    return Ok(Some(frame));
                }
                Err(ParseError::Incomplete) => (),
                Err(ParseError::Other(err)) => return Err(err),
            }
            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(anyhow!("connection reset py peer"));
            }
        }
    }

    pub async fn write_frame(&mut self, frame: Frame) -> anyhow::Result<()> {
        println!("write {:?}", frame);
        match frame {
            Frame::Bulk(mut bytes) => {
                self.stream.write_all(b"$").await?;
                self.stream
                    .write_all(bytes.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all_buf(&mut bytes).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(frames) => {
                self.stream.write_all(b"*").await?;
                self.stream
                    .write_all(frames.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                for frame in frames {
                    Box::pin(self.write_frame(frame)).await?;
                }
            }
            Frame::Error(mut bytes) => {
                self.stream.write_all(b"-").await?;
                self.stream.write_all_buf(&mut bytes).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"_\r\n").await?;
            }
        }
        self.stream.flush().await?;

        Ok(())
    }

    pub async fn write_bulk(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.write_frame(Frame::Bulk(Bytes::copy_from_slice(bytes)))
            .await
    }

    pub async fn write_array(
        &mut self,
        frames: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> anyhow::Result<()> {
        let frames: Vec<_> = frames
            .into_iter()
            .map(|f| Frame::Bulk(Bytes::copy_from_slice(f.as_ref())))
            .collect();
        self.write_frame(Frame::Array(frames)).await
    }
}
