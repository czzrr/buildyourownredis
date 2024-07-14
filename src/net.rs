use std::io::Cursor;

use anyhow::anyhow;
use bytes::{Buf, BytesMut};
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
            dbg!(&self.buf);
            let mut cursor = Cursor::new(&self.buf[..]);
            match Frame::parse(&mut cursor) {
                Ok(frame) => {
                    let frame_len = cursor.position();
                    self.buf.advance(frame_len as usize);
                    return Ok(Some(frame));
                }
                Err(ParseError::Incomplete) => (),
                Err(ParseError::Other(err)) => return Err(err),
            }
            let n = self.stream.read_buf(&mut self.buf).await?;
            dbg!(n);
            if n == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(anyhow!("connection reset py peer"));
            }
        }
    }

    pub async fn write_frame(&mut self, frame: Frame) -> anyhow::Result<()> {
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
                for frame in frames {
                    Box::pin(self.write_frame(frame)).await?;
                }
            }
            Frame::Error(mut bytes) => {
                self.stream.write_all(b"-").await?;
                self.stream.write_all_buf(&mut bytes).await?;
                self.stream.write_all(b"\r\n").await?;
            },
            Frame::Null => {
                self.stream.write_all(b"_\r\n").await?;
            }
        }
        self.stream.flush().await?;

        Ok(())
    }
}
