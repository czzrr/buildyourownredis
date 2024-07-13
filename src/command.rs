use anyhow::anyhow;
use bytes::Bytes;

use crate::frame::Frame;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(Bytes),
}

impl Command {
    pub fn parse(frame: Frame) -> anyhow::Result<Self> {
        match frame {
            Frame::Array(frames) => {
                if frames.is_empty() {
                    return Err(anyhow!("empty array"));
                }

                let mut elements = Vec::new();
                for frame in frames {
                    match frame {
                        Frame::Bulk(bytes) => elements.push(bytes),
                        _ => return Err(anyhow!("only bulk strings are supported")),
                    }
                }

                match &elements[0][..] {
                    b"PING" => Ok(Command::Ping),
                    b"ECHO" => {
                        if elements.len() < 2 {
                            return Err(anyhow!("expected: ECHO <message>"));
                        }
                        Ok(Command::Echo(elements.remove(1)))
                    }
                    _ => Err(anyhow!("unknown command: {:?}", elements[0])),
                }
            }
            _ => Err(anyhow!("invalid frame for command: {:?}", frame)),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn parse_ping() {
        let ping_frame = Frame::Array(vec![Frame::Bulk(Bytes::from("PING"))]);

        let command = Command::parse(ping_frame).unwrap();

        assert!(matches![command, Command::Ping]);
    }

    #[test]
    fn parse_echo() {
        let ping_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("ECHO")),
            Frame::Bulk(Bytes::from("hey")),
        ]);

        let command = Command::parse(ping_frame).unwrap();

        assert!(matches![command, Command::Echo(bytes) if &bytes[..] == b"hey"]);
    }
}
