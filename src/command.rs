use anyhow::anyhow;
use bytes::Bytes;

use crate::frame::Frame;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(Bytes),
    Get(String),
    Set { key: String, value: Bytes },
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
                    b"PING" => {
                        if elements.len() != 1 {
                            return Err(anyhow!("expected: PING (no arguments)"));
                        }

                        Ok(Command::Ping)
                    }
                    b"ECHO" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("expected: ECHO <message>"));
                        }

                        Ok(Command::Echo(elements[1].clone()))
                    }
                    b"GET" => {
                        if elements.len() != 2 {
                            return Err(anyhow!("expected: GET <key>"));
                        }
                        let key = String::from_utf8(elements[1].clone().to_vec())?;

                        Ok(Command::Get(key))
                    }
                    b"SET" => {
                        if elements.len() != 3 {
                            return Err(anyhow!("expected: SET <key> <value>"));
                        }
                        let key = String::from_utf8(elements[1].to_vec())?;
                        let value = elements[2].clone();

                        Ok(Command::Set { key, value })
                    }
                    _ => Err(anyhow!("unknown command: {}", elements[0].escape_ascii())),
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
