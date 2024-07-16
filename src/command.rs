use anyhow::anyhow;
use bytes::Bytes;

use crate::frame::Frame;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(Bytes),
    Get(String),
    Set {
        key: String,
        value: Bytes,
        px: Option<u64>,
    },
    Info,
    Replconf,
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
                        if elements.len() < 3 {
                            return Err(anyhow!("expected: SET <key> <value> [PX milliseconds] "));
                        }
                        let key = String::from_utf8(elements[1].to_vec())?;
                        let value = elements[2].clone();

                        let mut px = None;
                        if let Some(px_pos) = elements.iter().position(|e| &e[..] == b"px") {
                            if let Some(millis) = elements.get(px_pos + 1) {
                                if let Some(millis) = atoi::atoi::<u64>(&millis) {
                                    px = Some(millis);
                                }
                            }
                        }

                        Ok(Command::Set { key, value, px })
                    }
                    b"INFO" => {
                        if elements.len() > 2 {
                            return Err(anyhow!("expected: INFO [replication] "));
                        }

                        Ok(Command::Info)
                    }
                    b"REPLCONF" => Ok(Command::Replconf),
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
