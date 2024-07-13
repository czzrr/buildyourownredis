use std::{
    fmt::Display,
    io::{Cursor, Read, Seek},
};

use anyhow::{anyhow, Context};
use bytes::{Buf, Bytes};

#[derive(Debug)]
pub enum Frame {
    Bulk(Bytes),
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    Other(anyhow::Error),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => f.write_str("incomplete input"),
            Self::Other(err) => f.write_fmt(format_args!("{}", err)),
        }
    }
}

impl Frame {
    pub fn check(_input: &mut Cursor<&[u8]>) -> bool {
        todo!();
    }

    pub fn parse(input: &mut Cursor<&[u8]>) -> Result<Self, ParseError> {
        if !input.has_remaining() {
            return Err(ParseError::Incomplete);
        }

        match input.get_u8() {
            // Array
            b'*' => {
                let len = Self::parse_u64(input)? as usize;
                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    frames.push(Self::parse(input)?);
                }

                Ok(Frame::Array(frames))
            }
            // Bulk string
            b'$' => {
                let len = Self::parse_u64(input)? as usize;
                if input.remaining() < len {
                    return Err(ParseError::Incomplete);
                }

                let mut buf = vec![0u8; len];
                input
                    .read_exact(&mut buf)
                    .map_err(|err| ParseError::Other(anyhow!(err)))?;
                if buf.as_slice().windows(2).find(|w| w == b"\r\n").is_some() {
                    return Err(ParseError::Other(anyhow!(
                        "CRLF not allowed in bulk string"
                    )));
                }
                Self::parse_crlf(input)?;

                Ok(Frame::Bulk(Bytes::from(buf)))
            }
            b => Err(ParseError::Other(anyhow!("unknown data type token: {}", b))),
        }
    }

    fn parse_u64(input: &mut Cursor<&[u8]>) -> Result<u64, ParseError> {
        if input.remaining() < 3 {
            return Err(ParseError::Incomplete);
        }
        let (len, used) = <u64 as atoi::FromRadix10Checked>::from_radix_10_checked(
            &input.get_ref()[input.position() as usize..],
        );
        if len.is_none() {
            return Err(ParseError::Other(anyhow!(
                "length is too large to fit into u64"
            )));
        }
        if used == 0 {
            return Err(ParseError::Other(anyhow!("expected length, got nothing")));
        }
        input.advance(used);
        Self::parse_crlf(input)?;

        Ok(len.unwrap())
    }

    fn get_line<'a>(input: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ParseError> {
        let crlf_pos = Self::crlf_pos(input).ok_or(ParseError::Incomplete)?;
        let line = &input.get_ref()[input.position() as usize..crlf_pos as usize];
        input.set_position(crlf_pos + 2);

        Ok(line)
    }

    fn parse_crlf(input: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
        if input.remaining() < 2 {
            return Err(ParseError::Incomplete);
        }

        let pos = input.position() as usize;
        if &input.get_ref()[pos..pos + 2] == b"\r\n" {
            input.advance(2);
            Ok(())
        } else {
            Err(ParseError::Other(anyhow!(
                "expected CRLF, got {}",
                input.get_ref().escape_ascii()
            )))
        }
    }

    fn crlf_pos(input: &Cursor<&[u8]>) -> Option<u64> {
        input
            .get_ref()
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| p as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u64_success() {
        assert_eq!(
            1234,
            Frame::parse_u64(&mut Cursor::new(b"1234\r\n")).unwrap()
        );
    }

    #[test]
    fn test_parse_u64_error() {
        let n = Frame::parse_u64(&mut Cursor::new(b"12three4\r\n"));
        assert!(matches![n, Err(ParseError::Other(_))]);

        let n = Frame::parse_u64(&mut Cursor::new(b"99999999999999999999\r\n"));
        assert!(matches![n, Err(ParseError::Other(_))]);

        let n = Frame::parse_u64(&mut Cursor::new(b"1234\r"));
        assert!(matches![n, Err(ParseError::Incomplete)]);
    }

    #[test]
    fn test_parse_bulk_string_success() {
        assert!(
            matches![Frame::parse(&mut Cursor::new(b"$5\r\nhello\r\n")).unwrap(), Frame::Bulk(bytes) if &bytes[..] == b"hello"]
        )
    }

    #[test]
    fn test_parse_bulk_string_error() {
        let s = Frame::parse(&mut Cursor::new(b"$5\r\nhey\r\n"));
        assert!(matches![s, Err(ParseError::Other(_))]);

        let s = Frame::parse(&mut Cursor::new(b"$3\r\nhey"));
        assert!(matches![s, Err(ParseError::Incomplete)]);

        let s = Frame::parse(&mut Cursor::new(b"$5\r\nhell"));
        assert!(matches![s, Err(ParseError::Incomplete)]);

        let s = Frame::parse(&mut Cursor::new(b"$5"));
        assert!(matches![s, Err(ParseError::Incomplete)]);
    }

    #[test]
    fn test_parse_array_success() {
        let array = Frame::parse(&mut Cursor::new(b"*1\r\n$4\r\nPING\r\n")).unwrap();

        assert!(matches![array, Frame::Array(_)]);
        if let Frame::Array(frames) = array {
            assert_eq!(1, frames.len());
            let bulk = &frames[0];
            assert!(matches![bulk, Frame::Bulk(bytes) if bytes == "PING"]);
        }
    }

    #[test]
    fn test_parse_array_error() {
        let a = Frame::parse(&mut Cursor::new(b"*1"));
        assert!(matches![a, Err(ParseError::Incomplete)]);

        let a = Frame::parse(&mut Cursor::new(b"*1\r\n$4\r\nPI"));
        assert!(matches![a, Err(ParseError::Incomplete)]);
    }
}
