use crate::{ProtocolError, tickers::REGISTRY};
use std::{fmt, net::SocketAddr};

/// The parsed form of a `STREAM <ticker> <ip> <port>\n` line.
#[derive(Debug)]
pub struct StreamCommand {
    pub ticker: String,
    pub udp_addr: SocketAddr,
}

/// Server → client response.
pub enum Response {
    Ok,
    Err(String),
}

pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl<T, E> IntoResponse for Result<T, E>
where
    T: IntoResponse,
    E: IntoResponse,
{
    fn into_response(self) -> Response {
        match self {
            Result::Ok(t) => t.into_response(),
            Result::Err(e) => e.into_response(),
        }
    }
}
impl Response {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Response::Ok => b"OK\n".to_vec(),
            Response::Err(reason) => format!("ERR {reason}\n").into_bytes(),
        }
    }
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::Ok => f.write_str("OK\n"),
            Response::Err(reason) => write!(f, "ERR {reason}\n"),
        }
    }
}

impl StreamCommand {
    /// Parse a raw line (without the trailing `\n`) into a StreamCommand.
    ///
    /// Expected format: `STREAM <TICKER> <IP> <PORT>`
    ///
    /// Returns `ProtocolError` on any malformed input.
    pub fn parse(line: &str) -> Result<Self, ProtocolError> {
        let mut tokens = line.split_whitespace();

        match tokens.next() {
            Some("STREAM") => {}
            _ => return Err(ProtocolError::UnknownCommand(line.to_string())),
        }

        let ticker = tokens
            .next()
            .ok_or(ProtocolError::InvalidArguments)?
            .to_string();

        if !REGISTRY.validate(&ticker) {
            return Err(ProtocolError::UnknownCommand(ticker.to_string()));
        };

        let ip = tokens.next().ok_or(ProtocolError::InvalidArguments)?;

        let port = tokens.next().ok_or(ProtocolError::InvalidArguments)?;

        if tokens.next().is_some() {
            return Err(ProtocolError::InvalidArguments); // too many tokens
        }

        Ok(StreamCommand {
            ticker,
            udp_addr: format!("{}:{}", ip, port).parse()?,
        })
    }
}
