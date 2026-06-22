use crate::{ProtocolError, tickers::REGISTRY};
use std::{fmt, net::SocketAddr, str::FromStr};

/// The parsed form of a `STREAM <ip:port> <TICKER1,TICKER2,...>\n` line.
#[derive(Debug)]
pub struct StreamCommand {
    pub udp_addr: SocketAddr,
    pub tickers: Vec<String>,
}

/// Server → client response.
pub enum Response {
    Ok,
    Err(String),
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
            Response::Ok => f.write_str("OK"),
            Response::Err(reason) => write!(f, "ERR {reason}"),
        }
    }
}

impl StreamCommand {
    /// Parse a raw line (without trailing `\n`) into a StreamCommand.
    ///
    /// Expected format: `STREAM <ip:port> <TICKER1,TICKER2,...>`
    ///
    /// Error mapping:
    ///   wrong/missing verb      → ProtocolError::InvalidCommand
    ///   unparseable ip:port     → ProtocolError::InvalidAddr
    ///   missing or empty tickers → ProtocolError::EmptyTickerList
    ///   ticker not in registry  → ProtocolError::UnknownTicker
    pub fn parse(line: &str) -> Result<Self, ProtocolError> {
        let mut tokens = line.split_whitespace();

        if !tokens.next().is_some_and(|t| t == "STREAM") {
            return Err(ProtocolError::InvalidCommand);
        }

        let udp_addr = SocketAddr::from_str(tokens.next().unwrap_or_default())
            .map_err(|_| ProtocolError::InvalidAddr)?;

        let mut tickers = Vec::new();

        for ticker in tokens.next().unwrap_or_default().split(',') {
            if REGISTRY.validate(&ticker) {
                tickers.push(ticker.to_string());
            } else {
                return Err(ProtocolError::UnknownTicker(ticker.to_string()));
            }
        }

        if tickers.is_empty() {
            return Err(ProtocolError::EmptyTickerList);
        }

        Ok(Self { udp_addr, tickers })
    }
}
