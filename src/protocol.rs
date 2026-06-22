use crate::{ProtocolError, tickers::REGISTRY};
use std::{
    collections::HashSet,
    fmt::{self, Display},
    net::SocketAddr,
    str::FromStr,
};

/// The parsed form of a `STREAM <ip:port> <TICKER1,TICKER2,...>\n` line.
///
/// All tickers are validated and deduped
#[derive(Debug)]
pub struct StreamCommand {
    pub udp_addr: SocketAddr,
    pub tickers: HashSet<String>,
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
    pub fn parse(line: &str) -> Result<Self, ProtocolError> {
        let mut tokens = line.split_whitespace();

        if !tokens.next().is_some_and(|t| t == "STREAM") {
            return Err(ProtocolError::InvalidCommand);
        }

        let udp_addr = SocketAddr::from_str(tokens.next().unwrap_or_default())
            .map_err(|_| ProtocolError::InvalidAddr)?;

        let mut tickers = HashSet::new();

        for ticker in tokens.next().unwrap_or_default().split(',') {
            if REGISTRY.validate(&ticker) {
                tickers.insert(ticker.to_string());
            } else {
                return Err(ProtocolError::UnknownTicker(ticker.to_string()));
            }
        }

        if tickers.is_empty() {
            return Err(ProtocolError::EmptyTickerList);
        }

        Ok(Self { udp_addr, tickers })
    }

    pub fn construct(
        tickers: Vec<String>,
        udp_port: u16,
    ) -> Result<Self, ProtocolError> {
        let ticker_list = tickers.join(",");
        let local_udp = format!("127.0.0.1:{}", udp_port);

        format!("STREAM {local_udp} {ticker_list}\n");

        Ok(Self {
            udp_addr: (),
            tickers: (),
        })
    }
}

impl Display for StreamCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
