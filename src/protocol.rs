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

        let ticker_token = tokens.next().ok_or(ProtocolError::EmptyTickerList)?;

        let mut tickers = HashSet::new();

        for ticker in ticker_token.split(',') {
            if REGISTRY.validate(ticker) {
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

    pub fn construct(tickers: Vec<String>, udp_addr: SocketAddr) -> Self {
        Self {
            udp_addr,
            tickers: tickers.into_iter().collect(),
        }
    }
}

impl Display for StreamCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "STREAM {} ", self.udp_addr)?;
        let mut first = true;
        for value in &self.tickers {
            if !first {
                write!(f, ",")?;
            }
            first = false;
            write!(f, "{value}")?;
        }
        writeln!(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_stream_command() {
        let cmd = StreamCommand::parse("STREAM 127.0.0.1:9000 AAPL,MSFT").unwrap();
        assert_eq!(cmd.udp_addr, "127.0.0.1:9000".parse().unwrap());
        assert!(cmd.tickers.contains("AAPL"));
        assert!(cmd.tickers.contains("MSFT"));
    }

    #[test]
    fn single_ticker_is_accepted() {
        let cmd = StreamCommand::parse("STREAM 0.0.0.0:4000 NVDA").unwrap();
        assert_eq!(cmd.tickers.len(), 1);
        assert!(cmd.tickers.contains("NVDA"));
    }

    #[test]
    fn duplicate_tickers_are_deduped() {
        let cmd = StreamCommand::parse("STREAM 127.0.0.1:9000 AAPL,AAPL").unwrap();
        assert_eq!(cmd.tickers.len(), 1);
    }

    #[test]
    fn wrong_command_keyword_returns_invalid_command() {
        let err = StreamCommand::parse("SUBSCRIBE 127.0.0.1:9000 AAPL").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommand));
    }

    #[test]
    fn missing_command_keyword_returns_invalid_command() {
        let err = StreamCommand::parse("").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommand));
    }

    #[test]
    fn bad_address_returns_invalid_addr() {
        let err = StreamCommand::parse("STREAM not-an-addr AAPL").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidAddr));
    }

    #[test]
    fn missing_address_returns_invalid_addr() {
        let err = StreamCommand::parse("STREAM").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidAddr));
    }

    #[test]
    fn empty_ticker_list_returns_empty_ticker_list_error() {
        // valid address, but no tickers token at all
        let err = StreamCommand::parse("STREAM 127.0.0.1:9000").unwrap_err();
        assert!(matches!(err, ProtocolError::EmptyTickerList));
    }

    #[test]
    fn unknown_ticker_returns_error() {
        let err =
            StreamCommand::parse("STREAM 127.0.0.1:9000 AAPL,FAKEXYZ").unwrap_err();
        assert!(matches!(err, ProtocolError::UnknownTicker(_)));
    }
}
