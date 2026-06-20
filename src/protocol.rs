use crate::{ProtocolError, tickers::TickerRegistry};
use std::net::SocketAddr;

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
    pub fn to_wire(&self) -> String {
        match self {
            Response::Ok => "OK\n".to_string(),
            Response::Err(reason) => format!("ERR {reason}\n"),
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
        // TODO: implement this function
        // Steps:
        //   1. Split `line` by whitespace — expect exactly 4 tokens
        //   2. Check the first token is "STREAM" (else UnknownCommand)
        //   3. Extract ticker (token[1])
        //   4. Parse IP+port into a SocketAddr — hint: format!("{}:{}", ip, port)
        //      then call .parse::<SocketAddr>()
        //   5. Return Ok(StreamCommand { ticker, udp_addr })
        todo!()
    }

    fn validate(&self, reg: &TickerRegistry) -> Result<(), ProtocolError> {
        if !reg.is_valid(&self.ticker) {
            Err(ProtocolError::UnknownCommand(self.ticker.to_string()))
        } else {
            Ok(())
        }
    }
}
