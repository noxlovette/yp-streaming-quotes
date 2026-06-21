use std::{
    net::AddrParseError,
    num::{ParseFloatError, ParseIntError},
    time::Duration,
};

use thiserror::Error;
use tokio::io;

pub mod protocol;
pub mod quote;
pub mod tickers;

pub(crate) type QuoteResult<T> = Result<T, QuoteError>;
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum QuoteError {
    #[error("float parsing failed: {0}")]
    Float(#[from] ParseFloatError),

    #[error("integer parsing failed: {0}")]
    Int(#[from] ParseIntError),

    #[error("corrupt stock line")]
    CorruptLine,

    #[error("builder error: {0}")]
    Builder(&'static str),
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("malformed command: {0}")]
    Malformed(&'static str),

    #[error("unknown command: {0}")]
    UnknownCommand(String),

    #[error("unknown ticker: {0}")]
    UnknownTicker(String),

    #[error("IO Error: {0}")]
    Io(#[from] io::Error),

    #[error("invalid addr: {0}")]
    InvalidAddr(#[from] AddrParseError),

    #[error("invalid arguments")]
    InvalidArguments,
}
