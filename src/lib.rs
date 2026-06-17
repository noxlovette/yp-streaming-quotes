use std::num::{ParseFloatError, ParseIntError};

use thiserror::Error;

pub mod protocol;
pub mod quote;
pub mod tickers;

pub(crate) type CrateResult<T> = Result<T, Error>;
pub(crate) type QuoteResult<T> = Result<T, QuoteError>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Quote parsing error: {0}")]
    Quote(#[from] QuoteError),
}

#[derive(Debug, Error)]
pub enum QuoteError {
    #[error("float parsing failed: {0}")]
    Float(#[from] ParseFloatError),

    #[error("integer parsing failed: {0}")]
    Int(#[from] ParseIntError),

    #[error("Corrupt stock line")]
    CorruptLine,

    #[error("Builder error: {0}")]
    Builder(&'static str),
}
