use thiserror::Error;

pub mod quote;
pub mod protocol;
pub mod tickers;

pub(crate) type CrateResult<T> = Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Quote parsing error")]
    Quote
}
