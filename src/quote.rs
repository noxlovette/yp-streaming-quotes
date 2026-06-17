use crate::{QuoteError, QuoteResult};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
pub struct StockQuote {
    pub ticker: String,
    pub price: f64,
    pub volume: u32,
    pub timestamp_ms: u64,
}

#[derive(Debug, Default)]
struct QuoteBuilder {
    ticker: Option<String>,
    price: Option<f64>,
    volume: Option<u32>,
    timestamp_ms: Option<u64>,
}

impl QuoteBuilder {
    fn set_ticker(&mut self, s: &str) {
        self.ticker = Some(s.to_string())
    }

    fn set_price(&mut self, p: &str) -> QuoteResult<()> {
        Ok(self.price = Some(f64::from_str(p)?))
    }

    fn set_volume(&mut self, p: &str) -> QuoteResult<()> {
        Ok(self.volume = Some(u32::from_str(p)?))
    }

    fn set_timestamp(&mut self, p: &str) -> QuoteResult<()> {
        Ok(self.timestamp_ms = Some(u64::from_str(p)?))
    }

    fn build(self) -> QuoteResult<StockQuote> {
        type Err = QuoteError;
        Ok(StockQuote {
            ticker: self.ticker.ok_or(Err::Builder("Ticker not set"))?,
            price: self.price.ok_or(Err::Builder("Price not set"))?,
            volume: self.volume.ok_or(Err::Builder("Volume not set"))?,
            timestamp_ms: self
                .timestamp_ms
                .ok_or(Err::Builder("Timestamp not set"))?,
        })
    }
}

impl StockQuote {
    /// Строка без завершающего `\n`.
    pub fn to_wire_line(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.ticker, self.price, self.volume, self.timestamp_ms
        )
    }

    /// Разбор полезной нагрузки без `\n`.
    pub fn from_wire_line(line: &str) -> QuoteResult<Self> {
        let mut builder = QuoteBuilder::default();

        for (i, part) in line.split('|').enumerate() {
            match i {
                0 => builder.set_ticker(part),
                1 => builder.set_price(part)?,
                2 => builder.set_volume(part)?,
                3 => builder.set_timestamp(part)?,
                _ => return Err(QuoteError::CorruptLine),
            }
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StockQuote {
        StockQuote {
            ticker: "AAPL".to_string(),
            price: 189.45,
            volume: 1_000_000,
            timestamp_ms: 1_718_000_000_000,
        }
    }

    #[test]
    fn round_trip() {
        let q = sample();
        let line = q.to_wire_line();
        let parsed = StockQuote::from_wire_line(&line).unwrap();
        assert_eq!(q, parsed);
    }

    #[test]
    fn wire_format_has_four_pipe_separated_fields() {
        let line = sample().to_wire_line();
        assert_eq!(line.split('|').count(), 4);
    }

    #[test]
    fn missing_fields_returns_builder_error() {
        let err = StockQuote::from_wire_line("AAPL").unwrap_err();
        assert!(matches!(err, QuoteError::Builder(_)));
    }

    #[test]
    fn too_many_fields_returns_corrupt_line() {
        let err =
            StockQuote::from_wire_line("AAPL|1.0|100|1234|extra").unwrap_err();
        assert!(matches!(err, QuoteError::CorruptLine));
    }

    #[test]
    fn invalid_price_returns_float_error() {
        let err = StockQuote::from_wire_line("AAPL|not_a_price|100|1234")
            .unwrap_err();
        assert!(matches!(err, QuoteError::Float(_)));
    }

    #[test]
    fn invalid_volume_returns_int_error() {
        let err = StockQuote::from_wire_line("AAPL|1.0|not_a_volume|1234")
            .unwrap_err();
        assert!(matches!(err, QuoteError::Int(_)));
    }
}
