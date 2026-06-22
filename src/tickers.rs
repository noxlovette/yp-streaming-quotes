use std::{
    collections::HashSet,
    io::BufRead,
    sync::{Arc, LazyLock},
};
pub struct TickerRegistry(HashSet<String>);
pub static REGISTRY: LazyLock<Arc<TickerRegistry>> =
    LazyLock::new(|| TickerRegistry::new());

impl TickerRegistry {
    const TICKERS_FILE: &str = "assets/tickers.txt";

    pub fn from_reader<R: BufRead>(reader: R) -> Self {
        let tickers = reader
            .lines()
            .filter_map(|l| l.ok())
            .map(|l| l.trim().to_uppercase())
            .filter(|l| !l.is_empty())
            .collect();
        Self(tickers)
    }

    fn load() -> Self {
        let file = std::fs::File::open(Self::TICKERS_FILE)
            .expect("cannot open tickers file");
        Self::from_reader(std::io::BufReader::new(file))
    }

    fn new() -> Arc<Self> {
        Arc::new(Self::load())
    }

    pub fn validate(&self, ticker: &str) -> bool {
        self.0.contains(&ticker.to_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn registry_from_str(s: &str) -> TickerRegistry {
        TickerRegistry::from_reader(Cursor::new(s))
    }

    #[test]
    fn loads_known_tickers() {
        let r = registry_from_str("AAPL\nMSFT\nGOOGL\n");
        assert!(r.validate("AAPL"));
        assert!(r.validate("MSFT"));
        assert!(r.validate("GOOGL"));
    }

    #[test]
    fn validate_is_case_insensitive() {
        let r = registry_from_str("AAPL\nMSFT\n");
        assert!(r.validate("aapl"));
        assert!(r.validate("Msft"));
    }

    #[test]
    fn unknown_ticker_is_rejected() {
        let r = registry_from_str("AAPL\nMSFT\n");
        assert!(!r.validate("TSLA"));
        assert!(!r.validate(""));
    }

    #[test]
    fn blank_lines_are_ignored() {
        let r = registry_from_str("AAPL\n\n  \nMSFT\n");
        assert_eq!(r.0.len(), 2);
    }

    #[test]
    fn from_reader_real_file() {
        let file = std::fs::File::open("assets/tickers.txt").unwrap();
        let r = TickerRegistry::from_reader(std::io::BufReader::new(file));
        // AAPL is the first line of assets/tickers.txt
        assert!(r.validate("AAPL"));
    }
}
