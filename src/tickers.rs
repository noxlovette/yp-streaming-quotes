use std::{collections::HashSet, sync::Arc};

pub struct TickerRegistry(HashSet<String>);

impl TickerRegistry {
    const TICKERS_FILE: &str = "assets/tickers.txt";
    pub fn load() -> Self {
        let content = std::fs::read_to_string(Self::TICKERS_FILE)
            .expect("cannot read tickers file");

        let tickers = content
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(|l| l.to_uppercase())
            .collect();

        Self(tickers)
    }

    pub fn new() -> Arc<Self> {
        Arc::new(Self::load())
    }

    pub fn is_valid(&self, ticker: &str) -> bool {
        self.0.contains(&ticker.to_uppercase())
    }
}
