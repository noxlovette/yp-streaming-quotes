use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
};
pub struct TickerRegistry(HashSet<String>);
pub static REGISTRY: LazyLock<Arc<TickerRegistry>> =
    LazyLock::new(|| TickerRegistry::new());

impl TickerRegistry {
    const TICKERS_FILE: &str = "assets/tickers.txt";

    fn load() -> Self {
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

    fn new() -> Arc<Self> {
        Arc::new(Self::load())
    }

    pub fn validate(&self, ticker: &str) -> bool {
        self.0.contains(&ticker.to_uppercase())
    }
}
