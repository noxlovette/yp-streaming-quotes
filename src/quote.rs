use crate::{CrateResult, Error};

#[derive(Debug, Clone, PartialEq)]
pub struct StockQuote {
    pub ticker: String,
    pub price: f64,
    pub volume: u32,
    pub timestamp_ms: u64,
}

impl StockQuote {
    /// Строка без завершающего `\n`.
    pub fn to_wire_line(&self) -> String {
        format!("{}|{}|{}|{}", self.ticker, self.price, self.volume, self.timestamp_ms)
    }

    /// Разбор полезной нагрузки без `\n`.
    pub fn from_wire_line(line: &str) -> CrateResult<Self> {
        todo!("разбить по '|', распарсить числа, вернуть ошибку при неверном числе полей")
    }
}
