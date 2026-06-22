use std::{
    collections::HashSet,
    num::{ParseFloatError, ParseIntError},
    sync::mpsc::{self, Receiver, Sender},
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::quote::StockQuote;
use thiserror::Error;

pub mod protocol;
pub mod quote;
pub mod tickers;

pub(crate) type QuoteResult<T> = Result<T, QuoteError>;
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);
pub const PING_INTERVAL: Duration = Duration::from_secs(2);
pub const QUOTE_INTERVAL: Duration = Duration::from_millis(300);
pub const TCP_PORT: u16 = 7878;

/// This guy represents a UDP thread stored inside the generator
pub struct Subscriber {
    /// The tickers that a given subscriber wants
    tickers: HashSet<String>,
    /// Receives stuff from the generator
    tx: Sender<HashSet<StockQuote>>,
}

/// This guy generates quotes
pub struct Generator {
    /// Stored subscribers
    subscribers: Vec<Subscriber>,

    /// A filter to generated only the tickers inside the subscribers
    filter: HashSet<String>,

    /// Receives new subscribers from elsewhere
    rx: Receiver<Subscriber>,
}

impl Generator {
    /// Creates a new generator, returning self and the receiver handle for the
    /// channel
    pub fn new() -> (Self, Sender<Subscriber>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                subscribers: Vec::new(),
                filter: HashSet::new(),
                rx,
            },
            tx,
        )
    }

    /// Runs the generator in a separate thread
    pub fn run(mut self) -> JoinHandle<Self> {
        thread::spawn(move || {
            loop {
                // check if there are any new subs pending
                while let Ok(s) = self.rx.try_recv() {
                    self.add_subscriber(s);
                }

                let mut out = HashSet::new();

                // generates quotes for this cycle
                for t in self.filter.iter() {
                    out.insert(StockQuote::generate(t));
                }

                // notify threads, removing any whose receiver has been dropped
                self.subscribers.retain(|s| {
                    let to_send: HashSet<StockQuote> = out
                        .iter()
                        .filter(|q| s.tickers.contains(&q.ticker))
                        .cloned()
                        .collect();
                    s.tx.send(to_send).is_ok()
                });

                // rebuild ticker filter if any subscribers were removed
                self.filter = self
                    .subscribers
                    .iter()
                    .flat_map(|s| s.tickers.iter().cloned())
                    .collect();

                thread::sleep(QUOTE_INTERVAL);
            }
        })
    }

    pub fn add_subscriber(&mut self, s: Subscriber) {
        self.filter.extend(s.tickers.clone());
        self.subscribers.push(s);
    }
}

impl Subscriber {
    pub fn new(
        tickers: HashSet<String>,
        tx: Sender<HashSet<StockQuote>>,
    ) -> Self {
        Self { tickers, tx }
    }
}

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
    #[error("invalid command")]
    InvalidCommand,

    #[error("invalid udp address")]
    InvalidAddr,

    #[error("empty ticker list")]
    EmptyTickerList,

    #[error("unknown ticker: {0}")]
    UnknownTicker(String),
}
