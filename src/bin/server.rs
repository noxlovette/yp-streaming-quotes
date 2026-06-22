use std::{
    collections::HashSet,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::mpsc,
    thread,
    time::Duration,
};
use streaming_quotes::{
    PING_TIMEOUT, ProtocolError,
    protocol::{Response, StreamCommand},
    quote::StockQuote,
};
use tracing::{error, info, warn};

const TCP_PORT: u16 = 7878;
const QUOTE_INTERVAL: Duration = Duration::from_secs(1);

struct Subscriber {
    tickers: Vec<String>,
    sender: mpsc::Sender<StockQuote>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = format!("127.0.0.1:{TCP_PORT}");
    let listener = TcpListener::bind(&addr)?;
    info!("TCP server listening on {addr}");

    let (new_sub_tx, new_sub_rx) = mpsc::channel::<Subscriber>();
    thread::spawn(move || run_generator(new_sub_rx));

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let peer = stream.peer_addr().unwrap();
                let sub_tx = new_sub_tx.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, peer, sub_tx) {
                        error!("connection error from {peer}: {e}");
                    }
                });
            }
            Err(e) => error!("accept error: {e}"),
        }
    }
    Ok(())
}

fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    new_sub_tx: mpsc::Sender<Subscriber>,
) -> Result<(), ProtocolError> {
    let mut line = String::new();
    BufReader::new(&stream).read_line(&mut line)?;

    match StreamCommand::parse(line.trim()) {
        Ok(command) => {
            (&stream).write_all(&Response::Ok.as_bytes())?;
            info!(
                "{peer} subscribed to {:?} → {}",
                command.tickers, command.udp_addr
            );

            let (quote_tx, quote_rx) = mpsc::channel::<StockQuote>();

            // Register with the generator before spawning the sender thread
            new_sub_tx
                .send(Subscriber {
                    tickers: command.tickers,
                    sender: quote_tx,
                })
                .ok();

            thread::spawn(move || run_udp_sender(quote_rx, command.udp_addr));
        }
        Err(e) => {
            let res = Response::Err(e.to_string());
            error!("rejected {peer}: {res}");
            (&stream).write_all(&res.as_bytes())?;
        }
    }
    Ok(())
}

fn run_generator(new_sub_rx: mpsc::Receiver<Subscriber>) {
    let mut subscribers: Vec<Subscriber> = Vec::new();

    loop {
        // Drain newly registered subscribers before each tick
        while let Ok(sub) = new_sub_rx.try_recv() {
            info!("generator: new subscriber for {:?}", sub.tickers);
            subscribers.push(sub);
        }

        // Union of all tickers across active subscribers
        let tickers: HashSet<String> = subscribers
            .iter()
            .flat_map(|s| s.tickers.iter().cloned())
            .collect();

        for ticker in &tickers {
            let quote = StockQuote::generate(ticker);
            // Fan out: send to every subscriber that wants this ticker.
            // retain drops the subscriber if its channel is closed (client gone).
            subscribers.retain(|sub| {
                if sub.tickers.contains(ticker) {
                    sub.sender.send(quote.clone()).is_ok()
                } else {
                    true
                }
            });
        }

        thread::sleep(QUOTE_INTERVAL);
    }
}

fn run_udp_sender(rx: mpsc::Receiver<StockQuote>, udp_addr: SocketAddr) {
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(e) => {
            error!("UDP bind for {udp_addr} failed: {e}");
            return;
        }
    };

    loop {
        match rx.recv_timeout(PING_TIMEOUT) {
            Ok(quote) => {
                let payload = format!("{}\n", quote.to_wire_line());
                if let Err(e) = socket.send_to(payload.as_bytes(), udp_addr) {
                    warn!("UDP send to {udp_addr} failed: {e}");
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Err(e) = socket.send_to(b"PING\n", udp_addr) {
                    warn!("UDP PING to {udp_addr} failed: {e}");
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    info!("UDP sender for {udp_addr} exiting");
}
