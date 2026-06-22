use std::{
    collections::HashSet,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::mpsc::{self, channel},
    thread::{self},
    time::Duration,
};
use streaming_quotes::{
    Generator, Subscriber, TCP_PORT,
    protocol::{Response, StreamCommand},
    quote::StockQuote,
};
use tracing::{error, info};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = format!("127.0.0.1:{TCP_PORT}");
    let listener = TcpListener::bind(&addr)?;
    info!("TCP server listening on {addr}");

    let (generator, tx) = Generator::new();

    generator.run();

    // accept incoming connections
    for incoming in listener.incoming() {
        match incoming {
            // got the connection
            Ok(stream) => {
                let peer = stream.peer_addr()?;
                let new_tx = tx.clone();

                thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, peer, new_tx) {
                        error!("connection error from {peer}: {e}");
                    }
                });
            }
            Err(e) => error!("accept error: {e}"),
        }
    }

    Ok(())
}

/// Informs the generator about new subscribers
fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    new_tx: mpsc::Sender<Subscriber>,
) -> anyhow::Result<()> {
    let mut line = String::new();
    // read what we're getting
    BufReader::new(&stream).read_line(&mut line)?;

    // parse the line
    match StreamCommand::parse(line.trim()) {
        Ok(command) => {
            (&stream).write_all(&Response::Ok.as_bytes())?;
            info!(
                "{peer} subscribed to {:?} → {}",
                command.tickers, command.udp_addr
            );

            let (sub_tx, sub_rx) = channel();
            let new_sub = Subscriber::new(command.tickers, sub_tx);

            match new_tx.send(new_sub) {
                Ok(_) => {
                    info!("new subscriber sent to the generator")
                }
                Err(_) => {
                    error!("error sending subscriber to the generator")
                }
            };

            thread::spawn(move || run_udp_sender(sub_rx, command.udp_addr));
        }
        Err(e) => {
            let res = Response::Err(e.to_string());
            error!("rejected {peer}: {res}");
            (&stream).write_all(&res.as_bytes())?;
        }
    }
    Ok(())
}

fn run_udp_sender(
    rx: mpsc::Receiver<HashSet<StockQuote>>,
    udp_addr: SocketAddr,
) {
    let socket = match UdpSocket::bind(udp_addr) {
        Ok(s) => s,
        Err(e) => {
            error!("UDP bind for {udp_addr} failed: {e}");
            return;
        }
    };

    // auto-closes thread after generator dies
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(s) => {
            for quote in s {
                match socket.send(quote.to_wire_line().as_bytes()) {
                    Ok(sent) => {
                        tracing::debug!("sent {sent} bytes to destination")
                    }
                    Err(e) => {
                        error!(
                            "error sending quote {quote} to address {udp_addr}; Error: {e}"
                        )
                    }
                }
            }
        }
        Err(e) => {
            error!("sender hung up: {e}")
        }
    }

    info!("UDP sender for {udp_addr} exiting");
}
