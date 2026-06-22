use std::{
    collections::HashSet,
    io::{self, BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::mpsc::{self, TryRecvError, channel},
    thread::{self},
    time::{Duration, Instant},
};
use streaming_quotes::{
    Generator, PING_TIMEOUT, Subscriber, TCP_PORT,
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
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(e) => {
            error!("UDP bind failed: {e}");
            return;
        }
    };

    if let Err(e) = socket.set_read_timeout(Some(Duration::from_millis(100))) {
        error!("set_read_timeout failed: {e}");
        return;
    }

    // None until the first quote packet is sent — the client can't PING us
    // until it learns our ephemeral UDP port from that first packet.
    let mut last_ping: Option<Instant> = None;
    let mut buf = [0u8; 64];

    loop {
        // Poll for keepalive PINGs from the client.
        match socket.recv_from(&mut buf) {
            Ok(_) => {
                last_ping = Some(Instant::now());
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if last_ping.is_some_and(|t| t.elapsed() > PING_TIMEOUT) {
                    info!("client {udp_addr} timed out, closing UDP sender");
                    break;
                }
            }
            Err(e) => {
                error!("UDP recv error for {udp_addr}: {e}");
                break;
            }
        }

        // Drain any quotes the generator has ready.
        match rx.try_recv() {
            Ok(quotes) => {
                for quote in quotes {
                    match socket
                        .send_to(quote.to_wire_line().as_bytes(), udp_addr)
                    {
                        Ok(sent) => {
                            tracing::debug!("sent {sent} bytes to {udp_addr}");
                            // First successful send: start the ping clock.
                            last_ping.get_or_insert_with(Instant::now);
                        }
                        Err(e) => error!(
                            "error sending quote {quote} to {udp_addr}: {e}"
                        ),
                    }
                }
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => break,
        }
    }

    info!("UDP sender for {udp_addr} exiting");
}
