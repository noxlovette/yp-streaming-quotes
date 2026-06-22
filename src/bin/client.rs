use clap::Parser;
use std::{
    fs,
    io::{self, BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream, UdpSocket},
    str::from_utf8,
};
use streaming_quotes::{
    PING_TIMEOUT, protocol::StreamCommand, quote::StockQuote,
};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:7878")]
    server: String,

    #[arg(short, long)]
    udp_port: u16,

    path: String,
}

fn main() -> anyhow::Result<()> {
    // machinery
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    // ticker init
    let tickers = read_tickers(&args.path);
    if tickers.is_empty() {
        anyhow::bail!("no tickers found in {}", args.path);
    }

    info!("loaded {} tickers: {:?}", tickers.len(), tickers);

    // set up UDP
    let udp = UdpSocket::bind(format!("127.0.0.1:{}", args.udp_port))?;
    udp.set_read_timeout(Some(PING_TIMEOUT))?;

    info!("UDP listening on :{}", args.udp_port);

    // notify the server
    let tcp = TcpStream::connect(&args.server)?;
    (&tcp).write_all(
        StreamCommand::construct(tickers, udp.local_addr()?)
            .to_string()
            .as_bytes(),
    )?;

    // read the response
    let mut resp = String::new();
    BufReader::new(&tcp).read_line(&mut resp)?;
    let resp = resp.trim();
    if resp != "OK" {
        anyhow::bail!("server rejected: {resp}");
    }
    drop(tcp);

    let mut server_udp: Option<SocketAddr> = None;
    let mut buf = vec![0u8; 2048];

    loop {
        match udp.recv_from(&mut buf) {
            Ok((len, from)) => {
                server_udp.get_or_insert(from);

                let text = match from_utf8(&buf[..len]) {
                    Ok(s) => s.trim_end_matches('\n'),
                    Err(_) => {
                        warn!("non-UTF8 datagram from {from}");
                        continue;
                    }
                };

                match StockQuote::from_wire_line(text) {
                    Ok(q) => info!(
                        "[{}] ${:.2}  vol={}  ts={}",
                        q.ticker, q.price, q.volume, q.timestamp_ms
                    ),
                    Err(e) => warn!("bad datagram from {from}: {e} ({text:?})"),
                }
            }

            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if let Some(addr) = server_udp {
                    udp.send_to(b"PING\n", addr)?;
                    info!("PING → {addr}");
                }
            }

            Err(e) => return Err(e.into()),
        }
    }
}

fn read_tickers(path: &str) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}
