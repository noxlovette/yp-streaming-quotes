use clap::Parser;
use std::{fs, net::SocketAddr, str::from_utf8, sync::Arc};
use streaming_quotes::quote::StockQuote;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UdpSocket},
    sync::watch,
};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    udp_port: u16,

    path: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let args = Args::parse();

    let tickers = read_tickers(&args.path);
    if tickers.is_empty() {
        anyhow::bail!("no tickers found in {}", args.path);
    }
    info!("loaded {} tickers from {}", tickers.len(), args.path);

    let socket = Arc::new(
        UdpSocket::bind(format!("127.0.0.1:{}", args.udp_port)).await?,
    );

    info!("UDP socket bound on port {}", args.udp_port);

    let (tx, rx) = watch::channel(None::<SocketAddr>);

    for ticker in &tickers {
        match register_ticker(&args.addr, ticker, "127.0.0.1", args.udp_port)
            .await
        {
            Ok(()) => info!("registered {ticker} with server"),
            Err(e) => warn!("skipping {ticker}: {e}"),
        }
    }

    let ping_handle = tokio::spawn(ping_task(Arc::clone(&socket), rx));

    let mut buf = vec![0u8; 2048];
    loop {
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                let (len, from) = result?;
                let _ = tx.send(Some(from));
                match StockQuote::from_wire_line(from_utf8(&buf[..len])?) {
                    Ok(quote) => info!("quote from {from}: {quote:?}"),
                    Err(e) => error!("bad quote from {from}: {e}"),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutting down");
                break;
            }
        }
    }

    ping_handle.abort();
    Ok(())
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

async fn register_ticker(
    server_addr: &str,
    ticker: &str,
    ip: &str,
    udp_port: u16,
) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(server_addr).await?;
    let cmd = format!("STREAM {ticker} {ip} {udp_port}\n");
    let (reader, mut writer) = stream.split();
    writer.write_all(cmd.as_bytes()).await?;

    let mut resp = String::new();
    BufReader::new(reader).read_line(&mut resp).await?;

    if resp.trim() != "OK" {
        anyhow::bail!("{}", resp.trim());
    }
    Ok(())
}

async fn ping_task(
    socket: Arc<UdpSocket>,
    rx: watch::Receiver<Option<SocketAddr>>,
) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let addr = *rx.borrow();
        if let Some(addr) = addr {
            tracing::debug!("PING → {addr}");
            let _ = socket.send_to(b"PING", addr).await;
        }
    }
}
