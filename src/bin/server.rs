use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use streaming_quotes::ProtocolError;
use streaming_quotes::protocol::StreamCommand;
use streaming_quotes::quote::StockQuote;
use streaming_quotes::tickers::TickerRegistry;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

const TCP_PORT: u16 = 7777;
const UDP_INTERVAL_MS: u64 = 200;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let registry = TickerRegistry::new();
    let addr = format!("0.0.0.0:{TCP_PORT}");
    let listener = TcpListener::bind(&addr).await?;
    println!("TCP server listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        println!("connection from {peer}");
        tokio::spawn(handle_connection(stream, peer, registry.clone()));
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    registry: Arc<TickerRegistry>,
) -> Result<(), ProtocolError> {
    let (reader, mut writer) = stream.split();
    let mut lines = BufReader::new(reader).lines();

    let line = lines
        .next_line()
        .await?
        .ok_or(ProtocolError::Malformed("Empty line"))?;
    let command = StreamCommand::parse(line.trim())?;
    tokio::spawn(async move {
        if let Err(e) =
            stream_udp(command.ticker.clone(), command.udp_addr.clone()).await
        {
            eprintln!("udp stream error for {peer}: {e}");
        }
    });
    // TCP connection closes here; UDP stream continues independently.
    Ok(())
}

async fn stream_udp(
    ticker: String,
    udp_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    println!("streaming {ticker} → {udp_addr}");

    loop {
        let quote = StockQuote::generate(&ticker);
        let line = quote.to_wire_line();
        socket.send_to(line.as_bytes(), udp_addr).await?;
        tokio::time::sleep(Duration::from_millis(UDP_INTERVAL_MS)).await;
    }
}
