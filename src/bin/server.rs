use std::net::SocketAddr;
use std::time::Duration;
use streaming_quotes::ProtocolError;
use streaming_quotes::protocol::{Response, StreamCommand};
use streaming_quotes::quote::StockQuote;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

const TCP_PORT: u16 = 7777;
const UDP_INTERVAL_MS: u64 = 200;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{TCP_PORT}");
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("TCP server listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        println!("connection from {peer}");
        tokio::spawn(handle_connection(stream, peer));
    }
}
async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
) -> Result<(), ProtocolError> {
    let (reader, mut writer) = stream.split();
    let mut lines = BufReader::new(reader).lines();

    let result = async {
        let line = lines
            .next_line()
            .await?
            .ok_or(ProtocolError::Malformed("Empty line"))?;
        StreamCommand::parse(line.trim())
    }
    .await;

    match result {
        Ok(command) => {
            writer.write_all(&Response::Ok.as_bytes()).await?;
            tokio::spawn(async move {
                if let Err(e) =
                    stream_udp(command.ticker, command.udp_addr).await
                {
                    tracing::error!("udp stream error for {peer}: {e}");
                }
            });
        }
        Err(e) => {
            let res = Response::Err(e.to_string());
            tracing::error!("error in connection: {res}");
            writer.write_all(&res.as_bytes()).await?;
        }
    }

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
