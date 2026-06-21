use std::{net::SocketAddr, time::Duration};
use streaming_quotes::{
    PING_TIMEOUT, ProtocolError,
    protocol::{Response, StreamCommand},
    quote::StockQuote,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UdpSocket},
    select,
    time::{Instant, sleep, sleep_until},
};

const TCP_PORT: u16 = 7777;
const UDP_INTERVAL_MS: u64 = 200;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let addr = format!("127.0.0.1:{TCP_PORT}");
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("TCP server listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::info!("connection from {peer}");
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
) -> Result<(), ProtocolError> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    tracing::info!("streaming {ticker} → {udp_addr}");
    let mut buf = vec![0u8; 2048];
    let mut last_ping = Instant::now();

    loop {
        select! {
            _ = sleep(Duration::from_millis(UDP_INTERVAL_MS)) => {
                let quote = StockQuote::generate(&ticker);
                let line = quote.to_wire_line();
                socket.send_to(line.as_bytes(), udp_addr).await?;
            }
            _ = sleep_until(last_ping + PING_TIMEOUT) => {
                    return Ok(());
                }
            Ok((len, _from)) = socket.recv_from(&mut buf) => {
                if std::str::from_utf8(&buf[..len]).is_ok_and(|msg| msg == "PING") {
                   last_ping = Instant::now()
               }
            }
        }
    }
}
