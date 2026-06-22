use std::{
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};
use streaming_quotes::{
    ProtocolError,
    protocol::{Response, StreamCommand},
};
use tracing::{error, info};

const TCP_PORT: u16 = 7878;

fn main() -> anyhow::Result<()> {
    let addr = format!("127.0.0.1:{TCP_PORT}");
    let listener = TcpListener::bind(&addr)?;
    info!("TCP server listening on {addr}");

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let peer = stream.peer_addr().unwrap();
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, peer) {
                        eprintln!("connection error from {peer}: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }

    Ok(())
}

fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
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
            // Stage 3: launch generator fan-out and UDP stream thread here
        }
        Err(e) => {
            let res = Response::Err(e.to_string());
            error!("rejected {peer}: {res}");
            (&stream).write_all(&res.as_bytes())?;
        }
    }

    Ok(())
}
