use std::time::Duration;

use net::TaggedPacket;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:42001").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        net::configure_performance_tcp_socket(&mut socket)?;
        tokio::spawn(async move {
            let (read, mut write) = socket.split();
            let mut read = net::new_framed_reader(read);

            loop {
                println!("Receiving data...");
                let packet = match net::recv_tagged_packet(&mut read).await {
                    Ok(buffer) => buffer,
                    Err(err) => {
                        eprintln!("Error receiving data: {}", err);
                        break;
                    }
                };
                println!("Received data: {:?}", packet);

                let mut line = String::new();
                std::io::stdin().read_line(&mut line).unwrap();
                let packet = TaggedPacket::Data {
                    client_id: packet.client_id(),
                    data: line.into_bytes(),
                };

                // Echo the data back to the client
                if let Err(err) = net::send_tagged_packet(&mut write, packet).await {
                    eprintln!("Error sending data: {}", err);
                    break;
                }
            }
        });
    }
}
