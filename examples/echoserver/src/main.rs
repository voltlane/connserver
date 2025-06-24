
use net::TaggedPacket;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:42001").await?;
    println!("Server listening on {}", listener.local_addr()?);
    loop {
        let (mut socket, _) = listener.accept().await?;
        net::configure_performance_tcp_socket(&mut socket)?;
        tokio::spawn(async move {
            let (read, mut write) = socket.split();
            let mut read = net::new_framed_reader(read);

            loop {
                let packet = match net::recv_tagged_packet(&mut read).await {
                    Ok(buffer) => buffer,
                    Err(err) => {
                        eprintln!("Error receiving data: {}", err);
                        break;
                    }
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
