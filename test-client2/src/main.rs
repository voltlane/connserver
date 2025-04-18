use std::io::{Read, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = clientcom::Connection::new("127.0.0.1:42000").await?;

    loop {
        loop {
            let mut input_buf = [0; 1024];
            let bytes_read = match std::io::stdin().read(&mut input_buf) {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("Error reading from stdin: {}", err);
                    break;
                }
            };
            if bytes_read == 0 {
                break;
            }

            // Send the data to the server
            if let Err(e) =
                net::send_size_prefixed(&mut client.write, &input_buf[..bytes_read]).await
            {
                eprintln!("Error sending data to server: {}", e);
                break;
            }

            // Receive the response from the server
            let mut response_buf = Vec::new();
            if let Err(e) = net::recv_size_prefixed(&mut client.read, &mut response_buf).await {
                eprintln!("Error receiving data from server: {}", e);
                break;
            }

            // Print the response to stdout
            if let Err(e) = std::io::stdout().write_all(&response_buf) {
                eprintln!("Error writing to stdout: {}", e);
                break;
            }
        }
        if let Ok(()) = client.reconnect().await {
            println!("Reconnected successfully");
        } else {
            println!("Reconnect attempt failed!");
            break;
        }
    }
    eprintln!("Exiting...");
    Ok(())
}
