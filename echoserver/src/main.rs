use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:42001").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        tokio::spawn(async move {
            let mut buf = [0; 8192]; // 8 KB buffer
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 { return; } // EOF
                socket.write_all(&buf[..n]).await.unwrap();
            }
        });
    }
}
