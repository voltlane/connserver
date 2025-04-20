use tokio::net::{tcp::OwnedReadHalf, TcpStream};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub struct Connection {
    pub read: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    pub write: tokio::net::tcp::OwnedWriteHalf,
    pub keys: enc::easy::Keys,
    pub server_pubkey: enc::easy::PubKey,
    pub client_id: u64,
    pub addr: String,
}

impl Connection {
    /// Creates a new connection to the server, with a new set of keys.
    /// To re-connect an existing connection, use `Connection::reconnect` instead.
    pub async fn new(addr: &str) -> anyhow::Result<Self> {
        let mut stream = TcpStream::connect(addr).await?;
        net::configure_performance_tcp_socket(&mut stream)?;
        let (read, mut write) = stream.into_split();
        let mut read = net::new_framed_reader(read);

        // 0. send the current protocol version
        net::send_size_prefixed(
            &mut write,
            &net::ClientServerPacket::ProtocolVersion(net::PROTOCOL_VERSION).into_vec()?,
        )
        .await?;

        // 1. receive server public key
        let buffer = net::recv_size_prefixed(&mut read).await?;
        let pubkey_bytes = match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::PubKey(key)) => key,
            Ok(_) => {
                return Err(anyhow::format_err!("Expected server public key packet"));
            }
            Err(e) => return Err(anyhow::format_err!("Invalid server public key packet: {}", e)),
        };
        // validate it
        let server_pubkey = match enc::easy::pubkey_from_bytes(&pubkey_bytes) {
            Ok(val) => val,
            Err(e) => return Err(anyhow::format_err!("Invalid server public key: {}", e)),
        };

        // 2. send my public key
        let keys = enc::easy::Keys::new();
        let pubkey = keys.pubkey_to_bytes();
        let packet = net::ClientServerPacket::PubKey(pubkey);
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 3. receive my client id
        let buffer = net::recv_size_prefixed(&mut read).await?;
        let client_id = match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::ClientId(id)) => id,
            Ok(_) => return Err(anyhow::format_err!("Expected client ID packet")),
            Err(e) => return Err(anyhow::format_err!("Invalid client ID packet: {}", e)),
        };

        Ok(Connection {
            addr: addr.to_string(),
            read,
            write,
            keys,
            client_id,
            server_pubkey,
        })
    }

    pub async fn reconnect(&mut self) -> anyhow::Result<()> {
        // to reconnect, we first send the protocol version,
        // then we receive the server's public key and validate that it's the same we already know.
        // then we send out client id, which signals to the server that we're trying to reconnect.
        // then the server sends us a challenge, which we have to decrypt, and re-encrypt with the
        // server's public key, and send it.
        // if the server hasn't kicked us out by then, we're done :) we send a ping to confirm that it's
        // working and we're done.
        let mut stream = TcpStream::connect(&self.addr).await?;
        net::configure_performance_tcp_socket(&mut stream)?;
        let (read, mut write) = stream.into_split();
        let mut read = net::new_framed_reader(read);

        // 0. send the current protocol version
        net::send_size_prefixed(
            &mut write,
            &net::ClientServerPacket::ProtocolVersion(net::PROTOCOL_VERSION).into_vec()?,
        )
        .await?;

        let buffer = net::recv_size_prefixed(&mut read).await?;
        let pubkey_bytes = match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::PubKey(key)) => key,
            Ok(_) => {
                return Err(anyhow::format_err!("Expected server public key packet"));
            }
            Err(e) => return Err(anyhow::format_err!("Invalid server public key packet: {}", e)),
        };

        // validate it
        let server_pubkey = match enc::easy::pubkey_from_bytes(&pubkey_bytes) {
            Ok(val) => val,
            Err(e) => return Err(anyhow::format_err!("Invalid server public key: {}", e)),
        };

        // check if the server's public key is the same as the one we already know
        if server_pubkey != self.server_pubkey {
            return Err(anyhow::format_err!("Server public key does not match"));
        }

        // 2. send my client id
        let packet = net::ClientServerPacket::ClientId(self.client_id);
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 3. receive challenge
        let buffer = net::recv_size_prefixed(&mut read).await?;
        let challenge = match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::Challenge(challenge)) => challenge,
            Ok(_) => return Err(anyhow::format_err!("Expected challenge packet")),
            Err(e) => return Err(anyhow::format_err!("Invalid challenge packet: {}", e)),
        };

        // 4. decrypt challenge
        let encryption = self.keys.create_encryption(&self.server_pubkey);
        let decrypted_challenge = match encryption.decrypt(challenge) {
            Ok(val) => val,
            Err(e) => return Err(anyhow::format_err!("Failed to decrypt challenge: {}", e)),
        };

        // 5. re-encrypt challenge with server's public key
        let encrypted_challenge = encryption.encrypt(decrypted_challenge);
        let packet = net::ClientServerPacket::ChallengeResponse(encrypted_challenge);
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 6. send ping
        let packet = net::ClientServerPacket::Ping;
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 7. receive pong
        let buffer = net::recv_size_prefixed(&mut read).await?;
        match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::Ping) => (),
            Ok(_) => return Err(anyhow::format_err!("Expected pong packet")),
            Err(e) => return Err(anyhow::format_err!("Invalid pong packet: {}", e)),
        };

        self.read = read;
        self.write = write;
        Ok(())
    }
}
