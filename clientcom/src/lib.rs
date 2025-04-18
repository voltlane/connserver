use tokio::net::TcpStream;

pub struct Connection {
    pub read: tokio::net::tcp::OwnedReadHalf,
    pub write: tokio::net::tcp::OwnedWriteHalf,
    pub keys: enc::easy::Keys,
    pub server_pubkey: enc::easy::PubKey,
    pub client_id: u64,
    pub addr: String,
}

impl Connection {
    /// Creates a new connection to the server, with a new set of keys.
    /// To re-connect an existing connection, use `Connection::reconnect` instead.
    pub async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut stream = TcpStream::connect(addr).await?;
        net::configure_performance_tcp_socket(&mut stream)?;
        let (mut read, mut write) = stream.into_split();

        let mut buffer = Vec::new();

        // 0. send the current protocol version
        net::send_size_prefixed(
            &mut write,
            &net::ClientServerPacket::ProtocolVersion(net::PROTOCOL_VERSION).into_vec()?,
        )
        .await?;

        // 1. receive server public key
        net::recv_size_prefixed(&mut read, &mut buffer).await?;
        let pubkey_bytes = match net::ClientServerPacket::from_vec(buffer.clone()) {
            Ok(net::ClientServerPacket::PubKey(key)) => key,
            Ok(_) => {
                return Err("Expected server public key packet".into());
            }
            Err(e) => return Err(format!("Invalid server public key packet: {}", e).into()),
        };
        // validate it
        let server_pubkey = match enc::easy::pubkey_from_bytes(&pubkey_bytes) {
            Ok(val) => val,
            Err(e) => return Err(format!("Invalid server public key: {}", e).into()),
        };

        // 2. send my public key
        let keys = enc::easy::Keys::new();
        let pubkey = keys.pubkey_to_bytes();
        let packet = net::ClientServerPacket::PubKey(pubkey);
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 3. receive my client id
        net::recv_size_prefixed(&mut read, &mut buffer).await?;
        let client_id = match net::ClientServerPacket::from_vec(buffer) {
            Ok(net::ClientServerPacket::ClientId(id)) => id,
            Ok(_) => return Err("Expected client ID packet".into()),
            Err(e) => return Err(format!("Invalid client ID packet: {}", e).into()),
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

    pub async fn reconnect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // to reconnect, we first send the protocol version,
        // then we receive the server's public key and validate that it's the same we already know.
        // then we send out client id, which signals to the server that we're trying to reconnect.
        // then the server sends us a challenge, which we have to decrypt, and re-encrypt with the
        // server's public key, and send it.
        // if the server hasn't kicked us out by then, we're done :) we send a ping to confirm that it's
        // working and we're done.
        let mut stream = TcpStream::connect(&self.addr).await?;
        net::configure_performance_tcp_socket(&mut stream)?;
        let (mut read, mut write) = stream.into_split();

        // 0. send the current protocol version
        net::send_size_prefixed(
            &mut write,
            &net::ClientServerPacket::ProtocolVersion(net::PROTOCOL_VERSION).into_vec()?,
        )
        .await?;

        // 1. receive server public key
        let mut buffer = Vec::new();

        net::recv_size_prefixed(&mut read, &mut buffer).await?;
        let pubkey_bytes = match net::ClientServerPacket::from_vec(buffer.clone()) {
            Ok(net::ClientServerPacket::PubKey(key)) => key,
            Ok(_) => {
                return Err("Expected server public key packet".into());
            }
            Err(e) => return Err(format!("Invalid server public key packet: {}", e).into()),
        };

        // validate it
        let server_pubkey = match enc::easy::pubkey_from_bytes(&pubkey_bytes) {
            Ok(val) => val,
            Err(e) => return Err(format!("Invalid server public key: {}", e).into()),
        };

        // check if the server's public key is the same as the one we already know
        if server_pubkey != self.server_pubkey {
            return Err("Server public key does not match".into());
        }

        // 2. send my client id
        let packet = net::ClientServerPacket::ClientId(self.client_id);
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 3. receive challenge
        net::recv_size_prefixed(&mut read, &mut buffer).await?;
        let challenge = match net::ClientServerPacket::from_vec(buffer.clone()) {
            Ok(net::ClientServerPacket::Challenge(challenge)) => challenge,
            Ok(_) => return Err("Expected challenge packet".into()),
            Err(e) => return Err(format!("Invalid challenge packet: {}", e).into()),
        };

        // 4. decrypt challenge
        let encryption = self.keys.create_encryption(&self.server_pubkey);
        let decrypted_challenge = match encryption.decrypt(challenge) {
            Ok(val) => val,
            Err(e) => return Err(format!("Failed to decrypt challenge: {}", e).into()),
        };

        // 5. re-encrypt challenge with server's public key
        let encrypted_challenge = encryption.encrypt(decrypted_challenge);
        let packet = net::ClientServerPacket::ChallengeResponse(encrypted_challenge);
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 6. send ping
        let packet = net::ClientServerPacket::Ping;
        net::send_size_prefixed(&mut write, &packet.into_vec()?).await?;

        // 7. receive pong
        net::recv_size_prefixed(&mut read, &mut buffer).await?;
        match net::ClientServerPacket::from_vec(buffer) {
            Ok(net::ClientServerPacket::Ping) => (),
            Ok(_) => return Err("Expected pong packet".into()),
            Err(e) => return Err(format!("Invalid pong packet: {}", e).into()),
        };
        Ok(())
    }
}
