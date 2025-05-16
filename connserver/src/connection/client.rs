use log::{error, info};
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc,
};
use tokio_util::bytes::BytesMut;

use crate::connserver::{Either, Msg};

use super::stale::{StaleClient, StaleConnectionManager};

pub struct Client {
    pub read: net::FramedReader<OwnedReadHalf>,
    pub write: OwnedWriteHalf,
    send_receiver: mpsc::Receiver<Msg>,
    pub recv_sender: mpsc::Sender<Msg>,
    client_id: u64,
}

impl Client {
    pub fn new(
        mut stream: tokio::net::TcpStream,
        send_receiver: mpsc::Receiver<Msg>,
        recv_sender: mpsc::Sender<Msg>,
        client_id: u64,
    ) -> Self {
        // NOTE(lion): We deliberately ignore this, because dealing with this here sucks
        // and we don't care about the result, because it cannot really fail
        _ = net::configure_performance_tcp_socket(&mut stream);
        let (read, write) = stream.into_split();
        let read = net::new_framed_reader(read);
        Self {
            read,
            write,
            send_receiver,
            recv_sender,
            client_id,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            if !self.run_once().await? {
                break;
            }
        }
        Ok(())
    }

    async fn run_once(&mut self) -> anyhow::Result<bool> {
        tokio::select! {
            res = net::recv_size_prefixed(&mut self.read) => {
                if let Some(value) = self.handle_packet(res).await {
                    return value;
                }
            }
            Some(msg) = self.send_receiver.recv() => {
                if let Some(value) = self.handle_msg(msg).await {
                    return value;
                }
            }
        }
        Ok(true)
    }

    async fn handle_msg(&mut self, msg: Msg) -> Option<Result<bool, anyhow::Error>> {
        match msg {
            Msg::Data(packet) => match packet {
                net::TaggedPacket::Data {
                    client_id: id,
                    data,
                } => {
                    if id != self.client_id {
                        error!(
                            "Client: Received message for client {} but expected {}",
                            id, self.client_id
                        );
                    }
                    if let Err(e) = net::send_size_prefixed(&mut self.write, &data).await {
                        error!(
                            "Client: Error sending message for client {}: {}",
                            self.client_id, e
                        );
                        return Some(Err(e.into()));
                    }
                }
                _ => {
                    error!("Client: Unexpected packet type from master: {:?}", packet);
                }
            },
            Msg::Stop => {
                info!("Stopping client duplex for client {}", self.client_id);
                return Some(Ok(false));
            }
        }
        None
    }

    async fn handle_packet(
        &mut self,
        res: anyhow::Result<BytesMut>,
    ) -> Option<Result<bool, anyhow::Error>> {
        match res {
            Ok(buffer) => {
                if let Err(e) = self
                    .recv_sender
                    .send(Msg::Data(net::TaggedPacket::Data {
                        client_id: self.client_id,
                        data: buffer.to_vec(),
                    }))
                    .await
                {
                    error!(
                        "Client: Error sending message for client {}: {}",
                        self.client_id, e
                    );
                    return Some(Err(e.into()));
                }
            }
            Err(e) => {
                error!(
                    "Client: Error receiving packet for client {}: {}",
                    self.client_id, e
                );
                return Some(Err(e.into()));
            }
        }
        None
    }

    pub async fn try_reconnect(
        &mut self,
        keys: &enc::easy::Keys,
        mut client_id: u64,
        stale_conn_manager: &StaleConnectionManager,
    ) -> Result<
        Either<(enc::easy::Encryption, u64), StaleClient>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let buffer = net::recv_size_prefixed(&mut self.read).await?;
        match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::ProtocolVersion(version)) => {
                if version != net::PROTOCOL_VERSION {
                    return Err(format!("Unsupported protocol version: {}", version).into());
                }
            }
            Ok(_) => {
                return Err("Expected protocol version packet".into());
            }
            Err(e) => {
                return Err(format!("Invalid protocol version packet: {}", e).into());
            }
        }

        // 0. send server key
        let packet = net::ClientServerPacket::PubKey(keys.pubkey_to_bytes());
        net::send_size_prefixed(&mut self.write, &packet.into_vec()?).await?;

        // 1. receive either pubkey or client id, depending if its a new connection or reconnection attempt
        let buffer = net::recv_size_prefixed(&mut self.read).await?;
        match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::PubKey(key)) => {
                // new connection
                info!("New client {} is connecting", client_id);
                let their_pubkey = enc::easy::pubkey_from_bytes(&key)?;
                // 2. send client id
                let packet = net::ClientServerPacket::ClientId(client_id);
                net::send_size_prefixed(&mut self.write, &packet.into_vec()?).await?;
                Ok(Either::Left((
                    keys.create_encryption(&their_pubkey),
                    client_id,
                )))
            }
            Ok(net::ClientServerPacket::ClientId(id)) => {
                info!("A client is trying to reconnect as client {}", id);
                client_id = id;
                // reconnection attempt
                let stale_client = match stale_conn_manager.remove_stale_client(client_id).await {
                    Some(stale_client) => stale_client,
                    None => {
                        return Err(format!("Client ID {} not found in stale clients", id).into())
                    }
                };

                if let Err(e) = self.do_reconnect(&stale_client.encryption).await {
                    error!("Error during reconnection: {}", e);
                    stale_conn_manager
                        .add_stale_client(
                            client_id,
                            stale_client.encryption,
                            stale_client.disconnected,
                        )
                        .await;
                    return Err(format!("Failed to reconnect client {}: {}", client_id, e).into());
                } else {
                    Ok(Either::Right(stale_client))
                }
            }
            _ => Err("Expected client ID or public key packet".into()),
        }
    }

    async fn do_reconnect(
        &mut self,
        stale_client_enc: &enc::easy::Encryption,
    ) -> anyhow::Result<()> {
        let challenge_bytes = enc::easy::random_bytes(32);
        let encrypted_challenge = stale_client_enc.encrypt(challenge_bytes.clone());
        let packet = net::ClientServerPacket::Challenge(encrypted_challenge);
        net::send_size_prefixed(&mut self.write, &packet.into_vec()?).await?;
        let buffer = net::recv_size_prefixed(&mut self.read).await?;
        let response = match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::ChallengeResponse(response)) => response,
            Ok(_) => return Err(anyhow::format_err!("Expected challenge response packet")),
            Err(e) => {
                return Err(anyhow::format_err!(
                    "Invalid challenge response packet: {}",
                    e
                ))
            }
        };
        let decrypted_response = stale_client_enc
            .decrypt(response)
            .map_err(|e| anyhow::format_err!("Failed to decrypt challenge response: {}", e))?;
        if decrypted_response != challenge_bytes {
            return Err(anyhow::format_err!("Challenge response does not match"));
        }
        let buffer = net::recv_size_prefixed(&mut self.read).await?;
        match net::ClientServerPacket::from_slice(&buffer) {
            Ok(net::ClientServerPacket::Ping) => {}
            Ok(_) => return Err(anyhow::format_err!("Expected ping packet")),
            Err(e) => return Err(anyhow::format_err!("Invalid ping packet: {}", e)),
        }
        let packet = net::ClientServerPacket::Ping;
        net::send_size_prefixed(&mut self.write, &packet.into_vec()?).await?;
        Ok(())
    }
}
