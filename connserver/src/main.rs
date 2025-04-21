use std::{collections::HashMap, sync::Arc};

use connection::stale::{StaleClient, StaleConnectionManager};
use ids::IdGenerator;
use log::{debug, error, info};
use net::{ClientServerPacket, TaggedPacket};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::{mpsc, Mutex},
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

mod args;
mod config;
mod connection;
mod ids;

#[derive(Clone)]
enum Msg {
    Stop,
    Data(TaggedPacket),
}

struct ConnServer {
    keys: Arc<enc::easy::Keys>,
    config: config::Config,
}

impl ConnServer {
    async fn new(config: config::Config) -> Self {
        let keys = Arc::new(enc::easy::Keys::new());
        ConnServer { keys, config }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut master = TcpStream::connect(&self.config.master.address).await?;
        net::configure_performance_tcp_socket(&mut master)?;

        let acceptor = TcpListener::bind(&self.config.listener.address).await?;
        info!("Listening on {}", acceptor.local_addr()?);

        let keys = Arc::new(enc::easy::Keys::new());

        let id_gen = Arc::new(Mutex::new(IdGenerator::new()));

        let (master_send_sender, master_send_receiver) =
            mpsc::channel::<Msg>(self.config.master.channel_capacity);
        let (master_recv_sender, master_recv_receiver) =
            mpsc::channel::<Msg>(self.config.master.channel_capacity);

        let (removed_sender, removed_receiver) = mpsc::channel(2);

        tokio::spawn({
            async move {
                if let Err(e) = handle_socket_duplex_master(
                    master,
                    master_send_receiver,
                    master_recv_sender,
                    removed_receiver,
                )
                .await
                {
                    error!("Error handling master duplex: {}", e);
                }
            }
        });

        let clients = Arc::new(Mutex::new(HashMap::<u64, mpsc::Sender<Msg>>::new()));
        let stale_conn_manager = StaleConnectionManager::new(self.config.clone());

        tokio::spawn(stale_conn_manager.clone().run_cleanup(removed_sender));

        tokio::spawn({
            let clients = clients.clone();
            let master_send_sender = master_send_sender.clone();
            let stale_conn_manager = stale_conn_manager.clone();
            let config = self.config.clone();
            async move {
                if let Err(e) = master_recv_main_loop(
                    master_recv_receiver,
                    master_send_sender,
                    clients,
                    stale_conn_manager,
                    config,
                )
                .await
                {
                    error!("Error handling master recv: {}", e);
                }
            }
        });

        loop {
            if let Err(err) = self
                .accept_client(
                    &acceptor,
                    master_send_sender.clone(),
                    keys.clone(),
                    clients.clone(),
                    stale_conn_manager.clone(),
                    &id_gen,
                )
                .await
            {
                error!("Failed accepting client: {}", err);
            }
        }
    }

    async fn accept_client(
        &self,
        acceptor: &TcpListener,
        master_send_sender: mpsc::Sender<Msg>,
        keys: Arc<enc::easy::Keys>,
        clients: Arc<Mutex<HashMap<u64, mpsc::Sender<Msg>>>>,
        stale_conn_manager: StaleConnectionManager,
        id_gen: &Arc<Mutex<IdGenerator>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut client, _) = acceptor.accept().await?;
        net::configure_performance_tcp_socket(&mut client)?;

        let client_id = id_gen.lock().await.next_id();
        info!(
            "Accepted client connection id={}, endpoint={:?}",
            client_id,
            client.peer_addr()
        );
        // these are messages from master to client
        let (client_sender, client_receiver) =
            mpsc::channel::<Msg>(self.config.clients.channel_capacity);
        {
            clients.lock().await.insert(client_id, client_sender.clone());
        }
        tokio::spawn({
            let master_send_sender = master_send_sender.clone();
            let keys = keys.clone();
            let clients = clients.clone();
            let id_gen = id_gen.clone();
            let client_sender = client_sender.clone();
            async move {
                handle_client(
                    client,
                    master_send_sender,
                    client_receiver,
                    keys,
                    clients,
                    stale_conn_manager,
                    client_id,
                    id_gen,
                    client_sender,
                )
                .await
            }
        });
        Ok(())
    }
}

async fn handle_socket_duplex_slave(
    read: &mut net::FramedReader<OwnedReadHalf>,
    write: &mut OwnedWriteHalf,
    mut send_receiver: mpsc::Receiver<Msg>,
    recv_sender: mpsc::Sender<Msg>,
    client_id: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            res = net::recv_size_prefixed(read) => {
                match res {
                    Ok(buffer) => {
                        debug!("Slave: Received data for client {}: {:?}", client_id, buffer);
                        if let Err(e) = recv_sender.send(Msg::Data(TaggedPacket::Data { client_id, data: buffer.to_vec() })).await {
                            error!("Slave: Error sending message for client {}: {}", client_id, e);
                            return Err(e.into());
                        }
                        debug!("Slave: Sent data to master for client {}: {:?}", client_id, buffer);
                    }
                    Err(e) => {
                        error!("Slave: Error receiving packet for client {}: {}", client_id, e);
                        return Err(e.into());
                    }
                }
            }
            Some(msg) = send_receiver.recv() => {
                match msg {
                    Msg::Data(packet) => {
                        if let TaggedPacket::Data { client_id: id, data } = packet {
                            if id != client_id {
                                error!("Slave: Received message for client {} but expected {}", id, client_id);
                                continue;
                            }
                            if let Err(e) = net::send_size_prefixed(write, &data).await {
                                error!("Slave: Error sending message for client {}: {}", client_id, e);
                                return Err(e.into());
                            }
                        } else {
                            error!("Slave: Unexpected packet type from master: {:?}", packet);
                            continue;
                        }
                    }
                    Msg::Stop => {
                        info!("Stopping slave duplex for client {}", client_id);
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_socket_duplex_master(
    socket: TcpStream,
    mut send_receiver: mpsc::Receiver<Msg>,
    recv_sender: mpsc::Sender<Msg>,
    mut removed_receiver: mpsc::Receiver<Vec<u64>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (read, mut write) = socket.into_split();
    let mut read = net::new_framed_reader(read);
    loop {
        tokio::select! {
            removed_client_ids = removed_receiver.recv() => {
                if let Some(client_ids) = removed_client_ids {
                    for client_id in client_ids {
                        let packet = TaggedPacket::Failure { client_id, error: "Client disconnected".to_string() };
                        if let Err(e) = net::send_tagged_packet(&mut write, packet).await {
                            error!("Master: Error sending message for client {}: {}", client_id, e);
                            return Err(e.into());
                        }
                    }
                } else {
                    error!("Master: Error receiving removed client IDs");
                    return Err(anyhow::format_err!("Error receiving removed client IDs").into());
                }
            }
            packet = net::recv_tagged_packet(&mut read) => {
                match packet {
                    Ok(packet) => {
                        if let TaggedPacket::Data { client_id, .. } = packet {
                            if let Err(e) = recv_sender.send(Msg::Data(packet.clone())).await {
                                panic!("Master: Error sending message for client {}: {}", client_id, e);
                            }
                        } else {
                            error!("Master: Unexpected packet type from client {}: {:?}", packet.client_id(), packet);
                            continue;
                        }
                    }
                    Err(e) => {
                        error!("Master: Error receiving packet for client: {}", e);
                        return Err(e.into());
                    }
                }
            }
            Some(msg) = send_receiver.recv() => {
                match msg {
                    Msg::Data(packet) => {
                        if let Err(e) = net::send_tagged_packet(&mut write, packet.clone()).await {
                            panic!("Master: Error sending message for client {}: {}", packet.client_id(), e);
                        }
                    }
                    Msg::Stop => {
                        info!("Stopping master duplex");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

pub enum Either<T, U> {
    Left(T),
    Right(U),
}

async fn handle_connect(
    read: &mut net::FramedReader<OwnedReadHalf>,
    write: &mut OwnedWriteHalf,
    keys: &enc::easy::Keys,
    mut client_id: u64,
    stale_conn_manager: &StaleConnectionManager,
) -> Result<
    Either<(enc::easy::Encryption, u64), StaleClient>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let buffer = net::recv_size_prefixed(read).await?;
    match ClientServerPacket::from_slice(&buffer) {
        Ok(ClientServerPacket::ProtocolVersion(version)) => {
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
    let packet = ClientServerPacket::PubKey(keys.pubkey_to_bytes());
    net::send_size_prefixed(write, &packet.into_vec()?).await?;

    // 1. receive either pubkey or client id, depending if its a new connection or reconnection attempt
    let buffer = net::recv_size_prefixed(read).await?;
    match ClientServerPacket::from_slice(&buffer) {
        Ok(ClientServerPacket::PubKey(key)) => {
            // new connection
            info!("New client {} is connecting", client_id);
            let their_pubkey = enc::easy::pubkey_from_bytes(&key)?;
            // 2. send client id
            let packet = ClientServerPacket::ClientId(client_id);
            net::send_size_prefixed(write, &packet.into_vec()?).await?;
            Ok(Either::Left((
                keys.create_encryption(&their_pubkey),
                client_id,
            )))
        }
        Ok(ClientServerPacket::ClientId(id)) => {
            info!("A client is trying to reconnect as client {}", id);
            client_id = id;
            // reconnection attempt
            let stale_client = match stale_conn_manager.remove_stale_client(client_id).await {
                Some(stale_client) => stale_client,
                None => return Err(format!("Client ID {} not found in stale clients", id).into()),
            };

            if let Err(e) = do_reconnect(read, write, &stale_client.encryption).await {
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
    read: &mut FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    write: &mut OwnedWriteHalf,
    stale_client_enc: &enc::easy::Encryption,
) -> anyhow::Result<()> {
    let challenge_bytes = enc::easy::random_bytes(32);
    let encrypted_challenge = stale_client_enc.encrypt(challenge_bytes.clone());
    let packet = ClientServerPacket::Challenge(encrypted_challenge);
    net::send_size_prefixed(write, &packet.into_vec()?).await?;
    let buffer = net::recv_size_prefixed(read).await?;
    let response = match ClientServerPacket::from_slice(&buffer) {
        Ok(ClientServerPacket::ChallengeResponse(response)) => response,
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
    let buffer = net::recv_size_prefixed(read).await?;
    match ClientServerPacket::from_slice(&buffer) {
        Ok(ClientServerPacket::Ping) => {}
        Ok(_) => return Err(anyhow::format_err!("Expected ping packet")),
        Err(e) => return Err(anyhow::format_err!("Invalid ping packet: {}", e)),
    }
    let packet = ClientServerPacket::Ping;
    net::send_size_prefixed(write, &packet.into_vec()?).await?;
    Ok(())
}

async fn master_recv_main_loop(
    mut master_recv_receiver: mpsc::Receiver<Msg>,
    master_send_sender: mpsc::Sender<Msg>,
    clients: Arc<Mutex<HashMap<u64, mpsc::Sender<Msg>>>>,
    stale_conn_manager: StaleConnectionManager,
    config: config::Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // take messages from the master_recv_receiver and send them to each client
    loop {
        if let Some(msg) = master_recv_receiver.recv().await {
            match msg {
                Msg::Data(packet) => match packet {
                    TaggedPacket::Data { client_id, .. } => {
                        let client = {
                            let clients = clients.lock().await;
                            clients.get(&client_id).cloned()
                        };
                        match client {
                            Some(client_sender) => {
                                if let Err(e) = client_sender.send(Msg::Data(packet.clone())).await
                                {
                                    error!("Error sending message to client {}: {}", client_id, e);
                                }
                            }
                            None => {
                                // Client not found, check if it's a stale client
                                // in that case we want to enqueue the packet if that's configured.
                                // if not, we fail the client right here.
                                if config.clients.missed_packets_buffer_size > 0
                                    && stale_conn_manager.has_client(client_id).await
                                {
                                    if let Err(e) = stale_conn_manager
                                        .add_missed_packet(
                                            client_id,
                                            packet,
                                            config.clients.missed_packets_buffer_size,
                                        )
                                        .await
                                    {
                                        error!(
                                            "Error adding missed packet for client {}: {}, this fails the client",
                                            client_id, e
                                        );
                                        stale_conn_manager.remove_stale_client(client_id).await;
                                        let packet = TaggedPacket::Failure {
                                            client_id,
                                            error: "Client disconnected".to_string(),
                                        };
                                        if let Err(e) =
                                            master_send_sender.send(Msg::Data(packet)).await
                                        {
                                            error!(
                                                "Error sending failure message to master: {}",
                                                e
                                            );
                                        }
                                    }
                                } else {
                                    error!("Client {} not found, missed packet buffering not enabled, failing client", client_id);
                                    stale_conn_manager.remove_stale_client(client_id).await;
                                    let packet = TaggedPacket::Failure {
                                        client_id,
                                        error: "Client not found".to_string(),
                                    };
                                    if let Err(e) = master_send_sender.send(Msg::Data(packet)).await
                                    {
                                        error!("Error sending failure message to master: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        error!("Unexpected packet type from master: {:?}", packet);
                        continue;
                    }
                },
                Msg::Stop => {
                    info!("Stopping master recv");
                    break Ok(());
                }
            }
        }
    }
}

async fn handle_client(
    client: TcpStream,
    master_send_sender: mpsc::Sender<Msg>,
    client_receiver: mpsc::Receiver<Msg>,
    keys: Arc<enc::easy::Keys>,
    clients: Arc<Mutex<HashMap<u64, mpsc::Sender<Msg>>>>,
    stale_conn_manager: StaleConnectionManager,
    mut client_id: u64,
    id_gen: Arc<Mutex<IdGenerator>>,
    send_sender: mpsc::Sender<Msg>,
) {
    let (read, mut write) = client.into_split();
    let mut read = net::new_framed_reader(read);
    let client_id_before = client_id;
    let encryption =
        match handle_connect(&mut read, &mut write, &keys, client_id, &stale_conn_manager).await {
            Ok(Either::Left((enc, new_id))) => {
                client_id = new_id;
                info!("Client {} connected", client_id);
                enc
            }
            Ok(Either::Right(stale_client)) => {
                info!("Client {} reconnected", client_id);
                for packet in stale_client.missed_packets {
                    send_sender.send(Msg::Data(packet)).await.unwrap();
                }
                stale_client.encryption
            }
            Err(e) => {
                error!("Error handling client {}: {}", client_id, e);
                return;
            }
        };
    if client_id != client_id_before {
        info!(
            "Client ID changed from {} to {} due to reconnect",
            client_id_before, client_id
        );
        let mut clients = clients.lock().await;
        let client_sender = match clients.remove(&client_id_before) {
            Some(sender) => sender,
            None => {
                error!(
                    "Client ID {} not found in clients, but must exist",
                    client_id_before
                );
                return;
            }
        };
        clients.insert(client_id, client_sender);
        id_gen.lock().await.release_id(client_id_before);
    }
    if let Err(e) = handle_socket_duplex_slave(
        &mut read,
        &mut write,
        client_receiver,
        master_send_sender,
        client_id,
    )
    .await
    {
        error!(
            "Error handling client duplex for client {}: {}",
            client_id, e
        );
        stale_conn_manager
            .add_stale_client(client_id, encryption, std::time::Instant::now())
            .await;
        let mut clients = clients.lock().await;
        clients.remove(&client_id);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .format_timestamp(None)
        .init();

    let args = match args::Args::from_env() {
        Ok(val) => val,
        Err(e) => {
            error!("Error: {}. Try --help.", e);
            std::process::exit(1);
        }
    };

    let mut config = match config::Config::load_or_new("connserver.toml") {
        Ok(config) => config,
        Err(e) => {
            error!("Error loading config: {}", e);
            std::process::exit(1);
        }
    };

    if let Some(listen_addr) = args.listen_addr {
        info!("Overriding listener.address from commandline");
        config.listener.address = listen_addr;
    }
    if let Some(master_addr) = args.master_addr {
        info!("Overriding master.address from commandline");
        config.master.address = master_addr;
    }

    let mut server = ConnServer::new(config).await;
    if let Err(err) = server.run().await {
        error!("Error: {}", err);
        std::process::exit(1);
    }
}
