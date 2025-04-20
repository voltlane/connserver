use std::{collections::HashMap, sync::Arc};

use connection::stale::StaleConnectionManager;
use enc::encrypt;
use ids::IdGenerator;
use log::{error, info};
use net::{ClientServerPacket, TaggedPacket};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::{mpsc, Mutex},
};

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

        tokio::spawn({
            async move {
                if let Err(e) =
                    handle_socket_duplex_master(master, master_send_receiver, master_recv_sender)
                        .await
                {
                    error!("Error handling master duplex: {}", e);
                }
            }
        });

        let clients = Arc::new(Mutex::new(HashMap::<u64, mpsc::Sender<Msg>>::new()));
        let stale_conn_manager = StaleConnectionManager::new(self.config.clone());

        tokio::spawn(stale_conn_manager.clone().run_cleanup());

        tokio::spawn({
            let clients = clients.clone();
            async move {
                if let Err(e) = master_recv_main_loop(master_recv_receiver, clients).await {
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
            clients.lock().await.insert(client_id, client_sender);
        }
        tokio::spawn({
            let master_send_sender = master_send_sender.clone();
            let keys = keys.clone();
            let clients = clients.clone();
            let id_gen = id_gen.clone();
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
                )
                .await
            }
        });
        Ok(())
    }
}

async fn handle_socket_duplex_slave(
    read: &mut OwnedReadHalf,
    write: &mut OwnedWriteHalf,
    mut send_receiver: mpsc::Receiver<Msg>,
    recv_sender: mpsc::Sender<Msg>,
    client_id: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = Vec::new();
    loop {
        tokio::select! {
            res = net::recv_size_prefixed(read, &mut buffer) => {
                match res {
                    Ok(()) => {
                        if let Err(e) = recv_sender.send(Msg::Data(TaggedPacket { client_id, data: buffer.clone() })).await {
                            error!("Slave: Error sending message for client {}: {}", client_id, e);
                            return Err(e.into());
                        }
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
                        if let Err(e) = net::send_size_prefixed(write, &packet.data).await {
                            error!("Slave: Error sending message for client {}: {}", client_id, e);
                            return Err(e.into());
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut read, mut write) = socket.into_split();
    let mut buffer = Vec::new();
    loop {
        tokio::select! {
            packet = net::recv_tagged_packet(&mut read, &mut buffer) => {
                match packet {
                    Ok(packet) => {
                        if let Err(e) = recv_sender.send(Msg::Data(packet.clone())).await {
                            panic!("Master: Error sending message for client {}: {}", packet.client_id, e);
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
                            panic!("Master: Error sending message for client {}: {}", packet.client_id, e);
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

struct StaleClient {
    encryption: enc::easy::Encryption,
    disconnected: std::time::Instant,
}

async fn handle_connect(
    read: &mut OwnedReadHalf,
    write: &mut OwnedWriteHalf,
    keys: &enc::easy::Keys,
    mut client_id: u64,
    stale_conn_manager: &StaleConnectionManager,
) -> Result<(enc::easy::Encryption, u64), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = Vec::new();
    net::recv_size_prefixed(read, &mut buffer).await?;
    match ClientServerPacket::from_vec(buffer.clone()) {
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
    net::recv_size_prefixed(read, &mut buffer).await?;
    match ClientServerPacket::from_vec(buffer.clone()) {
        Ok(ClientServerPacket::PubKey(key)) => {
            // new connection
            info!("New client {} is connecting", client_id);
            let their_pubkey = enc::easy::pubkey_from_bytes(&key)?;
            // 2. send client id
            let packet = ClientServerPacket::ClientId(client_id);
            net::send_size_prefixed(write, &packet.into_vec()?).await?;
            Ok((keys.create_encryption(&their_pubkey), client_id))
        }
        Ok(ClientServerPacket::ClientId(id)) => {
            info!("A client is trying to reconnect as client {}", id);
            client_id = id;
            // reconnection attempt
            let (stale_client_enc, stale_client_time) = match stale_conn_manager
                .remove_stale_client(client_id)
                .await
            {
                Some(stale_client) => stale_client,
                None => return Err(format!("Client ID {} not found in stale clients", id).into()),
            };

            if let Err(e) = do_reconnect(read, write, buffer, &stale_client_enc).await {
                error!("Error during reconnection: {}", e);
                stale_conn_manager
                    .add_stale_client(client_id, stale_client_enc, stale_client_time)
                    .await?;
                return Err(format!("Failed to reconnect client {}: {}", client_id, e).into());
            } else {
                Ok((stale_client_enc, client_id))
            }
        }
        _ => Err("Expected client ID or public key packet".into()),
    }
}

async fn do_reconnect(
    read: &mut OwnedReadHalf,
    write: &mut OwnedWriteHalf,
    mut buffer: Vec<u8>,
    stale_client_enc: &enc::easy::Encryption,
) -> anyhow::Result<()> {
    let challenge_bytes = enc::easy::random_bytes(32);
    let encrypted_challenge = stale_client_enc.encrypt(challenge_bytes.clone());
    let packet = ClientServerPacket::Challenge(encrypted_challenge);
    net::send_size_prefixed(write, &packet.into_vec()?).await?;
    net::recv_size_prefixed(read, &mut buffer).await?;
    let response = match ClientServerPacket::from_vec(buffer.clone()) {
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
    net::recv_size_prefixed(read, &mut buffer).await?;
    match ClientServerPacket::from_vec(buffer.clone()) {
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
    clients: Arc<Mutex<HashMap<u64, mpsc::Sender<Msg>>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // take messages from the master_recv_receiver and send them to each client
    loop {
        if let Some(msg) = master_recv_receiver.recv().await {
            match msg {
                Msg::Data(packet) => {
                    let client = {
                        let clients = clients.lock().await;
                        clients.get(&packet.client_id).cloned()
                    };
                    match client {
                        Some(client_sender) => {
                            if let Err(e) = client_sender.send(Msg::Data(packet.clone())).await {
                                error!(
                                    "Error sending message to client {}: {}",
                                    packet.client_id, e
                                );
                            }
                        }
                        None => {
                            error!(
                                "Client {} not found; master server is sending bogus?",
                                packet.client_id
                            );
                        }
                    }
                }
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
) {
    let (mut read, mut write) = client.into_split();
    let client_id_before = client_id;
    let encryption =
        match handle_connect(&mut read, &mut write, &keys, client_id, &stale_conn_manager).await {
            Ok((enc, new_id)) => {
                client_id = new_id;
                info!("Client {} connected", client_id);
                enc
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
        stale_conn_manager.add_stale_client(client_id, encryption, std::time::Instant::now());
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
