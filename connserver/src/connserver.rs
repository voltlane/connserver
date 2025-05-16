use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use connection::{client::Client, stale::StaleConnectionManager};
use ids::IdGenerator;
use log::{error, info};
use net::TaggedPacket;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};

use crate::{config, connection, ids, master::MasterDuplex};

#[derive(Clone)]
pub enum Msg {
    Stop,
    Data(TaggedPacket),
}

pub struct ConnServer<T> {
    config: config::Config,
    _marker: std::marker::PhantomData<T>,
}

impl<T> ConnServer<T>
where
    T: MasterDuplex + Send + Sync + 'static,
{
    pub async fn new(config: config::Config) -> Self {
        ConnServer {
            config,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn run(&mut self, mut master_duplex: T) -> anyhow::Result<()> {
        let acceptor = TcpListener::bind(&self.config.listener.address).await?;
        info!("Listening on {}", acceptor.local_addr()?);

        let keys = Arc::new(enc::easy::Keys::new());
        let dead = Arc::new(AtomicBool::new(false));

        let id_gen = Arc::new(Mutex::new(IdGenerator::new()));

        let (master_send_sender, master_send_receiver) =
            mpsc::channel::<Msg>(self.config.master.channel_capacity);
        let (master_recv_sender, master_recv_receiver) =
            mpsc::channel::<Msg>(self.config.master.channel_capacity);

        let (removed_sender, removed_receiver) = mpsc::channel(2);

        let clients = Arc::new(Mutex::new(HashMap::<u64, mpsc::Sender<Msg>>::new()));
        let stale_conn_manager = StaleConnectionManager::new(self.config.clone());

        tokio::spawn({
            let dead = dead.clone();
            async move {
                if let Err(e) = handle_socket_duplex_master(
                    &mut master_duplex,
                    master_send_receiver,
                    master_recv_sender,
                    removed_receiver,
                )
                .await
                {
                    error!("Error handling master duplex: {}", e);
                    error!("FATAL: Master server is down, exiting to fail hard");
                    dead.store(true, Ordering::Relaxed);
                }
            }
        });

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

        while !dead.load(Ordering::Relaxed) {
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
        info!("Stopping server");
        let clients = clients.lock().await;
        let mut tmp = clients.values();
        while let Some(client_sender) = tmp.next() {
            if let Err(e) = client_sender.send(Msg::Stop).await {
                error!("Error sending stop message to client: {}", e);
            }
        }
        if let Err(e) = master_send_sender.send(Msg::Stop).await {
            error!("Error sending stop message to master: {}", e);
        }
        // NOTE(lion): This is a bad situation, and most likely the master server is down.
        // We should never reach this point, but if we do, we should exit with an error, so that any sane
        // monitoring system can pick it up and restart the server, and hopefully the master-server too.
        info!("Assuming this server should never go down, shutting down is considered an error, but something forced this server to shut down (probably an error)");
        Err(anyhow::format_err!("Unexpected server shutdown"))
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
        let (client, _) = match tokio::time::timeout(
            std::time::Duration::from_secs(1),
            acceptor.accept(),
        )
        .await
        {
            Ok(res) => res?,
            Err(_) => return Ok(()),
        };

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
            clients
                .lock()
                .await
                .insert(client_id, client_sender.clone());
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

async fn handle_socket_duplex_master(
    master: &mut impl MasterDuplex,
    mut send_receiver: mpsc::Receiver<Msg>,
    recv_sender: mpsc::Sender<Msg>,
    mut removed_receiver: mpsc::Receiver<Vec<u64>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            removed_client_ids = removed_receiver.recv() => {
                if let Some(client_ids) = removed_client_ids {
                    for client_id in client_ids {
                        let packet = TaggedPacket::Failure { client_id, error: "Client disconnected".to_string() };
                        if let Err(e) = master.send_packet(packet).await {
                            error!("Master: Error sending message for client {}: {}", client_id, e);
                            return Err(e.into());
                        }
                    }
                } else {
                    error!("Master: Error receiving removed client IDs");
                    return Err(anyhow::format_err!("Error receiving removed client IDs").into());
                }
            }
            packet = master.recv_packet() => {
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
                        if let Err(e) = master.send_packet(packet.clone()).await {
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
    let mut client = Client::new(client, client_receiver, master_send_sender, client_id);
    let client_id_before = client_id;
    let encryption = match client
        .try_reconnect(&keys, client_id, &stale_conn_manager)
        .await
    {
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

    if let Err(e) = client.run().await {
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
