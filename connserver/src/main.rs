use std::{collections::HashMap, sync::Arc};

use ids::IdGenerator;
use log::{error, info};
use net::{ClientServerPacket, LowLevelPacket};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::{mpsc, Mutex},
};

mod args;
mod ids;

#[derive(Clone)]
enum Msg {
    Stop,
    Data(LowLevelPacket),
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
                        if let Err(e) = recv_sender.send(Msg::Data(LowLevelPacket { client_id, data: buffer.clone() })).await {
                            error!("Slave: Error sending message for client {}: {}", client_id, e);
                            return Err(e.into());
                        }
                    }
                    Err(e) => {
                        error!("Slave: Error receiving packet for client {}: {}", client_id, e);
                        break;
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
            packet = net::recv_packet(&mut read, &mut buffer) => {
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
                        if let Err(e) = net::send_packet(&mut write, packet.clone()).await {
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
}

async fn multi_client_echo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut master = TcpStream::connect(("127.0.0.1", 42001)).await?;
    net::configure_performance_tcp_socket(&mut master)?;

    let acceptor = TcpListener::bind(("0.0.0.0", 42000)).await?;
    info!("Listening on {}", acceptor.local_addr()?);

    let keys = Arc::new(enc::easy::Keys::new());

    let mut id_gen = IdGenerator::new();

    let (master_send_sender, master_send_receiver) = mpsc::channel::<Msg>(32_768);
    let (master_recv_sender, mut master_recv_receiver) = mpsc::channel::<Msg>(32_768);

    tokio::spawn({
        async move {
            if let Err(e) =
                handle_socket_duplex_master(master, master_send_receiver, master_recv_sender).await
            {
                error!("Error handling master duplex: {}", e);
            }
        }
    });

    let clients = Arc::new(Mutex::new(HashMap::<u64, mpsc::Sender<Msg>>::new()));
    let stale_clients = Arc::new(Mutex::new(HashMap::<u64, StaleClient>::new()));

    tokio::spawn({
        let clients = clients.clone();
        let stale_clients = stale_clients.clone();
        async move {
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
                                    if let Err(e) =
                                        client_sender.send(Msg::Data(packet.clone())).await
                                    {
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
                            break;
                        }
                    }
                }
            }
        }
    });

    loop {
        let (mut client, _) = acceptor.accept().await?;
        net::configure_performance_tcp_socket(&mut client)?;

        let client_id = id_gen.next_id();
        info!("Accepted client connection id={}", client_id);
        // these are messages from master to client
        let (client_sender, client_receiver) = mpsc::channel::<Msg>(8_192);
        {
            clients.lock().await.insert(client_id, client_sender);
        }
        tokio::spawn({
            let master_send_sender = master_send_sender.clone();
            let stale_clients = stale_clients.clone();
            let keys = keys.clone();
            async move {
                let (mut read, mut write) = client.into_split();
                // handle id and pubkey exchange
                // first, send our pubkey
                // then receive their pubkey
                // then send their id
                // then start the duplex
                let mut buffer = Vec::new();
                if let Err(e) = net::send_size_prefixed(&mut write, &keys.pubkey_to_bytes()).await {
                    error!("Error sending pubkey to client {}: {}", client_id, e);
                    return;
                }
                if let Err(e) = net::recv_size_prefixed(&mut read, &mut buffer).await {
                    error!("Error receiving pubkey from client {}: {}", client_id, e);
                    return;
                }
                if buffer.len() < 8 {
                    error!(
                        "Error receiving pubkey from client {}: too small",
                        client_id
                    );
                    return;
                }
                let their_pubkey = match enc::easy::pubkey_from_bytes(&buffer[0..8]) {
                    Ok(pubkey) => Some(pubkey),
                    Err(e) => {
                        error!("Error parsing pubkey from client {}: {}", client_id, e);
                        return;
                    }
                };

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
                    if let Some(their_pubkey) = their_pubkey {
                        let mut stale_clients = stale_clients.lock().await;
                        stale_clients.insert(
                            client_id,
                            StaleClient {
                                encryption: keys.create_encryption(&their_pubkey),
                            },
                        );
                    }
                }

            }
        });
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .format_timestamp(None)
        .init();

    let args = args::Args::from_env();
    if let Err(e) = args {
        error!("Error: {}. Try --help.", e);
        std::process::exit(1);
    }

    if let Err(err) = multi_client_echo().await {
        error!("Error: {}", err);
        std::process::exit(1);
    }
}
