use log::{debug, info};
use net::TaggedPacket;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex, RwLock,
};

use crate::{config, ids::IdGenerator};

struct StaleClient {
    encryption: enc::easy::Encryption,
    disconnected: std::time::Instant,
    missed_packets: Vec<TaggedPacket>,
}

#[derive(Clone)]
pub struct StaleConnectionManager {
    inner: Arc<StaleConnectionManagerInner>,
}

struct StaleConnectionManagerInner {
    clients: RwLock<HashMap<u64, StaleClient>>,
    config: config::Config,
    id_gen: Arc<Mutex<IdGenerator>>,
}

impl StaleConnectionManager {
    pub fn new(config: config::Config) -> Self {
        Self {
            inner: Arc::new(StaleConnectionManagerInner {
                clients: RwLock::new(HashMap::new()),
                config,
                id_gen: Arc::new(Mutex::new(IdGenerator::new())),
            }),
        }
    }

    pub async fn add_missed_packet(
        &self,
        client_id: u64,
        packet: TaggedPacket,
        limit: usize,
    ) -> anyhow::Result<()> {
        let mut clients = self.inner.clients.write().await;
        if let Some(client) = clients.get_mut(&client_id) {
            if client.missed_packets.len() >= limit {
                return Err(anyhow::anyhow!("Missed packets limit reached"));
            }
            client.missed_packets.push(packet);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Client not found"))
        }
    }

    pub async fn add_stale_client(
        &self,
        client_id: u64,
        encryption: enc::easy::Encryption,
        disconnected: std::time::Instant,
    ) {
        let mut clients = self.inner.clients.write().await;
        clients.insert(
            client_id,
            StaleClient {
                encryption,
                disconnected,
                missed_packets: Vec::new(),
            },
        );
    }

    pub async fn remove_stale_client(
        &self,
        client_id: u64,
    ) -> Option<(enc::easy::Encryption, std::time::Instant)> {
        let mut clients = self.inner.clients.write().await;
        if let Some(client) = clients.remove(&client_id) {
            Some((client.encryption, client.disconnected))
        } else {
            None
        }
    }

    pub async fn has_client(&self, client_id: u64) -> bool {
        let clients = self.inner.clients.read().await;
        clients.contains_key(&client_id)
    }

    pub async fn run_cleanup(self, on_remove: Sender<Vec<u64>>) {
        loop {
            let removed = self.cleanup().await;
            if !removed.is_empty() {
                on_remove.send(removed).await.unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_secs(
                self.inner.config.clients.stale_reap_interval_secs,
            ))
            .await
        }
    }

    async fn cleanup(&self) -> Vec<u64> {
        let mut stale_clients = self.inner.clients.write().await;

        let mut removed = Vec::new();
        let now = std::time::Instant::now();
        let mut to_remove = Vec::new();
        for (client_id, client) in stale_clients.iter_mut() {
            if now.duration_since(client.disconnected).as_secs()
                > self.inner.config.clients.stale_timeout_secs
            {
                to_remove.push(*client_id);
            }
        }
        for client_id in to_remove {
            let client = stale_clients.remove(&client_id);
            self.inner.id_gen.lock().await.release_id(client_id);
            info!("Removed stale client {} due to stale_timeout", client_id,);
            if let Some(client) = client {
                debug!(
                    "Client {} had {} missed packets queued",
                    client_id,
                    client.missed_packets.len()
                );
            }
            removed.push(client_id);
        }
        // check if we have too many stale clients
        if stale_clients.len() > self.inner.config.clients.max_stale_clients as usize {
            let mut clients = stale_clients
                .iter()
                .map(|(id, client)| (*id, client.disconnected))
                .collect::<Vec<_>>();
            // Sorting clients by their disconnection time, oldest to newest
            clients.sort_by(|(_, a), (_, b)| a.cmp(b));
            let to_remove = clients
                .iter()
                .take(clients.len() - self.inner.config.clients.max_stale_clients as usize)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            for client_id in to_remove {
                let client = stale_clients.remove(&client_id);
                self.inner.id_gen.lock().await.release_id(client_id);
                info!(
                    "Removed stale client {} due to max_stale_clients",
                    client_id
                );
                if let Some(client) = client {
                    debug!(
                        "Client {} had {} missed packets queued",
                        client_id,
                        client.missed_packets.len()
                    );
                }
                removed.push(client_id);
            }
        }
        removed
    }
}
