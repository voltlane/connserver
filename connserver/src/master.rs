use std::future::Future;

use net::TaggedPacket;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

pub trait MasterDuplex: Send + Sync {
    /// Send a tagged packet to the master.
    fn send_packet(&mut self, packet: TaggedPacket) -> impl Future<Output = anyhow::Result<()>> + Send;
    /// Receive a tagged packet from the master.
    fn recv_packet(&mut self) -> impl Future<Output = anyhow::Result<TaggedPacket>> + Send;
}

pub struct TcpMasterDuplex {
    write: OwnedWriteHalf,
    read: net::FramedReader<OwnedReadHalf>,
}

impl TcpMasterDuplex {
    pub async fn connect(address: &str) -> anyhow::Result<Self> {
        let mut stream = TcpStream::connect(address).await?;
        net::configure_performance_tcp_socket(&mut stream)?;
        let (read, write) = stream.into_split();
        Ok(Self {
            write: write,
            read: net::new_framed_reader(read),
        })
    }
}

impl MasterDuplex for TcpMasterDuplex {
    async fn send_packet(&mut self, packet: TaggedPacket) -> anyhow::Result<()> {
        net::send_tagged_packet(&mut self.write, packet).await
    }

    async fn recv_packet(&mut self) -> anyhow::Result<TaggedPacket> {
        net::recv_tagged_packet(&mut self.read).await
    }
}
