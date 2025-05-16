use net::TaggedPacket;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

#[async_trait::async_trait]
pub trait MasterDuplex: Send + Sync {
    /// Send a tagged packet to the master.
    async fn send_packet(&mut self, packet: TaggedPacket) -> anyhow::Result<()>;
    /// Receive a tagged packet from the master.
    async fn recv_packet(&mut self) -> anyhow::Result<TaggedPacket>;
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

#[async_trait::async_trait]
impl MasterDuplex for TcpMasterDuplex {
    async fn send_packet(&mut self, packet: TaggedPacket) -> anyhow::Result<()> {
        net::send_tagged_packet(&mut self.write, packet).await
    }

    async fn recv_packet(&mut self) -> anyhow::Result<TaggedPacket> {
        net::recv_tagged_packet(&mut self.read).await
    }
}
