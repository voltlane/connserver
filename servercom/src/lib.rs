use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[async_trait]
pub trait MasterServer {
    async fn handle_packet(
        &mut self,
        packet: net::TaggedPacket,
        send_packet: impl AsyncFnMut(net::TaggedPacket) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;
}

pub struct ServerCore<Client, Master>
where
    Master: MasterServer,
{
    pub clients: Vec<Client>,
    pub read: net::FramedReader<OwnedReadHalf>,
    pub write: OwnedWriteHalf,
    pub master: Master,
}

impl<ClientT, Master> ServerCore<ClientT, Master>
where
    Master: MasterServer,
{
    pub fn new(mut stream: TcpStream, master: Master) -> anyhow::Result<Self> {
        net::configure_performance_tcp_socket(&mut stream)?;
        let (read, write) = stream.into_split();
        let read = net::new_framed_reader(read);
        Ok(Self {
            read,
            write,
            clients: Vec::new(),
            master,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let packet = net::recv_tagged_packet(&mut self.read).await?;
            self.master
                .handle_packet(packet, async |packet| -> anyhow::Result<()> {
                    net::send_tagged_packet(&mut self.write, packet).await
                })
                .await?;
        }
    }
}
