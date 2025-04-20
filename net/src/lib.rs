//! HERE BE DRAGONS, IF YOU TOUCH THIS CODE YOU WILL BE CURSED
//!
//! This module provides utilities for working with TCP sockets in an asynchronous context using Tokio.
//! It handles sending and receiving size-prefixed messages and packets with client IDs. The module
//! ensures cancellation safety where applicable, allowing for robust handling of asynchronous operations.
//!
//! If you call ANYTHING from this module, make SURE to read the notes about cancellation safety in each
//! doc comment. If you don't, you will be cursed and your code will be haunted by the ghosts of the braincells
//! who had to die in the tens of hours of work debugging and testing this code to make sure it's cancellation safe
//! where needed.
//!
//! If you want to touch this, read up on cancellation safety in Rust and the Tokio documentation, first.
//! Then, grab a {drink_of_choice}, block a time slot of a few hours, and enjoy yourself.
//! Finally, make sure you can explain why the code is the way it is before you change it. And please, don't
//! assume that it was ever correct; so if you find a bug, it's probably real.
//!
use tokio::{
    io::AsyncReadExt as _,
    io::AsyncWriteExt as _,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, bincode::Encode, bincode::Decode)]
pub enum ClientServerPacket {
    /// Bidirectional
    /// used for checking the connection after reconnect
    Ping,
    /// Unidirectional Client -> Server
    ProtocolVersion(u32),
    /// Bidirectional
    PubKey(Vec<u8>),
    /// Unidirectional Server -> Client
    ClientId(u64),
    /// Unidirectional Server -> Client
    Challenge(Vec<u8>),
    /// Unidirectional Client -> Server
    ChallengeResponse(Vec<u8>),
}

impl ClientServerPacket {
    pub fn into_vec(self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::encode_to_vec(self, bincode::config::standard())
    }

    pub fn from_vec(data: Vec<u8>) -> Result<Self, bincode::error::DecodeError> {
        bincode::decode_from_slice(&data, bincode::config::standard()).map(|(packet, _)| packet)
    }
}

#[derive(Clone)]
pub struct TaggedPacket {
    pub client_id: u64,
    pub data: Vec<u8>,
}

/// Configures a TCP socket for performance by setting relevant socket options.
pub fn configure_performance_tcp_socket(
    stream: &mut TcpStream,
) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_linger(Some(std::time::Duration::from_secs(5)))?;
    Ok(())
}

/// Peeks all data from the stream until the buffer is filled.
///
/// # Cancellation Safety
///
/// This method IS cancellation safe, however it does not commit the read
/// it performs. The caller must commit the read later to "confirm" it.
async fn peek_all(
    stream: &mut OwnedReadHalf,
    buf: &mut [u8],
) -> anyhow::Result<()> {
    let mut total_read = 0;
    while total_read < buf.len() {
        total_read = stream.peek(buf).await?;
        if total_read == 0 {
            return Err(anyhow::format_err!("Connection closed or Eof"));
        }
    }
    Ok(())
}

/// Receives a size-prefixed message.
///
/// # Cancellation Safety
///
/// This method IS cancellation safe. It peeks the data until it knows that enough data is available,
/// and then reads it in a cancellation safe way.
pub async fn recv_size_prefixed(
    stream: &mut OwnedReadHalf,
    buffer: &mut Vec<u8>,
) -> anyhow::Result<()> {
    let mut size_buf = [0u8; 4];
    // NOTE(lion): peek here to make sure we can get cancelled in the next read
    // without violating cancel safety
    peek_all(stream, &mut size_buf).await?;

    let size = u32::from_le_bytes(size_buf) as usize;
    if size == 0 {
        return Err(anyhow::format_err!("Packet too small"));
    }

    // NOTE(lion): we need to peek the size + 4 bytes for the size itself,
    // because we need to read the size and the data in a cancellation safe way
    // otherwise we could read the size and then get cancelled before reading
    // the data, which would leave us with a partial read
    buffer.reserve(size + 4);
    unsafe {
        buffer.set_len(size + 4);
    }
    peek_all(stream, buffer).await?;

    let n = stream.read(buffer).await?;
    if n == 0 {
        return Err(anyhow::format_err!("Connection closed or Eof"));
    }
    if n != buffer.len() {
        return Err(anyhow::format_err!("Partial read, expected {} got {} byte(s)", buffer.len(), n));
    }

    // NOTE(lion): we need to remove the size bytes from the buffer
    // because we only want the data. We need to avoid a copy here, too
    // because we don't want to allocate a new buffer.
    buffer.drain(..4);

    Ok(())
}

/// Sends a size-prefixed message.
///
/// # Cancellation Safety
///
/// This method is NOT cancellation safe.
pub async fn send_size_prefixed(
    stream: &mut OwnedWriteHalf,
    message: &[u8],
) -> anyhow::Result<()> {
    let size = message.len() as u32;
    let size_bytes = size.to_le_bytes();
    let mut combined_message = Vec::with_capacity(4 + message.len());
    combined_message.extend_from_slice(&size_bytes);
    combined_message.extend_from_slice(message);
    stream.write_all(&combined_message).await?;
    Ok(())
}

/// Receives a packet with a client id.
///
/// # Cancellation Safety
///
/// This method IS cancellation safe. It peeks the data until it knows that enough data is available,
/// and then reads it in a cancellation safe way.
pub async fn recv_tagged_packet(
    stream: &mut OwnedReadHalf,
    buffer: &mut Vec<u8>,
) -> anyhow::Result<TaggedPacket> {
    recv_size_prefixed(stream, buffer).await?;
    if buffer.len() < 8 {
        return Err(anyhow::format_err!("Packet too small"));
    }
    let client_id = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
    let buf = buffer[8..].into();

    Ok(TaggedPacket {
        client_id,
        data: buf,
    })
}

/// Sends a packet with a client id.
///
/// # Cancellation Safety
///
/// This method is NOT cancellation safe.
pub async fn send_tagged_packet(
    stream: &mut OwnedWriteHalf,
    packet: TaggedPacket,
) -> anyhow::Result<()> {
    let size = packet.data.len() as u32 + 8;
    let size_bytes = size.to_le_bytes();
    let client_id_bytes = packet.client_id.to_le_bytes();

    let mut combined_message = Vec::with_capacity(4 + 8 + packet.data.len());
    combined_message.extend_from_slice(&size_bytes);
    combined_message.extend_from_slice(&client_id_bytes);
    combined_message.extend_from_slice(&packet.data);

    stream.write_all(&combined_message).await?;
    Ok(())
}
