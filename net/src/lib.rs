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
use std::io::{Bytes, Read};

use tokio::{
    io::{AsyncRead, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpStream},
};
use tokio_stream::StreamExt;
use tokio_util::{bytes::BytesMut, codec::{FramedRead, LengthDelimitedCodec}};

pub const PROTOCOL_VERSION: u32 = 1;

pub type FramedReader<T> = FramedRead<T, LengthDelimitedCodec>;

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

    pub fn from_slice(data: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        bincode::decode_from_slice(&data, bincode::config::standard()).map(|(packet, _)| packet)
    }
}

#[derive(Clone)]
pub struct TaggedPacket {
    pub client_id: u64,
    pub data: Vec<u8>,
}

/// Configures a TCP socket for performance by setting relevant socket options.
pub fn configure_performance_tcp_socket(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_linger(Some(std::time::Duration::from_secs(5)))?;
    Ok(())
}

pub fn new_framed_reader<T: AsyncRead + Unpin>(stream: T) -> FramedRead<T, LengthDelimitedCodec> {
    LengthDelimitedCodec::builder()
        // NOTE(lion): do we want .max_frame_length(1024 * 1024 * 10) or something here?
        .length_field_type::<u32>()
        .little_endian()
        .new_read(stream)
}

/// Receives a size-prefixed message.
///
/// Create a `FramedRead` with `new_framed_reader` and pass it to this function.
pub async fn recv_size_prefixed<'a, T: AsyncRead + Unpin>(
    read: &'a mut FramedRead<T, LengthDelimitedCodec>,
) -> anyhow::Result<BytesMut> {
    Ok(read
        .next()
        .await
        .ok_or_else(|| anyhow::format_err!("Connection closed or Eof"))??)
}

pub async fn send_size_prefixed<T: AsyncWriteExt + Unpin>(
    stream: &mut T,
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
pub async fn recv_tagged_packet<T: AsyncRead + Unpin>(
    read: &mut FramedRead<T, LengthDelimitedCodec>,
) -> anyhow::Result<TaggedPacket> {
    let buffer = recv_size_prefixed(read).await?;
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
