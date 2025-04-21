# Voltlane

Stop letting connection management dictate the architecture of your app:

Voltlane collapses thousands of concurrent connections into one high-speed stream, slashing server complexity and attack vectors.
 Voltlane’s multiplexed stream heals dropped clients and deflects attacks, letting your backend work like every day's a sunny day.

## The Problem

You're building a product which needs `N` connections; an IoT app, a multiplayer game, an app. You've named your product, picked a solid authentication method, chose a language and tech stack. Now, you're staring down the barrel of handling N concurrent connections:

1. Concurrency tax: Every client demands a dedicated socket and a thread or task to go with it
2. Reconnect hell: Network hops and drops mean you need to solve graceful reconnection
3. Edge-layer attacks: Your public-facing raw socket endpoints are magnets for attacks

## The Solution

**Voltlane** rearchitects the transport layer:

- `N` clients are multiplexed into a single TCP stream (a "firehose") with minimal-overhead
- Cryptographically secure **re**connects: Reconnecting users are validated by solving a challenge via ECDH (k256) + XChaCha20-Poly1305 encryption.
- Packets to- and from Voltlane are tagged with unique client IDs to keep server code simple
- Fully configurable parameters, including DDoS protection

### What's the catch?

**Voltlane** is:
- NOT a load-balancer (yet)
- NOT a session manager—your app has to still handle what happens after a successful reconnect, voltlane only guarantees that it's the same client. Clients which lose connection WILL miss messages.
- NOT a source of truth for your app (that's still your backend-/master-server)
- NOT an authentication service—you will still need auth.
- NOT a cryptographically secure data stream—you have to provide encryption of your streams (for now)

## Spec

See the [SPEC.txt](./SPEC.txt).

## Configuration

All configuration is found in `connserver.toml` after first startup. You should let the server generate this file for good starting defaults, and then tweak it.

### Common Configurations

1. Simple firehose: You just want N streams converted to 1 stream. The defaults are perfectly fine for you and you might only want to adjust the log level and the `clients.stale_timeout_secs`.
2. Game server: Turn up `clients.missed_packets_buffer_size`, and set `clients.channel_capacity` to at least twice that, to make for seamless and lossless gameplay reconnection by letting missed packets be buffered and resent on reconnection. Turn down the `clients.stale_timeout_secs` to a reasonable value for intermittent connection loss.

### All Config Values

**General**:
- `general.log_level`: Logging verbosity, possible values are `Off`, `Error`, `Warn`, `Info`, `Debug`, `Trace`

**Listener**:
- `listener.address`: Address to listen on. For example `127.0.0.1:42000`, `::1:42000`, `test.example.com:6969`, etc.

**Master**:
- `master.address`: Address of the master server (your backend). For example `test.example.com:1234`, `10.12.1.0:33445`, `localhost:8080`, etc.
- `master.channel_capacity`: Capacity of the buffer for messages to the master server. Must be at least 1. The higher the number, the higher the potential memory usage. This must be high if it's likely that the master server will be hammered with messages and not keep up sometimes, to avoid blocking the connection server due to exceedingly high backpressure. This value heavily depends on your use case, so make sure to tweak it accordingly.

**Clients**:
- `clients.channel_capacity`: Capacity of the buffer for messages to and from clients. The higher this number, the more memory is used—however, if this is too small and your clients can't keep up, the excessive backpressure will slow down or block the connection server. This is one of the most powerful performance dials.
- `clients.stale_timeout_secs`: The timeout of stale clients, in seconds. When a client disconnects ungracefully, they become a stale clients for `stale_timeout_secs` seconds, during which time they are allowed to reconnect.
- `clients.stale_reap_interval_secs`: Interval at which to check for clients who have been disconnected for over `stale_timeout_secs`. For perfect second accuracy, use `1`, for marginally lower CPU usage use a higher number like `5` or `15`.
- `clients.missed_packets_buffer_size`: Size of the stale client missed packet buffer. Set to `0` to turn off missed packet buffering. If this is greater than zero, the connection server will buffer messages meant for this client, and send them to the client in case it reconnects within `stale_timeout_secs`. If this is `0`, its disabled, and the master server attempting to send a message to a stale client will immediately fail (drop) that client.
