# Voltlane - Your TCP firehose

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
