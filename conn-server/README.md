## Goals:

- handle N clients and forward all messages to 1 master server N:1
- forward all master server messages to all N clients 1:N
- tag each message to/from the master server with a unique client id
- keep track of connections, reconnect them when they come back
- get close to memory speed (closer to 400 MB/s than 6MB/s)
