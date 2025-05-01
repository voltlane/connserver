#ifndef VOLTLANE_CLIENTCOM_H
#define VOLTLANE_CLIENTCOM_H

//! This file is the hand-written header file for the clientcom C bindings.
//! This is hand-written to ensure clean code and to avoid unnecessary steps
//! and include documentation, so people don't ever have to look at Rust if
//! they don't want to.
//!
//! NOTE: This also means that this API varies WILDLY from the Rust api, simply
//! because it's unnecessary to introduce the same abstractions in C as in Rust.
//! This, however, doesn't mean that this library loses out on any of the
//! features.
//!
//! NOTE 2: NOTHING here is threadsafe. If you want to use this in a multithreaded
//! application, you need to use your own mutexes. It's plenty if you lock every
//! vl_* call with a mutex, and ensure that you always copy out received messages
//! before unlocking.

#include <stddef.h>

// Opaque type, there's nothing to see or do here.
typedef void vl_connection;

typedef struct {
    // The message data. This is NOT OWNING, please don't try to free it.
    // This memory is invalidated by the next call to vl_connection_receive.
    char* data;
    // The size of the data in bytes.
    // Why is this not size_t? Because https://github.com/rust-lang/rust/issues/88345
    unsigned long long size;
} vl_message;

// Creates a new voltlane connection to the given address.
//
// Returns NULL on failure.
vl_connection* vl_connection_new(const char* address);

// Closes the connection and frees the memory.
void vl_connection_free(vl_connection* conn);

// Receives a message from the server.
//
// The returned memory is managed, and does not need to be freed (doing so
// is erroneous). The memory is reused for the next message, so if you want to
// keep the message for longer, you need to copy it.
// Returns a message with a NULL data pointer on failure, otherwise blocks until
// a message is received and returns it.
vl_message vl_connection_recv(vl_connection* conn);

// Sends a message to the server.
//
// The message is copied, so you don't need to worry about the memory being
// invalidated.
// Returns 0 on success, -1 on failure.
int vl_connection_send(vl_connection* conn, const char* message, size_t size);

// Returns the last error message.
//
// This is a static buffer, so you don't need to free it.
const char* vl_get_last_error(void);

// Attempts to reconnect to the server.
//
// Call this ONLY if _recv or _send has failed, or you *know* the
// connection is gone. You can try to send on the connection to see
// if it's still okay, but either way you MUST make sure that the
// connection has failed before calling this.
int vl_connection_reconnect(vl_connection* conn);

#endif // VOLTLANE_CLIENTCOM_H
