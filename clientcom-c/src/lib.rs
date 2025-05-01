use lazy_static::lazy_static;
use std::ffi::c_int;
use std::ffi::{self, c_char, c_ulonglong, c_void};

use clientcom::Connection;
use clientcom::net;

pub static mut LAST_ERROR: *const c_char = std::ptr::null();

lazy_static! {
    static ref tokio_rt: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
}

#[repr(C)]
pub struct VlMessage {
    data: *const c_char,
    size: c_ulonglong,
}

fn save_error(context: impl std::fmt::Display, err: impl std::fmt::Display) {
    eprintln!("VOLTLANE: {}", format!("{}: {}", context, err));
    unsafe {
        LAST_ERROR = ffi::CString::new(format!("{}: {}", context, err))
            .unwrap()
            .into_raw();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn vl_get_last_error() -> *const c_char {
    unsafe { LAST_ERROR }
}

#[unsafe(no_mangle)]
pub extern "C" fn vl_connection_new(address: *const c_char) -> *const c_void {
    let address = unsafe { ffi::CStr::from_ptr(address) };
    let address = match address.to_str() {
        Ok(val) => val,
        Err(err) => {
            save_error("vl_connection_new: address is invalid", err);
            return std::ptr::null();
        }
    };
    let _guard = tokio_rt.enter();
    let conn = tokio_rt.block_on(Connection::new(address));
    match conn {
        Ok(conn) => {
            let conn = Box::new(conn);
            let conn = Box::into_raw(conn);
            return conn as *const c_void;
        }
        Err(err) => {
            save_error("vl_connection_new: creating connection failed", err);
            return std::ptr::null();
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn vl_connection_free(conn: *const c_void) {
    if conn.is_null() {
        return;
    }
    let conn = unsafe { Box::from_raw(conn as *mut Connection) };
    drop(conn);
}

#[unsafe(no_mangle)]
pub extern "C" fn vl_connection_send(
    conn: *const c_void,
    message: *const c_char,
    size: c_ulonglong,
) -> c_int {
    if conn.is_null() {
        save_error("vl_connection_send: conn is null", "conn is null");
        return -1;
    }
    let conn = unsafe { &mut *(conn as *mut Connection) };
    let message = unsafe { std::slice::from_raw_parts(message as *const u8, size as usize) };
    let _guard = tokio_rt.enter();
    let result = tokio_rt.block_on(net::send_size_prefixed(&mut conn.write, message));
    if result.is_err() {
        save_error(
            "vl_connection_send: sending message failed",
            result.err().unwrap(),
        );
        return -1;
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn vl_connection_recv(conn: *const c_void) -> VlMessage {
    if conn.is_null() {
        save_error("vl_connection_recv: conn is null", "conn is null");
        return VlMessage {
            data: std::ptr::null(),
            size: 0,
        };
    }
    let conn = unsafe { &mut *(conn as *mut Connection) };

    let _guard = tokio_rt.enter();
    let result = tokio_rt.block_on(net::recv_size_prefixed(&mut conn.read));
    match result {
        Ok(buffer) => {
            let message = VlMessage {
                data: buffer.as_ptr() as *const c_char,
                size: buffer.len() as c_ulonglong,
            };
            // NOTE(lion): we dont need to mem::forget or anything, since the buffer is
            // BytesMut, which is a reference to the internal buffer. The C API we expose
            // documents that this buffer is only valid until the next call to vl_connection_recv,
            // which is accurate.
            return message;
        }
        Err(err) => {
            save_error("vl_connection_recv: receiving message failed", err);
            return VlMessage {
                data: std::ptr::null(),
                size: 0,
            };
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn vl_connection_reconnect(
    conn: *const c_void,
) -> c_int {
    if conn.is_null() {
        save_error("vl_connection_reconnect: conn is null", "conn is null");
        return -1;
    }
    let conn = unsafe { &mut *(conn as *mut Connection) };
    let _guard = tokio_rt.enter();
    let result = tokio_rt.block_on(conn.reconnect());
    if result.is_err() {
        save_error(
            "vl_connection_reconnect: reconnecting failed",
            result.err().unwrap(),
        );
        return -1;
    }
    0
}
