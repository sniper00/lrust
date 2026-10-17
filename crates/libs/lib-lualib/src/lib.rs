pub mod lua_crypto;
#[cfg(feature = "excel")]
pub mod lua_excel;
#[cfg(feature = "http")]
pub mod lua_http;
#[cfg(feature = "json")]
pub mod lua_json;
#[cfg(feature = "mongodb")]
pub mod lua_mongodb;
#[cfg(feature = "pg")]
pub mod lua_pg;
pub mod lua_runtime;
#[cfg(feature = "sqlx")]
pub mod lua_sqlx;
#[cfg(feature = "tiberius")]
pub mod lua_tiberius;
#[cfg(feature = "websocket")]
pub mod lua_websocket;

#[cfg(any(feature = "pg", feature = "sqlx", feature = "mongodb", feature = "tiberius"))]
mod request_pool;
mod host_bridge;
#[cfg(any(feature = "json", feature = "websocket"))]
mod owned_buffer;
#[allow(unused_imports)] // Shared helpers vary with enabled module features.
pub(crate) use host_bridge::*;
#[cfg(test)]
mod test_host;

pub fn moon_send<T>(protocol_type: u8, owner: u32, session: i64, res: T) {
    unsafe extern "C-unwind" {
        unsafe fn send_integer_message(type_: u8, receiver: u32, session: i64, val: isize);
    }

    if session == 0 {
        return;
    }
    #[cfg(test)]
    if owner == 0 { return; } // Protocol-only tests have no receiving actor.
    let ptr = Box::into_raw(Box::new(res));

    unsafe {
        send_integer_message(protocol_type, owner, session, ptr as isize);
    }
}

pub fn moon_send_bytes(protocol_type: u8, owner: u32, session: i64, data: &[u8]) {
    unsafe extern "C-unwind" {
        unsafe fn send_message(type_: u8, receiver: u32, session: i64, data: *const i8, len: usize);
    }

    unsafe {
        send_message(
            protocol_type,
            owner,
            session,
            data.as_ptr() as *const i8,
            data.len(),
        );
    }
}

pub const PTYPE_ERROR: u8 = 4;
pub const PTYPE_LOG: u8 = 13;

pub const LOG_LEVEL_ERROR: u8 = 1;
pub const LOG_LEVEL_WARN: u8 = 2;
pub const LOG_LEVEL_INFO: u8 = 3;
pub const LOG_LEVEL_DEBUG: u8 = 4;

pub fn moon_log(owner: u32, log_level: u8, data: String) {
    unsafe extern "C-unwind" {
        unsafe fn send_message(type_: u8, receiver: u32, session: i64, data: *const i8, len: usize);
    }
    let message = format!("{}{}", log_level, data);
    unsafe {
        send_message(
            PTYPE_LOG,
            owner,
            0,
            message.as_ptr() as *const i8,
            message.len(),
        );
    }
}
