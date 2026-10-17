//! Glue for the C++ Moon host; shared drivers do not depend on Rust Moon actors.
// Different feature sets use different parts of this bridge.
#![allow(dead_code)]
use lib_lua::{
    ffi,
    laux::{self, LuaStack, LuaState},
};
use std::sync::atomic::{AtomicI64, Ordering};

pub(crate) struct Limits {
    pub max_network_read_bytes: usize,
    pub listener_connections: usize,
    pub db_query_rows: usize,
    pub db_stream_batch_rows: i64,
    pub request_queue_capacity: usize,
    pub http_client_timeout_ms: u64,
    pub db_pool_size: u32,
}
pub(crate) const LIMITS: Limits = Limits {
    max_network_read_bytes: 512 * 1024 * 1024,
    listener_connections: 100_000,
    db_query_rows: 100_000,
    db_stream_batch_rows: 100,
    request_queue_capacity: 1024,
    http_client_timeout_ms: 10_000,
    db_pool_size: 5,
};

pub(crate) fn take_route(lua: &mut LuaStack<'_>, index: i32) -> Result<(u32, i64), String> {
    let owner = lua.get::<u32>(index)?;
    let session = lua.get::<i64>(index + 1)?;
    unsafe {
        ffi::lua_remove(lua.state().as_ptr(), index + 1);
        ffi::lua_remove(lua.state().as_ptr(), index);
    }
    Ok((owner, session))
}

pub(crate) fn lua_push_error_tuple(state: LuaState, message: &str) -> i32 {
    laux::lua_push(state, false);
    laux::lua_push(state, message);
    2
}

/// Response pointers originate in moon_send and must be consumed exactly once.
pub(crate) unsafe fn take_response<T>(lua: &LuaStack<'_>) -> Result<Box<T>, String> {
    let pointer = lua.get::<isize>(1)? as *mut T;
    if pointer.is_null() || !(pointer as usize).is_multiple_of(std::mem::align_of::<T>()) {
        return Err("invalid response pointer".into());
    }
    Ok(unsafe { Box::from_raw(pointer) })
}

pub(crate) fn response_error(_: u32, owner: u32, session: i64, error: String) {
    if session < 0 {
        crate::moon_send_bytes(crate::PTYPE_ERROR, owner, -session, error.as_bytes());
    } else {
        log::error!("{error}");
    }
}

pub(crate) fn next_net_fd() -> i64 {
    static NEXT: AtomicI64 = AtomicI64::new(1);
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |fd| fd.checked_add(1))
        .expect("network descriptor overflow")
}
