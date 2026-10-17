#![allow(dead_code)] // Producers and consumers are enabled by separate features.
//! Owned Rust buffers must never be confused with the C++ host Buffer ABI.
use lib_core::buffer::Buffer;
use lib_lua::{
    cstr, ffi,
    laux::{self, LuaStack, LuaState},
    lreg_null, lreg_try,
};
const META: *const std::ffi::c_char = cstr!("lrust_owned_buffer");
struct OwnedBuffer(Option<Buffer>);

fn get(lua: &LuaStack<'_>, index: i32) -> Result<std::ptr::NonNull<OwnedBuffer>, String> {
    let pointer = unsafe { ffi::luaL_testudata(lua.state().as_ptr(), index, META) };
    std::ptr::NonNull::new(pointer.cast()).ok_or_else(|| "Rust buffer userdata expected".into())
}
fn bytes(lua: &mut LuaStack<'_>) -> Result<i32, String> {
    let pointer = get(lua, 1)?;
    let value = unsafe { pointer.as_ref() }
        .0
        .as_ref()
        .ok_or("buffer already consumed")?;
    laux::lua_push(lua.state(), value.as_slice());
    Ok(1)
}
fn len(lua: &mut LuaStack<'_>) -> Result<i32, String> {
    let pointer = get(lua, 1)?;
    let size = unsafe { pointer.as_ref() }
        .0
        .as_ref()
        .ok_or("buffer already consumed")?
        .len();
    lua.push(size);
    Ok(1)
}
pub(crate) fn push(state: LuaState, buffer: Buffer) {
    let methods = [
        lreg_try!("bytes", bytes),
        lreg_try!("__tostring", bytes),
        lreg_try!("__len", len),
        lreg_null!(),
    ];
    laux::lua_newuserdata(state, OwnedBuffer(Some(buffer)), META, &methods);
    // lua_newuserdata installs ordinary methods in __index. Metamethods must
    // live directly on the userdata metatable for Lua to invoke them.
    unsafe {
        ffi::lua_getmetatable(state.as_ptr(), -1);
        ffi::luaL_setfuncs(state.as_ptr(), methods.as_ptr().cast(), 0);
    }
    laux::lua_pop(state, 1);
}
pub(crate) fn take(lua: &mut LuaStack<'_>, index: i32) -> Result<Buffer, String> {
    unsafe { get(lua, index)?.as_mut() }
        .0
        .take()
        .ok_or_else(|| "buffer already consumed".into())
}
