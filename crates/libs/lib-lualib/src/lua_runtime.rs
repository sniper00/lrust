use lib_core::context::CONTEXT;
use lib_lua::{
    self, cstr,
    ffi::{self},
    laux::{self, LuaState}, lreg, lreg_null, luaL_newlib,
};

extern "C-unwind" fn num_alive_tasks(state: LuaState) -> i32 {
    laux::lua_push(
        state,
        CONTEXT.tokio_runtime.metrics().num_alive_tasks() as i64,
    );
    1
}

#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn luaopen_rust_runtime(state: LuaState) -> i32 {
    let l = [lreg!("num_alive_tasks", num_alive_tasks), lreg_null!()];
    luaL_newlib!(state, l);
    1
}
