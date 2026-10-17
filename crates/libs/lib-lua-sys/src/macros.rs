#[allow(unused_macros)]
#[macro_export]
macro_rules! cstr {
    ($s:expr) => {
        concat!($s, "\0") as *const str as *const [::std::os::raw::c_char]
            as *const ::std::os::raw::c_char
    };
}

#[macro_export]
macro_rules! lreg_raw {
    ($name:expr, $func:expr) => {
        laux::LuaReg {
            name: cstr!($name),
            func: $func,
        }
    };
}

#[macro_export]
macro_rules! lreg_null {
    () => {
        laux::LuaReg {
            name: std::ptr::null(),
            func: laux::lua_null_function,
        }
    };
}

/// Registers a lifetime-aware callback without exposing a separate wrapper
/// function at the module level.
#[macro_export]
macro_rules! lreg {
    ($name:expr, $implementation:path) => {{
        extern "C-unwind" fn callback(state: $crate::laux::LuaState) -> ::std::ffi::c_int {
            let mut lua = unsafe { $crate::laux::LuaStack::from_raw(state) };
            $implementation(&mut lua)
        }
        laux::LuaReg {
            name: cstr!($name),
            func: callback,
        }
    }};
}

/// Registers a lifetime-aware callback whose implementation returns
/// `Result<c_int, String>`. Errors are converted to Lua errors at the ABI
/// boundary, after the context borrow has ended.
#[macro_export]
macro_rules! lreg_try {
    ($name:expr, $implementation:path) => {{
        extern "C-unwind" fn callback(state: $crate::laux::LuaState) -> ::std::ffi::c_int {
            let result = {
                let mut lua = unsafe { $crate::laux::LuaStack::from_raw(state) };
                $implementation(&mut lua)
            };
            match result {
                Ok(result) => result,
                Err(error) => $crate::laux::lua_error(state, error),
            }
        }
        laux::LuaReg {
            name: cstr!($name),
            func: callback,
        }
    }};
}

#[macro_export]
macro_rules! lua_rawsetfield {
    ($state:expr, $tbindex:expr, $kname:expr, $valueexp:expr) => {
        unsafe {
            ffi::lua_pushstring($state, cstr!($kname));
            $valueexp;
            ffi::lua_rawset($state, $tbindex - 2);
        }
    };
}

#[macro_export]
macro_rules! push_lua_table {
    ($state:expr, $( $key:expr => $value:expr ),* ) => {
        unsafe {
            ffi::lua_createtable($state.as_ptr(), 0, 0);
            $(
                laux::lua_push($state, $key);
                laux::lua_push($state, $value);
                ffi::lua_settable($state.as_ptr(), -3);
            )*
        }
    };
}

#[macro_export]
macro_rules! luaL_newlib {
    ($state:expr, $l:expr) => {
        unsafe {
            ffi::lua_createtable($state.as_ptr(), 0, $l.len() as i32);
            ffi::luaL_setfuncs($state.as_ptr(), $l.as_ptr() as *const ffi::luaL_Reg, 0);
        }
    };
}

#[cfg(test)]
mod callback_tests {
    use crate::{
        ffi,
        laux::{self, LuaStack, LuaState},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Probe;
    impl Drop for Probe {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn fail(_: &mut LuaStack<'_>) -> Result<i32, String> {
        let _probe = Probe;
        Err("context failure".to_owned())
    }
    fn owned_length(lua: &mut LuaStack<'_>) -> Result<i32, String> {
        let bytes = lua.get::<Vec<u8>>(1)?;
        lua.push(bytes.len());
        Ok(1)
    }
    #[test]
    fn callbacks_drop_owned_resources_before_raising_lua_errors() {
        let state = LuaState::new(unsafe { ffi::luaL_newstate() }).unwrap();
        let _owner = laux::LuaGlobalState::new(state);
        unsafe {
            ffi::luaL_requiref(state.as_ptr(), cstr!("_G"), ffi::luaopen_base, 1);
            ffi::lua_pop(state.as_ptr(), 1);
        }
        let methods = [
            lreg_try!("fail", fail),
            lreg_try!("length", owned_length),
            lreg_null!(),
        ];
        luaL_newlib!(state, methods);
        unsafe {
            ffi::lua_setglobal(state.as_ptr(), cstr!("test"));
        }
        let status = unsafe {
            ffi::luaL_dostring(state.as_ptr(), cstr!(
            "assert(test.length('hello')==5); local ok,err=pcall(test.fail); assert(not ok and type(err)=='string'); assert(test.length('ok')==2)"
        ))
        };
        assert_eq!(status, ffi::LUA_OK);
        assert_eq!(DROPS.load(Ordering::SeqCst), 1);
        assert_eq!(laux::lua_top(state), 0);
    }
}
