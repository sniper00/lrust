# Lua 5.5 callback API

`lib-lua-sys` now uses the Lua 5.5 FFI bindings and the moon-base `laux` layer
directly. The old conversion trait, `LuaValue`, unbounded borrowed conversions,
legacy iterators, and the temporary compatibility namespace have been removed.
All extension modules use the context API. Shared implementation status and
host boundary differences are documented in [shared-modules.md](shared-modules.md). Load this library into a Lua 5.5 Moon
host; Lua 5.4 is no longer a supported ABI. Lua symbols are resolved from the
host, not from a bundled second Lua runtime.

## Callback registration

```rust
use lib_lua::{cstr, ffi, lreg, lreg_try, lreg_null, luaL_newlib};
use lib_lua::laux::{self, LuaStack, LuaState};

fn echo(lua: &mut LuaStack<'_>) -> Result<i32, String> {
    let bytes = lua.get::<Vec<u8>>(1)?;
    lua.push(bytes);
    Ok(1)
}

fn height(lua: &mut LuaStack<'_>) -> i32 {
    lua.push(lua.top());
    1
}

pub extern "C-unwind" fn open(state: LuaState) -> i32 {
    let methods = [lreg_try!("echo", echo), lreg!("height", height), lreg_null!()];
    luaL_newlib!(state, methods);
    1
}
```

`lreg!` wraps a context callback returning an integer; `lreg_try!` wraps one
returning `Result<i32, String>`. Errors are raised after the callback has returned
and its Rust locals have been dropped. `lreg_raw!` is reserved for raw ABI
callbacks. Exported `luaopen_*` entry points still accept `LuaState`.

## Parameters and traversal

- Use `get::<String>` for owned UTF-8 strings and `get::<Vec<u8>>` for arbitrary
  Lua bytes. Capture owned values in async tasks; the context is not Send/Sync.
- `get::<Option<T>>` reports malformed present values. `opt::<T>` also suppresses
  conversion errors. `opt_truthy` explicitly applies Lua truthiness rules.
- `lua.value(index)` borrows a rooted stack slot. Consume its borrowed string or
  bytes before mutating the stack; do not retain them beyond the callback.
- `table_cursor`, `array_cursor`, and nested cursors restore temporary slots on
  drop, including error returns. Arrays use strict dense-sequence semantics.
- Prefer lending `next()`. The table cursor's `Iterator` implementation requires
  entries to be consumed immediately: do not collect or retain entries.
- `lua_newuserdata` installs a Rust destructor in `__gc`. Create only one owning
  `LuaGlobalState` per state. Normal extension callbacks borrow the host state.

JSON encoding/decoding and MongoDB BSON conversion now use scoped cursors.
JSON decoding only parses in-memory bytes; `decode_file` is the explicit file
API. JSON/JSONB SQL columns still return JSON strings. Async connections and
requests own strings/bytes. WebSocket text uses validated/lossy UTF-8 conversion.

Raw FFI, unchecked userdata pointers, Lua allocation failures and Lua reentry
still require the documented safety invariants. Removing legacy APIs does not
make arbitrary raw pointer access safe.

## Verification

From this extension root, with `MOON_ROOT` pointing to the Lua 5.5 host repository:

```sh
RUSTFLAGS="-L native=$MOON_ROOT/target/release -l static=lua -l stdc++ -l m -l dl" \
  cargo test -p lib-lua-sys --features lua55 --locked
cargo check -p lib-lualib --all-features --locked
cargo build -p lib-lualib --all-features --locked
```

The static Lua/system link flags above describe the Linux Moon build. Other
hosts should provide their corresponding library and link flags. Run the new
library without replacing an installed extension:

```sh
LRUST_TEST_LIBRARY="$MOON_ROOT/ext/lrust/target/debug/librust.so" \
  "$MOON_ROOT/target/release/moon" test/test_lua55.lua
```

`test/test_sqlx_jsonb.lua` additionally uses `MOON_PG_TEST_URL` and checks JSONB,
binary parameters, transaction commit and rollback using a temporary table.


The native PostgreSQL driver is available as `ext.pg` with the default `pg`
feature. See [pg.md](pg.md) for its API and integration tests. Its protocol and
queue tests can run against the host's static Lua library:

```sh
RUSTFLAGS="-L native=$MOON_ROOT/target/release -l static=lua -l stdc++ -l m -l dl" \
  cargo test -p lib-lualib --no-default-features --features pg --locked --lib
```

Rust unit tests capture expected WebSocket replies at the host ABI and reject
unexpected delivery; network integration tests use the C++ host itself.
`test/test_pg.lua` and `test/test_pg_array.lua` verify actual async delivery on
the Lua 5.5 Moon host using `MOON_PG_TEST_URL`.
