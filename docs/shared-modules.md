# Shared module migration

The implementations below now follow `moon_rs_origin`, adapted to the Lua 5.5
C++ Moon host. Shared callback and Buffer code live in `lib-lua-sys` and
`lib-core`. No Rust Moon actor context is embedded in this extension.

| Source | Extension | Migrated behavior |
| --- | --- | --- |
| `moon-base/src/laux.rs`, `macros.rs`, `lua55/` | `lib-lua-sys/src/` | Scoped Lua access, owned conversions, fallible callbacks, Lua 5.5 ABI |
| `moon-base/src/buffer.rs` | `lib-core/src/buffer.rs` | Buffer safety checks, spare-capacity writes, allocation reuse, ownership transfer with unread offset |
| `lua_websocket.rs` | `lua_websocket.rs`, `ext.websocket` | Client/server, listen/accept, Origin allow-list, message/buffer limits, close handshake and queued-reader cleanup |
| `lua_httpc.rs`, `moon/http/client.lua` | `lua_http.rs`, `ext.httpc` | Proxy-keyed client reuse, binary bodies, body limits, HTTP parsers, streaming, idle timeouts, SSE (including mixed LF/CRLF event separators) |
| `lua_sqlx.rs`, `moon/db/sqlx.lua` | `lua_sqlx.rs`, `ext.sqlx` | Decimal/binary/UUID/JSON conversion, JSON parameters, queue limits/stats, streaming cursors, retries and close/replacement cleanup |
| `lua_mongodb.rs`, `moon/db/mongodb.lua` | `lua_mongodb.rs`, `ext.mongodb` | BSON/options conversion, bounded results and queues, streaming cursors, retry policy, stats and close/replacement cleanup |
| `lua_excel.rs` | `lua_excel.rs`, `rust.excel` | Scoped callback, table building, CSV/XLSX and row limits; CSV reads do not exhaust the reader during preallocation |
| `lua_json.rs` | `lua_json.rs`, `luaopen_json` | Shared encoder/scoped decoder, object/array tags, file decode; Rust decoder replaces the source's host-specific yyjson entry point |
| `lua_pg.rs`, `lua_pg/array.rs` | `lua_pg.rs`, `ext.pg` | Native driver, arrays, bulk writes, pipelines and pool lifecycle; see [pg.md](pg.md) |

Crypto, SQL Server/Tiberius and runtime metrics are extension-specific and retain
their implementations on the new callback API. Rust Moon actor/socket/server
modules without an existing extension counterpart are outside this migration.

## Calling conventions

Use the `ext.*` Lua wrappers. They supply `moon.id` and `moon.next_sequence()`;
regular native callbacks receive these two routing arguments before their
original arguments, or immediately after `self` for userdata methods. Cursor
`next()` calls also carry explicit routing. Async response unpacking uses the
extension's own `decode(pointer)` callback and consumes the response once.
Protocols remain HTTP 21, SQLx 23, MongoDB 24, WebSocket 25, SQL Server 26 and PG 27.
The C++ host's exported send functions return void, so immediate delivery
rejection cannot be detected as it can in the Rust host.

Notable public API changes follow the source implementation:

- HTTP `timeout` and streaming `read_timeout` are **milliseconds**. Streaming and
  SSE handles support `close()` and Lua 5.5 to-be-closed values.
- SQLx defaults to five pool connections; use `connect(url, name, 5000, 1)` for
  session-local temporary tables. Non-streaming queries cap results at 100,000 rows.
- SQLx Lua tables bind as JSON. Use `sqlx.json(json_string)` for JSON strings and
  scalar JSON values (for example `sqlx.json("null")`); ordinary strings bind as text.
  JSON/JSONB results are JSON strings. Integral decimals in i64 range become Lua
  integers; fractional/out-of-range decimals remain strings. PostgreSQL decimal
  string formatting can include trailing zeros.
- `sqlx.find_connection(name)` returns a wrapped connection so routing is supplied
  consistently. `query_stream` and MongoDB `find_stream` expose generic-for iterators
  with automatic cursor release on early exit.

## Rust Buffer ownership

Rust and C++ Moon Buffers have different ABIs. The Rust JSON extension's
`concat` and `concat_resp` return **Rust userdata**, not raw lightuserdata.
Use `buffer:bytes()` or `tostring(buffer)` for a Lua string, and `#buffer` for its
unread length. WebSocket `write`, `write_text`, and `write_ping` accept strings or
these Rust buffers. Writing transfers ownership without copying; reuse of a
consumed buffer raises an error. Unsent buffers are freed by Lua GC. Host Buffer
lightuserdata is rejected. Convert a C++ Buffer to a Lua string using the host's
buffer API before passing it into this extension.

The host's existing `require("json")` remains its own library. To use the Rust
JSON implementation directly without replacing it, load the `luaopen_json`
entry point from the built shared library.

## Validation

From this extension directory, with `MOON_ROOT` pointing to the C++ host:

```sh
cargo build -p lib-lualib --all-features --locked
cargo test -p lib-core --locked
RUSTFLAGS="-L native=$MOON_ROOT/target/release -l static=lua -l stdc++ -l m -l dl" \
  cargo test -p lib-lua-sys --features lua55 --locked
RUSTFLAGS="-L native=$MOON_ROOT/target/release -l static=lua -l stdc++ -l m -l dl" \
  cargo test -p lib-lualib --all-features --locked --lib
```

Rust unit tests also cover queued WebSocket reader shutdown, request accounting
and SQLite row conversion.

Set `LRUST_TEST_LIBRARY` to the absolute `target/debug/librust.so` path and
`MOON_PG_TEST_URL` to the database URL, then run `test/test_sqlx_jsonb.lua`,
`test/test_pg.lua` and `test/test_pg_array.lua` with the Moon host. These use
session-local temporary tables. SQLx tests cover JSON/JSONB, decimals, binary,
UUID, transactions, result limits, full streaming and early cursor close.
