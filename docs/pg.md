# PostgreSQL Native Driver (`lua_pg`)

High-performance native PostgreSQL driver implementing the v3 wire protocol in Rust.

Requires the C++ Moon host built with Lua 5.5. The `pg` Cargo feature is enabled
by default; a minimal build is `cargo build -p lib-lualib --no-default-features --features pg`.
The normal extension build installs `lualib/pg.lua` as `ext.pg` and exports
`luaopen_rust_pg` from `rust.so`. It uses protocol **27**, distinct from SQLx
(23) and SQL Server (26). Callers use the same public API as the Rust Moon driver;
actor IDs and sessions are supplied by the Lua wrapper to the extension.

For testing an uninstalled build, set `LRUST_TEST_LIBRARY` to the absolute path
to `target/debug/librust.so`, and `MOON_PG_TEST_URL` to the database URL. Tests use
a single connection and temporary tables. Both `test/test_pg.lua` and
`test/test_pg_array.lua` run with the Moon host binary.

JSON and JSONB query results remain JSON strings; decode them with `json.decode`.
Lua tables bound as parameters are JSON encoded; PostgreSQL arrays use `pg.array`.
This driver uses plain TCP, matching the source implementation; it does not
implement TLS (`sslmode=require` is rejected).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Lua Actor Thread                                            │
│                                                             │
│  pg:query(sql)                                              │
│    → encode wire bytes (Parse/Bind/Describe/Execute/Sync)   │
│    → select worker (round-robin)                            │
│    → send request via mpsc channel                          │
│    → moon.wait(session) — coroutine yields                  │
│                                                             │
│  Protocol 27 message arrives → rust.pg.decode(pointer)     │
│    → parse DataRow/CommandComplete into Lua tables           │
│    → coroutine resumes with result                          │
└─────────────┬───────────────────────────────────────────────┘
              │ mpsc channel (pre-built wire buffer)
              ▼
┌─────────────────────────────────────────────────────────────┐
│ IO Runtime (tokio) — Worker Tasks (one per connection)       │
│                                                             │
│  Worker #0: TcpStream → write request → read response       │
│  Worker #1: TcpStream → write request → read response       │
│  ...                                                        │
│  Worker #N: TcpStream → write request → read response       │
│                                                             │
│  Each worker owns its TCP connection and reconnects on error │
└─────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **No PostgreSQL client crate dependency** — hand-written PG v3 wire protocol for full control.
2. **Minimal copies** — request wire bytes are built once on the Lua thread, then moved to the worker. Response bytes are parsed directly into Lua values.
3. **Connection pool** — named pools with configurable `max_connections` workers.
4. **Auto-reconnect** — workers reconnect transparently on socket errors.
5. **SCRAM-SHA-256 authentication** — implementation using SCRAM-SHA-256 (without TLS channel binding).

The receive path scans complete messages already in the socket buffer before
performing asynchronous reads. Row bodies are stored together in a
length-prefixed buffer per statement, avoiding one allocation per row; fragmented
rows are appended directly to that buffer. Header and body reads retain separate
timeout budgets, and body fragments do not restart the body deadline.

Fire-and-forget `execute_*` calls consume responses without retaining result rows,
column descriptions, or command tags. They still process database errors and
transaction status and drain through `ReadyForQuery`. Every pooled request must
finish with an idle connection. Open or failed transactions are rolled back;
an otherwise successful request that leaves a transaction open returns `25000`.
Use a single request or the transactional pipeline API for a whole transaction.
Existing message-size and row-count limits still apply.

Numeric parameters are formatted directly into request buffers. Result decoding
borrows valid UTF-8 column names and reuses Lua column keys across rows. Malformed
row descriptions or row values return a `PROTOCOL` error rather than partial
values; the response has already been drained before actor-side decoding.

## Lua API

```lua
local pg = require("ext.pg")

-- Connect (creates a named pool, async). All settings live in the URL; pool
-- options are supplied as ?param=value query parameters (name is required).
local db = pg.connect("postgres://user:pass@host:5432/dbname?name=main&max_connections=5")

-- Or find an existing pool by name
local db = pg.find_connection("main")

-- Simple query (may contain multiple statements separated by ;)
local res = db:query("SELECT * FROM users WHERE id = 1")

-- Extended query with parameters ($1, $2, ...)
local res = db:query_params("SELECT * FROM users WHERE id = $1", 42)

-- Pipeline multiple statements in a transaction
local res = db:pipe({
    {"INSERT INTO log (msg) VALUES ($1)", "hello"},
    {"UPDATE counters SET n = n + 1 WHERE id = $1", 1},
})

-- Bulk insert (auto-chunked, transactional)
local res = db:insert_many("users", {"name", "age"}, {
    {"Alice", 30},
    {"Bob", 25},
})

-- Bulk upsert
local res = db:insert_many("users", {"id", "name"}, {
    {1, "Alice Updated"},
}, "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")

-- Bulk update
local res = db:update_many("users", "id", {"name", "age"}, {
    {1, "New Name", 31},
    {2, "Bob2", 26},
}, "bigint")

-- Fire-and-forget variants (no response awaited)
db:execute("NOTIFY channel, 'payload'")
db:execute_params("INSERT INTO log (msg) VALUES ($1)", "fire-and-forget")
db:execute_pipe({...})
db:execute_insert_many(...)
db:execute_update_many(...)

-- Pool stats
local pending = db:len()         -- per-worker queue lengths
local total = pg.stats()         -- total pending across all pools

-- Close
db:close()
```

## Arrays

`pg.array(values)` encodes a dense Lua sequence as PostgreSQL array text for
bound parameters. Use explicit SQL casts when the target type cannot be inferred:

```lua
db:query_params("INSERT INTO items(ids) VALUES ($1::int4[])",
    pg.array({1, pg.NULL, 3}))
db:update_many("items", "id", {"ids"}, {
    {1, pg.array({4, 5})},
}, "bigint", {"int4[]"}) -- optional set_types, in set_columns order
```

Common built-in arrays decode to Lua sequences, including rectangular nested
arrays up to six dimensions. `pg.NULL` (equal to `json.null`) preserves NULL
array elements; an entire SQL NULL column retains the existing missing-key
behavior. Empty arrays use `{}`. Numeric array elements remain exact strings;
date/time, UUID, JSON and bytea elements retain PostgreSQL text formatting.
Bytea values are not decoded from their PostgreSQL hex/escape representation.
Unknown array OIDs retain raw text. Arrays with non-one lower bounds return a
`PROTOCOL` error rather than losing their indexing information. Sparse input,
ragged input and empty nested arrays are rejected.

Type hints (`key_type` and `set_types`) accept simple unquoted identifiers,
known multiword built-in types and `[]` suffixes; SQL expressions, quoted names,
and schema-qualified names are rejected. Connections start with `array_nulls=on`.
Before each parameterized request the driver pipelines a setting reset and drains
its response separately, preserving results and NULL elements even if a preceding
request disabled the setting. Do not disable `array_nulls` inside the same pipeline
or a function used by the request.

Run the temporary-table integration test with `MOON_PG_TEST_URL` set:
`target/release/moon ext/lrust/test/test_pg_array.lua` (from the Moon host root). Use a single connection for session-local
temporary tables.

## Result Format

Successful query:
```lua
{
    num_queries = 1,
    data = {
        { id = 1, name = "Alice", age = 30 },
        { id = 2, name = "Bob",   age = 25 },
    },
}
```

Error:
```lua
{
    code = "23505",           -- PostgreSQL SQLSTATE or "SOCKET"/"CONFIG"/"ENCODE"
    message = "duplicate key value violates unique constraint..."
}
```

Pipeline result (array of results per statement):
```lua
{
    num_queries = 4,
    data = {
        true,                  -- BEGIN
        {affected_rows = 1},    -- INSERT
        {affected_rows = 1},    -- UPDATE
        true,                  -- COMMIT
    },
}
```

## Type Mapping

| PostgreSQL Type | Lua Type |
|----------------|----------|
| `int2/int4/int8` | number (integer) |
| `float4/float8/numeric` | number (float) |
| `bool` | boolean |
| `text/varchar/char` | string |
| `json/jsonb` | string (JSON text; use `json.decode`) |
| `bytea` | string (PostgreSQL hex/escape text) |
| `timestamp/date/time` | string (ISO format) |
| `uuid` | string |
| supported built-in arrays | Lua table (nested sequence); unknown arrays remain text |
| `NULL` | nil (field absent) |

## Connection URL Format

```
postgresql://username:password@host:port/database?param=value&...
```

All connect settings are carried in the single URL. The pool settings are
supplied as `?param=value` query parameters:

| Query param | Default | Description |
|-------------|---------|-------------|
| `name` | *(required)* | Pool name for `find_connection` |
| `application_name` | `moon` | PostgreSQL `application_name` |
| `connect_timeout` | 5000ms | Connect timeout |
| `max_connections` / `pool_size` | 5 | Pool size (worker count) |
| `read_timeout` | 10000ms | Response read timeout |
| `queue_capacity` | 1024 | Per-worker bounded request queue capacity |

Example:

```
postgres://postgres:123456@127.0.0.1:5432/postgres?name=main&max_connections=8&read_timeout=20000
```

## Wire Protocol Details

- **Simple Query**: Single `'Q'` message, returns complete result set.
- **Extended Query**: Parse → Bind → Describe → Execute → Sync pipeline for parameterized queries.
- **Pipeline Mode**: Multiple Parse/Bind/Execute sequences batched in one write, wrapped in implicit `BEGIN`/`COMMIT`.
- **Bulk Operations**: Multi-row `VALUES` clauses auto-chunked to stay under the 65535 parameter limit.

## Files

| Path | Role |
|------|------|
| `crates/libs/lib-lualib/src/lua_pg.rs` | Rust driver and protocol regression tests |
| `lualib/pg.lua` | Lua wrapper with async/await |
| `crates/libs/lib-lualib/src/lua_pg/array.rs` | Array encoding and decoding |
| `test/test_pg.lua` | JSON/JSONB, sessions, transactions and pool lifecycle tests |
| `test/test_pg_array.lua` | Array read/write integration tests |
