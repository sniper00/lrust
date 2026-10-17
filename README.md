# Moon Extensions (lrust)

Rust extensions for [Moon](https://github.com/sniper00/moon), providing database
clients, HTTP and WebSocket networking, file readers, JSON, and cryptography.
Network and database operations run on Tokio workers; the Lua wrappers use
`moon.wait` to resume the calling coroutine when a response arrives.

The extension requires a **Lua 5.5 Moon host** and a Rust toolchain supporting
**edition 2024**. Lua 5.4 and the previous Rust helper API are no longer supported.
This is a Moon extension: the shared library relies on Lua and messaging symbols
exported by the host.

- [Modules](#modules)
- [Installation and build](#installation-and-build)
- [Quick start](#quick-start)
- [Usage examples](#usage-examples)
- [Results and resource lifecycle](#results-and-resource-lifecycle)
- [Development and validation](#development-and-validation)
- [Project layout and documentation](#project-layout-and-documentation)

## Modules

Use `ext.*` for asynchronous clients. These wrappers register Moon response
protocols and supply the actor ID and session number to the native bindings.

| Module | Lua entry point | Cargo feature | Capabilities |
| --- | --- | --- | --- |
| HTTP | `ext.httpc` | `http` | HTTP/HTTPS, proxy, binary bodies, forms, JSON requests, streaming, SSE |
| WebSocket | `ext.websocket` | `websocket` | WS/WSS client, server listen/accept, text/binary messages, Origin allow-list, size limits |
| SQLx | `ext.sqlx` | `sqlx` | PostgreSQL, MySQL, SQLite, bound parameters, transactions, streamed rows |
| PostgreSQL | `ext.pg` | `pg` | Native PG v3 protocol, arrays, parameter binding, transactional pipelines, bulk insert/update |
| MongoDB | `ext.mongodb` | `mongodb` | CRUD, indexes, BSON, ObjectId, streamed documents |
| SQL Server | `ext.sqlserver` | `tiberius` | Tiberius client, bound parameters, statement batches |
| Excel / CSV | `rust.excel` | `excel` | XLSX and CSV reading, sheet names, row limits |
| JSON | `luaopen_json` | `json` | Encode/decode, object/array tags, file decoding, owned concatenation buffers |
| Crypto | `rust.crypto` | Always built | AES-128-GCM / AES-256-GCM, random keys and nonces |
| Runtime | `rust.runtime` | Always built | Tokio alive-task count |

**SQLx or native PostgreSQL?** Use SQLx for a common API across PostgreSQL,
MySQL, and SQLite, or for streamed queries. Use `ext.pg` for PostgreSQL arrays,
pipelines, and bulk operations. The native PG driver currently uses plain TCP
and rejects `sslmode=require`; it has no streaming query API. SQLx supports TLS
through its driver configuration.

The host's `require("json")` continues to load Moon's JSON library. The Rust JSON
implementation is a separate exported entry point; see the loading example below.

## Installation and build

Install [Rust](https://www.rust-lang.org/tools/install), along with the compiler
and build tools required by the Moon host. From the **Moon host root**:

```sh
premake5 add --package=https://github.com/sniper00/lrust.git
premake5 build --release
```

The extension's Premake project builds the Rust shared library, installs it in
Moon's `clib/` directory, and copies `lualib/*.lua` into Moon's `lualib/ext/`.
For example, Linux builds install `target/release/librust.so` as `clib/rust.so`.

To build only the Rust library, run these commands from the **lrust directory**:

```sh
# Default modules
cargo build -p lib-lualib --release --locked

# Default modules plus SQL Server
cargo build -p lib-lualib --release --locked --features tiberius

# All modules
cargo build -p lib-lualib --release --locked --all-features

# Only HTTP and WebSocket, plus the always-built modules
cargo build -p lib-lualib --release --locked \
  --no-default-features --features http,websocket
```

A Cargo-only build does not install the library or Lua wrappers into Moon.
When installing a custom feature build, copy those outputs to the host as above;
the normal Premake build uses the default features.

Default features are `excel`, `sqlx`, `mongodb`, `websocket`, `http`, `json`, and
`pg`. SQL Server (`tiberius`) is opt-in. The `http`, `sqlx`, `pg`, and `tiberius`
features also enable `json`. Disabling a feature removes its native entry point;
its Lua wrapper cannot be used until that feature is enabled.
See [the manifest](crates/libs/lib-lualib/Cargo.toml) for the full dependency list.

## Quick start

After installing the extension, save this as `example/lrust_quickstart.lua` in
the Moon host checkout. It uses an in-memory SQLite database and needs no server:

```lua
local moon = require "moon"
local sqlx = require "ext.sqlx"

moon.async(function()
    local db = sqlx.connect("sqlite::memory:", "quickstart", 5000, 1)
    local function checked(result)
        assert(not result.kind, result.message)
        return result
    end

    checked(db:query("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)"))
    checked(db:query("INSERT INTO users VALUES (?, ?)", 1, "Alice"))
    checked(db:query("INSERT INTO users VALUES (?, ?)", 2, "Bob"))
    checked(db:query("UPDATE users SET name = ? WHERE id = ?", "Alice Updated", 1))

    local rows = checked(db:query("SELECT id, name FROM users ORDER BY id"))
    for _, row in ipairs(rows) do
        print(row.id, row.name) -- 1, Alice Updated; then 2, Bob
    end
    checked(db:query("DELETE FROM users WHERE id = ?", 2))
    assert(checked(db:query("SELECT COUNT(*) AS n FROM users"))[1].n == 1)
    db:close()
    moon.quit()
end)
```

Run it from the Moon host root:

```sh
target/release/moon example/lrust_quickstart.lua
```

## Usage examples

The networking and database snippets below belong inside a Moon coroutine,
such as the `moon.async(function() ... end)` block above. Connection URLs are
examples; supply credentials and endpoints for your environment. Each fenced
Lua block is an independent example unless stated otherwise. Wrap an async
example in `moon.async`, and call `moon.quit()` after its resources are closed
when running it as a standalone script. Keep a server's Moon process running
while it is accepting connections.

### HTTP and streaming

```lua
local httpc = require "ext.httpc"

local response = httpc.get("https://example.com", {timeout = 5000})
assert(response.status_code == 200, response.body)
print(response.body)

-- Binary-safe POST; post_json and post_form are also available.
local reply = httpc.post("http://127.0.0.1:8080/echo", "hello\0world")
print(reply.status_code)

local headers, handle = httpc.stream("http://127.0.0.1:8080/events", {
    timeout = 0,
    read_timeout = 10000,
})
assert(headers ~= false, handle)
local stream <close> = handle
while true do
    local chunk, err = stream:next()
    if not chunk then
        assert(not err, err)
        break
    end
    print(chunk)
end
```

Timeouts are in **milliseconds**. The local `/echo`, `/login`, `/users`, and
`/events` endpoints in these examples must be supplied by your application.
`get` and `post` return a response table with `status_code`, `headers`, and `body`;
check the status before consuming the body.

For JSON and form requests:

```lua
local httpc = require "ext.httpc"

local created = httpc.post_json("http://127.0.0.1:8080/users", {
    name = "Alice",
    tags = {"reader", "writer"},
}, {
    timeout = 5000,
    headers = {["X-Request-ID"] = "readme-example"},
})
assert(created.status_code == 200, tostring(created.body))
-- post_json decodes the response body only when the status is 200.
print(created.body.id)

local login = httpc.post_form("http://127.0.0.1:8080/login", {
    username = "alice",
    token = "example-token",
})
print(login.status_code, login.body)
```

`stream_sse` decodes each event's `data:` payload as JSON. This example expects
an SSE endpoint emitting JSON objects such as `data: {"message":"hello"}`:

```lua
local httpc = require "ext.httpc"

local response, handle = httpc.stream_sse("http://127.0.0.1:8080/events", {
    timeout = 0,
    read_timeout = 10000,
})
assert(response ~= false, handle)
local events <close> = handle
assert(response.status_code == 200, "unexpected SSE HTTP status")
while true do
    local event, err = events:next()
    if event == nil then
        assert(not err, err)
        break
    end
    print(event.message)
end
```

The `<close>` variable releases the stream on normal completion, early return,
or an error. A `[DONE]` SSE payload ends iteration.

### WebSocket

```lua
local websocket = require "ext.websocket"

local ws = websocket.connect("ws://127.0.0.1:8080/echo", 5000)
ws:write_text("hello")
local data, kind = ws:read(5000)
print(data, kind) -- kind: t=text, b=binary, p=ping, o=pong, c=close
ws:close()
```

The client above expects an echo server. This standalone Moon script provides
one, accepting multiple clients and handling each in a separate coroutine:

```lua
local moon = require "moon"
local websocket = require "ext.websocket"

local listener = websocket.listen("127.0.0.1:8080", {
    max_connections = 100,
    max_message_size = 1024 * 1024,
    max_frame_size = 1024 * 1024,
    -- For browser clients, set origins = {"https://your-app.example"}.
    -- When set, missing and unlisted Origin headers are rejected.
})

moon.async(function()
    while true do
        local ws, err = websocket.accept(listener)
        if not ws then
            moon.error(tostring(err))
            break
        end
        moon.async(function()
            local ok, failure = xpcall(function()
                while true do
                    local data, kind = ws:read(30000)
                    if not data or kind == "c" then break end
                    if kind == "t" then
                        ws:write_text(data)
                    elseif kind == "b" then
                        ws:write(data)
                    end
                end
            end, debug.traceback)
            ws:close()
            if not ok then moon.error(failure) end
        end)
    end
end)
```

Run this separately from the client. See the
[wrapper annotations](lualib/websocket.lua) for options and method signatures.

### SQLx: JSONB, transactions, and streaming

```lua
local sqlx = require "ext.sqlx"
local json = require "json"

-- A single connection keeps the temporary table visible between requests.
local db = sqlx.connect("postgres://user:password@127.0.0.1/app", "main", 5000, 1)
local function checked(result)
    assert(not result.kind, result.message)
    return result
end

checked(db:query([[
    CREATE TEMP TABLE demo_users (
        id integer PRIMARY KEY,
        profile jsonb NOT NULL
    )
]]))
checked(db:query("INSERT INTO demo_users VALUES ($1, $2)",
    1, {name = "Alice", active = true}))
-- Explicit JSON text; ordinary Lua strings bind as SQL text.
checked(db:query("INSERT INTO demo_users VALUES ($1, $2)",
    2, sqlx.json('{"name":"Bob","active":false}')))

local rows = checked(db:query("SELECT profile FROM demo_users WHERE id = $1", 1))
print(json.decode(rows[1].profile).name) -- Alice
checked(db:query("UPDATE demo_users SET profile = $1 WHERE id = $2",
    {name = "Alice Updated", active = true}, 1))

-- Transaction entries are {sql, parameter1, ...}.
checked(db:transaction({
    {"INSERT INTO demo_users VALUES ($1, $2)", 3, {name = "Carol"}},
    {"INSERT INTO demo_users VALUES ($1, $2)", 4, {name = "Dave"}},
}))
-- A duplicate key rolls back the entire transaction, including id=5.
local failed = db:transaction({
    {"INSERT INTO demo_users VALUES ($1, $2)", 5, {name = "Rolled back"}},
    {"INSERT INTO demo_users VALUES ($1, $2)", 1, {name = "Duplicate"}},
})
assert(failed.kind)
assert(checked(db:query("SELECT COUNT(*)::int AS n FROM demo_users"))[1].n == 4)

-- Fetch 2 rows per batch; the loop receives one row at a time.
for row in db:query_stream("SELECT id, profile FROM demo_users ORDER BY id", 2) do
    print(row.id, json.decode(row.profile).name)
    if row.id == 2 then break end -- releases the cursor on early exit
end

checked(db:query("DELETE FROM demo_users WHERE id = $1", 2))
checked(db:query("DROP TABLE demo_users"))
db:close()
```

PostgreSQL uses `$1`, `$2`, … placeholders; MySQL and SQLite use `?`.
The default pool size is five. For session-local state such as temporary tables,
pass `1` as the fourth argument to `connect(url, name, timeout, max_connections)`.
The example uses a single connection so the temporary table survives across
its separate requests. Use a regular application table when sharing a pool
across services.

Lua tables bind as JSON; `sqlx.json("null")` binds JSON null. JSON/JSONB results
are JSON strings. SQL NULL fields are absent from result rows. Integral decimals
in the signed 64-bit range become Lua integers; fractional or larger decimals
remain strings to preserve precision. Non-streaming queries cap results at
100,000 rows; use `query_stream` for larger reads.

### Native PostgreSQL: arrays and pipelines

```lua
local pg = require "ext.pg"

local db = pg.connect(
    "postgres://user:password@127.0.0.1/app?name=pg_main&max_connections=1")
assert(not db.code, db.message)

local function checked(result)
    assert(not result.code, result.message)
    return result.data
end

checked(db:query([[
    CREATE TEMP TABLE demo_items (
        id bigint PRIMARY KEY,
        ids int4[],
        doc jsonb
    )
]]))
checked(db:query_params("INSERT INTO demo_items VALUES ($1, $2::int4[], $3::jsonb)",
    1, pg.array({1, pg.NULL, 3}), {name = "Alice"}))
local rows = checked(db:query_params("SELECT ids, doc FROM demo_items WHERE id = $1", 1))
assert(rows[1].ids[2] == pg.NULL) -- NULL element keeps its array position
print(require("json").decode(rows[1].doc).name) -- Alice

-- All statements in a pipeline commit or roll back together.
checked(db:pipe({
    {"INSERT INTO demo_items VALUES ($1, $2::int4[], $3::jsonb)",
        2, pg.array({4, 5}), {name = "Bob"}},
    {"UPDATE demo_items SET doc = $1::jsonb WHERE id = $2",
        {name = "Alice Updated"}, 1},
}))

-- Bulk UPSERT: columns and update targets use structured identifiers.
checked(db:insert_many("demo_items", {"id", "ids", "doc"}, {
    {2, pg.array({6, 7}), {name = "Bob Updated"}},
    {3, pg.array({8, 9}), {name = "Carol"}},
}, {columns = {"id"}, update = {"ids", "doc"}}))

-- The first value in each row is the key. Supply SQL types for assignments.
checked(db:update_many("demo_items", "id", {"ids", "doc"}, {
    {3, pg.array({10, 11}), {name = "Carol Updated"}},
}, "bigint", {"int4[]", "jsonb"}))

rows = checked(db:query("SELECT id, ids FROM demo_items ORDER BY id"))
assert(#rows == 3 and rows[3].ids[1] == 10)
checked(db:query_params("DELETE FROM demo_items WHERE id = $1", 3))
checked(db:query("DROP TABLE demo_items"))
db:close()
```

Arrays support rectangular nested sequences up to six dimensions. Use `pg.NULL`
for NULL elements and explicit SQL casts when the type cannot be inferred.
`pipe` runs its statements in a transaction. `insert_many` and `update_many`
provide bulk writes; structured conflict options support UPSERT.
See [the PostgreSQL guide](docs/pg.md) for result formats, type mappings,
connection options, array limitations, and bulk-operation examples.

### MongoDB

```lua
local mongodb = require "ext.mongodb"

local db = mongodb.connect(
    "mongodb://127.0.0.1:27017/?serverSelectionTimeoutMS=2000", "mongo_main")
-- Use a dedicated collection for this example.
local users = db:collection("lrust_examples", "readme_users")
local function checked(result)
    assert(result and not result.kind, result and result.message or "missing result")
    return result
end
checked(users:create_coll())
checked(users:create_index({name = 1}, {name = "unique_name", unique = true}))
checked(users:insert_one({_id = 1, name = "Alice", active = true, score = 10}))
checked(users:insert_many({
    {_id = 2, name = "Bob", active = false, score = 20},
    {_id = 3, name = "Carol", active = true, score = 30},
}))

local alice = checked(users:find_one({_id = 1}))
print(alice.name, alice.score) -- Alice, 10
assert(users:find_one({_id = 999}) == nil) -- no matching document
local updated = checked(users:update_one({_id = 1}, {
    ["$set"] = {active = true},
    ["$inc"] = {score = 5},
}))
assert(updated.matched_count == 1 and updated.modified_count == 1)

local rows = checked(users:find({active = true}, {
    sort = {score = -1}, limit = 10, projection = {_id = 1, name = 1, score = 1},
}))
for _, doc in ipairs(rows) do print(doc.name, doc.score) end

for doc in db:find_stream("lrust_examples", "readme_users",
    {active = true}, {sort = {_id = 1}}, 2) do
    print(doc.name)
    break -- automatically releases the cursor
end

local deleted = checked(users:delete_one({_id = 2}))
assert(deleted.deleted_count == 1)
checked(users:delete_many({_id = {["$in"] = {1, 3}}}))
assert(checked(users:count({})).count == 0)
db:close()
```

This example creates `lrust_examples.readme_users`, inserts three documents,
and removes them at the end; the collection and index remain. Start with an empty
collection to avoid duplicate IDs on a rerun after an interrupted example.
See the [MongoDB wrapper](lualib/mongodb.lua) for supported operations.

### SQL Server

Build with `--features tiberius` before using this module:

```lua
local sqlserver = require "ext.sqlserver"

local config = sqlserver.build_connection_string({
    server = "127.0.0.1",
    database = "app",
    username = "app_user",
    password = assert(os.getenv("SQLSERVER_PASSWORD")),
})
local db = sqlserver.connect(config, "mssql_main", 5000)
local function checked(result)
    assert(not result.kind, result.message)
    return result
end

-- A table variable is scoped to this single parameterized request.
local rows = checked(db:query([[
    DECLARE @users TABLE (id int PRIMARY KEY, name nvarchar(100));
    INSERT INTO @users VALUES (@P1, @P2), (@P3, @P4);
    UPDATE @users SET name = @P5 WHERE id = @P1;
    SELECT id, name FROM @users ORDER BY id;
]], 1, "Alice", 2, "Bob", "Alice Updated"))
for _, row in ipairs(rows) do print(row.id, row.name) end
-- Prints 1, Alice Updated; then 2, Bob. No persistent table is created.

local failed = db:query("SELECT * FROM missing_readme_table")
assert(failed.kind)
print(failed.message)
-- An SQL error does not require recreating the client.
assert(checked(db:query("SELECT @P1 AS value", 42))[1].value == 42)
db:close()
```

`query` binds values using `@P1`, `@P2`, … . The legacy `transaction` method
batches trusted SQL strings and flattens result rows; it does **not** automatically
wrap them in an atomic database transaction.

### Excel and CSV

`read(filename, max_rows)` returns an array of sheets. Each sheet has
`sheet_name` and a `data` array containing rows of cell values. On a file-reading
failure it returns `false, error_message`. XLSX cell values retain supported
numeric/boolean types; CSV fields are strings, including the first row.

```lua
local excel = require "rust.excel"
local sheets, err = excel.read("example.xlsx", 1000) -- max rows per sheet
assert(sheets, err)
for _, sheet in ipairs(sheets) do
    print(sheet.sheet_name)
    for _, row in ipairs(sheet.data) do
        print(table.unpack(row))
    end
end
-- excel.read("example.csv") returns the same sheet/data structure.
```

For a reproducible CSV example, create a file before reading it:

```lua
local excel = require "rust.excel"
local file = assert(io.open("readme_example.csv", "wb"))
file:write("id,name\n1,Alice\n2,Bob\n")
file:close()
local sheets, err = excel.read("readme_example.csv", 3)
assert(sheets, err)
assert(sheets[1].data[1][1] == "id") -- header is returned as a normal row
assert(sheets[1].data[2][1] == "1")  -- CSV numbers remain strings
assert(sheets[1].data[3][2] == "Bob")
assert(os.remove("readme_example.csv"))
```

### Crypto and runtime metrics

AES-GCM returns ciphertext (including its authentication tag) and a nonce. Store
or transmit the nonce with the ciphertext; keep the key separately.

```lua
local crypto = require "rust.crypto"
local key = crypto.generate_key() -- 32 bytes; use generate_key(16) for AES-128
local ciphertext, nonce = crypto.aes_encrypt("hello\0world", key)
assert(crypto.aes_decrypt(ciphertext, key, nonce) == "hello\0world")
-- Encryption generates a random 12-byte nonce unless one is supplied.
-- Never reuse a nonce with the same key.

-- Authentication fails if ciphertext, key, or nonce does not match.
local wrong_key = crypto.generate_key()
local ok, err = pcall(crypto.aes_decrypt, ciphertext, wrong_key, nonce)
assert(not ok)
print(err)

-- AES-128 uses the same functions with a 16-byte key.
local key128 = crypto.generate_key(16)
local encrypted128, nonce128 = crypto.aes_encrypt("AES-128 example", key128)
assert(crypto.aes_decrypt(encrypted128, key128, nonce128) == "AES-128 example")

local runtime = require "rust.runtime"
print(runtime.num_alive_tasks())
```

### Rust JSON entry point

To load the Rust JSON implementation explicitly on Linux:

```lua
local open_json = assert(package.loadlib("clib/rust.so", "luaopen_json"))
local rust_json = open_json()
local text = rust_json.encode({name = "Alice"})
assert(rust_json.decode(text).name == "Alice")

local value = rust_json.decode('{"items":[1,null,3],"enabled":true}')
assert(value.items[2] == rust_json.null)
assert(value.enabled == true)

-- Catch malformed input without terminating the calling coroutine.
local ok, err = pcall(rust_json.decode, "{invalid json}")
assert(not ok)
print(err)
```

Adjust the shared-library path for your installation/platform. `concat` and
`concat_resp` return GC-owned Rust userdata. Use `buffer:bytes()`, `tostring(buffer)`,
or `#buffer` to read it. WebSocket writes can consume this userdata directly;
a consumed buffer cannot be reused. Rust buffers and C++ Moon buffers have
different ABIs; convert host buffers to Lua strings before passing them here.

### Handling failures and closing connections

For standalone scripts, keep cleanup outside the protected body so a failed
assertion also closes the pool. This complete example can be run without a server:

```lua
local moon = require "moon"
local sqlx = require "ext.sqlx"

moon.async(function()
    local db
    local ok, err = xpcall(function()
        db = sqlx.connect("sqlite::memory:", "cleanup_example", 5000, 1)
        local result = db:query("SELECT ? AS value", "hello")
        assert(not result.kind, result.message)
        print(result[1].value)
    end, debug.traceback)
    if db then db:close() end
    if not ok then moon.error(err) end
    moon.quit()
end)
```

## Results and resource lifecycle

- Run asynchronous calls in Moon coroutines and use the `ext.*` wrappers to
  handle routing and response decoding.
- SQLx, MongoDB, and SQL Server connection failures raise Lua errors. Query and
  operation failures normally return `{kind = ..., message = ...}`. Native PG
  uses `{code = ..., message = ...}` for connection and query failures, with
  successful query rows in `result.data`. Check these fields before using results.
- Reuse named connections with `find_connection(name)`. For SQLx and SQL Server,
  a missing name returns `nil`; native PG and MongoDB wrappers raise an error.
- `execute` / `execute_*` database methods are fire-and-forget. Use awaited
  queries when the caller needs confirmation. Before shutting down, wait for
  the relevant pool's `stats()` pending count to reach zero, then close it.
  A zero pending count alone does not report whether each operation succeeded.
- Close database pools, WebSockets, and HTTP streams when finished. Keep the
  fourth return value from database streaming iterators by using them directly
  in generic `for` loops; this enables cursor release on early exit and errors.
- Bind user values through placeholders. Native PG `query` and SQL Server batch
  methods accept SQL text directly; keep that text trusted.

## Development and validation

From the lrust directory:

```sh
cargo check -p lib-lualib --all-features --locked
cargo test -p lib-core --locked
```

Lua binding tests require the host's Lua library at link time. Linux example,
with `MOON_ROOT` pointing to a built Lua 5.5 Moon checkout:

```sh
export MOON_ROOT=/path/to/moon
export RUSTFLAGS="-L native=$MOON_ROOT/target/release -l static=lua -l stdc++ -l m -l dl"
cargo test -p lib-lua-sys --features lua55 --locked
cargo test -p lib-lualib --all-features --locked --lib
```

Integration scripts in `test/` run with the Moon host. PostgreSQL tests use
`MOON_PG_TEST_URL`; `LRUST_TEST_LIBRARY` can point to an uninstalled build.
See [shared module validation](docs/shared-modules.md#validation) for commands.

## Project layout and documentation

| Path | Purpose |
| --- | --- |
| `crates/libs/lib-lua-sys/` | Lua 5.5 FFI, scoped stack access, conversions, callback macros |
| `crates/libs/lib-core/` | Rust Buffer and shared Tokio runtime |
| `crates/libs/lib-lualib/` | Native extension implementations and feature configuration |
| `lualib/` | Public asynchronous Lua wrappers, installed as `ext.*` |
| `test/` | Lua regression tests and integration runners |
| `premake5.lua` | Moon build and installation integration |

- [Lua callback API and migration guide](docs/lua-context.md)
- [Shared modules, calling conventions, and Buffer ownership](docs/shared-modules.md)
- [Native PostgreSQL API, arrays, and wire protocol](docs/pg.md)
