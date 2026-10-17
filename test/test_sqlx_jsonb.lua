-- MOON_PG_TEST_URL must be set. Writes use a session-local temporary table.
-- Set LRUST_TEST_LIBRARY to test a freshly built librust.so without installing it.
local moon = require "moon"
local json = require "json"
local library = os.getenv("LRUST_TEST_LIBRARY")
if library then
    package.preload["rust.sqlx"] = assert(package.loadlib(library, "luaopen_rust_sqlx"))
end
local script_dir = debug.getinfo(1, "S").source:sub(2):match("^(.*[/\\])")
    or "" -- Moon runs from the script directory with a basename chunk name.
local sqlx = dofile(script_dir .. "../lualib/sqlx.lua")

local function checked(result)
    assert(not result.kind, (result.kind or "") .. ": " .. (result.message or ""))
    return result
end

moon.async(function()
    local db
    local ok, err = xpcall(function()
        db = sqlx.connect(assert(os.getenv("MOON_PG_TEST_URL")), "jsonb_regression", 5000, 1)
        local row = checked(db:query([[
            SELECT '{"name":"alice","nested":{"label":"中文"},"values":[1,null,3]}'::jsonb AS object,
                   '[true,"two",null]'::jsonb AS array,
                   'null'::jsonb AS json_null, NULL::jsonb AS sql_null,
                   'false'::jsonb AS boolean, '42'::jsonb AS number,
                   '"hello"'::jsonb AS string, '{"plain":true}'::json AS plain
        ]]))[1]
        assert(type(row.object) == "string" and row.object:byte(1) ~= 1)
        local object = json.decode(row.object)
        assert(object.name == "alice" and object.nested.label == "中文")
        assert(object.values[1] == 1 and object.values[3] == 3)
        local array = json.decode(row.array)
        assert(array[1] == true and array[2] == "two")
        assert(row.json_null == "null" and row.sql_null == nil)
        assert(row.boolean == "false" and row.number == "42")
        assert(json.decode(row.string) == "hello" and json.decode(row.plain).plain == true)
        row = checked(db:query("SELECT $1::jsonb AS data", {name="bound", values={1,2}}))[1]
        assert(json.decode(row.data).name == "bound")
        row = checked(db:query("SELECT $1::jsonb AS data", sqlx.json('{"enabled":true}')))[1]
        assert(json.decode(row.data).enabled == true)
        -- Owned parameters remain valid after Lua drops temporary arguments.
        local bytes = "\0\255payload"
        row = checked(db:query("SELECT $1::bytea AS data", bytes))[1]
        assert(row.data == bytes)
        checked(db:query("CREATE TEMP TABLE moon_lrust_migration(id int PRIMARY KEY, data jsonb)"))
        checked(db:transaction({
            {"INSERT INTO moon_lrust_migration VALUES ($1,$2)", 1, {name="transaction"}},
            {"INSERT INTO moon_lrust_migration VALUES ($1,$2)", 2, {name="second"}},
        }))
        row = checked(db:query("SELECT data FROM moon_lrust_migration WHERE id=$1", 1))[1]
        assert(json.decode(row.data).name == "transaction")
        local failure = db:transaction({
            {"INSERT INTO moon_lrust_migration VALUES ($1,$2)", 3, {name="rollback"}},
            {"INSERT INTO moon_lrust_migration VALUES ($1,$2)", 1, {name="duplicate"}},
        })
        assert(failure.kind)
        assert(checked(db:query("SELECT count(*)::int AS n FROM moon_lrust_migration"))[1].n == 2)
        checked(db:query("DROP TABLE moon_lrust_migration"))
        row = checked(db:query([[SELECT 42.000::numeric AS whole,
            9223372036854775808::numeric AS large, 1.125::numeric AS fraction,
            '2026-10-17 12:34:56.123456'::timestamp AS stamp,
            '00000000-0000-0000-0000-000000000001'::uuid AS id]]))[1]
        assert(row.whole == 42 and row.large == "9223372036854775808" and type(row.fraction) == "string" and row.fraction:match("^1%.1250*$"), tostring(row.whole).."/"..tostring(row.large).."/"..tostring(row.fraction))
        assert(row.stamp == "2026-10-17T12:34:56.123456")
        assert(row.id == "00000000-0000-0000-0000-000000000001")
        row = checked(db:query("SELECT $1::jsonb AS data", sqlx.json('null')))[1]
        assert(row.data == "null")
        local n = 0
        for value in db:query_stream("SELECT generate_series(1,7) AS n", 2) do
            n = n + 1; assert(value.n == n)
        end
        assert(n == 7)
        for value in db:query_stream("SELECT generate_series(1,20) AS n", 2) do
            assert(value.n == 1); break
        end
        assert(checked(db:query("SELECT 42 AS n"))[1].n == 42)
        local capped = db:query("SELECT generate_series(1,100001) AS n")
        assert(capped.kind)
        assert(checked(db:query("SELECT 43 AS n"))[1].n == 43)
        print("LRUST JSON/JSONB REGRESSION PASSED")
    end, debug.traceback)
    if db then db:close() end
    if not ok then moon.error(err) end
    moon.exit(ok and 0 or -1)
end)
moon.shutdown(function() moon.quit() end)
