-- Session-local writes only. Set MOON_PG_TEST_URL and optionally LRUST_TEST_LIBRARY.
local moon = require "moon"
local json = require "json"
local library = os.getenv("LRUST_TEST_LIBRARY")
if library then
    package.preload["rust.pg"] = assert(package.loadlib(library, "luaopen_rust_pg"))
end
local script_dir = debug.getinfo(1,"S").source:sub(2):match("^(.*[/\\])") or ""
local pg = dofile(script_dir .. "../lualib/pg.lua")
local function checked(result)
    assert(not result.code, (result.code or "") .. ": " .. (result.message or ""))
    return result.data
end
moon.async(function()
    local db
    local ok, err = xpcall(function()
        assert(_VERSION == "Lua 5.5")
        local bad = pg.connect("invalid://localhost/test?name=invalid")
        assert(bad.code == "CONFIG")
        local url = assert(os.getenv("MOON_PG_TEST_URL"), "MOON_PG_TEST_URL is required")
        db = pg.connect(url .. (url:find("?",1,true) and "&" or "?") ..
            "name=pg_integration&max_connections=1&connect_timeout=5000")
        assert(db.obj and not db.code)
        local row = checked(db:query([[
            SELECT '{"nested":{"name":"中文"},"values":[1,null]}'::jsonb AS doc,
                '[true,"two"]'::json AS list, 'null'::jsonb AS json_null,
                NULL::jsonb AS sql_null, 'false'::jsonb AS boolean,
                '42'::jsonb AS number, '"hello"'::jsonb AS string
        ]]))[1]
        assert(json.decode(row.doc).nested.name == "中文")
        assert(json.decode(row.list)[2] == "two")
        assert(row.json_null == "null" and row.sql_null == nil)
        assert(row.boolean == "false" and row.number == "42" and json.decode(row.string) == "hello")
        row = checked(db:query_params("SELECT $1::jsonb AS doc, $2::json AS list",
            {name="bound",enabled=true}, '[1,"two"]'))[1]
        assert(json.decode(row.doc).name == "bound" and json.decode(row.list)[2] == "two")
        checked(db:query("CREATE TEMP TABLE moon_lrust_pg(id bigint PRIMARY KEY, doc jsonb)"))
        local shared = pg.find_connection("pg_integration")
        shared:execute("INSERT INTO moon_lrust_pg VALUES (1, '{\"kind\":\"simple\"}')")
        db:execute_params("INSERT INTO moon_lrust_pg VALUES ($1,$2::jsonb)",2,{kind="params"})
        db:execute_pipe({{"INSERT INTO moon_lrust_pg VALUES ($1,$2::jsonb)",3,{kind="pipe"}}})
        db:execute_insert_many("moon_lrust_pg",{"id","doc"},{{4,{kind="bulk"}}})
        db:execute_update_many("moon_lrust_pg","id",{"doc"},{{4,{kind="updated"}}},"bigint",{"jsonb"})
        local rows = checked(db:query("SELECT * FROM moon_lrust_pg ORDER BY id"))
        assert(#rows == 4 and json.decode(rows[1].doc).kind == "simple")
        assert(json.decode(rows[2].doc).kind == "params" and json.decode(rows[3].doc).kind == "pipe")
        assert(json.decode(rows[4].doc).kind == "updated")
        local failed = db:pipe({
            {"INSERT INTO moon_lrust_pg VALUES ($1,$2::jsonb)",5,{rollback=true}},
            {"INSERT INTO moon_lrust_pg VALUES ($1,$2::jsonb)",1,{duplicate=true}},
        })
        assert(failed.code == "23505")
        assert(checked(db:query("SELECT count(*)::int AS n FROM moon_lrust_pg"))[1].n == 4)
        checked(db:pipe({{"INSERT INTO moon_lrust_pg VALUES ($1,$2::jsonb)",5,{committed=true}}}))
        checked(db:query("LISTEN moon_lrust_pg_test; NOTIFY moon_lrust_pg_test, 'payload'"))
        local stat = pg.stats().pg_integration
        assert(stat.workers == 1 and stat.total >= 10 and stat.peak >= 1)
        assert(#db:len() == 1)
        checked(db:query("DROP TABLE moon_lrust_pg"))
        db:close()
        assert(pg.stats().pg_integration == nil)
        assert(not pcall(pg.find_connection,"pg_integration"))
        assert(db:query("SELECT 1").code == "SOCKET")
        print("LRUST PG INTEGRATION PASSED")
    end, debug.traceback)
    if db then db:close() end
    if not ok then moon.error(err) end
    moon.exit(ok and 0 or -1)
end)
moon.shutdown(function() moon.quit() end)
