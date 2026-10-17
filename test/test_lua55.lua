-- Run using a Lua 5.5 Moon host with LRUST_TEST_LIBRARY pointing to librust.so.
-- This test exercises local extension APIs without contacting external services.
local moon = require "moon"
local library = assert(os.getenv("LRUST_TEST_LIBRARY"), "LRUST_TEST_LIBRARY is required")
local function load(name)
    return assert(package.loadlib(library, name))()
end

local ok, err = xpcall(function()
    assert(_VERSION == "Lua 5.5", "a Lua 5.5 host is required")
    local json = load("luaopen_json")
    local sample = {name="中文", enabled=true, scores={1,2,3}, nested={empty=json.null}}
    local decoded = json.decode(json.encode(sample))
    assert(decoded.name==sample.name and decoded.enabled and decoded.scores[3]==3)
    assert(decoded.nested.empty==json.null)
    assert(json.encode(json.array({}))=="[]" and json.encode(json.object({}))=="{}")
    assert(json.decode('null')==json.null)
    for _=1,50 do
        assert(not pcall(json.encode, {nested={function() end}}))
        assert(not pcall(json.decode, '{broken}'))
        assert(not pcall(json.decode, '{} trailing'))
        assert(json.decode(json.encode({value=42})).value==42)
    end
    local trap=setmetatable({value=7}, {
        __pairs=function() error('must use raw traversal') end,
        __index=function() error('must not invoke __index') end,
    })
    assert(json.decode(json.encode(trap)).value==7)

    local crypto=load("luaopen_rust_crypto")
    local key=crypto.generate_key(32)
    local nonce=crypto.generate_nonce()
    local message="binary\0payload\255中文"
    local ciphertext, used_nonce=crypto.aes_encrypt(message,key,nonce)
    assert(crypto.aes_decrypt(ciphertext,key,used_nonce)==message)
    assert(not pcall(crypto.aes_encrypt, message, 'short'))
    assert(not pcall(crypto.aes_decrypt, ciphertext, key, 'short'))
    assert(not pcall(crypto.aes_encrypt, {}, key))

    local http=load("luaopen_rust_httpc")
    local fields={name="中文", value="space + comma, equals="}
    local form=http.form_urldecode(http.form_urlencode(fields))
    assert(form.name==fields.name and form.value==fields.value)
    assert(http.form_urlencode({raw="\255"}):find("%%FF"))
    assert(not pcall(http.form_urlencode, 'not a table'))
    assert(not pcall(http.form_urldecode, {}))
    assert(not pcall(http.request, 'not a request',21))

    local sqlx=load("luaopen_rust_sqlx")
    for _=1,50 do
        local tx=sqlx.make_transaction()
        tx:push('SELECT $1', {value="owned"})
    end
    collectgarbage('collect')
    assert(sqlx.find_connection('absent')==nil)
    assert(not pcall(sqlx.find_connection, {}))
    assert(not pcall(sqlx.connect, 23, 1, 1, {}, 'bad'))

    local websocket=load("luaopen_rust_websocket")
    assert(websocket.find_connection(0)==nil)
    assert(not pcall(websocket.find_connection, {}))
    local mongo=load("luaopen_rust_mongodb")
    assert(mongo.find_connection('absent')==nil)
    assert(not pcall(mongo.find_connection, {}))
    local excel=load("luaopen_rust_excel")
    assert(not pcall(excel.read, {}))
    local tiberius=load("luaopen_rust_tiberius")
    assert(tiberius.find_connection('absent')==nil)
    assert(not pcall(tiberius.find_connection, {}))
    local runtime=load("luaopen_rust_runtime")
    assert(type(runtime.num_alive_tasks())=='number')
    collectgarbage('collect')
    print('LRUST LUA 5.5 MIGRATION PASSED')
end, debug.traceback)
if not ok then moon.error(err) end
moon.exit(ok and 0 or -1)
moon.shutdown(function() moon.quit() end)
