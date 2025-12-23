---@diagnostic disable: inject-field, undefined-global
local moon = require "moon"
---@type any
local c = require "rust.sqlx"

local protocol_type = 23

moon.register_protocol {
    name = "database",
    PTYPE = protocol_type,
    pack = function(...) return ... end,
    unpack = function(val)
        return c.decode(val)
    end
}

---@class SqlX
local M = {}

--- Connect to a database
--- Supported database types: MySQL (mysql://), PostgreSQL (postgres://), SQLite (sqlite://)
--- For SQLite, the database will be automatically created if it doesn't exist
---@nodiscard
---@param database_url string Database connection URL, e.g. "postgres://postgres:123456@localhost/postgres"
---@param name string Connection name for finding by other services
---@param timeout? integer Connection timeout in milliseconds. Default 5000ms
---@return SqlX Returns a database connection object
function M.connect(database_url, name, timeout)
    local res = moon.wait(c.connect(protocol_type, moon.id, moon.next_sequence(), database_url, name, timeout))
    if res.kind then
        error(string.format("connect database failed: %s", res.message))
    end
    return M.find_connection(name)
end

--- Find an existing database connection by name
--- Returns a connection object that was previously created with M.connect
---@nodiscard
---@param name string Connection name
---@return SqlX Returns the database connection object
function M.find_connection(name)
    local o = {
        obj = c.find_connection(name)
    }
    return setmetatable(o, { __index = M })
end

--- Get statistics for all database connections
--- Returns a table with connection names as keys and pending query counts as values
--- The counter tracks the number of queries currently in the processing queue
---
--- IMPORTANT: When shutting down and you need to ensure all data is persisted to the database,
--- you must wait until the counter for the specific database connection in M.stats() returns to 0
--- before closing that connection or exiting the process
---@nodiscard
---@return table<string, integer> Table mapping connection names to their pending query counts
function M.stats()
    return c.stats()
end

--- Close the database connection
--- Sends a close request to the database handler
--- The connection will be gracefully closed after processing pending queries
function M:close()
    self.obj:close()
end

--- Execute an SQL statement without waiting for results (fire-and-forget)
--- Use this for INSERT, UPDATE, DELETE operations when you don't need the result
--- Any errors will be logged but not returned
--- Supports parameter binding with positional arguments (?, $1, etc.)
---@param sql string SQL statement to execute
---@vararg any Query parameters for parameter binding (bool, number, string, table as JSON, bytes)
function M:execute(sql, ...)
    local res = self.obj:query(moon.id, 0, sql, ...)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

--- Execute an SQL query and wait for results
--- Use this for SELECT queries or when you need the result of INSERT/UPDATE/DELETE
--- Supports parameter binding with positional arguments (?, $1, etc.)
--- Parameter types: bool, number (int/float), string, table (as JSON), bytes
--- Returns an array of result rows, each row is a table with column names as keys
--- Supported column types: INT8/16/32/64, UINT8/16/32/64, FLOAT32/64, TEXT, BOOL,
---                          TIMESTAMP, DATE, TIME, UUID, BYTES, JSON, NULL
---@async
---@nodiscard
---@param sql string SQL query to execute
---@vararg any Query parameters for parameter binding
---@return table Result rows array or error table with {kind, message}
function M:query(sql, ...)
    local session = self.obj:query(moon.id, moon.next_sequence(), sql, ...)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

--- Execute multiple SQL statements in a transaction
--- All statements will be executed atomically - either all succeed or all rollback
--- Each query in the querys array should be a table: {sql, param1, param2, ...}
--- Example: db:transaction({{"INSERT INTO users VALUES (?, ?)", "name", 25}, {"UPDATE stats SET count = count + 1"}})
---@async
---@nodiscard
---@param querys table Array of queries, each query is a table with SQL and parameters
---@return table Returns {message = "ok"} on success or {kind, message} on error
function M:transaction(querys)
    local trans = c.make_transaction()
    for _, v in ipairs(querys) do
        trans:push(table.unpack(v))
    end
    local session = self.obj:transaction(moon.id, moon.next_sequence(), trans)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

--- Execute a transaction without waiting for results (fire-and-forget)
--- Similar to execute(), but for multiple statements in a transaction
--- All statements will be executed atomically - either all succeed or all rollback
--- Any errors will be logged but not returned
--- Each query in the querys array should be a table: {sql, param1, param2, ...}
---@param querys table Array of queries, each query is a table with SQL and parameters
function M:execute_transaction(querys)
    local trans = c.make_transaction()
    for _, v in ipairs(querys) do
        trans:push(table.unpack(v))
    end
    local res = self.obj:transaction(moon.id, 0, trans)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

return M
