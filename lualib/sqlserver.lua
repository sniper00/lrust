---@diagnostic disable: inject-field, undefined-global
-- SQL Server Tiberius client for Lua
-- This provides SQL Server database connectivity using the Tiberius driver
local moon = require("moon")
local c = require("rust.tiberius")

local protocol_type = 26

moon.register_protocol {
    name = "database",
    PTYPE = protocol_type,
    pack = function(...) return ... end,
    unpack = function(val)
        return c.decode(val)
    end
}

local M = {}

-- Connect to SQL Server database
-- @param config_string: ADO.NET connection string format
--   Example: "Server=tcp:localhost,1433;Database=testdb;User Id=sa;Password=password;Encrypt=false"
-- @param name: Connection name for reuse
-- @param timeout: Connection timeout in milliseconds (default: 5000)
-- @return session_id or error table
function M.connect(config_string, name, timeout)
    local res = moon.wait(c.connect(protocol_type, moon.id, moon.next_sequence(), config_string, name, timeout))
    if res.kind then
        error(string.format("connect database failed: %s", res.message))
    end
    return M.find_connection(name)
end

-- Find an existing connection by name
-- @param name: Connection name
-- @return connection object or nil
function M.find_connection(name)
    local o = {
        obj = c.find_connection(name)
    }
    return setmetatable(o, { __index = M })
end

-- Execute a query that returns results
---@nodiscard
---@param sql string
---@vararg any
---@return table
function M:query(sql, ...)
    local session = self.obj:query(moon.id, moon.next_sequence(), sql, ...)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

-- Execute a command that doesn't return results (INSERT, UPDATE, DELETE)
---@param sql string
---@vararg any
function M:execute(sql, ...)
    local res = self.obj:query(moon.id, 0, sql, ...)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

--- Execute multiple queries, delimited with `;` and return multiple result sets; one for each query.
--- Do not use this with any user specified input. Please resort to prepared statements using the [`query`] method.
---@async
---@nodiscard
---@param querys string[]
---@return table
function M:transaction(querys)
    local trans = c.make_transaction()
    for _, v in ipairs(querys) do
        if type(v) ~= "string" then
            error("Each query must be a string, not support input params")
        end
        trans:push(table.unpack({v}))
    end
    local session = self.obj:transaction(moon.id, moon.next_sequence(), trans)
    if type(session) == "table" then
        return session
    end
    return moon.wait(session)
end

--- Execute multiple queries, delimited with `;` and return multiple result sets; one for each query.
--- Do not use this with any user specified input. Please resort to prepared statements using the [`query`] method.
---@param querys string[]
function M:execute_transaction(querys)
    local trans = c.make_transaction()
    for _, v in ipairs(querys) do
        if type(v) ~= "string" then
            error("Each query must be a string, not support input params")
        end
        trans:push(table.unpack({v}))
    end
    local res = self.obj:transaction(moon.id, 0, trans)
    if type(res) == "table" then
        moon.error(print_r(res, true))
    end
end

-- Close a database connection
-- @param connection: Database connection object
-- @return success boolean or error table
function M:close()
    self.obj:close()
end

-- Get connection statistics
-- @return table with connection statistics
function M.stats()
    return c.stats()
end

-- Helper function to build connection string
-- @param params: Table with connection parameters
--   - server: Server address (required)
--   - port: Server port (default: 1433)
--   - database: Database name (required)
--   - username: Username (required)
--   - password: Password (required)
--   - encrypt: Enable encryption (default: false)
--   - trust_server_certificate: Trust server certificate (default: false)
-- @return ADO.NET connection string
function M.build_connection_string(params)
    local parts = {}
    
    if params.server then
        if params.port and params.port ~= 1433 then
            table.insert(parts, "server=tcp:" .. params.server .. "," .. params.port)
        else
            table.insert(parts, "server=tcp:" .. params.server)
        end
    else
        error("Server is required")
    end
    
    if params.database then
        table.insert(parts, "Database=" .. params.database)
    else
        error("Database is required")
    end
    
    if params.username then
        table.insert(parts, "User Id=" .. params.username)
    else
        error("Username is required")
    end
    
    if params.password then
        table.insert(parts, "Password=" .. params.password)
    else
        error("Password is required")
    end
    
    if params.encrypt ~= nil then
        table.insert(parts, "IntegratedSecurity=" .. tostring(params.integrated_security))
    else
        table.insert(parts, "IntegratedSecurity=false")
    end
    
    if params.trust_server_certificate ~= nil then
        table.insert(parts, "TrustServerCertificate=" .. tostring(params.trust_server_certificate))
    end
    
    return table.concat(parts, ";")
end

return M
