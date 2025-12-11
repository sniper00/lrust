local moon = require "moon"
local sqlx = require "ext.sqlx"

moon.loglevel("INFO")

moon.async(function()
    -- 连接PostgreSQL数据库
    local db = sqlx.connect("postgres://postgres:123456@localhost/postgres", "pg_test")
    print("PostgreSQL Connection:", db)
    if db.kind then
        print("连接失败:", db.message)
        return
    end

    -- 删除测试表
    local res = db:query([[
        DROP TABLE IF EXISTS type_test;
    ]])
    print_r(res)

    -- 创建包含所有支持类型的测试表
    res = db:query([[
        CREATE TABLE type_test (
            id SERIAL PRIMARY KEY,
            
            -- 整数类型
            col_int2 INT2,
            col_int4 INT4,
            col_int8 INT8,
            col_smallint SMALLINT,
            col_integer INTEGER,
            col_bigint BIGINT,
            
            -- 浮点类型
            col_float4 FLOAT4,
            col_float8 FLOAT8,
            col_real REAL,
            col_double DOUBLE PRECISION,
            
            -- 布尔类型
            col_bool BOOL,
            col_boolean BOOLEAN,
            
            -- 字符串类型
            col_char CHAR(10),
            col_varchar VARCHAR(255),
            col_text TEXT,
            col_bpchar BPCHAR(10),
            col_name NAME,
            
            -- 二进制类型
            col_bytea BYTEA,
            
            -- 日期时间类型
            col_date DATE,
            col_time TIME,
            col_time_precision TIME(6),
            col_timestamp TIMESTAMP,
            col_timestamptz TIMESTAMPTZ,
            
            -- JSON类型
            col_json JSON,
            col_jsonb JSONB,
            
            -- UUID类型
            col_uuid UUID
        );
    ]])
    print_r(res)

    -- 插入测试数据
    res = db:query([[
        INSERT INTO type_test (
            col_int2, col_int4, col_int8, col_smallint, col_integer, col_bigint,
            col_float4, col_float8, col_real, col_double,
            col_bool, col_boolean,
            col_char, col_varchar, col_text, col_bpchar, col_name,
            col_bytea,
            col_date, col_time, col_time_precision, col_timestamp, col_timestamptz,
            col_json, col_jsonb,
            col_uuid
        ) VALUES (
            32767, 2147483647, 9223372036854775807, 32767, 2147483647, 9223372036854775807,
            3.14, 2.718281828, 1.414, 3.141592653589793,
            TRUE, FALSE,
            'char', 'varchar string', 'text content', 'bpchar', 'name_value',
            E'\\xDEADBEEF',
            '2025-12-11', '15:30:45', '15:30:45.123456', '2025-12-11 15:30:45', '2025-12-11 15:30:45+08',
            '{"name": "test", "value": 123}', '{"active": true, "count": 456}',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
        );
    ]])
    print_r(res)

    -- 插入包含NULL值的测试数据
    res = db:query([[
        INSERT INTO type_test (
            col_int4, col_varchar, col_date
        ) VALUES (
            NULL, 'partial data', '2025-12-11'
        );
    ]])
    print_r(res)

    -- 查询所有数据
    print("\n===== 查询所有测试数据 =====")
    res = db:query("SELECT * FROM type_test;")
    print_r(res)

    -- 使用参数化查询
    print("\n===== 参数化查询测试 =====")
    res = db:query(
        "INSERT INTO type_test (col_integer, col_varchar, col_json, col_date) VALUES ($1, $2, $3, $4)",
        999,
        "parametrized insert",
        {key = "value", number = 42},
        "2025-12-12"
    )
    print_r(res)

    res = db:query("SELECT * FROM type_test WHERE col_integer = $1;", 999)
    print_r(res)

    -- 测试TIME类型的各种值
    print("\n===== TIME 类型详细测试 =====")
    res = db:query([[
        DROP TABLE IF EXISTS time_demo;
        CREATE TABLE time_demo (
            id SERIAL PRIMARY KEY,
            event_name VARCHAR(50),
            start_time TIME,
            end_time TIME WITHOUT TIME ZONE,
            precise_time TIME(6)
        );
    ]])
    print_r(res)

    res = db:query([[
        INSERT INTO time_demo (event_name, start_time, end_time, precise_time) VALUES
        ('晨会', '09:00:00', '09:30:00', '09:00:00.123456'),
        ('午餐', '12:30:00', '13:45:00', '12:30:00.500000'),
        ('加班', '18:00:00', '20:15:00', '18:00:00.987654'),
        ('跨午夜', '23:30:00', '00:15:00', '23:59:59.999999');
    ]])
    print_r(res)

    res = db:query("SELECT * FROM time_demo;")
    print_r(res)

    -- 事务测试
    print("\n===== 事务测试 =====")
    local trans = {}
    trans[#trans+1] = {
        "INSERT INTO type_test (col_integer, col_varchar) VALUES ($1, $2)",
        1001, "transaction test 1"
    }
    trans[#trans+1] = {
        "INSERT INTO type_test (col_integer, col_varchar) VALUES ($1, $2)",
        1002, "transaction test 2"
    }
    res = db:transaction(trans)
    print_r(res)

    res = db:query("SELECT * FROM type_test WHERE col_integer >= 1000;")
    print_r(res)

    -- 测试不支持的类型 (TIMETZ - 应该返回错误)
    print("\n===== 测试不支持的 TIMETZ 类型 =====")
    db:query("DROP TABLE IF EXISTS timetz_test;")
    res = db:query([[
        CREATE TABLE timetz_test (
            id SERIAL PRIMARY KEY,
            test_timetz TIME WITH TIME ZONE
        );
    ]])
    print_r(res)

    res = db:query([[
        INSERT INTO timetz_test (test_timetz) VALUES ('15:30:00+08');
    ]])
    print_r(res)

    res = db:query("SELECT * FROM timetz_test;")
    print_r(res)
    print("注意: TIMETZ 类型不被支持，应该会看到错误信息")

    -- 统计信息
    print("\n===== SQLX 统计信息 =====")
    print_r(sqlx.stats())

    print("\n===== PostgreSQL 类型测试完成 =====")
end)
