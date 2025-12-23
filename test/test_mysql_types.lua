local moon = require "moon"
local sqlx = require "ext.sqlx"

moon.loglevel("INFO")

moon.async(function()
    -- 连接MySQL数据库
    local db = sqlx.connect("mysql://root:123456@127.0.0.1:3306/mysql?ssl-mode=DISABLED", "mysql_test")
    print("MySQL Connection:", db)
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
            id INT AUTO_INCREMENT PRIMARY KEY,
            
            -- 整数类型
            col_tinyint TINYINT,
            col_smallint SMALLINT,
            col_mediumint MEDIUMINT,
            col_int INT,
            col_bigint BIGINT,
            
            -- 无符号整数类型
            col_tinyint_unsigned TINYINT UNSIGNED,
            col_smallint_unsigned SMALLINT UNSIGNED,
            col_mediumint_unsigned MEDIUMINT UNSIGNED,
            col_int_unsigned INT UNSIGNED,
            col_bigint_unsigned BIGINT UNSIGNED,
            
            -- 浮点类型
            col_float FLOAT,
            col_double DOUBLE,
            col_real REAL,
            
            -- 布尔类型
            col_boolean BOOLEAN,
            
            -- 字符串类型
            col_char CHAR(10),
            col_varchar VARCHAR(255),
            col_tinytext TINYTEXT,
            col_text TEXT,
            col_mediumtext MEDIUMTEXT,
            col_longtext LONGTEXT,
            
            -- 二进制类型
            col_binary BINARY(16),
            col_varbinary VARBINARY(255),
            col_tinyblob TINYBLOB,
            col_blob BLOB,
            col_mediumblob MEDIUMBLOB,
            col_longblob LONGBLOB,
            
            -- 日期时间类型
            col_date DATE,
            col_datetime DATETIME,
            col_timestamp TIMESTAMP,
            col_time TIME,
            
            -- JSON类型
            col_json JSON
        );
    ]])
    print_r(res)

    -- 插入测试数据
    res = db:query([[
        INSERT INTO type_test (
            col_tinyint, col_smallint, col_mediumint, col_int, col_bigint,
            col_tinyint_unsigned, col_smallint_unsigned, col_mediumint_unsigned, col_int_unsigned, col_bigint_unsigned,
            col_float, col_double, col_real,
            col_boolean,
            col_char, col_varchar, col_tinytext, col_text, col_mediumtext, col_longtext,
            col_binary, col_varbinary, col_tinyblob, col_blob, col_mediumblob, col_longblob,
            col_date, col_datetime, col_timestamp, col_time,
            col_json
        ) VALUES (
            127, 32767, 8388607, 2147483647, 9223372036854775807,
            255, 65535, 16777215, 4294967295, 18446744073709551615,
            3.14, 2.718281828, 1.414,
            TRUE,
            'char', 'varchar string', 'tiny text', 'normal text', 'medium text', 'long text',
            UNHEX('0123456789ABCDEF0123456789ABCDEF'), UNHEX('DEADBEEF'), 
            'tiny blob', 'normal blob', 'medium blob', 'long blob',
            '2025-12-11', '2025-12-11 15:30:45', '2025-12-11 15:30:45', '15:30:45',
            '{"name": "test", "value": 123, "active": true}'
        );
    ]])
    print_r(res)

    -- 插入包含NULL值的测试数据
    res = db:query([[
        INSERT INTO type_test (
            col_tinyint, col_varchar, col_date
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
        "INSERT INTO type_test (col_int, col_varchar, col_json, col_date) VALUES (?, ?, ?, ?)",
        999,
        "parametrized insert",
        {key = "value", number = 42},
        "2025-12-12"
    )
    print_r(res)

    res = db:query("SELECT * FROM type_test WHERE col_int = ?;", 999)
    print_r(res)

    -- 事务测试
    print("\n===== 事务测试 =====")
    local trans = {}
    trans[#trans+1] = {
        "INSERT INTO type_test (col_int, col_varchar) VALUES (?, ?)",
        1001, "transaction test 1"
    }
    trans[#trans+1] = {
        "INSERT INTO type_test (col_int, col_varchar) VALUES (?, ?)",
        1002, "transaction test 2"
    }
    res = db:transaction(trans)
    print_r(res)

    res = db:query("SELECT * FROM type_test WHERE col_int >= 1000;")
    print_r(res)

    -- 统计信息
    print("\n===== SQLX 统计信息 =====")
    print_r(sqlx.stats())

    print("\n===== MySQL 类型测试完成 =====")
end)
