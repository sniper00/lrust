use std::sync::{Arc, atomic::AtomicI64};
use std::time::Duration;

use dashmap::DashMap;
use lazy_static::lazy_static;
use sqlx::types::{Json, Uuid};
use sqlx::{
    Column, ColumnIndex, Database, MySql, MySqlPool, PgPool, Postgres, Row, Sqlite, SqlitePool,
    TypeInfo, ValueRef,
    migrate::MigrateDatabase,
    mysql::MySqlRow,
    postgres::{PgPoolOptions, PgRow, PgValueRef},
    sqlite::SqliteRow,
    types::chrono::{NaiveDate, NaiveDateTime, NaiveTime},
};
use tokio::{sync::mpsc, time::timeout};

use lib_core::context::CONTEXT;
use lib_lua::{
    self, cstr, ffi, laux,
    laux::{LuaArgs, LuaNil, LuaState, LuaTable, LuaValue, lua_into_userdata},
    lreg, lreg_null, luaL_newlib, push_lua_table,
};

use crate::lua_json::{JsonOptions, encode_table};
use crate::{LOG_LEVEL_ERROR, LOG_LEVEL_INFO, moon_log, moon_send};

lazy_static! {
    static ref DATABASE_CONNECTIONSS: DashMap<String, DatabaseConnection> = DashMap::new();
}

enum DatabasePool {
    MySql(MySqlPool),
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl DatabasePool {
    async fn connect(database_url: &str, timeout_duration: Duration) -> Result<Self, sqlx::Error> {
        async fn connect_with_timeout<F, T>(
            timeout_duration: Duration,
            connect_future: F,
        ) -> Result<T, sqlx::Error>
        where
            F: std::future::Future<Output = Result<T, sqlx::Error>>,
        {
            timeout(timeout_duration, connect_future)
                .await
                .map_err(|err| {
                    sqlx::Error::Io(std::io::Error::other(format!("Connection error: {}", err)))
                })?
        }

        if database_url.starts_with("mysql://") {
            let pool =
                connect_with_timeout(timeout_duration, MySqlPool::connect(database_url)).await?;
            Ok(DatabasePool::MySql(pool))
        } else if database_url.starts_with("postgres://") {
            let pool = connect_with_timeout(
                timeout_duration,
                PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(Duration::from_secs(2))
                    .connect(database_url),
            )
            .await?;
            Ok(DatabasePool::Postgres(pool))
        } else if database_url.starts_with("sqlite://") {
            if !Sqlite::database_exists(database_url).await? {
                Sqlite::create_database(database_url).await?;
            }
            let pool =
                connect_with_timeout(timeout_duration, SqlitePool::connect(database_url)).await?;
            Ok(DatabasePool::Sqlite(pool))
        } else {
            Err(sqlx::Error::Configuration(
                "Unsupported database type".into(),
            ))
        }
    }

    fn make_query<'a, DB: sqlx::Database>(
        sql: &'a str,
        binds: &'a [QueryParams],
    ) -> Result<sqlx::query::Query<'a, DB, <DB as sqlx::Database>::Arguments<'a>>, sqlx::Error>
    where
        bool: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        i64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        f64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        &'a str: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        serde_json::Value: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
        &'a Vec<u8>: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = match bind {
                QueryParams::Bool(value) => query.bind(*value),
                QueryParams::Int(value) => query.bind(*value),
                QueryParams::Float(value) => query.bind(*value),
                QueryParams::Text(value) => query.bind(value.as_str()),
                QueryParams::Json(value) => query.bind(value),
                QueryParams::Bytes(value) => query.bind(value),
                QueryParams::PgBoolArray(_)
                | QueryParams::PgInt2Array(_)
                | QueryParams::PgInt4Array(_)
                | QueryParams::PgInt8Array(_)
                | QueryParams::PgFloat4Array(_)
                | QueryParams::PgFloat8Array(_)
                | QueryParams::PgTextArray(_)
                | QueryParams::PgBytesArray(_)
                | QueryParams::PgUuidArray(_)
                | QueryParams::PgJsonArray(_) => {
                    return Err(sqlx::Error::Configuration(
                        "PostgreSQL array parameter used with a non-PostgreSQL connection".into(),
                    ));
                }
            };
        }
        Ok(query)
    }

    fn make_pg_query<'a>(
        sql: &'a str,
        binds: &'a [QueryParams],
    ) -> Result<
        sqlx::query::Query<'a, Postgres, <Postgres as sqlx::Database>::Arguments<'a>>,
        sqlx::Error,
    > {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = match bind {
                QueryParams::Bool(value) => query.bind(*value),
                QueryParams::Int(value) => query.bind(*value),
                QueryParams::Float(value) => query.bind(*value),
                QueryParams::Text(value) => query.bind(value.as_str()),
                QueryParams::Json(value) => query.bind(value),
                QueryParams::Bytes(value) => query.bind(value),
                QueryParams::PgBoolArray(value) => query.bind(value.as_slice()),
                QueryParams::PgInt2Array(value) => query.bind(value.as_slice()),
                QueryParams::PgInt4Array(value) => query.bind(value.as_slice()),
                QueryParams::PgInt8Array(value) => query.bind(value.as_slice()),
                QueryParams::PgFloat4Array(value) => query.bind(value.as_slice()),
                QueryParams::PgFloat8Array(value) => query.bind(value.as_slice()),
                QueryParams::PgTextArray(value) => query.bind(value.as_slice()),
                QueryParams::PgBytesArray(value) => query.bind(value.as_slice()),
                QueryParams::PgUuidArray(value) => query.bind(value.as_slice()),
                QueryParams::PgJsonArray(value) => query.bind(value.as_slice()),
            };
        }
        Ok(query)
    }

    async fn query(&self, request: &DatabaseQuery) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::MysqlRows(rows))
            }
            DatabasePool::Postgres(pool) => {
                let query = Self::make_pg_query(&request.sql, &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::PgRows(rows))
            }
            DatabasePool::Sqlite(pool) => {
                let query = Self::make_query(&request.sql, &request.binds)?;
                let rows = query.fetch_all(pool).await?;
                Ok(DatabaseResponse::SqliteRows(rows))
            }
        }
    }

    async fn transaction(
        &self,
        requests: &[DatabaseQuery],
    ) -> Result<DatabaseResponse, sqlx::Error> {
        match self {
            DatabasePool::MySql(pool) => {
                let mut transaction = pool.begin().await?;
                for request in requests {
                    let query = Self::make_query(&request.sql, &request.binds)?;
                    query.execute(&mut *transaction).await?;
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction)
            }
            DatabasePool::Postgres(pool) => {
                let mut transaction = pool.begin().await?;
                for request in requests {
                    let query = Self::make_pg_query(&request.sql, &request.binds)?;
                    query.execute(&mut *transaction).await?;
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction)
            }
            DatabasePool::Sqlite(pool) => {
                let mut transaction = pool.begin().await?;
                for request in requests {
                    let query = Self::make_query(&request.sql, &request.binds)?;
                    query.execute(&mut *transaction).await?;
                }
                transaction.commit().await?;
                Ok(DatabaseResponse::Transaction)
            }
        }
    }
}

enum DatabaseRequest {
    Query(u32, i64, DatabaseQuery), //owner, session, QueryBuilder
    Transaction(u32, i64, Vec<DatabaseQuery>), //owner, session, Vec<QueryBuilder>
    Close(),
}

#[derive(Clone)]
struct DatabaseConnection {
    tx: mpsc::Sender<DatabaseRequest>,
    counter: Arc<AtomicI64>,
}

enum DatabaseResponse {
    Connect,
    PgRows(Vec<PgRow>),
    MysqlRows(Vec<MySqlRow>),
    SqliteRows(Vec<SqliteRow>),
    Error(sqlx::Error),
    Timeout(String),
    Transaction,
}

#[derive(Debug, Clone)]
enum QueryParams {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    PgBoolArray(Vec<Option<bool>>),
    PgInt2Array(Vec<Option<i16>>),
    PgInt4Array(Vec<Option<i32>>),
    PgInt8Array(Vec<Option<i64>>),
    PgFloat4Array(Vec<Option<f32>>),
    PgFloat8Array(Vec<Option<f64>>),
    PgTextArray(Vec<Option<String>>),
    PgBytesArray(Vec<Option<Vec<u8>>>),
    PgUuidArray(Vec<Option<Uuid>>),
    PgJsonArray(Vec<Option<Json<serde_json::Value>>>),
}

#[derive(Debug, Clone)]
struct DatabaseQuery {
    sql: String,
    binds: Vec<QueryParams>,
}

async fn handle_result(
    database_url: &str,
    failed_times: &mut i32,
    counter: &Arc<AtomicI64>,
    protocol_type: u8,
    owner: u32,
    session: i64,
    res: Result<DatabaseResponse, sqlx::Error>,
) -> bool {
    match res {
        Ok(rows) => {
            moon_send(protocol_type, owner, session, rows);
            if *failed_times > 0 {
                moon_log(
                    owner,
                    LOG_LEVEL_INFO,
                    format!(
                        "Database '{}' recover from error. Retry success.",
                        database_url
                    ),
                );
            }
            counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
            false
        }
        Err(err) => {
            if session != 0 {
                moon_send(protocol_type, owner, session, DatabaseResponse::Error(err));
                counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
                false
            } else {
                if *failed_times > 0 {
                    moon_log(
                        owner,
                        LOG_LEVEL_ERROR,
                        format!(
                            "Database '{}' error: '{:?}'. Will retry.",
                            database_url,
                            err.to_string()
                        ),
                    );
                }
                *failed_times += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
                true
            }
        }
    }
}

async fn database_handler(
    protocol_type: u8,
    pool: &DatabasePool,
    mut rx: mpsc::Receiver<DatabaseRequest>,
    database_url: &str,
    counter: Arc<AtomicI64>,
) {
    while let Some(op) = rx.recv().await {
        let mut failed_times = 0;
        match &op {
            DatabaseRequest::Query(owner, session, query_op) => {
                while handle_result(
                    database_url,
                    &mut failed_times,
                    &counter,
                    protocol_type,
                    *owner,
                    *session,
                    pool.query(query_op).await,
                )
                .await
                {}
            }
            DatabaseRequest::Transaction(owner, session, query_ops) => {
                while handle_result(
                    database_url,
                    &mut failed_times,
                    &counter,
                    protocol_type,
                    *owner,
                    *session,
                    pool.transaction(query_ops).await,
                )
                .await
                {}
            }
            DatabaseRequest::Close() => {
                break;
            }
        }
    }
}

extern "C-unwind" fn connect(state: LuaState) -> i32 {
    let protocol_type: u8 = laux::lua_get(state, 1);
    let owner = laux::lua_get(state, 2);
    let session: i64 = laux::lua_get(state, 3);

    let database_url: &str = laux::lua_get(state, 4);
    let name: &str = laux::lua_get(state, 5);
    let connect_timeout: u64 = laux::lua_opt(state, 6).unwrap_or(5000);

    CONTEXT.tokio_runtime.spawn(async move {
        match DatabasePool::connect(database_url, Duration::from_millis(connect_timeout)).await {
            Ok(pool) => {
                let (tx, rx) = mpsc::channel(100);
                let counter = Arc::new(AtomicI64::new(0));
                DATABASE_CONNECTIONSS.insert(
                    name.to_string(),
                    DatabaseConnection {
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                );
                moon_send(protocol_type, owner, session, DatabaseResponse::Connect);
                database_handler(protocol_type, &pool, rx, database_url, counter).await;
            }
            Err(err) => {
                moon_send(
                    protocol_type,
                    owner,
                    session,
                    DatabaseResponse::Timeout(err.to_string()),
                );
            }
        };
    });

    laux::lua_push(state, session);
    1
}

fn get_query_param(state: LuaState, i: i32) -> Result<QueryParams, String> {
    let options = JsonOptions::default();

    let res = match LuaValue::from_stack(state, i) {
        LuaValue::Boolean(val) => QueryParams::Bool(val),
        LuaValue::Number(val) => QueryParams::Float(val),
        LuaValue::Integer(val) => QueryParams::Int(val),
        LuaValue::String(val) => {
            if val.starts_with(b"{") || val.starts_with(b"[") {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(val) {
                    QueryParams::Json(value)
                } else {
                    QueryParams::Text(unsafe { String::from_utf8_unchecked(val.to_vec()) })
                }
            } else {
                QueryParams::Text(unsafe { String::from_utf8_unchecked(val.to_vec()) })
            }
        }
        LuaValue::Table(val) => {
            if val.getmetafield(cstr!("__sqlx_array")).is_some() {
                return get_pg_array_param(&val, &options);
            }

            let mut buffer = Vec::new();
            if let Err(err) = encode_table(&mut buffer, &val, 0, false, &options) {
                drop(buffer);
                laux::lua_error(state, err);
            }
            if buffer[0] == b'{' || buffer[0] == b'[' {
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(buffer.as_slice()) {
                    QueryParams::Json(value)
                } else {
                    QueryParams::Bytes(buffer)
                }
            } else {
                QueryParams::Bytes(buffer)
            }
        }
        _t => {
            return Err(format!(
                "get_query_param: unsupport value type :{}",
                laux::type_name(state, i)
            ));
        }
    };
    Ok(res)
}

fn collect_pg_array<T, F>(
    values: &LuaTable,
    expected: &str,
    mut convert: F,
) -> Result<Vec<Option<T>>, String>
where
    F: for<'a> FnMut(LuaValue<'a>) -> Option<T>,
{
    let (is_array, len) = values.array_len();
    if !is_array && values.iter().next().is_some() {
        return Err("sqlx.array values must be a Lua sequence".to_string());
    }

    let mut result = Vec::with_capacity(len);
    for (index, value) in values.array_iter().enumerate() {
        match value {
            LuaValue::LightUserData(ptr) if ptr.is_null() => result.push(None),
            value => match convert(value) {
                Some(value) => result.push(Some(value)),
                None => {
                    return Err(format!(
                        "sqlx.array element {} must be {}",
                        index + 1,
                        expected
                    ));
                }
            },
        }
    }
    Ok(result)
}

fn lua_value_to_json(value: LuaValue<'_>, options: &JsonOptions) -> Option<serde_json::Value> {
    match value {
        LuaValue::Nil => Some(serde_json::Value::Null),
        LuaValue::LightUserData(ptr) if ptr.is_null() => Some(serde_json::Value::Null),
        LuaValue::Boolean(value) => Some(serde_json::Value::Bool(value)),
        LuaValue::Integer(value) => Some(serde_json::Value::Number(value.into())),
        LuaValue::Number(value) => {
            serde_json::Number::from_f64(value).map(serde_json::Value::Number)
        }
        LuaValue::String(value) => Some(serde_json::Value::String(
            String::from_utf8_lossy(value).into_owned(),
        )),
        LuaValue::Table(value) => {
            let mut buffer = Vec::new();
            encode_table(&mut buffer, &value, 0, false, options).ok()?;
            serde_json::from_slice(&buffer).ok()
        }
        _ => None,
    }
}

fn get_pg_array_param(
    wrapper: &LuaTable,
    options: &JsonOptions,
) -> Result<QueryParams, String> {
    let type_name = {
        let field = wrapper.rawget("type");
        match &field.value {
            LuaValue::String(value) => String::from_utf8_lossy(value).into_owned(),
            _ => return Err("sqlx.array type must be a string".to_string()),
        }
    };

    let values_field = wrapper.rawget("values");
    let values = match &values_field.value {
        LuaValue::Table(value) => value,
        _ => return Err("sqlx.array values must be a table".to_string()),
    };

    let normalized = type_name.trim().to_ascii_lowercase();
    let element_type = normalized.strip_suffix("[]").unwrap_or(&normalized);

    match element_type {
        "bool" | "boolean" => Ok(QueryParams::PgBoolArray(collect_pg_array(
            values,
            "a boolean",
            |value| match value {
                LuaValue::Boolean(value) => Some(value),
                _ => None,
            },
        )?)),
        "int2" | "smallint" => Ok(QueryParams::PgInt2Array(collect_pg_array(
            values,
            "an int2 integer",
            |value| match value {
                LuaValue::Integer(value) => i16::try_from(value).ok(),
                _ => None,
            },
        )?)),
        "int4" | "integer" | "int" => Ok(QueryParams::PgInt4Array(collect_pg_array(
            values,
            "an int4 integer",
            |value| match value {
                LuaValue::Integer(value) => i32::try_from(value).ok(),
                _ => None,
            },
        )?)),
        "int8" | "bigint" => Ok(QueryParams::PgInt8Array(collect_pg_array(
            values,
            "an int8 integer",
            |value| match value {
                LuaValue::Integer(value) => Some(value),
                _ => None,
            },
        )?)),
        "float4" | "real" => Ok(QueryParams::PgFloat4Array(collect_pg_array(
            values,
            "a number",
            |value| match value {
                LuaValue::Integer(value) => Some(value as f32),
                LuaValue::Number(value) => Some(value as f32),
                _ => None,
            },
        )?)),
        "float8" | "double" | "double precision" => {
            Ok(QueryParams::PgFloat8Array(collect_pg_array(
                values,
                "a number",
                |value| match value {
                    LuaValue::Integer(value) => Some(value as f64),
                    LuaValue::Number(value) => Some(value),
                    _ => None,
                },
            )?))
        }
        "text" | "varchar" | "char" | "character varying" | "name" => {
            Ok(QueryParams::PgTextArray(collect_pg_array(
                values,
                "a string",
                |value| match value {
                    LuaValue::String(value) => {
                        Some(String::from_utf8_lossy(value).into_owned())
                    }
                    _ => None,
                },
            )?))
        }
        "bytea" | "bytes" => Ok(QueryParams::PgBytesArray(collect_pg_array(
            values,
            "a string",
            |value| match value {
                LuaValue::String(value) => Some(value.to_vec()),
                _ => None,
            },
        )?)),
        "uuid" => Ok(QueryParams::PgUuidArray(collect_pg_array(
            values,
            "a UUID string",
            |value| match value {
                LuaValue::String(value) => std::str::from_utf8(value)
                    .ok()
                    .and_then(|value| Uuid::parse_str(value).ok()),
                _ => None,
            },
        )?)),
        "json" | "jsonb" => {
            let values = collect_pg_array(values, "a JSON value", |value| {
                lua_value_to_json(value, options).map(Json)
            })?;
            Ok(QueryParams::PgJsonArray(values))
        }
        _ => Err(format!(
            "sqlx.array unsupported PostgreSQL element type: {}",
            type_name
        )),
    }
}

extern "C-unwind" fn query(state: LuaState) -> i32 {
    let mut args = LuaArgs::new(1);
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("Invalid database connect pointer");

    let owner = laux::lua_get(state, args.iter_arg());
    let session = laux::lua_get(state, args.iter_arg());

    let sql = laux::lua_get::<&str>(state, args.iter_arg());
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err
                );
                return 1;
            }
        }
    }

    match conn.tx.try_send(DatabaseRequest::Query(
        owner,
        session,
        DatabaseQuery {
            sql: sql.to_string(),
            binds: params,
        },
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            laux::lua_push(state, session);
            1
        }
        Err(err) => {
            push_lua_table!(
                state,
                "kind" => "ERROR",
                "message" => err.to_string()
            );
            1
        }
    }
}

struct TransactionQuerys {
    querys: Vec<DatabaseQuery>,
}

extern "C-unwind" fn push_transaction_query(state: LuaState) -> i32 {
    let querys = laux::lua_touserdata::<TransactionQuerys>(state, 1)
        .expect("Invalid transaction query pointer");

    let sql = laux::lua_get::<&str>(state, 2);
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in 3..=top {
        let param = get_query_param(state, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                drop(params);
                laux::lua_error(state, err);
            }
        }
    }

    querys.querys.push(DatabaseQuery {
        sql: sql.to_string(),
        binds: params,
    });

    0
}

extern "C-unwind" fn make_transaction(state: LuaState) -> i32 {
    laux::lua_newuserdata(
        state,
        TransactionQuerys { querys: Vec::new() },
        cstr!("sqlx_transaction_metatable"),
        &[lreg!("push", push_transaction_query), lreg_null!()],
    );
    1
}

extern "C-unwind" fn transaction(state: LuaState) -> i32 {
    let mut args = LuaArgs::new(1);
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, args.iter_arg())
        .expect("Invalid database connect pointer");

    let owner = laux::lua_get(state, args.iter_arg());
    let session = laux::lua_get(state, args.iter_arg());

    let querys = laux::lua_touserdata::<TransactionQuerys>(state, args.iter_arg())
        .expect("Invalid transaction query pointer");

    match conn.tx.try_send(DatabaseRequest::Transaction(
        owner,
        session,
        std::mem::take(&mut querys.querys),
    )) {
        Ok(_) => {
            conn.counter
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            laux::lua_push(state, session);
            1
        }
        Err(err) => {
            push_lua_table!(
                state,
                "kind" => "ERROR",
                "message" => err.to_string()
            );
            1
        }
    }
}

extern "C-unwind" fn close(state: LuaState) -> i32 {
    let conn = laux::lua_touserdata::<DatabaseConnection>(state, 1)
        .expect("Invalid database connect pointer");

    match conn.tx.try_send(DatabaseRequest::Close()) {
        Ok(_) => {
            laux::lua_push(state, true);
            1
        }
        Err(err) => {
            push_lua_table!(
                state,
                "kind" => "ERROR",
                "message" => err.to_string()
            );
            1
        }
    }
}

#[derive(Copy, Clone)]
enum DbType {
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Text,
    Bool,
    Timestamp,
    Date,
    Time,
    Uuid,
    Bytes,
    Json,
    Null,
    UnsupportedDecimal,
    UnsupportedTimeWithTz,
    Unknown,
}

static DB_TYPE_MAP: phf::Map<&'static str, DbType> = phf::phf_map! {
    // Int32 types
    "INT4" => DbType::Int32,
    "INT" => DbType::Int32,
    "INTEGER" => DbType::Int32,
    "MEDIUMINT" => DbType::Int32,
    // Int64 types
    "INT8" => DbType::Int64,
    "BIGINT" => DbType::Int64,
    // Int16 types
    "INT2" => DbType::Int16,
    "SMALLINT" => DbType::Int16,
    // Int8 type
    "TINYINT" => DbType::Int8,
    // Float64 types
    "FLOAT8" => DbType::Float64,
    "DOUBLE" => DbType::Float64,
    // Float32 types
    "FLOAT4" => DbType::Float32,
    "FLOAT" => DbType::Float32,
    "REAL" => DbType::Float32,
    // Text types
    "TEXT" => DbType::Text,
    "VARCHAR" => DbType::Text,
    "CHAR" => DbType::Text,
    "BPCHAR" => DbType::Text,
    "NAME" => DbType::Text,
    "TINYTEXT" => DbType::Text,
    "MEDIUMTEXT" => DbType::Text,
    "LONGTEXT" => DbType::Text,
    "NVARCHAR" => DbType::Text,
    "NCHAR" => DbType::Text,
    // Bool types
    "BOOL" => DbType::Bool,
    "BOOLEAN" => DbType::Bool,
    // Timestamp types
    "TIMESTAMP" => DbType::Timestamp,
    "TIMESTAMPTZ" => DbType::Timestamp,
    "DATETIME" => DbType::Timestamp,
    // Date type
    "DATE" => DbType::Date,
    // Time type
    "TIME" => DbType::Time,
    // UUID type
    "UUID" => DbType::Uuid,
    // Bytes types
    "BYTEA" => DbType::Bytes,
    "BLOB" => DbType::Bytes,
    "VARBINARY" => DbType::Bytes,
    "BINARY" => DbType::Bytes,
    "TINYBLOB" => DbType::Bytes,
    "MEDIUMBLOB" => DbType::Bytes,
    "LONGBLOB" => DbType::Bytes,
    // Json types
    "JSON" => DbType::Json,
    "JSONB" => DbType::Json,
    // Null type
    "NULL" => DbType::Null,
    // Unsupported decimal types
    "DECIMAL" => DbType::UnsupportedDecimal,
    "NUMERIC" => DbType::UnsupportedDecimal,
    "MONEY" => DbType::UnsupportedDecimal,
    // Unsupported time with timezone
    "TIMETZ" => DbType::UnsupportedTimeWithTz,
    // Unsigned types
    "TINYINT UNSIGNED" => DbType::UInt8,
    "SMALLINT UNSIGNED" => DbType::UInt16,
    "INT UNSIGNED" => DbType::UInt32,
    "MEDIUMINT UNSIGNED" => DbType::UInt32,
    "BIGINT UNSIGNED" => DbType::UInt64,
};

impl DbType {
    #[inline]
    fn from_name(name: &str) -> Self {
        DB_TYPE_MAP.get(name).copied().unwrap_or(Self::Unknown)
    }
}

fn process_rows<'a, DB>(state: LuaState, rows: &'a [<DB as Database>::Row]) -> Result<i32, String>
where
    DB: sqlx::Database,
    usize: ColumnIndex<<DB as Database>::Row>,
    i8: sqlx::Decode<'a, DB>,
    i16: sqlx::Decode<'a, DB>,
    i32: sqlx::Decode<'a, DB>,
    i64: sqlx::Decode<'a, DB>,
    f32: sqlx::Decode<'a, DB>,
    f64: sqlx::Decode<'a, DB>,
    bool: sqlx::Decode<'a, DB>,
    &'a str: sqlx::Decode<'a, DB>,
    &'a [u8]: sqlx::Decode<'a, DB>,
    NaiveDate: sqlx::Decode<'a, DB>,
    NaiveDateTime: sqlx::Decode<'a, DB>,
    NaiveTime: sqlx::Decode<'a, DB>,
    Uuid: sqlx::Decode<'a, DB>,
{
    let table = LuaTable::new(state, rows.len(), 0);
    if rows.is_empty() {
        return Ok(1);
    }

    let column_info: Vec<(usize, &str, DbType)> = rows
        .first()
        .unwrap()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let name = column.name();
            let db_type = DbType::from_name(column.type_info().name());
            (index, name, db_type)
        })
        .collect();

    for (i, row) in rows.iter().enumerate() {
        let row_table = LuaTable::new(state, 0, row.len());
        for (index, column_name, db_type) in column_info.iter() {
            match row.try_get_raw(*index) {
                Ok(value) => {
                    if value.is_null() {
                        row_table.insert(*column_name, LuaNil {});
                        continue;
                    }

                    match db_type {
                        DbType::Int8 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i8);
                            row_table.insert(*column_name, v);
                        }
                        DbType::UInt8 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i8) as u8;
                            row_table.insert(*column_name, v);
                        }
                        DbType::Int16 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i16);
                            row_table.insert(*column_name, v);
                        }
                        DbType::UInt16 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i16) as u16;
                            row_table.insert(*column_name, v);
                        }
                        DbType::Int32 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i32);
                            row_table.insert(*column_name, v);
                        }
                        DbType::UInt32 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i32) as u32;
                            row_table.insert(*column_name, v);
                        }
                        DbType::Int64 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i64);
                            row_table.insert(*column_name, v);
                        }
                        DbType::UInt64 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0i64) as u64;
                            row_table.insert(*column_name, v);
                        }
                        DbType::Float32 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0.0f32);
                            row_table.insert(*column_name, v);
                        }
                        DbType::Float64 => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(0.0f64);
                            row_table.insert(*column_name, v);
                        }
                        DbType::Text => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or("");
                            row_table.insert(*column_name, v);
                        }
                        DbType::Bool => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or(false);
                            row_table.insert(*column_name, v);
                        }
                        DbType::Timestamp => {
                            match <NaiveDateTime as sqlx::decode::Decode<DB>>::decode(value) {
                                Ok(dt) => {
                                    row_table.insert(
                                        *column_name,
                                        dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                                    );
                                }
                                Err(_) => {
                                    row_table.insert(*column_name, LuaNil {});
                                }
                            }
                        }
                        DbType::Date => {
                            match <NaiveDate as sqlx::decode::Decode<DB>>::decode(value) {
                                Ok(date) => {
                                    row_table.insert(*column_name, date.format("%Y-%m-%d").to_string());
                                }
                                Err(_) => {
                                    row_table.insert(*column_name, LuaNil {});
                                }
                            }
                        }
                        DbType::Time => {
                            match <NaiveTime as sqlx::decode::Decode<DB>>::decode(value) {
                                Ok(time) => {
                                    row_table.insert(*column_name, time.format("%H:%M:%S").to_string());
                                }
                                Err(_) => {
                                    row_table.insert(*column_name, LuaNil {});
                                }
                            }
                        }
                        DbType::Uuid => {
                            match <Uuid as sqlx::decode::Decode<DB>>::decode(value) {
                                Ok(uuid) => {
                                    row_table.insert(*column_name, uuid.to_string());
                                }
                                Err(_) => {
                                    row_table.insert(*column_name, LuaNil {});
                                }
                            }
                        }
                        DbType::Bytes => {
                            let v: &[u8] = sqlx::decode::Decode::decode(value).unwrap_or(b"");
                            row_table.insert(*column_name, v);
                        }
                        DbType::Json => {
                            let v = sqlx::decode::Decode::decode(value).unwrap_or("{}");
                            row_table.insert(*column_name, v);
                        }
                        DbType::Null => {
                            row_table.insert(*column_name, LuaNil {});
                        }
                        DbType::UnsupportedDecimal => {
                            return Err(format!(
                                "Unsupported decimal type for column '{}'",
                                column_name
                            ));
                        }
                        DbType::UnsupportedTimeWithTz => {
                            return Err(format!(
                                "Unsupported time with time zone type for column '{}'",
                                column_name
                            ));
                        }
                        DbType::Unknown => {
                            if let Ok(bytes) = sqlx::decode::Decode::decode(value) {
                                row_table.insert::<&str, &[u8]>(*column_name, bytes);
                            } else {
                                row_table.insert(*column_name, LuaNil {});
                            }
                        }
                    }
                }
                Err(error) => {
                    laux::lua_push(state, false);
                    laux::lua_push(state, format!("{} decode error: {}", column_name, error));
                    return Ok(2);
                }
            }
        }
        table.rawseti(i + 1);
    }
    Ok(1)
}

fn push_optional_array<T, F>(state: LuaState, values: Vec<Option<T>>, mut push_value: F)
where
    F: FnMut(LuaState, T),
{
    let table = LuaTable::new(state, values.len(), 0);
    for (index, value) in values.into_iter().enumerate() {
        match value {
            Some(value) => push_value(state, value),
            None => laux::lua_pushlightuserdata(state, std::ptr::null_mut()),
        }
        table.rawseti(index + 1);
    }
}

fn process_pg_array_value(
    state: LuaState,
    row_table: &LuaTable,
    column_name: &str,
    type_name: &str,
    value: PgValueRef<'_>,
) -> Result<bool, String> {
    macro_rules! decode_array {
        ($value_type:ty) => {{
            <Vec<Option<$value_type>> as sqlx::decode::Decode<Postgres>>::decode(value)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?
        }};
    }

    match type_name {
        "BOOL[]" => {
            let decoded = decode_array!(bool);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "INT2[]" => {
            let decoded = decode_array!(i16);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value as i64)
                })
            });
        }
        "INT4[]" => {
            let decoded = decode_array!(i32);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value as i64)
                })
            });
        }
        "INT8[]" => {
            let decoded = decode_array!(i64);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "FLOAT4[]" => {
            let decoded = decode_array!(f32);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value as f64)
                })
            });
        }
        "FLOAT8[]" => {
            let decoded = decode_array!(f64);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "TEXT[]" | "VARCHAR[]" | "CHAR[]" | "NAME[]" => {
            let decoded = decode_array!(String);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        "BYTEA[]" => {
            let decoded = decode_array!(Vec<u8>);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value.as_slice())
                })
            });
        }
        "UUID[]" => {
            let decoded = decode_array!(Uuid);
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, |state, value| {
                    laux::lua_push(state, value.to_string())
                })
            });
        }
        "JSON[]" | "JSONB[]" => {
            let decoded = decode_array!(Json<serde_json::Value>);
            let decoded: Result<Vec<Option<String>>, String> = decoded
                .into_iter()
                .map(|value| {
                    value
                        .map(|value| {
                            serde_json::to_string(&value.0).map_err(|err| {
                                format!("{} JSON encode error: {}", column_name, err)
                            })
                        })
                        .transpose()
                })
                .collect();
            let decoded = decoded?;
            row_table.insert_x(column_name, || {
                push_optional_array(state, decoded, laux::lua_push)
            });
        }
        _ => return Ok(false),
    }

    Ok(true)
}

fn insert_pg_scalar_value(
    row_table: &LuaTable,
    column_name: &str,
    db_type: DbType,
    value: PgValueRef<'_>,
) -> Result<(), String> {
    macro_rules! decode_value {
        ($value_type:ty) => {{
            <$value_type as sqlx::decode::Decode<Postgres>>::decode(value)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?
        }};
    }

    match db_type {
        DbType::Int8 => row_table.insert(column_name, decode_value!(i8)),
        DbType::UInt8 => row_table.insert(column_name, decode_value!(i8) as u8),
        DbType::Int16 => row_table.insert(column_name, decode_value!(i16)),
        DbType::UInt16 => row_table.insert(column_name, decode_value!(i16) as u16),
        DbType::Int32 => row_table.insert(column_name, decode_value!(i32)),
        DbType::UInt32 => row_table.insert(column_name, decode_value!(i32) as u32),
        DbType::Int64 => row_table.insert(column_name, decode_value!(i64)),
        DbType::UInt64 => row_table.insert(column_name, decode_value!(i64) as u64),
        DbType::Float32 => row_table.insert(column_name, decode_value!(f32)),
        DbType::Float64 => row_table.insert(column_name, decode_value!(f64)),
        DbType::Text => row_table.insert(column_name, decode_value!(&str)),
        DbType::Bool => row_table.insert(column_name, decode_value!(bool)),
        DbType::Timestamp => {
            let value = decode_value!(NaiveDateTime);
            row_table.insert(column_name, value.format("%Y-%m-%d %H:%M:%S").to_string())
        }
        DbType::Date => {
            let value = decode_value!(NaiveDate);
            row_table.insert(column_name, value.format("%Y-%m-%d").to_string())
        }
        DbType::Time => {
            let value = decode_value!(NaiveTime);
            row_table.insert(column_name, value.format("%H:%M:%S").to_string())
        }
        DbType::Uuid => row_table.insert(column_name, decode_value!(Uuid).to_string()),
        DbType::Bytes => row_table.insert(column_name, decode_value!(&[u8])),
        DbType::Json => {
            let value = decode_value!(serde_json::Value);
            let json = serde_json::to_string(&value)
                .map_err(|err| format!("{} JSON encode error: {}", column_name, err))?;
            row_table.insert(column_name, json)
        }
        DbType::Null => row_table.insert(column_name, LuaNil {}),
        DbType::UnsupportedDecimal => {
            return Err(format!("Unsupported decimal type for column '{}'", column_name));
        }
        DbType::UnsupportedTimeWithTz => {
            return Err(format!(
                "Unsupported time with time zone type for column '{}'",
                column_name
            ));
        }
        DbType::Unknown => {
            if let Ok(bytes) = <&[u8] as sqlx::decode::Decode<Postgres>>::decode(value) {
                row_table.insert(column_name, bytes)
            } else {
                row_table.insert(column_name, LuaNil {})
            }
        }
    };

    Ok(())
}

fn process_pg_rows(state: LuaState, rows: &[PgRow]) -> Result<i32, String> {
    let table = LuaTable::new(state, rows.len(), 0);
    if rows.is_empty() {
        return Ok(1);
    }

    let column_info: Vec<_> = rows[0]
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let type_name = column.type_info().name();
            (index, column.name(), type_name, DbType::from_name(type_name))
        })
        .collect();

    for (row_index, row) in rows.iter().enumerate() {
        let row_table = LuaTable::new(state, 0, row.len());
        for (index, column_name, type_name, db_type) in &column_info {
            let value = row
                .try_get_raw(*index)
                .map_err(|err| format!("{} decode error: {}", column_name, err))?;

            if value.is_null() {
                row_table.insert(*column_name, LuaNil {});
                continue;
            }

            if process_pg_array_value(
                state,
                &row_table,
                column_name,
                type_name,
                value.clone(),
            )? {
                continue;
            }

            insert_pg_scalar_value(&row_table, column_name, *db_type, value)?;
        }
        table.rawseti(row_index + 1);
    }

    Ok(1)
}

extern "C-unwind" fn find_connection(state: LuaState) -> i32 {
    let name = laux::lua_get::<&str>(state, 1);
    match DATABASE_CONNECTIONSS.get(name) {
        Some(pair) => {
            let l = [
                lreg!("query", query),
                lreg!("transaction", transaction),
                lreg!("close", close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                pair.value().clone(),
                cstr!("sqlx_connection_metatable"),
                l.as_ref(),
            )
            .is_none()
            {
                laux::lua_pushnil(state);
                return 1;
            }
        }
        None => {
            laux::lua_pushnil(state);
        }
    }
    1
}

extern "C-unwind" fn decode(state: LuaState) -> i32 {
    laux::lua_checkstack(state, 6, std::ptr::null());
    let result = lua_into_userdata::<DatabaseResponse>(state, 1);

    match *result {
        DatabaseResponse::PgRows(rows) => {
            return process_pg_rows(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::MysqlRows(rows) => {
            return process_rows::<MySql>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::SqliteRows(rows) => {
            return process_rows::<Sqlite>(state, &rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::Transaction => {
            push_lua_table!(
                state,
                "message" => "ok"
            );
            return 1;
        }
        DatabaseResponse::Connect => {
            push_lua_table!(
                state,
                "message" => "success"
            );
            return 1;
        }
        DatabaseResponse::Error(err) => match err.as_database_error() {
            Some(db_err) => {
                push_lua_table!(
                    state,
                    "kind" => "DB",
                    "message" => db_err.message()
                );
            }
            None => {
                push_lua_table!(
                    state,
                    "kind" => "ERROR",
                    "message" => err.to_string()
                );
            }
        },
        DatabaseResponse::Timeout(err) => {
            push_lua_table!(
                state,
                "kind" => "TIMEOUT",
                "message" => err.to_string()
            );
        }
    }

    1
}

extern "C-unwind" fn stats(state: LuaState) -> i32 {
    let table = LuaTable::new(state, 0, DATABASE_CONNECTIONSS.len());
    DATABASE_CONNECTIONSS.iter().for_each(|pair| {
        table.insert(
            pair.key().as_str(),
            pair.value()
                .counter
                .load(std::sync::atomic::Ordering::Acquire),
        );
    });
    1
}

#[cfg(feature = "sqlx")]
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn luaopen_rust_sqlx(state: LuaState) -> i32 {
    let l = [
        lreg!("connect", connect),
        lreg!("find_connection", find_connection),
        lreg!("decode", decode),
        lreg!("stats", stats),
        lreg!("make_transaction", make_transaction),
        lreg_null!(),
    ];

    luaL_newlib!(state, l);

    1
}
