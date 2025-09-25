use crate::lua_json::{encode_table, JsonOptions};
use crate::{moon_log, moon_send, LOG_LEVEL_ERROR, LOG_LEVEL_INFO};
use dashmap::DashMap;
use futures::TryFutureExt;
use lazy_static::lazy_static;
use lib_core::context::CONTEXT;
use lib_lua::laux::{lua_into_userdata, LuaArgs, LuaNil, LuaState, LuaTable, LuaValue};
use lib_lua::luaL_newlib;
use lib_lua::{self, cstr, ffi, laux, lreg, lreg_null, push_lua_table};

use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tiberius::{Client, Config, Result as TiberiusResult, Row};
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use tokio::net::TcpStream;

lazy_static! {
    static ref DATABASE_CONNECTIONS: DashMap<String, DatabaseConnection> = DashMap::new();
}

type TiberiusClient = Client<Compat<TcpStream>>;

struct DatabasePool {
    client: TiberiusClient,
}

impl DatabasePool {
    async fn connect(config_str: &str, timeout_duration: Duration) -> TiberiusResult<Self> {
        async fn connect_with_timeout<F, T>(
            timeout_duration: Duration,
            connect_future: F,
        ) -> TiberiusResult<T>
        where
            F: std::future::Future<Output = TiberiusResult<T>>,
        {
            timeout(timeout_duration, connect_future)
                .await
                .map_err(|_| tiberius::error::Error::Io {
                    kind: std::io::ErrorKind::Other,
                    message: "Connection timeout".to_string(),
                })?
        }

        let mut config = Config::from_ado_string(config_str)?;
        
        // Ensure proper configuration for SQL Server 2017
        config.trust_cert(); // Trust self-signed certificates
        
        let tcp = connect_with_timeout(
            timeout_duration,
            TcpStream::connect(config.get_addr()).map_err(|e| tiberius::error::Error::Io {
                kind: e.kind(),
                message: e.to_string(),
            }),
        ).await?;
        tcp.set_nodelay(true)?;

        let client = connect_with_timeout(
            timeout_duration,
            Client::connect(config, tcp.compat_write()),
        ).await?;

        Ok(DatabasePool { client })
    }

    async fn query(&mut self, request: &DatabaseQuery) -> TiberiusResult<Vec<Row>> {
        let mut query = tiberius::Query::new(&request.sql);
        
        for param in request.binds.iter() {
            match param {
                QueryParams::Bool(val) => query.bind(*val),
                QueryParams::Int(val) => query.bind(*val),
                QueryParams::Float(val) => query.bind(*val),
                QueryParams::Text(val) => query.bind(val.as_str()),
                QueryParams::Json(val) => query.bind(serde_json::to_string(val).unwrap()),
                QueryParams::Bytes(val) => query.bind(val.as_slice()),
            }
        }
        
        let stream = query.query(&mut self.client).await?;
        let result = stream.into_results().await?;
        
        let mut rows = Vec::new();
        for row_set in result {
            rows.extend(row_set);
        }
        
        Ok(rows)
    }

    async fn execute(&mut self, request: &DatabaseQuery) -> TiberiusResult<u64> {
        let mut query = tiberius::Query::new(&request.sql);
        
        for param in request.binds.iter() {
            match param {
                QueryParams::Bool(val) => query.bind(*val),
                QueryParams::Int(val) => query.bind(*val),
                QueryParams::Float(val) => query.bind(*val),
                QueryParams::Text(val) => query.bind(val.as_str()),
                QueryParams::Json(val) => query.bind(serde_json::to_string(val).unwrap()),
                QueryParams::Bytes(val) => query.bind(val.as_slice()),
            }
        }
        
        let result = query.execute(&mut self.client).await?;
        Ok(result.total())
    }

    async fn batch_execute(&mut self, requests: &[DatabaseQuery]) -> TiberiusResult<DatabaseResponse> {
        // Execute queries in batch without transaction
        for request in requests {
            let mut query = tiberius::Query::new(&request.sql);
            
            for param in request.binds.iter() {
                match param {
                    QueryParams::Bool(val) => query.bind(*val),
                    QueryParams::Int(val) => query.bind(*val),
                    QueryParams::Float(val) => query.bind(*val),
                    QueryParams::Text(val) => query.bind(val.as_str()),
                    QueryParams::Json(val) => query.bind(serde_json::to_string(val).unwrap()),
                    QueryParams::Bytes(val) => query.bind(val.as_slice()),
                }
            }
            
            query.execute(&mut self.client).await?;
        }
        
        Ok(DatabaseResponse::Transaction)
    }
}

enum DatabaseRequest {
    Query(u32, i64, DatabaseQuery),
    Execute(u32, i64, DatabaseQuery),
    Transaction(u32, i64, Vec<DatabaseQuery>),
    Close(),
}

#[derive(Clone)]
struct DatabaseConnection {
    tx: mpsc::Sender<DatabaseRequest>,
    counter: Arc<AtomicI64>,
}

enum DatabaseResponse {
    Connect,
    Rows(Vec<Row>),
    Execute(u64),
    Error(tiberius::error::Error),
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
}

#[derive(Debug, Clone)]
struct DatabaseQuery {
    sql: String,
    binds: Vec<QueryParams>,
}

async fn handle_result(
    config_str: &str,
    failed_times: &mut i32,
    counter: &Arc<AtomicI64>,
    protocol_type: u8,
    owner: u32,
    session: i64,
    res: TiberiusResult<DatabaseResponse>,
) -> bool {
    match res {
        Ok(response) => {
            moon_send(protocol_type, owner, session, response);
            if *failed_times > 0 {
                moon_log(
                    owner,
                    LOG_LEVEL_INFO,
                    format!(
                        "Database '{}' recover from error. Retry success.",
                        config_str
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
                            config_str,
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
    mut pool: DatabasePool,
    mut rx: mpsc::Receiver<DatabaseRequest>,
    config_str: &str,
    counter: Arc<AtomicI64>,
) {
    while let Some(op) = rx.recv().await {
        let mut failed_times = 0;
        match &op {
            DatabaseRequest::Query(owner, session, query_op) => {
                while handle_result(
                    config_str,
                    &mut failed_times,
                    &counter,
                    protocol_type,
                    *owner,
                    *session,
                    pool.query(query_op).await.map(DatabaseResponse::Rows),
                )
                .await
                {}
            }
            DatabaseRequest::Execute(owner, session, query_op) => {
                while handle_result(
                    config_str,
                    &mut failed_times,
                    &counter,
                    protocol_type,
                    *owner,
                    *session,
                    pool.execute(query_op).await.map(DatabaseResponse::Execute),
                )
                .await
                {}
            }
            DatabaseRequest::Transaction(owner, session, query_ops) => {
                while handle_result(
                    config_str,
                    &mut failed_times,
                    &counter,
                    protocol_type,
                    *owner,
                    *session,
                    pool.batch_execute(query_ops).await,
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

    let config_str: &str = laux::lua_get(state, 4);
    let name: &str = laux::lua_get(state, 5);
    let connect_timeout: u64 = laux::lua_opt(state, 6).unwrap_or(30000);

    let config_str = config_str.to_string();
    let name = name.to_string();

    CONTEXT.tokio_runtime.spawn(async move {
        println!("Attempting to connect to SQL Server with config: {}", config_str);
        println!("Connection timeout set to: {} ms", connect_timeout);
        match DatabasePool::connect(&config_str, Duration::from_millis(connect_timeout)).await {
            Ok(pool) => {
                let (tx, rx) = mpsc::channel(100);
                let counter = Arc::new(AtomicI64::new(0));
                DATABASE_CONNECTIONS.insert(
                    name.clone(),
                    DatabaseConnection {
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                );
                moon_send(protocol_type, owner, session, DatabaseResponse::Connect);
                database_handler(protocol_type, pool, rx, &config_str, counter).await;
            }
            Err(err) => {
                println!("SQL Server connection failed: {}", err);
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
            let mut buffer = Vec::new();
            if let Err(err) = encode_table(&mut buffer, &val, 0, false, &options) {
                drop(buffer);
                laux::lua_error(state, &err);
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

extern "C-unwind" fn execute(state: LuaState) -> i32 {
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

    match conn.tx.try_send(DatabaseRequest::Execute(
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
                laux::lua_error(state, err.as_ref());
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
        cstr!("tiberius_transaction_metatable"),
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

fn process_rows(state: LuaState, rows: &[Row]) -> Result<i32, String> {
    let table = LuaTable::new(state, rows.len(), 0);
    if rows.is_empty() {
        return Ok(1);
    }

    let mut column_info = Vec::new();
    if let Some(first_row) = rows.first() {
        for (index, column) in first_row.columns().iter().enumerate() {
            column_info.push((index, column.name()));
        }
    }

    let mut i = 0;
    for row in rows.iter() {
        let row_table = LuaTable::new(state, 0, row.len());
        for (index, column_name) in column_info.iter() {
            // Try to get the value as string first
            if let Ok(value) = row.try_get::<&str, _>(*index) {
                row_table.rawset(*column_name, value.unwrap_or_default());
            } else if let Ok(value) = row.try_get::<bool, _>(*index) {
                row_table.rawset(*column_name, value.unwrap_or_default());
            } else if let Ok(value) = row.try_get::<i32, _>(*index) {
                row_table.rawset(*column_name, value.unwrap_or_default() as i64);
            } else if let Ok(value) = row.try_get::<i64, _>(*index) {
                row_table.rawset(*column_name, value.unwrap_or_default());
            } else if let Ok(value) = row.try_get::<f32, _>(*index) {
                row_table.rawset(*column_name, value.unwrap_or_default() as f64);
            } else if let Ok(value) = row.try_get::<f64, _>(*index) {
                row_table.rawset(*column_name, value.unwrap_or_default());
            } else if let Ok(value) = row.try_get::<&[u8], _>(*index) {
                row_table.rawset(*column_name, value.unwrap_or_default());
            } else {
                row_table.rawset(*column_name, LuaNil {});
            }
        }
        i += 1;
        table.seti(i);
    }
    Ok(1)
}

extern "C-unwind" fn find_connection(state: LuaState) -> i32 {
    let name = laux::lua_get::<&str>(state, 1);
    match DATABASE_CONNECTIONS.get(name) {
        Some(pair) => {
            let l = [
                lreg!("query", query),
                lreg!("execute", execute),
                lreg!("transaction", transaction),
                lreg!("close", close),
                lreg_null!(),
            ];
            if laux::lua_newuserdata(
                state,
                pair.value().clone(),
                cstr!("tiberius_connection_metatable"),
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
    laux::luaL_checkstack(state, 6, std::ptr::null());
    let result = lua_into_userdata::<DatabaseResponse>(state, 1);

    match &*result {
        DatabaseResponse::Rows(rows) => {
            return process_rows(state, rows)
                .map_err(|e| {
                    push_lua_table!(
                        state,
                        "kind" => "ERROR",
                        "message" => e
                    );
                })
                .unwrap_or(1);
        }
        DatabaseResponse::Execute(count) => {
            push_lua_table!(
                state,
                "affected_rows" => *count as i64
            );
            return 1;
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
        DatabaseResponse::Error(err) => {
            push_lua_table!(
                state,
                "kind" => "ERROR",
                "message" => err.to_string()
            );
        }
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
    let table = LuaTable::new(state, 0, DATABASE_CONNECTIONS.len());
    DATABASE_CONNECTIONS.iter().for_each(|pair| {
        table.rawset(
            pair.key().as_str(),
            pair.value()
                .counter
                .load(std::sync::atomic::Ordering::Acquire),
        );
    });
    1
}

#[cfg(feature = "tiberius")]
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn luaopen_rust_tiberius(state: LuaState) -> i32 {
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