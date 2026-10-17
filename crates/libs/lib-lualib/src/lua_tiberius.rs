use crate::lua_json::{JsonOptions, encode_table};
use crate::request_pool::{
    PendingCounter, QueuedRequest, dispatch_request, drain_queued_requests, notify_shutdown,
};
use crate::{LOG_LEVEL_ERROR, LOG_LEVEL_INFO, moon_log, moon_send};
use dashmap::DashMap;
use futures::TryFutureExt;
use lazy_static::lazy_static;
use lib_core::context::CONTEXT;
use lib_lua::laux::{LuaArgs, LuaNil, LuaStack, LuaState, LuaTable, LuaType};
use lib_lua::luaL_newlib;
use lib_lua::{self, cstr, ffi, laux, lreg_null, lreg_try, push_lua_table};
use std::time::Duration;
use tiberius::{Client, Config, Result as TiberiusResult, Row};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
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
        config.trust_cert();
        let tcp = connect_with_timeout(
            timeout_duration,
            TcpStream::connect(config.get_addr()).map_err(|e| tiberius::error::Error::Io {
                kind: e.kind(),
                message: e.to_string(),
            }),
        )
        .await?;
        tcp.set_nodelay(true)?;
        let client = connect_with_timeout(
            timeout_duration,
            Client::connect(config, tcp.compat_write()),
        )
        .await?;
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
    async fn batch_execute(&mut self, requests: &[DatabaseQuery]) -> TiberiusResult<Vec<Row>> {
        let mut query = String::new();
        for request in requests {
            query.push_str(&request.sql);
            if !request.sql.trim_end().ends_with(';') {
                query.push(';');
            }
        }
        let stream = self.client.simple_query(query).await?;
        let result = stream.into_results().await?;
        let mut rows = Vec::new();
        for row_set in result {
            rows.extend(row_set);
        }
        Ok(rows)
    }
}
enum DatabaseRequest {
    Query(u32, i64, DatabaseQuery),
    Execute(u32, i64, DatabaseQuery),
    Transaction(u32, i64, Vec<DatabaseQuery>),
    Close(),
}
impl QueuedRequest for DatabaseRequest {
    fn owner_session(&self) -> Option<(u32, i64)> {
        match self {
            Self::Query(owner, session, _)
            | Self::Execute(owner, session, _)
            | Self::Transaction(owner, session, _) => Some((*owner, *session)),
            Self::Close() => None,
        }
    }
}
#[derive(Clone)]
struct DatabaseConnection {
    name: String,
    tx: mpsc::Sender<DatabaseRequest>,
    counter: PendingCounter,
}
enum DatabaseResponse {
    Connect,
    Rows(Vec<Row>),
    Execute(u64),
    Error(tiberius::error::Error),
    Timeout(String),
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
            false
        }
        Err(err) => {
            if session != 0 {
                moon_send(protocol_type, owner, session, DatabaseResponse::Error(err));
                false
            } else if !matches!(err, tiberius::error::Error::Io { .. }) && !err.is_deadlock() {
                // Permanent statement/conversion errors must not block the queue forever.
                moon_log(
                    owner,
                    LOG_LEVEL_ERROR,
                    format!("SQL Server '{config_str}' execute failed: {err}"),
                );
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
    name: String,
    counter: PendingCounter,
) {
    while let Some(op) = rx.recv().await {
        let _pending = op.owner_session().map(|_| counter.finish_on_drop());
        let mut failed_times = 0;
        match &op {
            DatabaseRequest::Query(owner, session, query_op) => {
                while handle_result(
                    &name,
                    &mut failed_times,
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
                    &name,
                    &mut failed_times,
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
                    &name,
                    &mut failed_times,
                    protocol_type,
                    *owner,
                    *session,
                    pool.batch_execute(query_ops)
                        .await
                        .map(DatabaseResponse::Rows),
                )
                .await
                {}
            }
            DatabaseRequest::Close() => {
                drain_queued_requests(&mut rx, &counter, |owner, session| {
                    moon_send(
                        protocol_type,
                        owner,
                        session,
                        DatabaseResponse::Error(tiberius::error::Error::Io {
                            kind: std::io::ErrorKind::BrokenPipe,
                            message: "SQL Server connection closed".into(),
                        }),
                    );
                })
                .await;
                break;
            }
        }
    }
    DATABASE_CONNECTIONS.remove_if(&name, |_, value| value.counter.ptr_eq(&counter));
}

fn connect(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let protocol_type: u8 = lua.get(1)?;
    let owner = lua.get(2)?;
    let session: i64 = lua.get(3)?;
    let config_str: String = lua.get(4)?;
    let name: String = lua.get(5)?;
    let connect_timeout: u64 = lua.opt(6).unwrap_or(30000);
    let config_str = config_str.to_string();
    let name = name.to_string();
    CONTEXT.tokio_runtime.spawn(async move {
        match DatabasePool::connect(&config_str, Duration::from_millis(connect_timeout)).await {
            Ok(pool) => {
                let (tx, rx) = mpsc::channel(100);
                let counter = PendingCounter::new();
                if let Some(old) = DATABASE_CONNECTIONS.insert(
                    name.clone(),
                    DatabaseConnection {
                        name: name.clone(),
                        tx: tx.clone(),
                        counter: counter.clone(),
                    },
                ) {
                    notify_shutdown(&old.tx, DatabaseRequest::Close());
                }
                moon_send(protocol_type, owner, session, DatabaseResponse::Connect);
                database_handler(protocol_type, pool, rx, name, counter).await;
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
    Ok(1)
}
fn get_query_param(lua: &mut LuaStack<'_>, i: i32) -> Result<QueryParams, String> {
    let value = lua.value(i);
    Ok(match value.kind() {
        LuaType::Boolean => QueryParams::Bool(value.as_bool().unwrap()),
        LuaType::Number => QueryParams::Float(value.as_number().unwrap()),
        LuaType::Integer => QueryParams::Int(value.as_integer().unwrap()),
        LuaType::String => {
            let bytes = value.as_bytes().unwrap();
            if matches!(bytes.first(), Some(b'{' | b'[')) {
                if let Ok(value) = serde_json::from_slice(bytes) {
                    return Ok(QueryParams::Json(value));
                }
            }
            match String::from_utf8(bytes.to_vec()) {
                Ok(text) => QueryParams::Text(text),
                Err(error) => QueryParams::Bytes(error.into_bytes()),
            }
        }
        LuaType::Table => {
            let mut buffer = Vec::new();
            encode_table(&mut buffer, lua, i, 0, false, &JsonOptions::default())?;
            QueryParams::Json(serde_json::from_slice(&buffer).map_err(|e| e.to_string())?)
        }
        _ => return Err(format!("unsupported parameter type: {}", value.name())),
    })
}
fn query(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let mut args = LuaArgs::new(1);
    let conn = unsafe {
        lua.value(args.iter_arg())
            .as_userdata::<DatabaseConnection>()
            .ok_or_else(|| String::from("invalid userdata"))?
            .as_mut()
    };
    let owner = lua.get(args.iter_arg())?;
    let session = lua.get(args.iter_arg())?;
    let sql = lua.get::<String>(args.iter_arg())?;
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        let param = get_query_param(lua, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                push_lua_table!(state, "kind" => "ERROR", "message" => err);
                return Ok(1);
            }
        }
    }
    Ok(dispatch_request(
        state,
        &conn.tx,
        &conn.counter,
        DatabaseRequest::Query(
            owner,
            session,
            DatabaseQuery {
                sql: sql.to_string(),
                binds: params,
            },
        ),
    ))
}
fn execute(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let mut args = LuaArgs::new(1);
    let conn = unsafe {
        lua.value(args.iter_arg())
            .as_userdata::<DatabaseConnection>()
            .ok_or_else(|| String::from("invalid userdata"))?
            .as_mut()
    };
    let owner = lua.get(args.iter_arg())?;
    let session = lua.get(args.iter_arg())?;
    let sql = lua.get::<String>(args.iter_arg())?;
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in args.iter_arg()..=top {
        let param = get_query_param(lua, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                push_lua_table!(state, "kind" => "ERROR", "message" => err);
                return Ok(1);
            }
        }
    }
    Ok(dispatch_request(
        state,
        &conn.tx,
        &conn.counter,
        DatabaseRequest::Execute(
            owner,
            session,
            DatabaseQuery {
                sql: sql.to_string(),
                binds: params,
            },
        ),
    ))
}
struct TransactionQuerys {
    querys: Vec<DatabaseQuery>,
}
fn push_transaction_query(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let querys = unsafe {
        lua.value(1)
            .as_userdata::<TransactionQuerys>()
            .ok_or_else(|| String::from("invalid userdata"))?
            .as_mut()
    };
    let sql = lua.get::<String>(2)?;
    let mut params = Vec::new();
    let top = laux::lua_top(state);
    for i in 3..=top {
        let param = get_query_param(lua, i);
        match param {
            Ok(value) => {
                params.push(value);
            }
            Err(err) => {
                drop(params);
                return Err(err);
            }
        }
    }
    querys.querys.push(DatabaseQuery {
        sql: sql.to_string(),
        binds: params,
    });
    Ok(0)
}
fn make_transaction(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    laux::lua_newuserdata(
        state,
        TransactionQuerys { querys: Vec::new() },
        cstr!("tiberius_transaction_metatable"),
        &[lreg_try!("push", push_transaction_query), lreg_null!()],
    );
    Ok(1)
}
fn transaction(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let mut args = LuaArgs::new(1);
    let conn = unsafe {
        lua.value(args.iter_arg())
            .as_userdata::<DatabaseConnection>()
            .ok_or_else(|| String::from("invalid userdata"))?
            .as_mut()
    };
    let owner = lua.get(args.iter_arg())?;
    let session = lua.get(args.iter_arg())?;
    let querys = unsafe {
        lua.value(args.iter_arg())
            .as_userdata::<TransactionQuerys>()
            .ok_or_else(|| String::from("invalid userdata"))?
            .as_mut()
    };
    Ok(dispatch_request(
        state,
        &conn.tx,
        &conn.counter,
        DatabaseRequest::Transaction(owner, session, std::mem::take(&mut querys.querys)),
    ))
}
fn close(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let conn = unsafe {
        lua.value(1)
            .as_userdata::<DatabaseConnection>()
            .ok_or_else(|| String::from("invalid userdata"))?
            .as_mut()
    };
    notify_shutdown(&conn.tx, DatabaseRequest::Close());
    DATABASE_CONNECTIONS.remove_if(&conn.name, |_, value| value.counter.ptr_eq(&conn.counter));
    laux::lua_push(state, true);
    Ok(1)
}
fn process_rows(state: LuaState, rows: &[Row]) -> Result<i32, String> {
    let table = LuaTable::new(state, rows.len(), 0);
    for (i, row) in rows.iter().enumerate() {
        let row_table = LuaTable::new(state, 0, row.len());
        // A batch may contain different result-set schemas.
        for (index, column) in row.columns().iter().enumerate() {
            let column_name = column.name();
            if let Ok(value) = row.try_get::<&str, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value);
                }
            } else if let Ok(value) = row.try_get::<bool, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value);
                }
            } else if let Ok(value) = row.try_get::<u8, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value);
                }
            } else if let Ok(value) = row.try_get::<i16, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value);
                }
            } else if let Ok(value) = row.try_get::<i32, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value as i64);
                }
            } else if let Ok(value) = row.try_get::<i64, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value);
                }
            } else if let Ok(value) = row.try_get::<f32, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value as f64);
                }
            } else if let Ok(value) = row.try_get::<f64, _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value);
                }
            } else if let Ok(value) = row.try_get::<&[u8], _>(index) {
                if let Some(value) = value {
                    row_table.insert(column_name, value);
                }
            } else {
                row_table.insert(column_name, LuaNil {});
            }
        }
        table.rawseti(i + 1);
    }
    Ok(1)
}
fn find_connection(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let name = lua.get::<String>(1)?;
    match DATABASE_CONNECTIONS.get(name.as_str()) {
        Some(pair) => {
            let l = [
                lreg_try!("query", query),
                lreg_try!("execute", execute),
                lreg_try!("transaction", transaction),
                lreg_try!("close", close),
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
                return Ok(1);
            }
        }
        None => {
            laux::lua_pushnil(state);
        }
    }
    Ok(1)
}
fn decode(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    laux::lua_checkstack(state, 6, std::ptr::null())?;
    let result = unsafe { crate::take_response::<DatabaseResponse>(lua)? };
    match &*result {
        DatabaseResponse::Rows(rows) => {
            return Ok(process_rows(state, rows)
                .map_err(|e| {
                    push_lua_table!(state, "kind" => "ERROR", "message" => e);
                })
                .unwrap_or(1));
        }
        DatabaseResponse::Execute(count) => {
            push_lua_table!(state, "affected_rows" => * count as i64);
            return Ok(1);
        }
        DatabaseResponse::Connect => {
            push_lua_table!(state, "message" => "success");
            return Ok(1);
        }
        DatabaseResponse::Error(err) => {
            push_lua_table!(state, "kind" => "ERROR", "message" => err.to_string());
        }
        DatabaseResponse::Timeout(err) => {
            push_lua_table!(state, "kind" => "TIMEOUT", "message" => err.to_string());
        }
    }
    Ok(1)
}
fn stats(lua: &mut laux::LuaStack<'_>) -> std::result::Result<i32, String> {
    let state = lua.state();
    let table = LuaTable::new(state, 0, DATABASE_CONNECTIONS.len());
    DATABASE_CONNECTIONS.iter().for_each(|pair| {
        table.insert(pair.key().as_str(), pair.value().counter.load());
    });
    Ok(1)
}
#[cfg(feature = "tiberius")]
#[unsafe(no_mangle)]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C-unwind" fn luaopen_rust_tiberius(state: LuaState) -> i32 {
    let l = [
        lreg_try!("connect", connect),
        lreg_try!("find_connection", find_connection),
        lreg_try!("decode", decode),
        lreg_try!("stats", stats),
        lreg_try!("make_transaction", make_transaction),
        lreg_null!(),
    ];
    luaL_newlib!(state, l);
    1
}
