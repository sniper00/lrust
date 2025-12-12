# lib-lualib Features

这个库现在支持通过 Cargo features 来控制哪些模块被编译和包含。

## 可用的 Features

- `excel`: Excel 文件读取支持 (使用 calamine 和 csv)
- `sqlx`: SQL 数据库支持 (MySQL, PostgreSQL, SQLite)
- `mongodb`: MongoDB 数据库支持
- `websocket`: WebSocket 客户端支持
- `http`: HTTP 客户端支持 (包含 reqwest, percent-encoding 等)
- `json`: JSON 处理支持 (使用 serde 和 serde_json)

## 默认 Features

默认情况下，以下 features 是启用的：
```toml
default = ["excel", "sqlx", "mongodb", "websocket", "http", "json"]
```

## Feature 依赖关系

- `http` feature 依赖于 `json` feature (因为 HTTP 响应经常需要 JSON 处理)

## 使用方法

### 使用所有默认 features
```toml
[dependencies]
lib-lualib = { path = "path/to/lib-lualib" }
```

### 禁用所有 features
```toml
[dependencies]
lib-lualib = { path = "path/to/lib-lualib", default-features = false }
```

### 只启用特定 features
```toml
[dependencies]
lib-lualib = { path = "path/to/lib-lualib", default-features = false, features = ["excel", "json"] }
```

### 添加额外的 features 到默认配置
```toml
[dependencies]
lib-lualib = { path = "path/to/lib-lualib", features = ["additional-feature"] }
```

## 对应的 Lua 函数

- `excel` feature: `luaopen_rust_excel`
- `sqlx` feature: `luaopen_rust_sqlx`
- `mongodb` feature: `luaopen_rust_mongodb`
- `websocket` feature: `luaopen_rust_websocket`
- `http` feature: `luaopen_rust_httpc`
- `json` feature: `luaopen_json`

当某个 feature 被禁用时，对应的 `luaopen_*` 函数将不会被编译到最终的库中。

## 编译测试

你可以使用以下命令来测试不同的 feature 组合：

```bash
# 测试无 features
cargo check --no-default-features

# 测试特定 features
cargo check --features="excel,json"
cargo check --features="http"  # 这会自动包含 json feature

# 测试所有默认 features
cargo check
```