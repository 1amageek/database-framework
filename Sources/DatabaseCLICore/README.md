# DatabaseCLI

FoundationDB の対話型 CLI。`@Persistable` 型のコンパイルなしで、Schema.Entity を使ってデータの読み書き・スキーマ確認ができる。

## Prerequisites

- FoundationDB must be running locally
- Schema.Entity must be written to FDB (via `FDBContainer(for: schema)` or `database schema apply`)

## Setup

### Initialize a Local Database

```bash
# Default port (4690)
database init

# Custom port
database init --port 5000
```

Creates a `.database/` directory in the current working directory:

```
.database/
├── fdb.cluster          # Cluster config (local:<id>@127.0.0.1:<port>)
├── data/                # FDB data files
└── logs/                # FDB log files
```

### Check Status

```bash
database status
# Database directory: /path/to/.database
# Cluster file: /path/to/.database/fdb.cluster
# Port: 4690
```

### Cluster File Auto-Discovery

All commands automatically walk up the directory tree from the current directory looking for `.database/fdb.cluster`. Falls back to the system default if not found.

## REPL Mode

Run `database` without arguments to enter interactive mode. Catalogs are loaded automatically:

```
database - FoundationDB Interactive CLI
Types: Order, User
Type 'help' for available commands, 'quit' to exit.

database>
```

## コマンド一覧

### スキーマ情報

```
schema list                        全型の一覧
schema show <TypeName>             フィールド・型・インデックス・ディレクトリ構造を表示
```

```
database> schema list
Registered types:
  Order  (5 fields, 2 indexes)
  User   (3 fields, 1 indexes)

database> schema show User
User
  Fields:
    id: string
    name: string
    age: int64
  Indexes:
    User_name (scalar) [name]
  Directory: ["app", "users"]
```

### データ操作

```
insert <TypeName> <json>           レコード挿入
get <TypeName> <id>                ID でレコード取得
update <TypeName> <id> <json>      レコード更新（フィールドをマージ）
delete <TypeName> <id>             レコード削除
```

```
database> insert User {"id": "user-001", "name": "Alice", "age": 30}
WARNING: CLI writes do NOT update indexes.
Inserted record into 'User'

database> get User user-001
{"age":30,"id":"user-001","name":"Alice"}

database> update User user-001 {"age": 31}
WARNING: CLI writes do NOT update indexes.
Updated record 'user-001' in 'User'

database> delete User user-001
Deleted record 'user-001' from 'User'
```

> **注意**: CLI からの書き込み（insert/update/delete）はインデックスを更新しない。データの確認・デバッグ用途を想定。

### クエリ

```
find <TypeName>                                 全レコード取得
find <TypeName> --limit N                       件数制限
find <TypeName> --where field op value          フィルタ
find <TypeName> --sort field [desc]             ソート
```

演算子: `==`, `!=`, `>`, `<`, `>=`, `<=`

```
database> find User --limit 5
database> find User --where name == "Alice"
database> find User --where age > 25 --sort age desc --limit 10
```

### パーティション（動的ディレクトリ）

マルチテナント型など動的ディレクトリを持つ型には `--partition` オプションを指定する:

```
database> get Order order-001 --partition tenantId=tenant_123
database> find Order --limit 10 --partition tenantId=tenant_123
database> insert Order {"id": "order-002", "total": 5000} --partition tenantId=tenant_123
```

`schema show` で動的フィールドを確認できる:

```
database> schema show Order
Order
  ...
  Directory: ["app", <tenantId>, "orders"]
    - Static: "app"
    - Dynamic: tenantId (use --partition tenantId=<value>)
    - Static: "orders"
```

### Raw FDB アクセス

```
raw get <key>                      キーの値を取得
raw set <key> <value>              キー・値を設定
raw delete <key>                   キーを削除
raw range <prefix> [limit N]       プレフィックスでキーをスキャン
```

キーは文字列またはタプル形式 `("key", 123)` で指定可能。

### Graph / History

Graph および History コマンドはコンパイル済み `@Persistable` 型が必要。埋め込みモードでのみ利用可能。

## 埋め込みモード

アプリケーションに CLI を組み込む場合は `DatabaseCLICore` ライブラリを使用する:

```swift
import DatabaseCLICore
import DatabaseEngine

// FDBContainer から初期化（カタログを自動読み込み）
let container = try await FDBContainer(for: schema)
let repl = try await DatabaseREPL(container: container)
try await repl.run()
```

## モジュール構成

```
Sources/
├── DatabaseCLI/                    # Executable（エントリポイント）
│   └── EntryPoint.swift
└── DatabaseCLICore/                # Library（埋め込み可能）
    ├── Core/
    │   ├── DatabaseREPL.swift      # REPL ループ
    │   ├── CommandRouter.swift     # コマンド解析・ディスパッチ
    │   └── CatalogDataAccess.swift # Schema.Entity ベースのデータアクセス
    ├── Commands/
    │   ├── DataCommands.swift      # insert/get/update/delete
    │   ├── FindCommands.swift      # find + filter/sort
    │   ├── SchemaInfoCommands.swift # schema list/show
    │   ├── RawCommands.swift       # raw FDB アクセス
    │   ├── GraphCommands.swift     # graph（埋め込みモード専用）
    │   └── HistoryCommands.swift   # history（埋め込みモード専用）
    ├── Cluster/
    │   └── LocalCluster.swift
    └── Util/
        ├── CLIError.swift
        ├── JSONParser.swift
        └── Output.swift
```
