# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Vision

**database-framework** is a **protocol-extensible, customizable index database** designed for the AI era, built on FoundationDB's transactional guarantees.

### Design Philosophy

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Protocol-Based Extensibility                     │
│                                                                      │
│   IndexKind (protocol)  ───▶  IndexMaintainer (protocol)            │
│        ↓                            ↓                                │
│   User defines           Framework executes                          │
│   WHAT to index          HOW to maintain                            │
└─────────────────────────────────────────────────────────────────────┘
```

1. **Protocol-Driven**: New index types can be added without modifying core code
2. **Composable**: Combine Vector + Graph + FullText in unified queries (Fusion API)
3. **Transactional**: All operations backed by FoundationDB's ACID guarantees
4. **AI-Native**: First-class support for embeddings, knowledge graphs, and RAG patterns

## Build and Test Commands

```bash
# Build the project
swift build

# Run all tests (requires local FoundationDB running)
swift test

# Run a specific test file
swift test --filter DatabaseEngineTests.ScalarIndexKindTests

# Run a specific test function
swift test --filter "DatabaseEngineTests.ScalarIndexKindTests/testScalarIndexKindIdentifier"

# Build with release optimization
swift build -c release
```

**Prerequisites**: FoundationDB must be installed and running locally for tests. The linker expects `libfdb_c` at `/usr/local/lib`.

## Dependencies

### database-kit

**database-kit は安定版のため、GitHub URL を使用すること。**

```swift
// ✅ 正しい: GitHub URL を使用
.package(url: "https://github.com/1amageek/database-kit.git", branch: "main"),

// ❌ 間違い: ローカルパスは使用しない（修正が必要な場合のみ）
.package(path: "../database-kit"),
```

ローカルパスを使用するのは、database-kit 自体に変更・修正が必要な場合のみ。

## Architecture Overview

This is the **server-side execution layer** for FoundationDB persistence. It implements index maintenance logic for index types defined in the sibling `database-kit` package.

### Two-Package Design

```
database-kit (client-safe)          database-framework (server-only)
├── Core/                           ├── DatabaseEngine/
│   ├── Persistable (protocol)      │   ├── FDBContainer
│   ├── IndexKind (protocol)        │   ├── FDBContext
│   ├── IndexDescriptor             │   ├── IndexMaintainer (protocol)
│   ├── FieldSchema                 │   ├── IndexKindMaintainable (protocol)
│   ├── PersistableEnum (protocol)  │   └── Registry/
│   └── EnumMetadata                │       ├── TypeCatalog
│                                   │       ├── SchemaRegistry
├── Vector/, FullText/, etc.        │       ├── DynamicProtobufDecoder
│   └── VectorIndexKind             │       └── DynamicProtobufEncoder
                                    ├── VectorIndex/, FullTextIndex/, etc.
                                    │   └── VectorIndexKind+Maintainable
                                    └── DatabaseCLI/
                                        ├── DatabaseREPL
                                        └── CatalogDataAccess
```

- **database-kit**: Platform-independent model definitions, index type specifications, and field schema metadata (works on iOS clients)
- **database-framework**: FoundationDB-dependent index execution, schema registry, and dynamic CLI (server-only, requires libfdb_c)

### Module Dependency Graph

```
Database (re-export all)
    ↓
┌───┴───┬───────┬───────┬───────┬───────┬───────┬───────┬───────┬─────────┐
Scalar  Vector  FullText Spatial Rank   Permuted Graph  Aggregation Version QueryAST
    ↓      ↓       ↓        ↓      ↓       ↓       ↓        ↓        ↓        ↓
    └──────┴───────┴────────┴──────┴───────┴───────┴────────┴────────┴────────┘
                                   ↓
                            DatabaseEngine
                                   ↓
                    ┌──────────────┼──────────────┐
                Core (database-kit)          FoundationDB (fdb-swift-bindings)
```

### Key Types

| Type | Role |
|------|------|
| `FDBContainer` | Application resource manager: database connection, schema, securityDelegate, directory resolution. Does NOT create transactions |
| `FDBContext` | Transaction manager + User-facing API: owns ReadVersionCache, creates transactions via TransactionRunner, change tracking |
| `FDBDataStore` | Data operations within transactions: receives transaction as parameter, does NOT create transactions |
| `IndexMaintainer<Item>` | Protocol for index update logic (`updateIndex`, `scanItem`) |
| `IndexMaintenanceService` | Centralized index maintenance: uniqueness checking, index updates, violation tracking |
| `IndexKindMaintainable` | Bridge protocol connecting IndexKind to IndexMaintainer |
| `TypeCatalog` | Codable schema metadata for a Persistable type (pg_catalog equivalent) |
| `SchemaRegistry` | Persists/loads TypeCatalog entries in FDB under `/_catalog/` |
| `DirectoryComponentCatalog` | Codable enum: `.staticPath(String)` or `.dynamicField(fieldName: String)` |

### Schema Registry (pg_catalog)

PostgreSQL は `pg_catalog` でスキーマ情報をデータと一緒に保存する。本フレームワークも同様のアプローチを採用。

```
PostgreSQL pg_catalog          → database-framework
─────────────────────────────────────────────────
pg_class (テーブル名)          → TypeCatalog.typeName
pg_attribute (カラム名・型)    → TypeCatalog.fields: [FieldSchema]
pg_type (データ型定義)         → FieldSchema.type: FieldSchemaType
pg_index (インデックス定義)    → TypeCatalog.indexes: [IndexCatalog]
pg_namespace (名前空間)        → TypeCatalog.directoryComponents
```

| コンポーネント | 責務 | ファイル |
|--------------|------|---------|
| `TypeCatalog` | 型のメタデータ（フィールド、インデックス、ディレクトリ構造） | `Sources/DatabaseEngine/Registry/TypeCatalog.swift` |
| `IndexCatalog` | インデックスのメタデータ（名前、種別、フィールド） | 同上 |
| `DirectoryComponentCatalog` | ディレクトリパスの各要素（静的パス or 動的フィールド参照） | 同上 |
| `SchemaRegistry` | FDB への TypeCatalog 永続化・読み取り | `Sources/DatabaseEngine/Registry/SchemaRegistry.swift` |
| `DynamicProtobufDecoder` | TypeCatalog を使った Protobuf 動的デコード | `Sources/DatabaseEngine/Registry/DynamicProtobufDecoder.swift` |
| `DynamicProtobufEncoder` | TypeCatalog を使った Protobuf 動的エンコード | `Sources/DatabaseEngine/Registry/DynamicProtobufEncoder.swift` |

**ライフサイクル**: `FDBContainer.init(for:)` → `ensureIndexesReady()` → `SchemaRegistry.persist(schema)` で自動的にカタログが FDB に書き込まれる。

### DatabaseCLI

`@Persistable` 型なしで FDB データにアクセスする対話型 CLI。TypeCatalog + DynamicProtobuf コーデックで動的アクセスを実現。

```
Sources/DatabaseCLI/
├── Core/
│   ├── DatabaseREPL.swift          # REPL ループ
│   ├── CommandRouter.swift         # コマンド解析・ディスパッチ
│   └── CatalogDataAccess.swift     # TypeCatalog ベースのデータアクセス
├── Commands/
│   ├── DataCommands.swift          # insert/get/update/delete
│   ├── FindCommands.swift          # find + filter/sort
│   ├── SchemaInfoCommands.swift    # schema list/show
│   ├── GraphCommands.swift         # graph/sparql (embedded mode)
│   └── HistoryCommands.swift       # history (embedded mode)
└── Util/
    ├── CLIError.swift              # エラー定義
    ├── JSONParser.swift            # JSON パース
    └── OutputFormatter.swift       # 出力フォーマット
```

**使用モード**:

```swift
// スタンドアロンモード（TypeCatalog のみ使用）
let database = try FDBClient.openDatabase()
let registry = SchemaRegistry(database: database)
let catalogs = try await registry.loadAll()
let repl = DatabaseREPL(database: database, catalogs: catalogs)
try await repl.run()

// 埋め込みモード（FDBContainer と連携）
let container = try await FDBContainer(for: schema)
let repl = try await DatabaseREPL(container: container)
try await repl.run()
```

**CLI コマンド**:

| コマンド | PostgreSQL 相当 | 説明 |
|---------|----------------|------|
| `schema list` | `\dt` | 全型一覧 |
| `schema show <Type>` | `\d tablename` | フィールド・型・インデックス・ディレクトリ構造 |
| `get <Type> <id>` | `SELECT * WHERE id = ?` | ID でレコード取得 |
| `find <Type> [--where ...]` | `SELECT * FROM t WHERE ...` | フィルタ・ソート・リミット |
| `insert <Type> <json>` | `INSERT INTO t VALUES (...)` | レコード挿入（インデックス更新なし） |
| `delete <Type> <id>` | `DELETE FROM t WHERE id = ?` | レコード削除（インデックス更新なし） |

**動的ディレクトリ（パーティション）対応**:

マルチテナント型など動的ディレクトリを持つ型には `--partition` オプションを使用：
```
get Order order-001 --partition tenantId=tenant_123
find Order --limit 10 --partition tenantId=tenant_123
```

### Core Architecture Design Principles (Context-Centric)

**設計哲学**: Container/Context パターンに準拠

| コンポーネント | 責務 | トランザクション |
|--------------|------|-----------------|
| `FDBContainer` | リソース管理（DB接続、Schema、Directory） | **作成しない** |
| `FDBContext` | データ操作、トランザクション管理、キャッシュ | **作成する** |
| `FDBDataStore` | 低レベル操作（トランザクション内） | **受け取る** |

**禁止事項**:
- ❌ `FDBContainer` にトランザクション作成メソッドを追加しない
- ❌ `FDBDataStore` が独自にトランザクションを作成しない
- ✅ トランザクション作成は `FDBContext` に集約

### Transaction API の使い分け（設計原則）

本フレームワークには**3つのトランザクション API** があり、それぞれ明確な用途がある：

| API | 戻り値 | ReadVersionCache | アクセスレベル | 用途 |
|-----|--------|------------------|---------------|------|
| `context.withTransaction()` | `TransactionContext` | ✅ 使用 | `public` | ユーザー向け高レベルAPI |
| `context.withRawTransaction()` | `TransactionProtocol` | ✅ 使用 | `internal` | 内部インフラ（キャッシュ必要） |
| `database.withTransaction()` | `TransactionProtocol` | ❌ 不使用 | `public` | システム操作 |

```
ユーザーデータの読み書き？
    │
    ├─ YES → context.withTransaction()
    │
    └─ NO → ReadVersionCache が必要？
              │
              ├─ YES → context.withRawTransaction()  [internal]
              │
              └─ NO → database.withTransaction()
                        (DirectoryLayer, Migration, OnlineIndexer, Graph Algorithms)
```

**⚠️ 重要: withTransaction 内で commit() を呼ばないこと**

`withTransaction` はクロージャが正常終了した後、自動的にコミットします。

### FDBContext の変更追跡 API パターン

**重要**: FDBContext は**変更追跡**方式を採用しており、他の ORM とは異なる API パターンを使用します。

#### 正しい API パターン

```swift
// ✅ 正しい: insert → save パターン
let context = FDBContext(container: container)

context.insert(user1)
context.insert(user2)
context.update(user3)
context.delete(user4)

try await context.save()  // 引数なし！一括コミット

// ❌ 間違い: save(item) は存在しない
try await context.save(user1)  // コンパイルエラー
```

#### FDBContext.save() の実装

```swift
// FDBContext.swift:672
public func save() async throws  // 引数なし！
```

このメソッドは：
- **引数を取らない**
- insert/update/delete で登録されたアイテムを一括保存
- 内部で `withTransaction()` を呼び出し
- トランザクション内でインデックス更新を実行

#### なぜこの設計なのか

| パターン | 使用例 | トランザクション境界 |
|---------|-------|------------------|
| **変更追跡** (FDBContext) | Core Data, Hibernate | 明示的な save() 呼び出し |
| **即時保存** (Active Record) | Ruby on Rails, Django ORM | 各メソッド呼び出しごと |

FDBContext は**変更追跡**パターンを採用：

1. **パフォーマンス**: 複数の変更を1トランザクションでコミット
2. **一貫性**: 全ての変更が成功するか、全て失敗するか（ACID）
3. **インデックス最適化**: 変更をバッチ処理してインデックス更新を最適化

#### テストでの使用例

```swift
@Test func testDataInsertion() async throws {
    let container = try await setupContainer()
    let context = FDBContext(container: container)

    // ✅ 正しい
    context.insert(User(name: "Alice"))
    context.insert(User(name: "Bob"))
    context.insert(User(name: "Carol"))
    try await context.save()  // 3つのアイテムを1トランザクションで保存

    // ❌ 間違い
    try await context.save(User(name: "Alice"))  // コンパイルエラー

    // ❌ 間違い（他のORMとの混同）
    let user = User(name: "Alice")
    user.save()  // Active Record パターン（このフレームワークでは使えない）
}
```

#### 変更のクリア

```swift
// 保存前に変更を破棄したい場合
context.insert(user)
context.clearChanges()  // 未保存の変更を全てクリア
```

#### よくある間違い

```swift
// ❌ 間違い: 1アイテムずつ保存しようとする
for user in users {
    context.insert(user)
    try await context.save()  // 毎回トランザクション作成（非効率）
}

// ✅ 正しい: バッチで保存
for user in users {
    context.insert(user)
}
try await context.save()  // 1トランザクションで全て保存

// ❌ 間違い: save() に引数を渡そうとする
let edge = SocialEdge(from: "alice", target: "bob", ...)
try await context.save(edge)  // コンパイルエラー

// ✅ 正しい
context.insert(edge)
try await context.save()
```

#### 他のフレームワークとの比較

| フレームワーク | パターン | API |
|--------------|---------|-----|
| **database-framework** | 変更追跡 | `context.insert(item)` + `context.save()` |
| Core Data | 変更追跡 | `context.insert(object)` + `context.save()` |
| Hibernate | 変更追跡 | `session.save(entity)` + `session.flush()` |
| Active Record | 即時保存 | `user.save()` |
| Eloquent (Laravel) | 即時保存 | `$user->save()` |

### Data Layout in FoundationDB

```
[fdb]/R/[PersistableType]/[id]           → ItemEnvelope(Protobuf-encoded item)
[fdb]/B/[blob-key]                       → Large value blob chunks
[fdb]/I/[indexName]/[values...]/[id]     → Index entry (empty value for scalar)
[fdb]/_metadata/schema/version           → Tuple(major, minor, patch)
[fdb]/_metadata/index/[indexName]/state  → IndexState (readable/write_only/disabled)
[fdb]/_catalog/[typeName]                → JSON-encoded TypeCatalog (schema metadata)
```

**ItemEnvelope Format**: All items are wrapped in `ItemEnvelope` with magic number `ITEM` (0x49 0x54 0x45 0x4D). Reading raw data without `ItemStorage.read()` will fail.

**Catalog Layout**: `/_catalog/` stores `TypeCatalog` entries as JSON, analogous to PostgreSQL's `pg_catalog`. Written by `SchemaRegistry.persist()` during `FDBContainer.init`.

## Implemented Features

### Index Types

各インデックスの詳細は各モジュールの README.md を参照してください。

| Index | Module | README |
|-------|--------|--------|
| Scalar | `ScalarIndex` | [README](Sources/ScalarIndex/README.md) |
| Vector | `VectorIndex` | [README](Sources/VectorIndex/README.md) |
| FullText | `FullTextIndex` | [README](Sources/FullTextIndex/README.md) |
| Spatial | `SpatialIndex` | [README](Sources/SpatialIndex/README.md) |
| Rank | `RankIndex` | [README](Sources/RankIndex/README.md) |
| Permuted | `PermutedIndex` | [README](Sources/PermutedIndex/README.md) |
| Graph | `GraphIndex` | [README](Sources/GraphIndex/README.md) |
| Aggregation | `AggregationIndex` | [README](Sources/AggregationIndex/README.md) |
| Version | `VersionIndex` | [README](Sources/VersionIndex/README.md) |
| Bitmap | `BitmapIndex` | [README](Sources/BitmapIndex/README.md) |
| Leaderboard | `LeaderboardIndex` | [README](Sources/LeaderboardIndex/README.md) |
| Relationship | `RelationshipIndex` | [README](Sources/RelationshipIndex/README.md) |

### QueryAST (クエリ抽象構文木)

SQL/SPARQL クエリの解析・変換・シリアライズを提供するモジュール。詳細は [README](Sources/QueryAST/README.md) を参照。

| コンポーネント | 説明 |
|--------------|------|
| `SQLParser` | SQL クエリのパーサー |
| `SPARQLParser` | SPARQL クエリのパーサー |
| `SelectQuery` | SELECT クエリの AST 表現 |
| `Expression` | 式（比較、算術、論理、集約） |
| `GraphTableSource` | SQL/PGQ GRAPH_TABLE 句 |
| `GraphPattern` | SPARQL グラフパターン |
| `SQLEscape` | SQL/SPARQL インジェクション対策ユーティリティ |

**主な機能**:
- SQL/SPARQL クエリの解析と AST 生成
- プログラマティックなクエリ構築（ビルダーパターン）
- AST から SQL/SPARQL への安全なシリアライズ（識別子エスケープ）
- SQL/PGQ グラフパターンマッチング対応 (ISO/IEC 9075-16:2023)
- SPARQL 1.1/1.2 プロパティパス対応
- クエリ分析（変数参照、集約関数検出）

### Extension Pattern for Optional Features

**設計原則**: このプロジェクトは SPM dependencies でカスタマイズ可能なデータベースを目指している。オプション機能は extension で提供し、コアを変更しない。

```swift
// 各機能モジュールが FDBContext に extension で API を追加
extension FDBContext {
    public func findSimilar<T: Persistable>(_ type: T.Type) -> VectorQueryBuilder<T>
    public func search<T: Persistable>(_ type: T.Type) -> FullTextQueryBuilder<T>
}
```

**重要な禁止事項**:
- ❌ コアの `FDBContext.save()` や `delete()` を直接変更してオプション機能を埋め込まない
- ✅ 新しいメソッドを extension で追加

## Index Implementation Pattern

インデックスモジュールは**書き込み（維持）**と**読み取り（クエリ）**の2つの責務を持つ。

### モジュール構成

```
Sources/{IndexName}Index/
├── {IndexName}IndexKind+Maintainable.swift  # IndexKindMaintainable conformance
├── {IndexName}IndexMaintainer.swift         # IndexMaintainer 実装 + search()
├── {IndexName}Query.swift                   # FDBContext extension + QueryBuilder
└── README.md                                # 使用方法とユースケース
```

### IndexMaintainer プロトコル

```swift
public protocol IndexMaintainer<Item>: Sendable {
    associatedtype Item: Persistable

    func updateIndex(oldItem: Item?, newItem: Item?, transaction: any TransactionProtocol) async throws
    func scanItem(_ item: Item, id: Tuple, transaction: any TransactionProtocol) async throws
    func computeIndexKeys(for item: Item, id: Tuple) async throws -> [FDB.Bytes]
    var customBuildStrategy: (any IndexBuildStrategy<Item>)? { get }
}
```

### Query Builder パターン

```
FDBContext.{queryMethod}()     ← Entry Point (extension で追加)
    ↓
{Index}EntryPoint<T>           ← フィールド/オプション選択
    ↓
{Index}QueryBuilder<T>         ← パラメータ設定 (Fluent API)
    ↓
execute()                      ← IndexQueryContext 経由で実行
```

### 命名規約

| コンポーネント | 命名パターン | 例 |
|--------------|-------------|-----|
| Entry Point メソッド | `context.{動詞}(Type.self)` | `findSimilar`, `search`, `nearby`, `rank` |
| Entry Point 型 | `{Index}EntryPoint<T>` | `VectorEntryPoint`, `FullTextEntryPoint` |
| Query Builder 型 | `{Index}QueryBuilder<T>` | `VectorQueryBuilder`, `SpatialQueryBuilder` |
| Maintainer 型 | `{Algorithm}IndexMaintainer<T>` | `HNSWIndexMaintainer`, `FlatVectorIndexMaintainer` |

## Value Access Architecture（値アクセス3層アーキテクチャ）

フィルタリング・ソートはすべてのインデックスモジュールから統一的に利用される共通基盤。各インデックスが独自に評価ロジックを実装してはならない。

### 3層構造

```
Layer 1: ゼロコピーパス（構築時クロージャ）
    FieldComparison._evaluate / SortDescriptor._compare
    → 型付き KeyPath<T, V> でネイティブ比較、存在型を経由しない

Layer 2: FieldReader パス（型消去後のフォールバック）
    FieldReader.read() → FieldValue 変換 → FieldValue 比較
    → QueryRewriter/DNFConverter で型情報が失われた場合のみ使用

Layer 3: DataAccess（ストレージ層）
    DataAccess.deserialize() → throws（エラーを伝播、握りつぶさない）
```

### ゼロコピー評価の仕組み

演算子（`==`, `<` 等）で `Predicate` を構築すると、型付きクロージャが `FieldComparison._evaluate` にキャプチャされる。

```swift
// 構築時: 型情報がクロージャに閉じ込められる
let predicate: Predicate<User> = \.age == 30
// 内部:
//   nonisolated(unsafe) let kp = keyPath  // KeyPath<User, Int>
//   _evaluate = { model in model[keyPath: kp] == v }

// 評価時: Any / FieldValue を経由しない
comparison.evaluate(on: user)  // → _evaluate?(user) → user[keyPath: kp] == 30
```

`SortDescriptor` も同様に `_compare` クロージャでゼロコピー比較を行う。

### 評価フロー

```
evaluate(on: model)
  ├─ _evaluate != nil → ゼロコピーパス（通常のユーザークエリ）
  └─ _evaluate == nil → evaluateViaFieldReader()（型消去後のフォールバック）

orderedComparison(lhs, rhs)
  ├─ _compare != nil → ゼロコピーパス
  └─ _compare == nil → compareViaFieldReader()
```

### 禁止事項

- ❌ `FDBDataStore`、`PlanExecutor`、各インデックスモジュールが独自の `evaluateComparison` を実装しない
- ❌ `try?` でエラーを握りつぶさない（`extractIndexValues`、`DataAccess.deserialize`、`valueToTuple` は全て `throws`）
- ❌ `String(describing:)` で型変換のフォールバックをしない（インデックスキーの順序が壊れる）
- ❌ `PartialKeyPath` の戻り値に対して `raw == nil` で null 判定しない（Optional-in-Any ボクシング問題）
- ✅ null 判定には `FieldValue` 変換後の `.isNull` を使用
- ✅ 全ての呼び出し元は `comparison.evaluate(on:)` と `descriptor.orderedComparison()` を使用

### KeyPath と Sendable

`KeyPath` は `Sendable` に準拠していないため、`@Sendable` クロージャでキャプチャする際は `nonisolated(unsafe) let` を使用する。

```swift
// ✅ 正しい
nonisolated(unsafe) let kp = keyPath
let closure: @Sendable (T) -> Bool = { model in model[keyPath: kp] == value }

// ❌ コンパイルエラー
let closure: @Sendable (T) -> Bool = { model in model[keyPath: keyPath] == value }
```

### 関連ファイル

| ファイル | 責務 |
|---------|------|
| `Sources/DatabaseEngine/Fetch/FDBFetchDescriptor.swift` | `FieldComparison.evaluate(on:)`, `SortDescriptor.orderedComparison()`, 演算子オーバーロード |
| `Sources/DatabaseEngine/Core/FieldReader.swift` | Layer 2: 非 throwing フィールド読み取り（dynamicMember / ネストフィールド） |
| `Sources/DatabaseEngine/Internal/FDBDataStore.swift` | `evaluatePredicate()` が `evaluate(on:)` を呼ぶ |
| `Sources/DatabaseEngine/QueryPlanner/PlanExecutor.swift` | フィルタ・ソートが `evaluate(on:)` / `orderedComparison()` を呼ぶ |
| `Sources/DatabaseEngine/QueryPlanner/AggregationPlanExecutor.swift` | 集約クエリのフィルタが `evaluate(on:)` を呼ぶ |

## Tuple Encoding Convention

### TupleEncoder / TupleDecoder

**必須**: 全ての Index モジュールは `TupleEncoder` / `TupleDecoder` を使用すること。

```swift
// ✅ 正しい: TupleEncoder / TupleDecoder を使用
import DatabaseEngine

let element = try TupleEncoder.encode(value)
let score = try TupleDecoder.decode(element, as: Int64.self)

// ❌ 禁止: 独自のエンコーディング実装
switch value {
case let d as Double:
    return String(format: "%020.6f", d)  // 禁止！順序が壊れる
case let i as Int:
    return String(format: "%020d", i)    // 禁止！
}
```

### 型マッピング

| Swift Type | TupleElement | Notes |
|------------|--------------|-------|
| String | String | そのまま |
| Int, Int8-64 | Int64 | 拡張 |
| UInt, UInt8-64 | Int64 | オーバーフローチェック付き |
| Double | Double | IEEE 754 (FDBが順序保証) |
| Float | Double | 拡張 |
| Bool | Bool | そのまま |
| Date | Date | fdb-swift-bindings が処理 |
| UUID | UUID | そのまま |
| Data | [UInt8] | バイト配列に変換 |

### 理由

- FDB Tuple Layer は Double の辞書順を保証する（IEEE 754 準拠）
- String フォーマットへの変換は順序を破壊する
- 型の一貫性がないとインデックスの整合性が失われる

### 参照

- `Sources/DatabaseEngine/Core/TupleEncoder.swift`
- `Sources/DatabaseEngine/Core/TupleDecoder.swift`

## Testing Pattern

### FDB 初期化

Tests use a shared singleton for FDB initialization:

```swift
import Testing
@testable import DatabaseEngine

@Suite struct MyTests {
    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test func myTest() async throws { ... }
}
```

### @Persistable マクロ必須

**重要**: テストモデルを含む全ての `Persistable` 型は `@Persistable` マクロを使用する必要がある。手動実装は `ItemEnvelope` 形式との互換性問題が発生する。

```swift
// ✅ 良い例
@Persistable
struct MyModel {
    #Directory<MyModel>("test", "models")
    var id: String = UUID().uuidString
    var name: String = ""
}
```

### テスト分離パターン

テストは並列実行されるため、UUID ベースのユニーク ID を使用する：

```swift
@Suite("My Tests", .serialized)
struct MyTests {
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test func testSomething() async throws {
        let customerId = uniqueID("C-test")
        // ...
    }
}
```

**ガイドライン**:
- ✅ 全ての ID を `uniqueID("prefix")` で生成
- ❌ `cleanup()` で `clearAll()` を呼ばない
- ❌ ハードコードされた ID を使用しない

## IndexKind と IndexDescriptor の正しい使い方

### 重要な原則: 全ての IndexKind は KeyPath ベース

**絶対に忘れないこと**: 全ての IndexKind（Vector, Graph, FullText, Spatial等）は **KeyPath ベースの初期化子**を持っており、これを使うのが正しい設計です。

```swift
// ✅ 正しい: KeyPath ベースの初期化
let graphIndex = GraphIndexKind<Edge>(
    from: \.source,     // KeyPath<Edge, String>
    edge: \.label,
    to: \.target,
    graph: \.graphId,
    strategy: .adjacency
)

let vectorIndex = VectorIndexKind<Product>(
    embedding: \.embedding,  // KeyPath<Product, [Float]>
    dimensions: 384,
    metric: .cosine
)

// ❌ 間違い: String ベースの初期化（Codable reconstruction 用）
let graphIndex = GraphIndexKind<Edge>(
    fromField: "source",     // String（内部用）
    edgeField: "label",
    toField: "target",
    graphField: "graphId",
    strategy: .adjacency
)
```

### なぜ String ベースの初期化子が存在するのか

```swift
// GraphIndexKind.swift:183-196
/// Initialize with field name strings (for Codable reconstruction)
public init(
    fromField: String,
    edgeField: String,
    toField: String,
    graphField: String? = nil,
    strategy: GraphIndexStrategy = .tripleStore
) { ... }
```

このイニシャライザは：
- **Codable conformance** で使用（JSON/Protobuf からのデシリアライズ）
- **SchemaRegistry** での TypeCatalog 復元用
- **通常のコードでは使用してはいけない**

### IndexDescriptor には必ず KeyPath を渡す

```swift
// ✅ 正しい
IndexDescriptor(
    name: "user_email_index",
    keyPaths: [\User.email],
    kind: ScalarIndexKind(),
    commonOptions: .init(unique: true)
)

IndexDescriptor(
    name: "social_graph_index",
    keyPaths: [\Edge.from, \Edge.label, \Edge.to],
    kind: GraphIndexKind<Edge>(
        from: \.from,
        edge: \.label,
        to: \.target,
        strategy: .tripleStore
    ),
    storedFieldNames: ["weight", "timestamp"]
)

// ❌ 間違い: anyKeyPaths を使う（型安全性を失う）
IndexDescriptor(
    name: "social_graph_index",
    anyKeyPaths: [],  // 型推論できない
    kind: GraphIndexKind<Edge>(
        fromField: "from",  // String ベース（内部用初期化子）
        edgeField: "label",
        toField: "target",
        ...
    )
)
```

### KeyPath を使う理由

| 利点 | 説明 |
|------|------|
| **型安全性** | コンパイル時にフィールドの存在を検証 |
| **リファクタリング耐性** | IDE でフィールド名を変更すると自動更新 |
| **自己文書化** | KeyPath を見るだけでどのフィールドか分かる |
| **実行時アクセス** | `Root.fieldName(for: keyPath)` で名前取得可能 |

### @Persistable マクロとの統合

```swift
@Persistable
struct SocialEdge {
    #Directory<SocialEdge>("graphs", "social")

    var id: String = UUID().uuidString
    var from: String = ""
    var target: String = ""
    var label: String = ""
    var weight: Double = 0.0

    // ✅ 正しい: KeyPath を使う
    #Index(GraphIndexKind<SocialEdge>(
        from: \.from,
        edge: \.label,
        to: \.target,
        strategy: .adjacency
    ), storedFields: [\SocialEdge.weight])
}
```

### よくある間違い

#### 間違い 1: 空の KeyPath 配列で型推論に失敗

```swift
// ❌ エラー: generic parameter 'Root' could not be inferred
IndexDescriptor(
    name: "index",
    keyPaths: [],  // 空配列から型推論できない
    kind: GraphIndexKind<T>(...)
)

// ✅ 解決策: 実際の KeyPath を渡す
IndexDescriptor(
    name: "index",
    keyPaths: [\T.field1, \T.field2],
    kind: GraphIndexKind<T>(
        from: \.field1,
        edge: \.field2,
        ...
    )
)
```

#### 間違い 2: anyKeyPaths を使って回避しようとする

```swift
// ❌ 技術的には動くが、型安全性を失う
IndexDescriptor(
    name: "index",
    anyKeyPaths: [],  // 内部用API
    kind: GraphIndexKind<T>(
        fromField: "field1",  // String ベース（内部用）
        edgeField: "field2",
        ...
    )
)
```

**anyKeyPaths パラメータは内部用**：
- SchemaRegistry での復元用
- 通常のコードでは使用禁止
- KeyPath の型安全性の恩恵を全て失う

#### 間違い 3: 手動 Persistable 実装

```swift
// ❌ 複雑で間違いやすい
struct MyModel: Persistable {
    static var indexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "...",
                anyKeyPaths: [],  // 手動実装では anyKeyPaths を使いがち
                kind: ...
            )
        ]
    }

    subscript(dynamicMember member: String) -> (any Sendable)? { ... }
    static func fieldName<Value>(for keyPath: KeyPath<MyModel, Value>) -> String { ... }
    // ... その他10個以上のプロトコル要件
}

// ✅ @Persistable マクロを使う
@Persistable
struct MyModel {
    #Index(...)  // マクロが正しい IndexDescriptor を生成
    var field: String = ""
}
```

### デバッグのヒント

IndexDescriptor でエラーが出たら：

1. **KeyPath を使っているか確認**:
   ```swift
   keyPaths: [\Type.field]  // ✅
   anyKeyPaths: [...]        // ❌
   ```

2. **IndexKind の初期化子が KeyPath ベースか確認**:
   ```swift
   GraphIndexKind(from: \.field, ...)  // ✅
   GraphIndexKind(fromField: "field", ...)  // ❌
   ```

3. **@Persistable マクロを使っているか確認**:
   ```swift
   @Persistable struct T { ... }  // ✅
   struct T: Persistable { ... }  // ❌（手動実装は避ける）
   ```

## Adding a New Index Type

1. Define `IndexKind` in database-kit (e.g., `TimeSeriesIndexKind`)
2. Create new module in `Sources/` (e.g., `TimeSeriesIndex/`)
3. Add `IndexKindMaintainable` extension
4. Implement `IndexMaintainer` struct
5. Add module to `Package.swift` and `Database` target dependencies
6. Create `README.md` with use cases and examples

## 統一型変換規約

**必須**: 全モジュールは `TypeConversion` を使用すること。独自の型変換実装は禁止。

```swift
import DatabaseEngine

// ✅ 正しい: TypeConversion を使用
let int64 = TypeConversion.asInt64(value)           // 値抽出 (比較用)
let double = TypeConversion.asDouble(value)         // 値抽出 (集計用)
let float = TypeConversion.asFloat(value)           // 値抽出 (ベクトル変換用)
let string = TypeConversion.asString(value)         // 値抽出 (文字列比較用)
let fieldValue = TypeConversion.toFieldValue(value) // FieldValue 変換
let element = try TypeConversion.toTupleElement(value)  // TupleElement 変換

// TupleElement からの抽出
let score = try TypeConversion.int64(from: element)
let price = try TypeConversion.double(from: element)
let name = try TypeConversion.string(from: element)

// ❌ 禁止: 独自の型変換実装
switch value {
case let v as Int64: return v
case let v as Int: return Int64(v)  // 禁止！
...
}
```

### 型マッピング仕様

| Swift Type       | Int64 | Double | Float | String | FieldValue      |
|------------------|-------|--------|-------|--------|-----------------|
| Int, Int8-64     | ✓     | ✓      | ✓     | -      | .int64          |
| UInt, UInt8-64   | ✓*    | ✓      | ✓     | -      | .int64          |
| Double           | -     | ✓      | ✓     | -      | .double         |
| Float            | -     | ✓      | ✓     | -      | .double         |
| String           | -     | -      | -     | ✓      | .string         |
| Bool             | ✓**   | -      | -     | -      | .bool           |
| Date             | -     | ✓***   | -     | -      | .double         |
| UUID             | -     | -      | -     | ✓      | .string         |

\* UInt64 > Int64.max はオーバーフロー（nil）
\** Bool: true=1, false=0
\*** Date: timeIntervalSince1970

### ラッパーメソッド禁止（直接呼出の原則）

`TypeConversion` / `TupleEncoder` / `TupleDecoder` を呼ぶだけの private メソッドを作成しない。直接呼び出すこと。

```swift
// ❌ 禁止: パススルーラッパー
private func convertToTupleElement(_ value: any Sendable) throws -> any TupleElement {
    try TupleEncoder.encode(value)
}

// ❌ 禁止: エラー再ラップだけのラッパー
private func extractScore(from element: any TupleElement) throws -> Score {
    do {
        return try TupleDecoder.decode(element, as: Score.self)
    } catch {
        throw MyError.invalidScore("...")
    }
}

// ❌ 禁止: 手動型スイッチ（TypeConversion/TupleDecoder が内部で処理済み）
private func extractNumericValue(from element: any TupleElement) -> Double? {
    if let d = element as? Double { return d }
    if let i = element as? Int64 { return Double(i) }
    return nil
}

// ✅ 正しい: 呼出元で直接使用
let element = try TupleEncoder.encode(value)
let score = try TupleDecoder.decode(element, as: Score.self)
let double = try TypeConversion.double(from: element)
let value = TypeConversion.asDouble(rawValue)
```

**許容されるケース**: 複数の API を組み合わせて構造化された結果を返す場合のみ、ユーティリティ関数を作成してよい。

```swift
// ✅ 許容: 複合的なロジックを持つユーティリティ
public static func extractNumeric<Value>(
    from element: any TupleElement, as valueType: Value.Type
) throws -> (int64: Int64?, double: Double?, isFloatingPoint: Bool) {
    switch valueType {
    case is Int64.Type, is Int.Type, is Int32.Type:
        return (int64: try TypeConversion.int64(from: element), double: nil, isFloatingPoint: false)
    case is Double.Type, is Float.Type:
        return (int64: nil, double: try TypeConversion.double(from: element), isFloatingPoint: true)
    default:
        throw IndexError.invalidConfiguration("Unsupported: \(valueType)")
    }
}
```

### 関連ファイル

- `Sources/DatabaseEngine/Core/TypeConversion.swift` - 統一型変換ユーティリティ
- `Sources/DatabaseEngine/Core/TupleEncoder.swift` - Any → TupleElement 変換
- `Sources/DatabaseEngine/Core/TupleDecoder.swift` - TupleElement → T 変換

## Swift Concurrency Pattern

### このプロジェクトでの使い分け

database-frameworkは**メモリ内キャッシュが多い**ため、ほとんどのケースでMutexが適切です。

#### 現在の実装（Mutex優先）

**キャッシュ層**: すべてMutexベース
```swift
// ✅ IndexStateCache: メモリアクセスのみ
final class IndexStateCache: Sendable {
    private let state: Mutex<State>

    func get(_ key: String) -> IndexState? {
        state.withLock { $0.cache[key] }  // マイクロ秒オーダー
    }
}

// ✅ ReadVersionCache: メモリアクセスのみ
final class ReadVersionCache: Sendable {
    private let cache: Mutex<CachedVersion?>

    func getCachedVersion(policy: CachePolicy) -> Int64? {
        // 高頻度、I/Oなし
    }
}
```

**理由**:
- I/O操作を含まない（メモリアクセスのみ）
- 高頻度アクセスで細粒度ロックが有利
- ロック中に`await`する必要がない

#### 将来actorが必要になる箇所

以下のような**I/O操作を含む**コンポーネントには`actor`を使用：

```swift
// ✅ Actor: FDB watch（I/O操作）
actor WatchManager {
    private var watchers: [String: Continuation] = [:]

    func watch(key: String) async -> ChangeEvent {
        // FDB watch API - I/O待ちの間に他のタスクを処理
        await fdbWatch(key)
    }
}

// ✅ Actor: オンラインインデックス構築（長時間I/O）
actor OnlineIndexBuilder {
    private var progress: Int = 0

    func buildIndex() async {
        while hasMore {
            let batch = await fetchBatch()  // I/O
            await processBatch(batch)       // I/O
            progress += batch.count
        }
    }
}
```

### 判断基準（`~/.claude/rules/swift.md`参照）

```
ロック中にI/O操作（await）が必要？
  ├─ YES → Actor
  └─ NO → 処理の実行順序が重要？
            ├─ YES → Actor
            └─ NO → Mutex
```

### Mutex パターンのテンプレート

```swift
import Synchronization

public final class ClassName: Sendable {
    // FDBなど、Sendableな外部リソース
    nonisolated(unsafe) private let database: any DatabaseProtocol

    // 可変状態をStructでまとめる
    private struct State: Sendable {
        var counter: Int = 0
        var cache: [String: Value] = [:]
    }
    private let state: Mutex<State>

    public func operation() async throws {
        // 1. ロック内でメモリアクセス（短時間）
        let count = state.withLock { state in
            state.counter += 1
            return state.counter
        }

        // 2. I/O操作はロック外（Mutexを保持しない）
        try await database.withTransaction { transaction in
            // FDB I/O
        }
    }
}
```

### ガイドライン

- ✅ メモリアクセスのみ → `final class + Mutex`
- ✅ I/O操作を含む → `actor`
- ✅ `DatabaseProtocol` には `nonisolated(unsafe)` を使用
- ✅ ロックスコープは最小限（I/O を含めない）
- ❌ `@unchecked Sendable` は避ける

## 実装品質ガイドライン

### 基本方針

**実装コストは考慮しない。最適化と完成度が最優先。**

### 必須要件

1. **学術的根拠**: アルゴリズムは論文・教科書に基づく。独自手法は禁止
2. **参照実装の調査**: PostgreSQL, FoundationDB Record Layer, CockroachDB 等を必ず参照
3. **包括的テスト**: 新機能には必ずユニットテストを追加
4. **エッジケース網羅**: 境界値、空入力、大規模データを全てカバー

### コーディング規約

```swift
// ✅ 良い例: 根拠を明記
/// Reservoir Sampling (Algorithm R)
/// Reference: Vitter, J.S. "Random Sampling with a Reservoir", ACM TOMS 1985
/// Time: O(n), Space: O(k) where k = reservoir size
public struct ReservoirSampling<T> { ... }

// ❌ 悪い例: 根拠不明のマジックナンバー
let base = 256.0  // なぜ256？
```

### デフォルト値の扱い

**デフォルト値がある場合、Optional + nil合体演算子ではなく、直接デフォルト値を設定する。**

```swift
// ✅ 良い例
public let retryLimit: Int
public init(retryLimit: Int = 5) {
    self.retryLimit = retryLimit
}

// ❌ 悪い例
public let retryLimit: Int?
public init(retryLimit: Int? = nil) { ... }
```

### 参照すべきリソース

| 分野 | 参照先 |
|------|--------|
| クエリ最適化 | PostgreSQL src/backend/optimizer/ |
| インデックス設計 | FoundationDB Record Layer |
| 分散システム | CockroachDB, TiDB |
| アルゴリズム全般 | CLRS "Introduction to Algorithms" |

## 開発方針

### プロジェクト状態

**このプロジェクトは開発中であり、後方互換性を考慮する必要はない。**

### コード管理原則

1. **不要なコードは削除**: 使用されていないコード・互換性レイヤー・デッドコードは即座に削除
2. **破壊的変更を恐れない**: API・データフォーマットの変更は自由に行う
3. **最適な設計を優先**: 後方互換性のためのワークアラウンドよりも、正しい設計を選択

### Subspace キー設計

整数キーを使用し、効率性を優先：

```swift
// ✅ 推奨: 整数キー
subspace.subspace(SubspaceKey.nodes.rawValue)

// ❌ 避ける: 文字列キー
subspace.subspace("nodes")
```

### Persistable フィールドアクセス（Mirror禁止）

`Persistable`型のフィールドアクセスには`Mirror`を使用しない。

```swift
// ✅ 良い例: dynamicMember subscript を使用
func extractField<T: Persistable>(from item: T, fieldName: String) -> Any? {
    return item[dynamicMember: fieldName]
}

// ❌ 悪い例: Mirror を使用
let mirror = Mirror(reflecting: item)
```

## Known Limitations

### Watch機能 (fdb-swift-bindings拡張が必要)

`WatchManager`はスタブ実装。fdb-swift-bindings に `watch(key:)` メソッドの追加が必要。

### インデックス再構築 (簡易実装)

`AdminContext.rebuildIndex()`は簡易実装。本番環境では`OnlineIndexer`を使用すべき。
