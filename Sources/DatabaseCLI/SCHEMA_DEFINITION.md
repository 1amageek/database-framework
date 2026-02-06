# Schema Definition Language Specification

database-frameworkのスキーマ定義仕様書

## 概要

database-frameworkは、Swiftコードの`@Persistable`マクロを使用せずに、YAMLファイルまたはCLIコマンドでスキーマとインデックスを定義できます。これにより：

- 動的なスキーマ定義が可能
- 他言語クライアントとのスキーマ共有
- バージョン管理とレビュー
- 実行時のスキーマ変更

## 基本構文

### YAML形式

```yaml
TypeName:
  #Directory: [path, components]

  fieldName: fieldType
  fieldName: fieldType#indexKind
  fieldName: fieldType#indexKind(option:value, ...)

  #Index(kind, param:value, ...)
  #Relationship(type, target:Type, ...)
```

### フィールド定義

#### 基本形式

```
fieldName: fieldType
```

#### インデックス付きフィールド

```
fieldName: fieldType#indexKind
fieldName: fieldType#indexKind(option:value, option:value)
```

## 型システム

### プリミティブ型

| 型名 | FieldSchemaType | Swift対応型 | 説明 |
|------|-----------------|-------------|------|
| `string` | `.string` | `String` | 文字列 |
| `int` | `.int64` | `Int`, `Int64` | 64ビット整数 |
| `int64` | `.int64` | `Int64` | 64ビット整数 |
| `double` | `.double` | `Double` | 浮動小数点 |
| `float` | `.double` | `Float` | 浮動小数点（内部はDouble） |
| `bool` | `.bool` | `Bool` | 真偽値 |
| `date` | `.date` | `Date` | 日付時刻 |
| `uuid` | `.uuid` | `UUID` | UUID |
| `data` | `.data` | `Data` | バイナリデータ |

### コンテナ型

#### 配列

```yaml
tags: array<string>
scores: array<double>
embedding: array<float>
```

#### Optional

```yaml
nickname: optional<string>
middleName: optional<string>
```

#### ネスト

```yaml
optionalTags: optional<array<string>>
```

## ディレクトリ定義

### 静的ディレクトリ

```yaml
User:
  #Directory: [app, users]

  id: string
  name: string
```

FoundationDBパス: `/app/users/`

### 動的ディレクトリ（パーティション）

```yaml
Order:
  #Directory:
    - orders
    - field: tenantId

  id: string
  tenantId: string
  amount: double
```

FoundationDBパス: `/orders/{tenantId}/`

## インデックス定義

### 1. Scalar Index（スカラーインデックス）

#### 単一フィールド

```yaml
User:
  #Directory: [app, users]

  id: string
  email: string#scalar(unique)
  age: int#scalar
```

#### 複合インデックス

```yaml
User:
  #Directory: [app, users]

  id: string
  name: string
  age: int
  city: string

  #Index(scalar, fields:[name, age])
  #Index(scalar, fields:[city, age], unique:true)
```

**オプション**:
- `unique: true` - ユニーク制約

**用途**: 等価検索、範囲検索、ソート

---

### 2. Vector Index（ベクトルインデックス）

```yaml
Product:
  #Directory: [catalog, products]

  id: string
  name: string
  embedding: array<float>#vector(dimensions:384, metric:cosine, algorithm:hnsw)
```

**必須パラメータ**:
- `dimensions: int` - ベクトル次元数

**オプション**:
- `metric: cosine|euclidean|dotProduct` - 距離関数（デフォルト: cosine）
- `algorithm: hnsw|flat` - アルゴリズム（デフォルト: hnsw）
- `m: int` - HNSW M パラメータ（デフォルト: 16）
- `efConstruction: int` - HNSW efConstruction（デフォルト: 200）

**用途**: 類似ベクトル検索、埋め込み検索、RAG

---

### 3. FullText Index（全文検索インデックス）

```yaml
Article:
  #Directory: [content, articles]

  id: string
  title: string#fulltext(language:english)
  body: string#fulltext(language:english, tokenizer:standard)
```

**オプション**:
- `language: english|japanese|...` - 言語（デフォルト: english）
- `tokenizer: standard|ngram|...` - トークナイザー

**用途**: テキスト検索、BM25ランキング

---

### 4. Spatial Index（空間インデックス）

```yaml
Store:
  #Directory: [locations, stores]

  id: string
  name: string
  location: string#spatial(strategy:geohash)
  bounds: string#spatial(strategy:rtree)
```

**オプション**:
- `strategy: geohash|rtree` - インデックス戦略（デフォルト: geohash）

**用途**: 位置情報検索、範囲検索

---

### 5. Rank Index（ランクインデックス）

```yaml
Product:
  #Directory: [catalog, products]

  id: string
  rating: double#rank
  popularity: double#rank
```

**用途**: TOP-K クエリ、ランキング

---

### 6. Permuted Index（順列インデックス）

```yaml
Product:
  #Directory: [catalog, products]

  id: string
  category: string
  brand: string
  price: double

  #Index(permuted, fields:[category, brand, price])
```

フィールドの全順列組み合わせでインデックス作成：
- `[category, brand, price]`
- `[category, price, brand]`
- `[brand, category, price]`
- `[brand, price, category]`
- `[price, category, brand]`
- `[price, brand, category]`

**用途**: 複数条件での効率的な検索

---

### 7. Graph Index（グラフインデックス）

#### Triple Store（RDFトリプルストア）

```yaml
RDFTriple:
  #Directory: [knowledge, rdf]

  id: string
  subject: string
  predicate: string
  object: string
  graph: string

  #Index(graph, from:subject, edge:predicate, to:object, graph:graph, strategy:tripleStore)
```

#### Adjacency List（隣接リスト）

```yaml
Follow:
  #Directory: [social, follows]

  id: string
  follower: string
  following: string
  timestamp: date
  weight: double

  #Index(graph, from:follower, edge:follows, to:following, strategy:adjacency, storedFields:[weight, timestamp])
```

**必須パラメータ**:
- `from: fieldName` - 始点フィールド
- `edge: fieldName|literal` - エッジラベル（フィールド名または固定文字列）
- `to: fieldName` - 終点フィールド

**オプション**:
- `graph: fieldName` - 名前付きグラフフィールド
- `strategy: tripleStore|adjacency` - 格納戦略（デフォルト: tripleStore）
- `storedFields: [fieldName, ...]` - 一緒に保存するフィールド

**用途**: グラフ探索、SPARQL クエリ、ソーシャルグラフ

---

### 8. Aggregation Index（集約インデックス）

```yaml
Order:
  #Directory: [ecommerce, orders]

  id: string
  amount: double#aggregation(functions:[sum, avg, min, max])
  quantity: int#aggregation(functions:[count, sum])
```

**必須パラメータ**:
- `functions: [count|sum|avg|min|max, ...]` - 集約関数リスト

**用途**: 事前計算された集計値、ダッシュボード

---

### 9. Version Index（バージョンインデックス）

```yaml
Document:
  #Directory: [docs, documents]

  id: string
  title: string
  content: string#version
  metadata: string#version
```

**用途**: 履歴管理、Point-in-Time クエリ、監査ログ

---

### 10. Bitmap Index（ビットマップインデックス）

```yaml
Product:
  #Directory: [catalog, products]

  id: string
  category: string#bitmap
  status: string#bitmap
  inStock: bool#bitmap
```

**用途**: 低カーディナリティフィールドの高速検索

---

### 11. Leaderboard Index（リーダーボードインデックス）

```yaml
Player:
  #Directory: [game, players]

  id: string
  name: string
  score: int#leaderboard(name:global_ranking)
  level: int#leaderboard(name:level_ranking)
```

**必須パラメータ**:
- `name: string` - リーダーボード名

**用途**: リアルタイムランキング、ゲームスコア

---

### 12. Relationship Index（リレーションシップインデックス）

```yaml
UserGroup:
  #Directory: [app, user_groups]

  id: string
  userId: string
  groupId: string
  role: string

  #Index(relationship, from:userId, to:groupId)
```

**必須パラメータ**:
- `from: fieldName` - 外部キー（始点）
- `to: fieldName` - 外部キー（終点）

**用途**: One-to-Many, Many-to-Many リレーション

---

## リレーションシップ定義

### One-to-Many（一対多）

#### 親側（One）

```yaml
User:
  #Directory: [app, users]

  id: string
  name: string

  #Relationship(hasMany, target:Post, foreignKey:authorId, name:posts)
```

#### 子側（Many）

```yaml
Post:
  #Directory: [app, posts]

  id: string
  authorId: string
  title: string

  #Relationship(belongsTo, target:User, foreignKey:authorId, name:author)
```

### Many-to-Many（多対多）

#### User側

```yaml
User:
  #Directory: [app, users]

  id: string
  name: string

  #Relationship(manyToMany, target:Group, through:UserGroup, name:groups)
```

#### Group側

```yaml
Group:
  #Directory: [app, groups]

  id: string
  name: string

  #Relationship(manyToMany, target:User, through:UserGroup, name:members)
```

#### 中間テーブル

```yaml
UserGroup:
  #Directory: [app, user_groups]

  id: string
  userId: string
  groupId: string
  role: string
  joinedAt: date

  #Index(relationship, from:userId, to:groupId)
```

### パーティション対応リレーション

```yaml
TenantOrder:
  #Directory:
    - orders
    - field: tenantId

  id: string
  tenantId: string
  userId: string
  amount: double

  #Relationship(belongsTo, target:TenantUser, foreignKey:userId, partition:tenantId, name:user)
```

### リレーションシップタイプ

| タイプ | 説明 | 必須パラメータ |
|--------|------|---------------|
| `hasMany` | 一対多（親） | `target`, `foreignKey`, `name` |
| `belongsTo` | 一対多（子） | `target`, `foreignKey`, `name` |
| `manyToMany` | 多対多 | `target`, `through`, `name` |

**パラメータ**:
- `target: TypeName` - 関連先の型名
- `foreignKey: fieldName` - 外部キーフィールド
- `through: TypeName` - 中間テーブル（Many-to-Manyのみ）
- `name: string` - リレーション名
- `partition: fieldName` - パーティションフィールド（動的ディレクトリ使用時）

---

## 完全な例

### E-Commerce スキーマ

```yaml
# User.yaml
User:
  #Directory: [ecommerce, users]

  id: string
  email: string#scalar(unique)
  name: string
  phone: string
  totalSpent: double#leaderboard(name:top_customers)
  createdAt: date#scalar

  #Relationship(hasMany, target:Order, foreignKey:userId, name:orders)
  #Relationship(hasMany, target:Review, foreignKey:userId, name:reviews)

# Product.yaml
Product:
  #Directory: [ecommerce, products]

  id: string
  name: string#fulltext(language:english)
  description: string#fulltext(language:english)
  sku: string#scalar(unique)
  category: string#bitmap
  brand: string
  price: double#aggregation(functions:[min, max, avg])
  stock: int#aggregation(functions:[sum])
  rating: double#rank
  embedding: array<float>#vector(dimensions:768, metric:cosine, algorithm:hnsw)
  location: string#spatial(strategy:geohash)

  #Index(scalar, fields:[category, price])
  #Index(permuted, fields:[category, brand, price])
  #Relationship(hasMany, target:Review, foreignKey:productId, name:reviews)
  #Relationship(hasMany, target:OrderItem, foreignKey:productId, name:orderItems)

# Order.yaml
Order:
  #Directory: [ecommerce, orders]

  id: string
  userId: string
  status: string#bitmap
  total: double#aggregation(functions:[sum])
  createdAt: date#scalar

  #Index(scalar, fields:[userId, createdAt])
  #Relationship(belongsTo, target:User, foreignKey:userId, name:user)
  #Relationship(hasMany, target:OrderItem, foreignKey:orderId, name:items)

# OrderItem.yaml
OrderItem:
  #Directory: [ecommerce, order_items]

  id: string
  orderId: string
  productId: string
  quantity: int
  price: double

  #Relationship(belongsTo, target:Order, foreignKey:orderId, name:order)
  #Relationship(belongsTo, target:Product, foreignKey:productId, name:product)

# Review.yaml
Review:
  #Directory: [ecommerce, reviews]

  id: string
  userId: string
  productId: string
  rating: int#scalar
  comment: string#fulltext(language:english)
  createdAt: date#scalar

  #Index(scalar, fields:[productId, rating])
  #Relationship(belongsTo, target:User, foreignKey:userId, name:user)
  #Relationship(belongsTo, target:Product, foreignKey:productId, name:product)
```

### ソーシャルネットワーク

```yaml
# User.yaml
User:
  #Directory: [social, users]

  id: string
  username: string#scalar(unique)
  displayName: string
  bio: string#fulltext(language:english)
  followerCount: int#leaderboard(name:popular_users)

  #Relationship(manyToMany, target:User, through:Follow, name:followers)
  #Relationship(manyToMany, target:User, through:Follow, name:following)

# Follow.yaml
Follow:
  #Directory: [social, follows]

  id: string
  follower: string
  following: string
  timestamp: date

  #Index(graph, from:follower, edge:follows, to:following, strategy:adjacency)
  #Index(relationship, from:follower, to:following)

# Post.yaml
Post:
  #Directory: [social, posts]

  id: string
  authorId: string
  content: string#fulltext(language:english)
  tags: array<string>
  likeCount: int#rank
  createdAt: date#scalar

  #Relationship(belongsTo, target:User, foreignKey:authorId, name:author)
```

### ナレッジグラフ

```yaml
# KnowledgeGraph.yaml
KnowledgeTriple:
  #Directory: [knowledge, triples]

  id: string
  subject: string
  predicate: string
  object: string
  graph: string
  confidence: double
  source: string

  #Index(graph, from:subject, edge:predicate, to:object, graph:graph, strategy:tripleStore, storedFields:[confidence, source])
  #Index(scalar, fields:[graph])
```

### マルチテナント

```yaml
# TenantUser.yaml
TenantUser:
  #Directory:
    - tenants
    - field: tenantId
    - users

  id: string
  tenantId: string#scalar
  email: string#scalar(unique)
  name: string
  role: string#bitmap

  #Index(scalar, fields:[tenantId, email])
  #Relationship(hasMany, target:TenantOrder, foreignKey:userId, partition:tenantId, name:orders)

# TenantOrder.yaml
TenantOrder:
  #Directory:
    - tenants
    - field: tenantId
    - orders

  id: string
  tenantId: string#scalar
  userId: string
  amount: double#aggregation(functions:[sum])
  status: string#bitmap

  #Index(scalar, fields:[tenantId, userId])
  #Relationship(belongsTo, target:TenantUser, foreignKey:userId, partition:tenantId, name:user)
```

---

## CLIコマンド

### スキーマ登録

```bash
# 単一ファイル
database apply schemas/User.yaml

# ディレクトリ一括
database apply schemas/

# 確認
database schema show User

# 一覧
database schema list
```

### スキーマ削除

```bash
database drop User
database drop --all
```

### スキーマエクスポート

```bash
# Schema.Entity を YAML に変換
database export User > User.yaml
database export --all > schemas/
```

### スキーマ検証

```bash
# 登録せずに検証のみ
database validate schemas/User.yaml
```

---

## Schema.Entity との対応

YAMLで定義したスキーマは、内部的に`Schema.Entity`に変換され、`SchemaRegistry`を通じてFoundationDBに永続化されます。

```
YAML Schema
    ↓ (SchemaFileParser)
Schema.Entity (Codable)
    ↓ (SchemaRegistry.persist)
FoundationDB (/_schema/{typeName})
    ↓ (SchemaRegistry.load)
Schema.Entity
    ↓ (DynamicProtobufCodec)
Runtime Data Access
```

SwiftコードとYAMLスキーマは相互運用可能です：

- Swiftの`@Persistable`で定義 → `database export`でYAML出力
- YAMLで定義 → SchemaRegistryに登録 → DatabaseCLI/動的アクセスで使用

---

## 制限事項

### 現在サポートされていない機能

1. **ネストした構造体**: フラットなフィールド定義のみ
2. **Enum型**: 文字列で代替
3. **カスタム型**: プリミティブ型とコンテナ型のみ
4. **計算プロパティ**: 保存されるフィールドのみ
5. **デフォルト値**: 現状未対応（将来対応予定）

### 回避策

```yaml
# Enum → String
status: string#bitmap  # "pending", "shipped", "delivered"

# Nested Struct → フラット化
# Swift: struct Address { var street: String, city: String }
# YAML:
addressStreet: string
addressCity: string
```

---

## バージョニング

スキーマファイルにバージョン情報を含めることを推奨：

```yaml
# User.yaml
_version: 1.0.0
_lastModified: 2026-02-04

User:
  #Directory: [app, users]

  id: string
  name: string
  email: string#scalar(unique)
```

---

## 参考文献

- [GraphIndex README](../GraphIndex/README.md) - グラフインデックス詳細
- [VectorIndex README](../VectorIndex/README.md) - ベクトルインデックス詳細
- [Schema.Entity](../../database-kit/Sources/Core/Schema.swift) - スキーマメタデータ
- [SchemaRegistry](../DatabaseEngine/Registry/SchemaRegistry.swift) - スキーマ永続化
