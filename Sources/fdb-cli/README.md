# fdb-cli

FoundationDB 上で動作する動的スキーマデータベースの対話型 CLI ツール。

## 概要

fdb-cli は、コンパイル時にスキーマを定義することなく、実行時に動的にスキーマを定義・操作できる CLI です。12種類のインデックスをサポートし、ベクトル検索、全文検索、グラフクエリなど多様な検索機能を提供します。

## 起動方法

```bash
swift run fdb-cli
```

## コマンド一覧

| コマンド | 用途 |
|---------|------|
| `admin` | スキーマ・インデックス管理 |
| `insert` | データ挿入 |
| `get` | ID で取得 |
| `update` | データ更新 |
| `delete` | データ削除 |
| `find` | 統合検索 |
| `graph` | グラフ操作 |
| `history` | 履歴操作 |
| `help` | ヘルプ表示 |
| `exit` | 終了 |

---

## スキーマ定義

### 基本構文

```bash
admin schema define <Schema> <field>:<type>[#modifier][@relationship] ... [--options]
```

### フィールド型

| 型 | 説明 | 例 |
|---|------|-----|
| `string` | 文字列 | `name:string` |
| `int` | 整数 | `age:int` |
| `double` | 浮動小数点 | `price:double` |
| `bool` | 真偽値 | `active:bool` |
| `date` | 日時（ISO8601） | `createdAt:date` |
| `[string]` | 文字列配列 | `tags:[string]` |
| `[double]` | 数値配列（ベクトル） | `embedding:[double]` |

### インデックス修飾子 `#`

| 修飾子 | インデックス | 説明 |
|-------|------------|------|
| `#indexed` | ScalarIndex | 等価・範囲検索 |
| `#unique` | ScalarIndex (unique) | ユニーク制約付き |
| `#vector(dim=N,metric=M)` | VectorIndex | K-NN類似検索 |
| `#fulltext(tokenizer=T)` | FullTextIndex | 全文検索 |
| `#bitmap` | BitmapIndex | 低カーディナリティ向け |
| `#rank` | RankIndex | ランキング |
| `#leaderboard(window=W)` | LeaderboardIndex | 時間窓付きランキング |

### リレーション修飾子 `@`

```bash
@relationship(TargetSchema,deleteRule)
```

**削除ルール:**
- `cascade` - 親削除時に子も削除
- `nullify` - 親削除時に FK を null に
- `deny` - 子が存在する場合は親削除を拒否
- `noAction` - 何もしない

### 複合オプション `--`

| オプション | 説明 |
|-----------|------|
| `--spatial name(fields=lat,lon)` | 地理空間インデックス |
| `--graph name(from=F,to=T)` | グラフインデックス |
| `--aggregate name(type=T,field=F,by=B)` | 集計インデックス |
| `--version name(retention=R)` | バージョン管理 |
| `--composite name(fields=F1,F2)` | 複合インデックス |
| `--permuted name(source=S,order=1,0,2)` | 順序変更インデックス |

---

## スキーマ定義例

### 基本的なユーザースキーマ

```bash
admin schema define User \
  id:string \
  name:string#indexed \
  email:string#unique \
  age:int#indexed \
  status:string#bitmap
```

### ベクトル検索対応ドキュメント

```bash
admin schema define Document \
  id:string \
  title:string#indexed \
  content:string#fulltext(tokenizer=stem) \
  embedding:[double]#vector(dim=384,metric=cosine)
```

### 地理空間対応店舗

```bash
admin schema define Store \
  id:string \
  name:string#indexed \
  category:string#bitmap \
  lat:double \
  lon:double \
  --spatial location(fields=lat,lon)
```

### グラフ（ソーシャルネットワーク）

```bash
admin schema define Follow \
  id:string \
  follower:string#indexed \
  followee:string#indexed \
  --graph edges(from=follower,to=followee)
```

### リレーション（EC サイト）

```bash
admin schema define Customer \
  id:string \
  name:string#indexed \
  email:string#unique

admin schema define Order \
  id:string \
  customerId:string@relationship(Customer,cascade) \
  amount:double#indexed \
  status:string#bitmap
```

### 集計インデックス

```bash
admin schema define Sale \
  id:string \
  region:string#bitmap \
  amount:double \
  --aggregate region_sum(type=sum,field=amount,by=region) \
  --aggregate region_count(type=count,by=region)
```

---

## データ操作

### 挿入

```bash
insert <Schema> <json>

# 例
insert User {"id": "u1", "name": "Alice", "age": 25, "email": "alice@example.com", "status": "active"}
```

### 取得

```bash
get <Schema> <id>

# 例
get User u1
```

### 更新

```bash
update <Schema> <id> <json>

# 例
update User u1 {"age": 26}
```

### 削除

```bash
delete <Schema> <id>

# 例
delete User u1
```

---

## 検索コマンド (find)

### スカラー検索

```bash
find <Schema> where <field> <op> <value> [limit N]

# 演算子: =, !=, >, <, >=, <=

# 例
find User where age > 20
find User where age >= 20 limit 10
find User where email = "alice@example.com"
```

### ベクトル検索

```bash
find <Schema> --vector <field> <vector> --k <N> [--metric <M>]

# 例
find Document --vector embedding [0.1,0.2,0.3,...] --k 10
find Document --vector embedding [0.1,0.2,0.3,...] --k 10 --metric cosine
```

### 全文検索

```bash
find <Schema> --text <field> "<query>" [--phrase] [--fuzzy N]

# 例
find Article --text body "machine learning"
find Article --text body "machine learning" --phrase
find Article --text body "machin" --fuzzy 2
```

### 地理空間検索

```bash
# 近傍検索
find <Schema> --near <lat> <lon> --radius <distance>

# バウンディングボックス
find <Schema> --bbox <minLat> <minLon> <maxLat> <maxLon>

# 例
find Store --near 35.68 139.73 --radius 5km
find Store --bbox 35.65 139.70 35.70 139.80
```

### ビットマップ検索

```bash
find <Schema> --bitmap <field> = <value> [--count]

# 例
find User --bitmap status = active
find User --bitmap status in [active,pending]
find User --bitmap status = active --count
```

### ランキング検索

```bash
find <Schema> --rank <field> --top N
find <Schema> --rank <field> --of <id>
find <Schema> --rank <field> --count

# 例
find Player --rank score --top 100
find Player --rank score --of player123
```

### リーダーボード検索

```bash
find <Schema> --leaderboard <indexName> --top N
find <Schema> --leaderboard <indexName> --of <id>
find <Schema> --leaderboard <indexName> --windows

# 例
find GameScore --leaderboard daily --top 100
find GameScore --leaderboard daily --of player123
```

### 集計クエリ

```bash
find <Schema> --aggregate <indexName>

# 例
find Sale --aggregate region_sum
find Sale --aggregate region_count
```

### JOIN クエリ

```bash
find <Schema> --join <relationName>

# 例
find Order --join customer
```

---

## グラフコマンド

```bash
# 隣接ノード取得
graph <Schema> from=<node>
graph <Schema> to=<node>

# 走査（BFS）
graph <Schema> from=<node> --depth <N>

# 最短パス
graph <Schema> --path from=<node> to=<node>

# アルゴリズム
graph <Schema> --pagerank --top <N>

# 例
graph Follow from=alice
graph Follow to=alice
graph Follow from=alice --depth 3
graph Follow --path from=alice to=frank
graph Follow --pagerank --top 10
```

---

## 履歴コマンド

```bash
# 履歴一覧
history <Schema> <id>

# 特定バージョン取得
history <Schema> <id> --at <version>

# 差分表示
history <Schema> <id> --diff <v1> <v2>

# ロールバック
history <Schema> <id> --rollback <version>

# 例
history Document doc123
history Document doc123 --at 2
history Document doc123 --diff 1 2
history Document doc123 --rollback 2
```

---

## 管理コマンド

### スキーマ管理

```bash
# スキーマ一覧
admin schema list

# スキーマ詳細
admin schema show <Name>

# スキーマ削除
admin schema drop <Name>
```

### インデックス管理

```bash
# インデックス追加
admin index add <Schema> <field>#<type>

# インデックス一覧
admin index list <Schema>

# インデックス削除
admin index drop <Schema> <indexName>

# インデックス再構築
admin index rebuild <Schema> <indexName>
```

---

## サポートするインデックス一覧

| インデックス | 用途 | 修飾子/オプション |
|------------|------|-----------------|
| ScalarIndex | 等価・範囲クエリ | `#indexed`, `#unique` |
| VectorIndex | K-NN類似検索 | `#vector(dim=N,metric=M)` |
| FullTextIndex | 全文検索・BM25 | `#fulltext(tokenizer=T)` |
| SpatialIndex | 地理空間クエリ | `--spatial` |
| RankIndex | ランキング・Top-K | `#rank` |
| PermutedIndex | 複合インデックス順序変更 | `--permuted` |
| GraphIndex | グラフ走査 | `--graph` |
| AggregationIndex | 集計（COUNT/SUM/AVG） | `--aggregate` |
| VersionIndex | 履歴・監査証跡 | `--version` |
| BitmapIndex | 集合演算 | `#bitmap` |
| LeaderboardIndex | 時間窓付きランキング | `#leaderboard(window=W)` |
| RelationshipIndex | FK・リレーション | `@relationship(T,rule)` |

---

## 制約とバリデーション

### ユニーク制約

`#unique` 修飾子を付けたフィールドは、重複する値を挿入・更新できません。

```bash
admin schema define User id:string email:string#unique

insert User {"id": "u1", "email": "test@example.com"}
insert User {"id": "u2", "email": "test@example.com"}  # エラー: ユニーク制約違反
```

### 参照整合性

`@relationship` 修飾子を付けたフィールドは、参照先のレコードが存在する必要があります。

```bash
admin schema define Customer id:string name:string
admin schema define Order id:string customerId:string@relationship(Customer,cascade)

insert Order {"id": "o1", "customerId": "c999"}  # エラー: Customer c999 が存在しない
```

### 削除ルール

親レコード削除時の子レコードの扱いを制御します。

- **cascade**: 子レコードも自動削除
- **nullify**: 子レコードの FK を null に設定
- **deny**: 子レコードが存在する場合は削除拒否
- **noAction**: 何もしない（手動管理）
