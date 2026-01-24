# QueryAST

SQL/SPARQL クエリの抽象構文木（AST）モジュール。クエリの解析、変換、シリアライズを提供します。

## 概要

QueryAST は以下の機能を提供します：

- **SQL/SPARQL パーサー**: クエリ文字列からASTを構築
- **クエリビルダー**: プログラマティックなクエリ構築
- **シリアライザー**: ASTからSQL/SPARQL文字列を生成
- **クエリ分析**: 変数参照、集約関数の検出
- **SQL/PGQ サポート**: グラフパターンマッチング (ISO/IEC 9075-16:2023)

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                         QueryAST                                 │
├─────────────────────────────────────────────────────────────────┤
│  Parser/                   │  Builder/                          │
│  ├── SQLParser             │  ├── SQLQueryBuilder               │
│  └── SPARQLParser          │  └── SPARQLQueryBuilder            │
├────────────────────────────┼────────────────────────────────────┤
│  SQL/                      │  SPARQL/                           │
│  ├── MatchPattern          │  ├── GraphPattern                  │
│  ├── PathPattern           │  ├── PropertyPath                  │
│  └── GraphTable            │  └── SPARQLTerm                    │
├────────────────────────────┴────────────────────────────────────┤
│  Core Types                                                      │
│  ├── Expression            ├── DataSource                       │
│  ├── SelectQuery           ├── Literal                          │
│  └── QueryStatement        └── Projection                       │
├─────────────────────────────────────────────────────────────────┤
│  Utils/                                                          │
│  └── SQLEscape (識別子/文字列エスケープ)                          │
└─────────────────────────────────────────────────────────────────┘
```

## 主要な型

### クエリ表現

| 型 | 説明 |
|----|------|
| `SelectQuery` | SELECT クエリのAST表現 |
| `Expression` | 式（比較、算術、論理、集約など） |
| `DataSource` | FROM句のデータソース（テーブル、JOIN、サブクエリ） |
| `Projection` | SELECT句の射影（カラム、式、*） |
| `Literal` | リテラル値（文字列、数値、日付など） |

### SQL/PGQ (グラフクエリ)

| 型 | 説明 |
|----|------|
| `GraphTableSource` | GRAPH_TABLE句 |
| `MatchPattern` | MATCH句のパターン |
| `PathPattern` | パスパターン（ノード、エッジ、量化子） |
| `NodePattern` | ノードパターン |
| `EdgePattern` | エッジパターン |

### SPARQL

| 型 | 説明 |
|----|------|
| `GraphPattern` | SPARQL グラフパターン（BGP、OPTIONAL、UNION等） |
| `PropertyPath` | プロパティパス（シーケンス、代替、繰り返し） |
| `SPARQLTerm` | RDF項（IRI、リテラル、変数、ブランクノード） |
| `TriplePattern` | トリプルパターン |

## 使用例

### SQL クエリのパース

```swift
import QueryAST

let parser = SQLParser()
let query = try parser.parseSelect("""
    SELECT u.name, COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.status = 'active'
    GROUP BY u.name
    ORDER BY order_count DESC
    LIMIT 10
""")

// AST を走査
print(query.projection)  // 射影
print(query.source)      // FROM句
print(query.filter)      // WHERE句
```

### プログラマティックなクエリ構築

```swift
let query = SelectQuery(
    projection: .items([
        ProjectionItem(.column(ColumnRef(table: "u", column: "name"))),
        ProjectionItem(
            .aggregate(.count(.column(ColumnRef(table: "o", column: "id")), distinct: false)),
            alias: "order_count"
        )
    ]),
    source: .join(JoinClause(
        type: .left,
        left: .table(TableRef(table: "users", alias: "u")),
        right: .table(TableRef(table: "orders", alias: "o")),
        condition: .on(.equal(
            .column(ColumnRef(table: "u", column: "id")),
            .column(ColumnRef(table: "o", column: "user_id"))
        ))
    )),
    filter: .equal(
        .column(ColumnRef(table: "u", column: "status")),
        .literal(.string("active"))
    ),
    groupBy: [.column(ColumnRef(table: "u", column: "name"))],
    orderBy: [SortKey(.column(ColumnRef(column: "order_count")), direction: .descending)],
    limit: 10
)

// SQL に変換
print(query.toSQL())
```

### SPARQL クエリのパース

```swift
let parser = SPARQLParser()
let query = try parser.parse("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?name ?email
    WHERE {
        ?person foaf:name ?name .
        OPTIONAL { ?person foaf:mbox ?email }
        FILTER(CONTAINS(?name, "Alice"))
    }
    ORDER BY ?name
    LIMIT 100
""")
```

### SQL/PGQ グラフクエリ

```swift
// GRAPH_TABLE を使用したグラフパターンマッチング
let graphQuery = GraphTableSource.match(
    graph: "social_network",
    from: NodePattern(variable: "p1", labels: ["Person"]),
    via: EdgePattern(variable: "f", labels: ["FRIEND"], direction: .outgoing),
    to: NodePattern(variable: "p2", labels: ["Person"])
).returning([
    (.column(ColumnRef(table: "p1", column: "name")), "person"),
    (.column(ColumnRef(table: "p2", column: "name")), "friend")
])

print(graphQuery.toSQL())
// GRAPH_TABLE(social_network,
//   MATCH (p1:Person)-[f:FRIEND]->(p2:Person)
//   COLUMNS (p1.name AS person, p2.name AS friend)
// )
```

## セキュリティ

### SQL インジェクション対策

全ての識別子と文字列リテラルは適切にエスケープされます：

```swift
// 識別子のエスケープ (ダブルクォート)
SQLEscape.identifier("user name")  // "\"user name\""
SQLEscape.identifier("table\"name") // "\"table\"\"name\""

// 文字列リテラルのエスケープ (シングルクォート)
SQLEscape.string("O'Brien")  // "'O''Brien'"

// SPARQL NCName 検証
try SPARQLEscape.ncName("validName")  // "validName"
try SPARQLEscape.ncName("invalid name")  // throws SPARQLEscapeError.invalidNCName

// IRI エスケープ
SPARQLEscape.iri("http://example.org/path")  // "<http://example.org/path>"
```

### 参照規格

- **SQL識別子**: ISO/IEC 9075:2023 Section 5.2 (Delimited Identifier)
- **SQL文字列**: ISO/IEC 9075:2023 Section 5.3 (Character String Literal)
- **SPARQL NCName**: W3C XML Namespaces 1.0
- **SPARQL IRI**: RFC 3987, SPARQL 1.1 Section 19.5

## クエリ分析

```swift
let query = try SQLParser().parseSelect("SELECT name, email FROM users WHERE age > 18")

// 参照されているカラムを取得
let columns = query.referencedColumns
// ["name", "email", "age"]

// 集約関数が含まれているか
let hasAgg = query.hasAggregation
// false

// 変数参照を取得 (SPARQL)
let sparqlQuery = try SPARQLParser().parse("SELECT ?name WHERE { ?s foaf:name ?name }")
let vars = sparqlQuery.referencedVariables
// ["name", "s"]
```

## クエリ最適化 (実験的)

```swift
// クエリプランの生成
let plan = QueryPlan(query: query, indexes: availableIndexes)

// コスト見積もり
print(plan.estimatedCost)

// 最適化されたプランの取得
let optimizedPlan = plan.optimized()
```

## 対応する SQL 機能

### SELECT句
- [x] カラム、式、エイリアス
- [x] DISTINCT / ALL
- [x] 集約関数 (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, ARRAY_AGG)
- [x] ウィンドウ関数 (OVER, PARTITION BY)

### FROM句
- [x] テーブル参照 (スキーマ修飾、エイリアス)
- [x] JOIN (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, LATERAL)
- [x] サブクエリ
- [x] GRAPH_TABLE (SQL/PGQ)
- [x] VALUES

### WHERE句
- [x] 比較演算子
- [x] 論理演算子 (AND, OR, NOT)
- [x] LIKE, BETWEEN, IN
- [x] IS NULL / IS NOT NULL
- [x] EXISTS / NOT EXISTS
- [x] サブクエリ

### その他
- [x] GROUP BY / HAVING
- [x] ORDER BY (ASC, DESC, NULLS FIRST/LAST)
- [x] LIMIT / OFFSET
- [x] WITH (CTE)
- [x] 集合演算 (UNION, INTERSECT, EXCEPT)

## 対応する SPARQL 機能

### クエリ形式
- [x] SELECT
- [x] CONSTRUCT
- [x] ASK
- [x] DESCRIBE

### グラフパターン
- [x] Basic Graph Pattern (BGP)
- [x] OPTIONAL
- [x] UNION
- [x] MINUS
- [x] FILTER
- [x] BIND
- [x] VALUES
- [x] SERVICE (FEDERATED)
- [x] GRAPH (Named Graph)

### プロパティパス
- [x] シーケンス (/)
- [x] 代替 (|)
- [x] 逆パス (^)
- [x] ゼロ以上 (*)
- [x] 一つ以上 (+)
- [x] ゼロか一つ (?)
- [x] 否定 (!)
- [x] 範囲 ({n,m})

### ソリューション修飾子
- [x] ORDER BY
- [x] LIMIT / OFFSET
- [x] DISTINCT / REDUCED
- [x] GROUP BY / HAVING
- [x] 集約関数

## テスト

```bash
# QueryAST テストの実行
swift test --filter QueryASTTests

# 特定のテストスイートの実行
swift test --filter "SQLParserTests"
swift test --filter "SPARQLParserTests"
swift test --filter "GraphTableTests"
```

## 参照規格

- [ISO/IEC 9075:2023](https://www.iso.org/standard/76583.html) - SQL Standard
- [ISO/IEC 9075-16:2023](https://www.iso.org/standard/76588.html) - SQL/PGQ (Property Graph Queries)
- [W3C SPARQL 1.1](https://www.w3.org/TR/sparql11-query/) - SPARQL Query Language
- [W3C RDF-star](https://w3c.github.io/rdf-star/) - RDF-star and SPARQL-star
