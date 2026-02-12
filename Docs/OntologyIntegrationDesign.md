# Ontology Integration Design

database-framework における OWL Ontology の統合設計。

## 現状の問題

### 1. OntologyStore のスコープがグローバル

OntologyStore は `Subspace(prefix: "O")` というハードコードされたプレフィックスで、クラスタ全体に1つだけ存在する。DirectoryLayer のスコープ外にあり、アプリケーション固有のスコープを持たない。

```
[FDB]
├── [DirectoryLayer]/app/knowledge/   ← 開発者が #Directory で定義
│   ├── R/ (items)
│   ├── I/ (indexes)
│   └── ...
│
└── O/                                 ← ハードコード。グローバル。誰のものか不明
    └── [ontologyIRI]/
```

### 2. Ontology と型の紐付けがない

`context.ontology.load(ontology)` はグローバル操作であり、どの Persistable 型とも紐づかない。開発者が毎回手動でロードする必要があり、忘れやすい。

### 3. フィールドに意味がない

`var departmentID: String` は単なる文字列フィールドであり、これが `ex:worksFor` というオントロジー上のプロパティを表すことをフレームワークは知らない。

---

## 設計方針

### 原則

1. **Ontology は Graph の関心事** — オントロジーの概念は Graph モジュールに閉じ込める。Core（`@Persistable`）は永続化のみを担当し、オントロジーを知らない
2. **データ構造は固定** — StoredClassDefinition 等は OWL 2 仕様に基づく Codable 型。開発者が変更する必要はない
3. **マクロの責務分離** — `@Persistable` は永続化、`@Ontology` はオントロジーマッピング。2つのマクロが独立して同じ型に適用される
4. **同期は暗黙にやらない** — Persistable 型とトリプルの双方向同期は開発者が明示的に制御

### モジュール境界

```
Core (永続化の関心事のみ)
├── @Persistable macro     ← フィールド、インデックス、ディレクトリ
├── IndexKind protocol
├── Schema
└── CoreMacros             ← @Persistable, #Index, #Directory のコンパイラプラグイン

Graph (オントロジー + グラフの関心事)
├── @Ontology macro        ← 型とオントロジークラスの紐付け
├── @OWLProperty macro     ← フィールドとオントロジープロパティの紐付け
├── OntologyEntity protocol
├── OntologyPropertyDescriptor
├── OWL 型 (OWLOntology, OWLClass, OWLAxiom, ...)
├── GraphIndexKind
├── RDFTerm, SHACL
└── GraphMacros            ← @Ontology, @OWLProperty のコンパイラプラグイン
```

**依存関係**:
```
Core ──→ CoreMacros
Graph ──→ Core, GraphMacros
```

Core は Graph を知らない。Graph は Core に依存する。オントロジーの概念は Graph に完全に閉じている。

---

## インターフェース

### Ontology の定義

Result builder による宣言的な API。classes / axioms の分離による矛盾を防ぐ。

```swift
let ontology = OWLOntology(iri: "http://example.org/company") {
    Class("ex:Person")
    Class("ex:Employee", subClassOf: "ex:Person")
    Class("ex:Manager", subClassOf: "ex:Employee")
    Class("ex:Department")

    ObjectProperty("ex:worksFor", domain: "ex:Employee", range: "ex:Department")
    ObjectProperty("ex:manages", domain: "ex:Manager", range: "ex:Department")
    TransitiveProperty("ex:reportsTo")
}
```

内部的には `Class("ex:Employee", subClassOf: "ex:Person")` が `OWLClass` 宣言と `.subClassOf` axiom の両方を自動生成する。

### Schema への登録

```swift
let schema = Schema(
    [Employee.self, Manager.self, Department.self, RDFTriple.self],
    ontology: ontology
)

let container = FDBContainer(database: database, schema: schema)
```

Schema が Ontology を保持し、フレームワークが自動的に OntologyStore へロードする。開発者による手動ロードは不要。

### `@Ontology` マクロ

`@Ontology` は Graph モジュールが提供するマクロ。Persistable 型にオントロジークラスの IRI を紐付け、`OntologyEntity` プロトコルへの準拠を自動生成する。

```swift
import Graph

/// 型をオントロジークラスに紐付けるマクロ
@attached(member, names: named(ontologyClassIRI), named(ontologyPropertyDescriptors))
@attached(extension, conformances: OntologyEntity)
public macro Ontology(_ iri: String) = #externalMacro(module: "GraphMacros", type: "OntologyMacro")
```

`@Ontology` マクロの責務:
1. `OntologyEntity` プロトコル準拠の extension を生成
2. `static var ontologyClassIRI: String` を生成
3. 型内の `@OWLProperty` アノテーションをスキャンし、`ontologyPropertyDescriptors` を生成
4. `@OWLProperty(to:)` のあるフィールドに逆引きインデックスを自動生成

### OntologyEntity プロトコル

`@Ontology` マクロが自動生成する準拠プロトコル。**Graph モジュールで定義**される。

```swift
/// Ontology に参加する Persistable 型が準拠するプロトコル
/// Graph モジュールで定義（Core には存在しない）
protocol OntologyEntity: Persistable {
    /// この型に対応するオントロジークラスの IRI
    static var ontologyClassIRI: String { get }

    /// この型のオントロジープロパティ記述子
    static var ontologyPropertyDescriptors: [OntologyPropertyDescriptor] { get }
}
```

開発者が手動で準拠を書く必要はない。`@Ontology` マクロが自動的に生成する。

```swift
// マクロが生成するコード（開発者は見えない）
extension Employee: OntologyEntity {
    static var ontologyClassIRI: String { "ex:Employee" }
    static var ontologyPropertyDescriptors: [OntologyPropertyDescriptor] {
        [
            OntologyPropertyDescriptor(name: "Employee_name", fieldName: "name", iri: "ex:name"),
            OntologyPropertyDescriptor(name: "Employee_departmentID", fieldName: "departmentID", iri: "ex:worksFor", targetTypeName: "Department", targetFieldName: "id"),
        ]
    }
}
```

これにより：
- `OntologyEntity` 準拠の型だけが SPARQL フェデレーションの対象になる
- `@OWLProperty` は `@Ontology` が付いた型でのみ使用可能（`@Ontology` なしではコンパイルエラー）
- ジェネリック制約 `<T: OntologyEntity>` でオントロジー参加型を型安全にフィルタリングできる

### Persistable 型の定義

`@Persistable` と `@Ontology` は独立したマクロとして同じ型に適用する。

```swift
import Core
import Graph

@Persistable
@Ontology("ex:Employee")
struct Employee {
    #Directory<Employee>("app", "employees")

    @OWLProperty("name")
    var name: String

    @OWLProperty("worksFor", label: "所属部署", to: \Department.id)
    var departmentID: String
}

@Persistable
@Ontology("ex:Contractor")
struct Contractor {
    #Directory<Contractor>("app", "contractors")

    @OWLProperty("name")
    var name: String

    @OWLProperty("worksFor", label: "派遣先", to: \Department.id)
    var clientDepartmentID: String
}

@Persistable
@Ontology("ex:Department")
struct Department {
    #Directory<Department>("app", "departments")

    var name: String
}
```

- `@Persistable` — 永続化の関心事（id、フィールド、Codable、インデックス）。Core が提供。オントロジーを知らない
- `@Ontology("ex:Employee")` — この型がオントロジー上の `ex:Employee` クラスに対応することを宣言。Graph が提供。`OntologyEntity` 準拠を自動生成する
- `@OWLProperty("worksFor", label:, to:)` — フィールドのオントロジー上の意味と接続先を宣言。Graph が提供
  - 第1引数 — プロパティ名。ローカル名（`"worksFor"`）、CURIE（`"foaf:name"`）、フル IRI のいずれか。ローカル名は `@Ontology` の名前空間で自動解決される（後述「IRI 解決ルール」参照）
  - `label` — 型固有の表示名（省略可、解決優先順位に従う）
  - `to` — 接続先の KeyPath（`\Department.id`）。KeyPath の `Root` 型から接続先の Persistable 型を、末端から対象フィールドを取得する。フレームワークはこれを使い：
    - グラフエッジの自動生成（`departmentID` の値 → `Department.id` へのエッジ）
    - コンパイル時の型一致検証（`departmentID: String` と `Department.id: String`）
    - Ontology の range 制約との整合性検証（KeyPath の Root 型の `ontologyClass` と `ObjectProperty.range` の一致）
    - UI ナビゲーション（プロパティ値タップ → 接続先の詳細画面へ遷移）
    - **逆引きインデックスの自動生成**（後述）

### `@OWLProperty` マクロ

`@OWLProperty` は Graph モジュールが提供するマーカーマクロ。`@Ontology` マクロがこれをスキャンして `OntologyPropertyDescriptor` を生成する。

```swift
// @OWLProperty のシグネチャ（マクロ）
@OWLProperty(
    _ iri: String,                   // プロパティ名（ローカル名、CURIE、またはフル IRI）
    label: String? = nil,            // 表示名（省略可）
    to keyPath: KeyPath             // 接続先フィールド（省略可）
)
```

#### IRI 解決ルール

`@OWLProperty` の第1引数は `@Ontology` の IRI から名前空間を自動解決する。

| 入力形式 | 判定 | 解決結果 |
|---|---|---|
| `"name"` | `:` なし → ローカル名 | `@Ontology` の名前空間を付与 |
| `"foaf:name"` | `:` あり、`://` なし → CURIE | そのまま使用 |
| `"http://example.org/name"` | `://` あり → フル IRI | そのまま使用 |

名前空間は `@Ontology` の IRI から以下のように抽出される:

| `@Ontology` の引数 | 形式 | 抽出される名前空間 | クラス IRI |
|---|---|---|---|
| `"Employee"` | ベア名（区切り文字なし） | `"ex:"`（デフォルト） | `"ex:Employee"` |
| `"ex:Employee"` | CURIE | `"ex:"` | `"ex:Employee"` |
| `"http://example.org/onto#Employee"` | フル IRI（`#` 区切り） | `"http://example.org/onto#"` | そのまま |
| `"http://example.org/onto/Employee"` | フル IRI（`/` 区切り） | `"http://example.org/onto/"` | そのまま |

ベア名（`:`, `#`, `/` を含まない文字列）はデフォルトの `"ex:"` 名前空間で自動解決される。クラス IRI も同様に `"ex:"` が付与される。

```swift
// 使用例: @Ontology("Employee") の場合（ベア名 → デフォルト ex:）
@OWLProperty("name")                    // → IRI: "ex:name"
@OWLProperty("foaf:mbox")              // → IRI: "foaf:mbox"（CURIE → そのまま）
// ontologyClassIRI → "ex:Employee"

// 使用例: @Ontology("ex:Employee") の場合（CURIE → 同じ結果）
@OWLProperty("name")                    // → IRI: "ex:name"（ローカル名 → 自動解決）
@OWLProperty("foaf:mbox")              // → IRI: "foaf:mbox"（CURIE → そのまま）
@OWLProperty("worksFor", to: \Dept.id) // → IRI: "ex:worksFor"（ローカル名 → 自動解決）

// 使用例: @Ontology("http://example.org/onto#Employee") の場合
@OWLProperty("name")                    // → IRI: "http://example.org/onto#name"
```

マクロはコンパイル時に KeyPath の式から `Root` 型と末端プロパティを抽出する。ジェネリクスではなく、マクロの構文解析によって型情報を取得するため、ジェネリックパラメータは不要。

```swift
// 開発者が書くコード
@OWLProperty("worksFor", to: \Department.id)
var departmentID: String

// @Ontology マクロが展開後に生成する情報（概念的）
// - iri: "ex:worksFor"（@Ontology("ex:Employee") から名前空間を解決）
// - targetType: Department （KeyPath の Root）
// - targetField: "id" （KeyPath の末端）
// - sourceField: "departmentID"
// - valueType: String
```

生成される具体的な型は `OntologyPropertyDescriptor`（`Graph` モジュール、`Descriptor` プロトコル準拠）。`OntologyEntity.ontologyPropertyDescriptors` でランタイムにアクセスできる。

### `@Ontology` による逆引きインデックスの自動生成

`@OWLProperty` に `to:` が指定されている場合、`@Ontology` マクロはそのフィールドに対する逆引きインデックスを自動的に生成する。

```swift
@Persistable
@Ontology("ex:Employee")
struct Employee {
    #Directory<Employee>("app", "employees")

    @OWLProperty("worksFor", to: \Department.id)
    var departmentID: String
    // ↑ @Ontology マクロが自動的に以下を生成：
    // ScalarIndexKind<Employee>(fields: [\.departmentID]) 相当のインデックス
}
```

これにより：
- Department 側から「この部署に所属する Employee の一覧」を効率的にクエリできる
- 開発者が手動でインデックスを定義する必要がない
- `@OWLProperty` の `to:` が接続先を宣言すると同時に、逆引きの手段も提供する

### GraphIndex の定義

GraphIndex は純粋なグラフストレージであり、`@Ontology` は不要。

```swift
@Persistable
struct RDFTriple {
    #Directory<RDFTriple>("app", "knowledge")

    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""

    #Index(GraphIndexKind<RDFTriple>(
        from: \.subject,
        edge: \.predicate,
        to: \.object,
        strategy: .hexastore
        // ontology は Schema から自動参照
    ))
}
```

GraphIndex は Schema の Ontology を自動的に使用する。明示的な指定は不要。

---

## マクロの責務分離

### `@Persistable`（CoreMacros）

永続化の関心事のみ。オントロジーの概念は一切含まない。

**生成するもの**:
- `var id: String`
- `static var persistableType: String`
- `static var allFields: [String]`
- `static var fieldSchemas: [FieldSchema]`
- `static var indexDescriptors: [any IndexDescriptor]`
- `Codable`, `Sendable` 準拠
- `#Index` / `#Directory` の展開

### `@Ontology`（GraphMacros）

オントロジーマッピングの関心事のみ。

**生成するもの**:
- `OntologyEntity` プロトコル準拠
- `static var ontologyClassIRI: String`
- `static var ontologyPropertyDescriptors: [OntologyPropertyDescriptor]`（`@OWLProperty` スキャン結果）
- `@OWLProperty(to:)` のあるフィールドの逆引きインデックス

### 独立性

`@Persistable` と `@Ontology` は相互に依存しない。

```swift
// ✅ 永続化のみ（オントロジー不参加）
@Persistable
struct AppSettings {
    var theme: String
    var language: String
}

// ✅ 永続化 + オントロジー
@Persistable
@Ontology("ex:Employee")
struct Employee {
    @OWLProperty("name")
    var name: String
}

// ❌ @Ontology は @Persistable なしでは使えない（OntologyEntity: Persistable）
@Ontology("ex:Something")
struct Invalid { ... }
```

---

## Ontology と型の関係

### 2つのスコープ

Ontology の適用のされ方は型によって異なる。

| | Persistable 型 | GraphIndex (トリプルストア) |
|---|---|---|
| 関係 | 型 = クラス (1:1) | 型 = コンテナ、クラスはデータ内 (1:N) |
| Ontology の役割 | 型のメタデータ・プロパティの意味付け | データの推論・分類 |
| 例 | Employee テーブルの全行が ex:Employee | RDFTriple テーブルに全クラスのトリプルが混在 |

```
Employee テーブル (1:1):
┌──────────────────────────────────┐
│ 全行が ex:Employee のインスタンス   │
│ alice | Alice | dept1            │
│ bob   | Bob   | dept2            │
└──────────────────────────────────┘

RDFTriple テーブル (1:N):
┌──────────────────────────────────────────┐
│ 全クラスのデータが混在                      │
│ alice | rdf:type    | ex:Employee        │
│ alice | ex:worksFor | dept1              │
│ dept1 | rdf:type    | ex:Department      │
│ bob   | ex:knows    | alice              │
└──────────────────────────────────────────┘
```

### ラベル解決の優先順位

プロパティの表示名は以下の順で解決される。

| 優先度 | ソース | 例 |
|---|---|---|
| 1（最優先） | `@OWLProperty` の `label` パラメータ | `"所属部署"` |
| 2 | `OWLObjectProperty.label` | `"Works For"` |
| 3（フォールバック） | IRI の `localName()` | `"worksFor"` |

同じオントロジープロパティ `ex:worksFor` でも、型ごとに異なる表示名を持てる：

```swift
// Employee: "所属部署" → Department.id
@OWLProperty("worksFor", label: "所属部署", to: \Department.id)
var departmentID: String

// Contractor: "派遣先" → Department.id
@OWLProperty("worksFor", label: "派遣先", to: \Department.id)
var clientDepartmentID: String
```

Ontology 上は同一の `ObjectProperty("ex:worksFor")` であり、意味的な同一性は保たれる。表示名だけが文脈に応じて変わる。

クラスの表示名も同様の優先順位に従う：

| 優先度 | ソース | 例 |
|---|---|---|
| 1（最優先） | `@Ontology` の `label` パラメータ（将来拡張） | `"従業員"` |
| 2 | `OWLClass.label` | `"Employee"` |
| 3（フォールバック） | IRI の `localName()` | `"Employee"` |

### 関連の実装手段

同じ Ontology 上のプロパティ `ex:worksFor` を、開発者は用途に応じて2通りで実装できる。

```swift
// 方法1: フィールドで直接関連（高頻度・型安全なクエリ向け）
@Persistable
@Ontology("ex:Employee")
struct Employee {
    @OWLProperty("worksFor", to: \Department.id)
    var departmentID: String   // → Department.id へのコンパイル時型安全な参照
    // → 逆引きインデックスが @Ontology により自動生成される
}

// 方法2: トリプルで関連（柔軟なグラフ探索・推論向け）
// (alice, ex:worksFor, dept1) → RDFTriple に格納
```

どちらを使っても Ontology 上は同じ `ObjectProperty("ex:worksFor")` であり、概念モデルは1つ。実装手段は開発者が選ぶ。

---

## データ格納

### SubspaceKey の拡張

```swift
public enum SubspaceKey {
    public static let items = "R"
    public static let indexes = "I"
    public static let state = "T"
    public static let metadata = "M"
    public static let blobs = "B"
    public static let ontology = "O"    // 新規追加
}
```

### キーレイアウト

Ontology データは Schema が管理するディレクトリ内に格納される。

```
[DirectoryLayer]/app/
├── employees/                          ← Employee 型
│   ├── R/ (items)
│   ├── I/ (indexes)
│   └── ...
├── departments/                        ← Department 型
│   ├── R/ (items)
│   └── ...
├── knowledge/                          ← RDFTriple 型
│   ├── R/ (items)
│   ├── I/ (indexes)
│   └── ...
└── O/                                  ← Ontology（Schema スコープ）
    └── http://example.org/company/
        ├── 0/ (metadata)
        ├── 1/ (classes)
        ├── 2/ (properties)
        ├── 3/ (axioms)
        ├── 4/ (classHierarchy)
        ├── 5/ (propertyHierarchy)
        ├── 6/ (inverse)
        ├── 7/ (transitive)
        ├── 8/ (chains)
        └── 9/ (sameAs)
```

### OntologyStore の内部構造（変更なし）

OntologyStore のデータ構造は固定スキーマ（Codable 型）。開発者が触る必要はない。

| サブスペース | 値の型 | 形式 |
|---|---|---|
| metadata | `OntologyMetadata` | JSON |
| classes | `StoredClassDefinition` | JSON |
| properties | `StoredPropertyDefinition` | JSON |
| axioms | `OWLAxiom` | JSON |
| classHierarchy | empty marker | key のみ |
| propertyHierarchy | empty marker | key のみ |
| inverse | UTF-8 IRI | raw |
| transitive | empty marker | key のみ |
| chains | `[String]` | JSON |
| sameAs | Union-Find | raw |

---

## フレームワークの自動処理

Schema に Ontology が登録されている場合、フレームワークが以下を自動的に行う。

### Schema 初期化時

1. OntologyStore に Ontology をロード（既存があれば更新）
2. クラス階層・プロパティ階層の推移的閉包を計算（`materializeClassHierarchy`）

### GraphIndex 書き込み時

1. `rdf:type` トリプル挿入 → OWL2RLMaterializer がスーパークラスの型推論を実行
2. プロパティトリプル挿入 → inverse / transitive / domain / range 推論を実行

### `@Ontology` を持つ型

1. 型のメタデータとして `OntologyEntity.ontologyClassIRI` を保持
2. Ontology の制約（domain/range）とフィールドの `@OWLProperty` 宣言を突合可能
3. `@OWLProperty(to:)` のあるフィールドに逆引きインデックスを `@Ontology` が自動生成

---

## グラフ表示アーキテクチャ（database-studio）

### 現状の問題

`GraphDocument` と `GraphNodeKind` が GraphIndex（RDF トリプル）に特化している。

```swift
// 現在: OWL 固有の kind
enum GraphNodeKind {
    case owlClass          // RDF クラス
    case individual        // RDF インスタンス
    case objectProperty    // RDF プロパティ
    case dataProperty
    case literal
}
```

Persistable 型（Employee テーブル）や Schema 関係を表示できない。

### 設計: 中間グラフ表現

複数のデータソースが共通の `GraphDocument` を経由して表示される。

```
データソース                     中間表現            表示
──────────                     ────────            ────
GraphIndex (トリプル)  ──┐
                         ├──→ GraphDocument ──→ GraphView
Persistable 型         ──┤    (nodes + edges)
(@Ontology)              │
                         │
OWLOntology (スキーマ) ──┘
```

### GraphNode の再設計

`GraphNodeKind` をグラフ内での**役割**と**ドメイン型**に分離する。

```swift
/// グラフ内でのノードの役割
public enum GraphNodeRole: String, Hashable, Sendable {
    case type          // クラス定義・テーブル定義（ex:Employee, Employee.self）
    case instance      // インスタンス（alice, bob）
    case property      // プロパティ定義（ex:worksFor）
    case literal       // リテラル値（"2024-01-01"）
}

/// グラフ内の単一ノード
public struct GraphNode: Identifiable, Hashable, Sendable {
    public let id: String
    public var label: String
    public var role: GraphNodeRole
    public var ontologyClass: String?        // "ex:Employee" — Ontology 上のクラス IRI
    public var source: GraphNodeSource       // このノードの出自
    public var metadata: [String: String]
    public var metrics: [String: Double]
    public var communityID: Int?
    public var isHighlighted: Bool
}

/// ノードの出自（どのデータソースから来たか）
public enum GraphNodeSource: String, Hashable, Sendable {
    case ontology       // OWLOntology のクラス・プロパティ定義
    case graphIndex     // GraphIndex（RDF トリプル）のデータ
    case persistable    // Persistable 型のテーブルデータ
    case derived        // 推論・計算で生成
}
```

### GraphEdge の再設計

```swift
/// グラフ内の単一エッジ
public struct GraphEdge: Identifiable, Hashable, Sendable {
    public let id: String
    public var sourceID: String
    public var targetID: String
    public var label: String
    public var ontologyProperty: String?     // "ex:worksFor" — Ontology 上のプロパティ IRI
    public var edgeKind: GraphEdgeKind
    public var weight: Double?
    public var isHighlighted: Bool
}

/// エッジの種類
public enum GraphEdgeKind: String, Hashable, Sendable {
    case subClassOf       // クラス階層（型 → 親型）
    case instanceOf       // インスタンスの型（alice → Employee）
    case relationship     // ドメイン関係（alice → dept1 via worksFor）
    case property         // プロパティ定義の接続（domain/range）
}
```

### GraphDocument（変更なし）

```swift
public struct GraphDocument: Sendable {
    public var nodes: [GraphNode]
    public var edges: [GraphEdge]
}
```

コンテナ構造は変わらない。中身の型が汎用化される。

### アダプター

各データソースから `GraphDocument` への変換。

```swift
// 1. GraphIndex → GraphDocument（既存の拡張を更新）
extension GraphDocument {
    init(items: [DecodedItem], graphIndex: AnyIndexDescriptor)
    init(triples: [RDFTripleData])
}

// 2. Persistable 型 → GraphDocument（新規）
extension GraphDocument {
    /// Schema から型関係グラフを構築
    /// Employee --[worksFor]--> Department のようなER図的グラフ
    init(schema: Schema)

    /// Persistable インスタンスからグラフを構築
    /// alice --[worksFor]--> dept1 のようなインスタンスグラフ
    init<T: OntologyEntity>(instances: [T], schema: Schema)
}

// 3. OWLOntology → GraphDocument（既存の拡張を更新）
extension GraphDocument {
    init(ontology: OWLOntology)
}

// 4. 複合: 複数ソースをマージ
extension GraphDocument {
    /// 別の GraphDocument をマージ（同一 ID のノードは統合）
    mutating func merge(_ other: GraphDocument)
}
```

### 表示例

#### Case 1: GraphIndex トリプル（現在と同等）
```
[ex:Person]──subClassOf──→[ex:Employee]
     ↑ instanceOf              ↑ instanceOf
   [bob]                    [alice]──relationship(worksFor)──→[dept1]
```

#### Case 2: Persistable 型のスキーマ（ER図的）
```
[Employee]──relationship(worksFor)──→[Department]
     ↑ subClassOf
[Manager]──relationship(manages)──→[Department]
```

#### Case 3: Persistable インスタンス
```
[alice:Employee]──worksFor──→[dept1:Department]
[bob:Employee]──worksFor──→[dept2:Department]
```

#### Case 4: 複合（Schema + トリプル + インスタンス）
全ソースを `merge` で統合し、1つの GraphDocument として表示。
同一 ID のノードは統合され、複数ソースのメタデータが合成される。

---

## SPARQL クエリ

### 現状

SPARQL は GraphIndex（RDFTriple）専用。Persistable 型のテーブルデータはクエリ対象外。

```swift
// 現在: GraphIndex のみ
let results = try await context.sparql(RDFTriple.self)
    .defaultIndex()
    .where("?employee", "ex:worksFor", "?dept")
    .select("?employee", "?dept")
    .execute()
```

### `@OWLProperty` による仮想トリプル

`@Ontology` と `@OWLProperty` の宣言により、Persistable 型のテーブル行を仮想的なトリプルとして解釈できる。

```swift
// Employee テーブルの1行
Employee(id: "alice", name: "Alice", departmentID: "dept1")

// 以下のトリプルとして解釈可能：
// (alice, rdf:type, ex:Employee)     ← @Ontology("ex:Employee") から
// (alice, ex:worksFor, dept1)        ← @OWLProperty("worksFor", to: \Department.id) から
```

テーブルデータを物理的にトリプル化する必要はない。クエリプランナーが `@OWLProperty` の定義を読み、テーブルスキャンをトリプルパターンマッチに変換する。

### Persistable 型への SPARQL

```swift
// 提案: Persistable 型に対しても SPARQL が使える
let results = try await context.sparql(Employee.self)
    .where("?employee", "ex:worksFor", "?dept")
    .select("?employee", "?dept")
    .execute()
// → Employee テーブルをスキャンし、departmentID フィールドから結果を返す
```

クエリプランナーは `@OWLProperty` の定義からフィールドマッピングを解決する：

| トリプルパターン | 解決先 |
|---|---|
| `(?x, rdf:type, ex:Employee)` | Employee テーブル全行 → `?x = row.id` |
| `(?x, ex:worksFor, ?y)` | Employee テーブル → `?x = row.id, ?y = row.departmentID` |
| `(?x, ex:worksFor, "dept1")` | Employee テーブル → `departmentID == "dept1"` でフィルタ（`@Ontology` の自動インデックスを活用） |

### フェデレーションクエリ

複数のデータソースを横断してクエリできる。

```swift
// GraphIndex と Persistable 型を横断
let results = try await context.sparql()
    .from(RDFTriple.self)         // トリプルストア
    .from(Employee.self)           // Employee テーブル
    .from(Department.self)         // Department テーブル
    .where("?person", "rdf:type", "ex:Employee")
    .where("?person", "ex:worksFor", "?dept")
    .where("?dept", "ex:locatedIn", "?city")   // トリプルにしかない情報
    .select("?person", "?city")
    .execute()
```

クエリプランナーはパターンごとに最適なソースを選択する：

| パターン | ソース | 理由 |
|---|---|---|
| `?person rdf:type ex:Employee` | Employee テーブル | `@Ontology("ex:Employee")` を持つ |
| `?person ex:worksFor ?dept` | Employee テーブル | `@OWLProperty("worksFor")` を持つ（自動インデックスで高速） |
| `?dept ex:locatedIn ?city` | RDFTriple GraphIndex | テーブルに該当する `@OWLProperty` がない |

同じプロパティが複数ソースに存在する場合は、UNION として両方からデータを取得する。

### クエリプランナーの最適化

`@OWLProperty` + `to:` KeyPath があることで、`@Ontology` が自動生成した逆引きインデックスを活用した効率的な実行が可能になる。

```swift
// 例: ?dept が "dept1" に束縛された後のパターン
.where("?person", "ex:worksFor", "dept1")

// テーブルクエリに変換（@Ontology が自動生成したインデックスを利用）：
// SELECT id FROM employees WHERE departmentID = "dept1"
```

GraphIndex の hexastore/tripleStore はパターンに応じた最適なインデックスを選択するが、
Persistable 型は `@Ontology` が自動生成したフィールドインデックスを使える。

---

## 追加考慮事項

### 1. 逆方向ナビゲーション

Employee が `@OWLProperty("worksFor", to: \Department.id)` を持つとき、Department 側から「所属する Employee 一覧」を辿れるか。

```swift
@Persistable
@Ontology("ex:Department")
struct Department {
    var id: String
    var name: String
    // ← Employee.departmentID からの逆参照をどう実現するか？
}
```

**解決策: `@Ontology` の逆引きインデックス自動生成**

`@OWLProperty("worksFor", to: \Department.id)` を宣言すると、`@Ontology` マクロが `departmentID` フィールドに対する逆引きインデックスを自動生成する。これにより：

```swift
// Department から Employee を逆引き
let employees = try await context.query(Employee.self)
    .filter(\.departmentID == "dept1")
    .execute()
// → @Ontology が自動生成したインデックスを使い、効率的にクエリ
```

Department 側に明示的な宣言は不要。`@OWLProperty` の `to:` が接続の「方向」を宣言し、`@Ontology` が逆引きインデックスを自動生成することで、双方向のナビゲーションが可能になる。

加えて、Ontology に `inverseObjectProperties("ex:worksFor", "ex:hasMember")` が定義されている場合、SPARQL フェデレーションクエリでは逆方向パターンも自動的に解決される。

### 2. DataProperty の扱い

`@OWLProperty` は ObjectProperty（エンティティ間の関係）を想定している。リテラル値を持つ DataProperty はどうするか。

```swift
@Persistable
@Ontology("ex:Employee")
struct Employee {
    var id: String

    @OWLProperty("worksFor", to: \Department.id)   // ObjectProperty → 他の型
    var departmentID: String

    var salary: Int       // ← DataProperty。他の型を指さない。
    var name: String      // ← rdfs:label に相当？
}
```

**推奨**: `to:` の有無で ObjectProperty / DataProperty を区別する。シンプルで一貫性がある。

```swift
@OWLProperty("worksFor", to: \Department.id)  // ObjectProperty（to: あり）
var departmentID: String

@OWLProperty("salary")                         // DataProperty（to: なし）
var salary: Int

@OWLProperty("rdfs:label")                     // DataProperty（to: なし、他の名前空間）
var name: String
```

`to:` なしの `@OWLProperty` は逆引きインデックスを生成しない（接続先がないため）。

### 3. 参照整合性

`departmentID: "dept1"` が指す Department が存在しない場合の挙動。

**方針**:
- **書き込み時**: フレームワークは参照先の存在を検証しない（FoundationDB は外部キー制約を持たない）
- **読み取り時**: `@OWLProperty` の `to:` KeyPath を使って resolve できなかった場合は nil を返す
- **削除時**: カスケード削除は自動で行わない。開発者が `@OWLProperty` の `onDelete:` ポリシーで明示する（将来拡張）
- **整合性チェック**: database-studio のインスペクタで「壊れた参照」を警告表示する

### 4. Ontology のバージョニング

Schema の Ontology を変更した場合（クラス追加、階層変更、プロパティ名変更）：

- **追加のみ**: 新しいクラス・プロパティの追加は安全。OntologyStore は冪等ロード（delete → reload）
- **階層変更**: `materializeClassHierarchy` が推移的閉包を再計算。既存のトリプル推論結果は再マテリアライズが必要
- **削除・改名**: 既存データとの整合性が崩れる。マイグレーション機構が必要（将来検討）
- **バージョン管理**: `OntologyMetadata.versionIRI` で変更を検知し、Schema 初期化時に自動リロードするか判定

### 5. 外部 OWL ファイルのインポート

Result builder は手書きの小規模オントロジー向け。大規模な既存オントロジー（schema.org、FOAF、Dublin Core 等）はファイルからインポートする必要がある。

```swift
// 将来: ファイルからのインポート
let schema = Schema(
    [Employee.self, RDFTriple.self],
    ontology: try OWLOntology(contentsOf: "schema.ttl")   // Turtle 形式
)

// または複数オントロジーのインポート + 合成
let schema = Schema(
    [Employee.self, RDFTriple.self],
    ontologies: [
        try OWLOntology(contentsOf: "schema.ttl"),         // 外部
        OWLOntology(iri: "http://example.org/company") {   // アプリ固有
            Class("ex:Employee", subClassOf: "schema:Person")
        }
    ]
)
```

### 6. Ontology 不参加型との共存

すべての Persistable 型がオントロジーに参加するわけではない。

```swift
@Persistable
@Ontology("ex:Employee")
struct Employee {
    // ← @Ontology により OntologyEntity プロトコルに自動準拠。オントロジー参加。
    // ...
}

@Persistable
struct AppSettings {
    // @Ontology なし → OntologyEntity に準拠しない。オントロジー不参加。
    var id: String
    var theme: String
}
```

- `@Ontology` のない `@Persistable` は `OntologyEntity` に準拠しない
- `OntologyEntity` 非準拠の型は SPARQL フェデレーションの対象外
- GraphDocument のアダプターはスキップする
- `@OWLProperty` は `@Ontology` が付いた型でのみ使用可能（`@Ontology` なしではコンパイルエラー）
- Schema には混在可能。Ontology は参加する型にのみ影響する

---

## 未解決の懸念事項

### インフラストラクチャ
- [ ] Ontology の格納場所: Schema スコープのディレクトリをどう決定するか
- [x] ~~`@Persistable(ontologyClass:)` と `@OWLProperty` の Macro 実装方針~~ → `@Ontology` + `@OWLProperty` に分離決定
- [ ] `@Ontology` マクロの GraphMacros 実装（`@OWLProperty` スキャン、インデックス生成）
- [ ] `SchemaOntology` プロトコルを Core に残すか、Schema から ontology を外して Graph 側で登録するか
- [ ] Result builder の設計（既存 `OWLOntology` との互換性維持）
- [ ] GraphIndex が Schema の Ontology を参照する仕組み

### データ整合性
- [ ] Ontology バージョン変更時の既存データマイグレーション戦略
- [ ] 複数 Ontology のインポート・合成時の IRI 衝突解決
- [ ] `@OWLProperty` 自動インデックスの更新・削除時の整合性

### クエリ
- [ ] フェデレーションクエリプランナーの設計（パターンごとのソース選択アルゴリズム）
- [ ] 仮想トリプルのコスト推定（テーブルスキャン vs GraphIndex ルックアップ）
- [ ] 同一プロパティが複数ソースにある場合の UNION 戦略と重複排除

### 表示
- [ ] GraphNodeKind → GraphNodeRole + ontologyClass への段階的マイグレーション
- [ ] merge 時の ID 衝突戦略（同一 IRI のノードの統合ルール）
- [ ] 大量インスタンスの表示パフォーマンス（Employee 10万行 → 間引きが必要）
