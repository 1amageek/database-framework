# GraphIndex と TripleIndex の統合設計

## 1. 設計目標

### 1.1 問題点（現状）

現在、GraphIndex と TripleIndex は独立したモジュールとして存在：

```
GraphIndex/
  ├── AdjacencyIndexKind (database-kit)
  └── AdjacencyIndexMaintainer (database-framework)

TripleIndex/
  ├── TripleIndexKind (database-kit)
  └── TripleIndexMaintainer (database-framework)
```

**問題**:
1. 本質的に同じもの（ラベル付き有向グラフ）を2つの実装で管理
2. コードの重複
3. RDF と一般グラフの相互運用性がない

### 1.2 目標

1. **統一されたグラフストレージ**: 一つの IndexKind で複数のストレージ戦略をサポート
2. **RDF 互換性**: SPARQL クエリパターンに対応
3. **柔軟性**: ユースケースに応じたインデックス戦略の選択
4. **後方互換性不要**: 開発中のため破壊的変更可

---

## 2. 統合アーキテクチャ

### 2.1 概念モデル

```
RDF Triple:    (Subject) --[Predicate]--> (Object)
Graph Edge:    (Source)  --[Label]------> (Target)
統一モデル:    (From)    --[Edge]-------> (To)
```

### 2.2 モジュール構成（統合後）

```
database-kit/
  └── Sources/Graph/
      ├── GraphIndexKind.swift          # 統合された IndexKind
      ├── GraphIndexStrategy.swift      # ストレージ戦略 enum
      └── GraphQueryPattern.swift       # クエリパターン定義

database-framework/
  └── Sources/GraphIndex/
      ├── GraphIndexKind+Maintainable.swift
      ├── GraphIndexMaintainer.swift    # 統合された Maintainer
      ├── Strategies/
      │   ├── AdjacencyStrategy.swift   # 2-index (現 GraphIndex)
      │   ├── TripleStoreStrategy.swift # 3-index (現 TripleIndex)
      │   └── HexastoreStrategy.swift   # 6-index (オプション)
      └── Query/
          ├── GraphQueryBuilder.swift
          └── SPARQLPatternMatcher.swift
```

### 2.3 TripleIndex モジュールの廃止

統合後、以下を削除：
- `database-kit/Sources/Triple/` → `Graph/` に統合
- `database-framework/Sources/TripleIndex/` → `GraphIndex/` に統合

---

## 3. ストレージ戦略

### 3.1 戦略一覧

| 戦略 | インデックス数 | 書き込みコスト | 対応クエリ | ユースケース |
|------|--------------|--------------|-----------|-------------|
| **adjacency** | 2 | 低 | 隣接ノード | ソーシャルグラフ |
| **tripleStore** | 3 | 中 | SPO/POS/OSP | RDF/知識グラフ |
| **hexastore** | 6 | 高 | 全パターン最適 | 大規模RDF |

### 3.2 ストレージレイアウト詳細

#### adjacency (2-index)
```
[out]/[label]/[from]/[to]     # From → To (outgoing)
[in]/[label]/[to]/[from]      # To ← From (incoming)
```

#### tripleStore (3-index) - SPO/POS/OSP
```
[spo]/[from]/[edge]/[to]      # S??, SP?, SPO クエリ
[pos]/[edge]/[to]/[from]      # ?P?, ?PO クエリ
[osp]/[to]/[from]/[edge]      # ??O クエリ
```

#### hexastore (6-index) - 全順列
```
[spo]/[from]/[edge]/[to]      # SPO
[sop]/[from]/[to]/[edge]      # SOP
[pso]/[edge]/[from]/[to]      # PSO
[pos]/[edge]/[to]/[from]      # POS
[osp]/[to]/[from]/[edge]      # OSP
[ops]/[to]/[edge]/[from]      # OPS
```

### 3.3 クエリパターンとインデックス選択

| パターン | adjacency | tripleStore | hexastore |
|----------|-----------|-------------|-----------|
| (s, ?, ?) | out[s] | spo[s] | spo[s] |
| (?, p, ?) | out[p]/* | pos[p] | pso[p] |
| (?, ?, o) | in[o] | osp[o] | osp[o] |
| (s, p, ?) | out[p][s] | spo[s][p] | spo[s][p] |
| (s, ?, o) | out[*][s] + filter | osp[o][s] | sop[s][o] |
| (?, p, o) | in[p][o] | pos[p][o] | pos[p][o] |
| (s, p, o) | out[p][s][o] | spo[s][p][o] | any |

---

## 4. API 設計

### 4.1 database-kit: GraphIndexKind

```swift
/// 統合されたグラフインデックス種別
///
/// RDF トリプルと一般グラフエッジの両方をサポート。
/// ストレージ戦略により異なるインデックスパターンを選択可能。
public struct GraphIndexKind<Root: Persistable>: IndexKind {
    public static var identifier: String { "graph" }
    public static var subspaceStructure: SubspaceStructure { .hierarchical }

    // MARK: - Fields (統一用語)

    /// From node field (RDF: Subject, Graph: Source)
    public let fromField: String

    /// Edge label field (RDF: Predicate, Graph: Label)
    public let edgeField: String

    /// To node field (RDF: Object, Graph: Target)
    public let toField: String

    // MARK: - Strategy

    /// ストレージ戦略
    public let strategy: GraphIndexStrategy

    // MARK: - Initialization

    /// グラフインデックスを作成
    ///
    /// - Parameters:
    ///   - from: From ノードのフィールド (Subject/Source)
    ///   - edge: エッジラベルのフィールド (Predicate/Label)
    ///   - to: To ノードのフィールド (Object/Target)
    ///   - strategy: ストレージ戦略 (デフォルト: .tripleStore)
    public init(
        from: PartialKeyPath<Root>,
        edge: PartialKeyPath<Root>,
        to: PartialKeyPath<Root>,
        strategy: GraphIndexStrategy = .tripleStore
    ) {
        self.fromField = Root.fieldName(for: from)
        self.edgeField = Root.fieldName(for: edge)
        self.toField = Root.fieldName(for: to)
        self.strategy = strategy
    }

    // MARK: - Convenience Initializers

    /// RDF トリプル用イニシャライザ
    public static func rdf(
        subject: PartialKeyPath<Root>,
        predicate: PartialKeyPath<Root>,
        object: PartialKeyPath<Root>,
        strategy: GraphIndexStrategy = .tripleStore
    ) -> GraphIndexKind {
        GraphIndexKind(
            from: subject,
            edge: predicate,
            to: object,
            strategy: strategy
        )
    }

    /// ソーシャルグラフ用イニシャライザ
    public static func social(
        source: PartialKeyPath<Root>,
        target: PartialKeyPath<Root>,
        label: PartialKeyPath<Root>? = nil
    ) -> GraphIndexKind {
        // label が nil の場合は空文字列を使用
        if let label = label {
            return GraphIndexKind(
                from: source,
                edge: label,
                to: target,
                strategy: .adjacency
            )
        } else {
            // edge フィールドなしの場合は特別処理
            // （実装で固定値を使用）
            return GraphIndexKind(
                fromField: Root.fieldName(for: source),
                edgeField: "",  // Empty = no edge field
                toField: Root.fieldName(for: target),
                strategy: .adjacency
            )
        }
    }
}
```

### 4.2 GraphIndexStrategy

```swift
/// グラフインデックスのストレージ戦略
public enum GraphIndexStrategy: String, Sendable, Codable, CaseIterable {
    /// 2-index: 出力/入力エッジのみ
    ///
    /// ストレージ効率重視。基本的な隣接クエリに最適。
    /// - 書き込み: 2 entries/edge
    /// - クエリ: (s,?,?), (?,?,o), (s,p,?), (?,p,o)
    case adjacency

    /// 3-index: SPO/POS/OSP
    ///
    /// RDF 標準パターン。ほとんどの SPARQL クエリに対応。
    /// - 書き込み: 3 entries/edge
    /// - クエリ: 全パターン（一部は2段階スキャン）
    case tripleStore

    /// 6-index: 全順列
    ///
    /// 読み取り性能最優先。全クエリパターンで O(1) アクセス。
    /// - 書き込み: 6 entries/edge
    /// - クエリ: 全パターン最適
    case hexastore

    /// 各戦略のインデックス数
    public var indexCount: Int {
        switch self {
        case .adjacency: return 2
        case .tripleStore: return 3
        case .hexastore: return 6
        }
    }
}
```

### 4.3 使用例

```swift
// RDF トリプルストア
@Persistable
struct Statement {
    var id: String = UUID().uuidString
    var subject: String
    var predicate: String
    var object: String

    #Index<Statement>(type: .rdf(
        subject: \.subject,
        predicate: \.predicate,
        object: \.object,
        strategy: .tripleStore
    ))
}

// ソーシャルグラフ（フォロー関係）
@Persistable
struct Follow {
    var id: String = UUID().uuidString
    var follower: String
    var followee: String

    #Index<Follow>(type: .social(
        source: \.follower,
        target: \.followee
    ))
}

// 知識グラフ（高性能読み取り）
@Persistable
struct KnowledgeTriple {
    var id: String = UUID().uuidString
    var entity: String
    var relation: String
    var value: String

    #Index<KnowledgeTriple>(type: GraphIndexKind(
        from: \.entity,
        edge: \.relation,
        to: \.value,
        strategy: .hexastore  // 読み取り性能最優先
    ))
}
```

---

## 5. database-framework 実装設計

### 5.1 GraphIndexMaintainer

```swift
public struct GraphIndexMaintainer<Item: Persistable>: IndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let fromField: String
    private let edgeField: String
    private let toField: String
    private let strategy: GraphIndexStrategy

    // 戦略別のサブスペースをキャッシュ
    private let strategySubspaces: StrategySubspaces

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        if let oldItem = oldItem {
            let keys = try buildIndexKeys(for: oldItem)
            for key in keys {
                transaction.clear(key: key)
            }
        }

        if let newItem = newItem {
            let keys = try buildIndexKeys(for: newItem)
            for key in keys {
                transaction.setValue([], for: key)
            }
        }
    }

    private func buildIndexKeys(for item: Item) throws -> [FDB.Bytes] {
        let from = try extractField(from: item, fieldName: fromField)
        let edge = try extractEdgeField(from: item)
        let to = try extractField(from: item, fieldName: toField)

        switch strategy {
        case .adjacency:
            return buildAdjacencyKeys(from: from, edge: edge, to: to)
        case .tripleStore:
            return buildTripleStoreKeys(from: from, edge: edge, to: to)
        case .hexastore:
            return buildHexastoreKeys(from: from, edge: edge, to: to)
        }
    }
}
```

### 5.2 戦略別キー生成

```swift
extension GraphIndexMaintainer {
    /// adjacency 戦略: 2 keys
    private func buildAdjacencyKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement
    ) -> [FDB.Bytes] {
        // [out]/[edge]/[from]/[to]
        let outKey = strategySubspaces.out.pack(Tuple([edge, from, to]))
        // [in]/[edge]/[to]/[from]
        let inKey = strategySubspaces.in.pack(Tuple([edge, to, from]))
        return [outKey, inKey]
    }

    /// tripleStore 戦略: 3 keys (SPO/POS/OSP)
    private func buildTripleStoreKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement
    ) -> [FDB.Bytes] {
        // [spo]/[from]/[edge]/[to]
        let spoKey = strategySubspaces.spo.pack(Tuple([from, edge, to]))
        // [pos]/[edge]/[to]/[from]
        let posKey = strategySubspaces.pos.pack(Tuple([edge, to, from]))
        // [osp]/[to]/[from]/[edge]
        let ospKey = strategySubspaces.osp.pack(Tuple([to, from, edge]))
        return [spoKey, posKey, ospKey]
    }

    /// hexastore 戦略: 6 keys (全順列)
    private func buildHexastoreKeys(
        from: any TupleElement,
        edge: any TupleElement,
        to: any TupleElement
    ) -> [FDB.Bytes] {
        return [
            strategySubspaces.spo.pack(Tuple([from, edge, to])),
            strategySubspaces.sop.pack(Tuple([from, to, edge])),
            strategySubspaces.pso.pack(Tuple([edge, from, to])),
            strategySubspaces.pos.pack(Tuple([edge, to, from])),
            strategySubspaces.osp.pack(Tuple([to, from, edge])),
            strategySubspaces.ops.pack(Tuple([to, edge, from])),
        ]
    }
}
```

### 5.3 StrategySubspaces

```swift
/// 戦略別のサブスペースをキャッシュ
struct StrategySubspaces {
    // adjacency
    let out: Subspace
    let `in`: Subspace

    // tripleStore (SPO/POS/OSP)
    let spo: Subspace
    let pos: Subspace
    let osp: Subspace

    // hexastore (追加 SOP/PSO/OPS)
    let sop: Subspace
    let pso: Subspace
    let ops: Subspace

    init(base: Subspace, strategy: GraphIndexStrategy) {
        // adjacency
        self.out = base.subspace(SubspaceKey.graphOut.rawValue)
        self.in = base.subspace(SubspaceKey.graphIn.rawValue)

        // tripleStore
        self.spo = base.subspace(SubspaceKey.graphSPO.rawValue)
        self.pos = base.subspace(SubspaceKey.graphPOS.rawValue)
        self.osp = base.subspace(SubspaceKey.graphOSP.rawValue)

        // hexastore
        self.sop = base.subspace(SubspaceKey.graphSOP.rawValue)
        self.pso = base.subspace(SubspaceKey.graphPSO.rawValue)
        self.ops = base.subspace(SubspaceKey.graphOPS.rawValue)
    }
}

// SubspaceKey に追加
extension SubspaceKey {
    static let graphOut: SubspaceKey = ...
    static let graphIn: SubspaceKey = ...
    static let graphSPO: SubspaceKey = ...
    static let graphPOS: SubspaceKey = ...
    static let graphOSP: SubspaceKey = ...
    static let graphSOP: SubspaceKey = ...
    static let graphPSO: SubspaceKey = ...
    static let graphOPS: SubspaceKey = ...
}
```

---

## 6. クエリ API 設計

### 6.1 GraphQueryBuilder

```swift
/// グラフクエリビルダー
///
/// SPARQL ライクなパターンマッチングを提供
public struct GraphQueryBuilder<Item: Persistable> {
    private let context: FDBContext
    private let indexName: String

    // クエリパターン
    private var fromPattern: QueryPattern = .any
    private var edgePattern: QueryPattern = .any
    private var toPattern: QueryPattern = .any

    /// From ノードを指定
    public func from(_ value: any TupleElement) -> Self {
        var copy = self
        copy.fromPattern = .exact(value)
        return copy
    }

    /// Edge ラベルを指定
    public func edge(_ value: any TupleElement) -> Self {
        var copy = self
        copy.edgePattern = .exact(value)
        return copy
    }

    /// To ノードを指定
    public func to(_ value: any TupleElement) -> Self {
        var copy = self
        copy.toPattern = .exact(value)
        return copy
    }

    /// クエリ実行
    public func execute() async throws -> [GraphEdge] {
        // 最適なインデックスを選択してスキャン
        let optimalIndex = selectOptimalIndex()
        return try await scanIndex(optimalIndex)
    }
}

/// クエリパターン
enum QueryPattern {
    case any           // ワイルドカード (?)
    case exact(any TupleElement)  // 完全一致
}

/// グラフエッジ結果
public struct GraphEdge: Sendable {
    public let from: any TupleElement
    public let edge: any TupleElement
    public let to: any TupleElement
}
```

### 6.2 使用例

```swift
// FDBContext extension
extension FDBContext {
    /// グラフクエリビルダーを取得
    public func graph<T: Persistable>(_ type: T.Type) -> GraphQueryBuilder<T> {
        GraphQueryBuilder(context: self, indexName: /* auto-detect */)
    }
}

// 使用例
let context = container.newContext()

// "Alice が知っている人" (s=Alice, p=knows, o=?)
let friends = try await context.graph(Statement.self)
    .from("Alice")
    .edge("knows")
    .execute()

// "Bob を知っている人" (s=?, p=knows, o=Bob)
let whoKnowsBob = try await context.graph(Statement.self)
    .edge("knows")
    .to("Bob")
    .execute()

// "Alice と Bob の関係" (s=Alice, p=?, o=Bob)
let relations = try await context.graph(Statement.self)
    .from("Alice")
    .to("Bob")
    .execute()
```

---

## 7. マイグレーション計画

### 7.1 削除対象

```
database-kit/
  - Sources/Triple/  ← 削除、Graph/ に統合

database-framework/
  - Sources/TripleIndex/  ← 削除、GraphIndex/ に統合
  - Tests/TripleIndexTests/  ← GraphIndexTests/ に統合
```

### 7.2 名前変更

| 現在 | 統合後 |
|------|--------|
| AdjacencyIndexKind | GraphIndexKind |
| AdjacencyIndexMaintainer | GraphIndexMaintainer |
| TripleIndexKind | (削除 → GraphIndexKind で代替) |
| TripleIndexMaintainer | (削除 → GraphIndexMaintainer で代替) |

### 7.3 Package.swift 変更

```swift
// 削除
.library(name: "TripleIndex", targets: ["TripleIndex"]),

// 維持（名前は GraphIndex のまま）
.library(name: "GraphIndex", targets: ["GraphIndex"]),

// database-kit 側
// Triple モジュールを Graph モジュールに統合
```

---

## 8. 将来の拡張

### 8.1 推論エンジン（Reasoning）

```swift
/// RDFS/OWL 推論エンジン（将来実装）
public struct GraphReasoner {
    /// Materialization: 推論結果を事前計算してインデックスに保存
    func materialize(rules: [InferenceRule]) async throws

    /// Forward chaining: 新しいトリプル追加時に推論を実行
    func forwardChain(newTriple: GraphEdge) async throws -> [GraphEdge]
}

/// 推論ルール
enum InferenceRule {
    case rdfsSubClassOf      // クラス階層
    case rdfsDomain          // ドメイン推論
    case rdfsRange           // レンジ推論
    case owlTransitive       // 推移的関係
    case owlInverse          // 逆関係
}
```

### 8.2 SPARQL サブセット

```swift
/// SPARQL パターンマッチング（将来実装）
public struct SPARQLQuery {
    var patterns: [TriplePattern]
    var filters: [FilterExpression]
    var limit: Int?
    var offset: Int?
}

// 例: SELECT ?x WHERE { ?x :knows :Bob . ?x :age ?age . FILTER(?age > 30) }
```

---

## 9. 実装順序

1. **Phase 1: database-kit の GraphIndexKind 統合**
   - GraphIndexStrategy enum 追加
   - GraphIndexKind に strategy プロパティ追加
   - 便利イニシャライザ追加 (.rdf(), .social())

2. **Phase 2: database-framework の GraphIndexMaintainer 統合**
   - StrategySubspaces 実装
   - 戦略別キー生成ロジック実装
   - 既存テストを新 API に移行

3. **Phase 3: TripleIndex モジュール削除**
   - database-kit/Sources/Triple/ 削除
   - database-framework/Sources/TripleIndex/ 削除
   - Package.swift 更新

4. **Phase 4: クエリ API 実装**
   - GraphQueryBuilder 実装
   - FDBContext extension 追加
   - クエリ最適化（インデックス選択）

5. **Phase 5: テスト・ドキュメント**
   - 統合テスト
   - CLAUDE.md 更新
   - 使用例ドキュメント
