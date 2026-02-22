# Phase 1: Reasoner 正確性修正 — 設計ドキュメント

## 対象バグ

| ID | 重要度 | 概要 | 影響 |
|----|--------|------|------|
| B-1 | CRITICAL | ≤-rule でノミナルノードをマージ可能 | 推論の **健全性 (soundness)** 違反 |
| B-2 | HIGH | backtrack 後に conceptSignature が stale | ブロッキング判定の偽陰性 → 潜在的 **非停止** |
| B-6 | HIGH | OWL2RLMaterializer の maxInferenceDepth 未強制 | 無限マテリアライゼーション |
| B-9 | MEDIUM | reachableIndividuals() がプロパティチェーン未対応 | 推論の **完全性 (completeness)** 欠落 |

---

## B-1: ノミナルマージ禁止

### 問題の根源

SHOIN(D) Tableaux において、ノミナル（名前付き個体）は Unique Name Assumption (UNA)
により同一性が固定されている。≤-rule（最大カーディナリティ制約）は qualified successor が
上限 n を超えた場合にノードをマージするが、2つの異なるノミナルをマージすると
UNA に違反し、推論が **unsound** になる。

**Reference**: Horrocks & Sattler (2007), Section 5.1 — "Nominals and the ≤-rule"

### 現在のコード

`ExpansionRules.swift:489-503`:
```swift
if qualified.count > n {
    let sortedByPriority = qualified.sorted { a, b in
        (a.isNominalNode ? 1 : 0) < (b.isNominalNode ? 1 : 0)
    }
    let survivor = sortedByPriority[0]
    let toMerge = Array(sortedByPriority.dropFirst(n))
    for mergeID in toMerge {
        graph.mergeNodes(survivor: survivor, merged: mergeID)  // ← ノミナル同士をマージ可能
    }
}
```

ソートにより非ノミナルが先に選ばれるが、全員がノミナルの場合でもマージが実行される。

### 修正設計

**原則**: ノミナル同士のマージ要求 = clash（矛盾）

#### 1. `CompletionGraph.mergeNodes()` にガード追加

```swift
enum MergeResult: Sendable {
    case success
    case nominalClash(survivor: NodeID, merged: NodeID)
}

func mergeNodes(survivor: NodeID, merged: NodeID) -> MergeResult {
    // UNA: 異なるノミナル同士のマージは禁止
    if survivor.isNominalNode && merged.isNominalNode && survivor != merged {
        return .nominalClash(survivor: survivor, merged: merged)
    }
    // ... 既存のマージロジック ...
    return .success
}
```

#### 2. `applyMaxCardinalityRule()` で clash を返す

```swift
static func applyMaxCardinalityRule(
    at nodeID: NodeID,
    in graph: CompletionGraph
) -> RuleApplicationResult {  // 戻り値型を Bool → RuleApplicationResult に変更
    // ...
    if qualified.count > n {
        // ノミナル数を確認
        let nominalCount = qualified.filter { $0.isNominalNode }.count
        let nonNominalCount = qualified.count - nominalCount

        // マージ後に残す数 = n、マージ対象 = qualified.count - n
        // マージ対象にノミナルが2つ以上含まれる = clash
        let mergeCount = qualified.count - n

        // 非ノミナルを優先的にマージ。非ノミナルで足りなければノミナルが対象に。
        // ノミナルが2つ以上マージ対象 → clash
        // survivor がノミナルで merged もノミナル → clash
        let sortedByPriority = qualified.sorted { a, b in
            (a.isNominalNode ? 1 : 0) < (b.isNominalNode ? 1 : 0)
        }
        let survivor = sortedByPriority[0]
        let toMerge = Array(sortedByPriority.dropFirst(n))

        for mergeID in toMerge {
            let result = graph.mergeNodes(survivor: survivor, merged: mergeID)
            if case .nominalClash(let s, let m) = result {
                return .clash(ClashInfo(
                    type: .nominal,
                    node: nodeID,
                    message: "Cannot merge nominals \(s) and \(m) — UNA violation"
                ))
            }
            changed = true
        }
    }
    // ...
    return changed ? .applied : .notApplicable
}
```

#### 3. `ClashInfo.ClashType` に `.nominal` を追加

```swift
public enum ClashType: Sendable {
    case complement
    case disjoint
    case nominal           // ← 新規追加
    case datatypeViolation
    case maxCardinality
}
```

#### 4. `applyMaxCardinalityRule` の呼び出し元を更新

`TableauxReasoner` で `applyMaxCardinalityRule` を呼ぶ箇所を、
新しい `RuleApplicationResult` 戻り値に対応させる。

### テスト設計

```swift
// T-B1-1: ノミナル同士のマージで clash が返る
@Test func nominalMergeClash() {
    // ≤1 R.⊤ ∈ L(x), R-successor に nominal(a) と nominal(b)
    // → clash を返すこと
}

// T-B1-2: ノミナルと非ノミナルのマージは成功
@Test func nominalNonNominalMerge() {
    // ≤1 R.⊤ ∈ L(x), R-successor に nominal(a) と generated(1)
    // → generated(1) が nominal(a) にマージされる
}

// T-B1-3: 全非ノミナルのマージは従来通り成功
@Test func nonNominalMerge() {
    // ≤1 R.⊤ ∈ L(x), R-successor に generated(1) と generated(2)
    // → 成功
}

// T-B1-4: 回帰テスト — 既存の maxCardinality テストが引き続きパス
```

---

## B-2: conceptSignature のバックトラック時復元

### 問題の根源

`CompletionGraph` は Bloom filter 風の 64-bit `conceptSignature` を
ブロッキング判定の O(1) プリチェックに使用する。

```swift
// findBlocker() のプリチェック
if (node.conceptSignature & ancestor.conceptSignature) == node.conceptSignature {
    // → node.concepts ⊆ ancestor.concepts の可能性あり → 完全チェックに進む
}
```

`undoAction(.addedConcept)` で `concepts.remove()` は行うが、
`conceptSignature` のビットは復元しない。コメントには
「Bloom filter は削除不可。false positive のみ発生し、false negative は起こらない」
とあるが、これは **誤り**。

**なぜ false negative が発生するか**:

1. バックトラック前: node X に concept A が追加 → `X.signature |= hash(A)`
2. バックトラック: concept A が remove されるが、signature のビットは残る
3. node X は stale bit を持つ（実際にはない concept のビットが立っている）
4. `findBlocker()` で ancestor Y との比較:
   - `(X.signature & Y.signature) == X.signature` の条件
   - X.signature に stale bit がある → Y にそのビットがなければ条件 **不成立**
   - しかし実際は `X.concepts ⊆ Y.concepts` が **成立**する場合がある
5. **有効なブロッカーを見逃す** → ノードが不必要に展開 → 非停止リスク

### 修正設計

**方針**: バックトラック時に `conceptSignature` を残存 concepts から再計算する。

コストは `O(|concepts|)` だが、バックトラックは非決定性ルール（⊔-rule）の
分岐失敗時のみ発生するため、頻度は限定的。

#### `undoAction(.addedConcept)` の修正

```swift
case .addedConcept(let nodeID, let concept):
    guard let node = nodes[nodeID] else { break }
    node.concepts.remove(concept)

    // Undo complement clash index
    node.complementClashes.remove(concept)
    if case .complement(let inner) = concept {
        node.complementClashes.remove(inner)
    }

    // Undo named class IRI tracking
    if case .named(let iri) = concept {
        node.namedClassIRIs.remove(iri)
    }

    // Recompute conceptSignature from remaining concepts
    node.recomputeSignature()
```

#### `CompletionNode` にヘルパー追加

```swift
extension CompletionNode {
    /// Recompute Bloom filter signature from current concepts
    func recomputeSignature() {
        var sig: UInt64 = 0
        for concept in concepts {
            sig |= 1 << UInt64(concept.hashValue & 0x3F)
        }
        conceptSignature = sig
    }
}
```

#### `undoAction(.mergedNodes)` でも再計算

```swift
case .mergedNodes(let survivor, let merged, let mergedConcepts, let mergedEdges, let survivorFlags):
    // ... 既存の復元ロジック ...

    // Recompute signatures for both nodes
    nodes[survivor]?.recomputeSignature()
    nodes[merged]?.recomputeSignature()
```

### テスト設計

```swift
// T-B2-1: バックトラック後の signature が正確に再計算される
@Test func signatureRecomputedAfterBacktrack() {
    let graph = CompletionGraph()
    let nodeID = graph.createNode()
    let conceptA = OWLClassExpression.named("A")
    let conceptB = OWLClassExpression.named("B")

    graph.addConcept(conceptA, to: nodeID)
    graph.addConcept(conceptB, to: nodeID)
    let sigBefore = graph.node(nodeID)!.conceptSignature

    // Save trail position, add concept C, then undo
    let pos = graph.trailPosition
    let conceptC = OWLClassExpression.named("C")
    graph.addConcept(conceptC, to: nodeID)
    graph.undoToTrailPosition(pos)

    // Signature should match the state with only A and B
    #expect(graph.node(nodeID)!.conceptSignature == sigBefore)
}

// T-B2-2: stale signature で blocker を見逃す問題が修正される
@Test func noMissedBlockerAfterBacktrack() {
    // Setup: node X with concepts {A, B}, ancestor Y with concepts {A, B}
    // Add concept C to X → undo → X should still be blocked by Y
    // (Before fix: stale bit from C causes X.sig & Y.sig != X.sig → blocker missed)
}

// T-B2-3: merge undo 後の signature が正確
@Test func signatureRecomputedAfterMergeUndo() {
    // Merge two nodes, then undo → both nodes' signatures should be accurate
}
```

---

## B-6: OWL2RLMaterializer の depth 制限強制

### 問題の根源

`OWL2RLMaterializer.Configuration.maxInferenceDepth` が定義されているが、
マテリアライゼーション中に一度も参照されない。
循環的なオントロジー（A rdfs:subClassOf B, B rdfs:subClassOf A）や
推移的プロパティチェーンで無限ループが発生する可能性。

**注**: 現在の実装は OntologyStore のマテリアライズ済み階層をルックアップするため、
直接的な無限再帰は発生しにくい。しかし、`materializeOnWrite` が書き込みトリガーで
他のルールを連鎖的に発火する場合に制限が必要。

### 修正設計

**方針**: `materializeOnWrite` に depth パラメータを追加し、再帰呼び出し時にデクリメント。

#### 1. 内部メソッドに depth パラメータ追加

```swift
public func materializeOnWrite(
    triple: (subject: String, predicate: String, object: String),
    ontologyIRI: String,
    transaction: any TransactionProtocol
) async throws -> InferenceResult {
    try await materializeOnWrite(
        triple: triple,
        ontologyIRI: ontologyIRI,
        transaction: transaction,
        depth: 0
    )
}

private func materializeOnWrite(
    triple: (subject: String, predicate: String, object: String),
    ontologyIRI: String,
    transaction: any TransactionProtocol,
    depth: Int
) async throws -> InferenceResult {
    guard depth < configuration.maxInferenceDepth else {
        var result = InferenceResult()
        result.statistics.depthLimitReached = true
        return result
    }
    // ... 既存ロジック（内部の再帰呼び出しで depth + 1 を渡す）...
}
```

#### 2. InferenceResult.Statistics に depth 超過フラグ追加

```swift
public struct Statistics: Sendable {
    public var ruleApplications: Int = 0
    public var triplesInferred: Int = 0
    public var inferenceTime: TimeInterval = 0
    public var depthLimitReached: Bool = false  // ← 新規追加
}
```

#### 3. 重複推論の防止（visited セット）

depth 制限に加え、同一トリプルの再推論を防ぐ visited セットも追加：

```swift
private func materializeOnWrite(
    triple: (subject: String, predicate: String, object: String),
    ontologyIRI: String,
    transaction: any TransactionProtocol,
    depth: Int,
    visited: inout Set<TripleKey>
) async throws -> InferenceResult {
    let key = TripleKey(triple.subject, triple.predicate, triple.object)
    guard !visited.contains(key) else {
        return InferenceResult()
    }
    visited.insert(key)
    guard depth < configuration.maxInferenceDepth else { ... }
    // ...
}
```

### テスト設計

```swift
// T-B6-1: 循環的サブクラス関係で depth 制限が発動
@Test func depthLimitOnCyclicSubClass() async throws {
    // A rdfs:subClassOf B, B rdfs:subClassOf A
    // materializeOnWrite で depth limit に到達すること
}

// T-B6-2: 正常なマテリアライゼーションは depth 制限に引っかからない
@Test func normalMaterializationWithinDepthLimit() async throws {
    // 線形階層 A < B < C で x rdf:type A を挿入
    // depthLimitReached == false
}

// T-B6-3: visited セットが同一トリプルの再推論を防ぐ
@Test func visitedSetPreventsRedundantInference() async throws {
    // 同一トリプルを2回 materialize → 2回目は空結果
}
```

---

## B-9: reachableIndividuals() のプロパティチェーン対応

### 問題の根源

`OWLReasoner.reachableIndividuals()` は inverse, symmetric, sub-property, transitive
を処理するが、`owl:propertyChainAxiom` を処理しない。

例: `ex:hasUncle owl:propertyChainAxiom (ex:hasParent ex:hasBrother)` の場合、
`reachableIndividuals(from: "Alice", via: "ex:hasUncle")` が
`Alice → (hasParent) → Bob → (hasBrother) → Charlie` を見つけられない。

### 修正設計

`reachableIndividuals()` の `includeInferred` ブロック末尾にチェーン処理を追加：

```swift
// Handle property chain axioms
let chains = roleHierarchy.propertyChains(for: property)
for chain in chains {
    // chain = ["ex:hasParent", "ex:hasBrother"]
    // Starting from 'individual', follow each step of the chain
    var currentIndividuals: Set<String> = [individual]

    for chainProperty in chain {
        var nextIndividuals = Set<String>()
        for ind in currentIndividuals {
            // Direct assertions for this chain step
            for (prop, obj) in ontologyIndex.objectPropertyAssertionsBySubject[ind] ?? [] {
                if prop == chainProperty {
                    nextIndividuals.insert(obj)
                }
            }
            // Also include sub-properties of chain step
            for subProp in subProperties(of: chainProperty) {
                for (prop, obj) in ontologyIndex.objectPropertyAssertionsBySubject[ind] ?? [] {
                    if prop == subProp {
                        nextIndividuals.insert(obj)
                    }
                }
            }
        }
        currentIndividuals = nextIndividuals
        if currentIndividuals.isEmpty { break }
    }

    result.formUnion(currentIndividuals)
}
```

### テスト設計

```swift
// T-B9-1: 2段プロパティチェーンの到達可能性
@Test func propertyChainReachability() {
    // hasUncle ← chain(hasParent, hasBrother)
    // Alice hasParent Bob, Bob hasBrother Charlie
    // reachableIndividuals(from: "Alice", via: "hasUncle") should contain "Charlie"
}

// T-B9-2: 3段プロパティチェーン
@Test func threeStepPropertyChain() {
    // hasCousin ← chain(hasParent, hasSibling, hasChild)
    // A → B → C → D
    // reachableIndividuals(from: "A", via: "hasCousin") should contain "D"
}

// T-B9-3: チェーンの途中で経路が切れる場合
@Test func brokenPropertyChain() {
    // hasUncle ← chain(hasParent, hasBrother)
    // Alice hasParent Bob (Bob has no hasBrother)
    // reachableIndividuals(from: "Alice", via: "hasUncle") should be empty
}

// T-B9-4: チェーンとサブプロパティの組み合わせ
@Test func propertyChainWithSubProperties() {
    // hasUncle ← chain(hasParent, hasBrother)
    // hasFather subPropertyOf hasParent
    // Alice hasFather Bob, Bob hasBrother Charlie
    // → "Charlie" reachable
}
```

---

## 戻り値型の変更に伴うリファクタリング

### `applyMaxCardinalityRule` の呼び出し元

`ExpansionRules` の各ルール関数は現在 `Bool` を返す。B-1 の修正で
`applyMaxCardinalityRule` のみ `RuleApplicationResult` を返すように変更する。

これは **他のルール関数には影響しない**。`applyMaxCardinalityRule` の呼び出し元
（`TableauxReasoner` の expansion loop）のみ修正が必要。

現在の呼び出しパターン:
```swift
changed = ExpansionRules.applyMaxCardinalityRule(at: nodeID, in: graph)
```

修正後:
```swift
let result = ExpansionRules.applyMaxCardinalityRule(at: nodeID, in: graph)
switch result {
case .applied:
    changed = true
case .clash(let info):
    return .clash(info)  // 上位に伝播
case .notApplicable:
    break
}
```

---

## 影響範囲

| ファイル | 変更内容 |
|---------|---------|
| `Sources/GraphIndex/Reasoning/CompletionGraph.swift` | `mergeNodes()` 戻り値変更、`undoAction()` signature 再計算、`recomputeSignature()` 追加 |
| `Sources/GraphIndex/Reasoning/ExpansionRules.swift` | `applyMaxCardinalityRule()` 戻り値変更、clash 返却 |
| `Sources/GraphIndex/Reasoning/TableauxReasoner.swift` | `applyMaxCardinalityRule` 呼び出し箇所の更新 |
| `Sources/GraphIndex/Reasoning/OWLReasoner.swift` | `reachableIndividuals()` にチェーン処理追加 |
| `Sources/GraphIndex/OWLReasoning/OWL2RLMaterializer.swift` | depth パラメータ追加、visited セット追加 |
| `Tests/GraphIndexTests/ReasonerPhase1Tests.swift` | 全テストケース（新規ファイル） |

## 実装順序

1. **B-1** (CompletionGraph.mergeNodes + ExpansionRules + TableauxReasoner)
2. **B-2** (CompletionGraph.undoAction + recomputeSignature)
3. **B-6** (OWL2RLMaterializer depth + visited)
4. **B-9** (OWLReasoner.reachableIndividuals)
5. **テスト一括** (ReasonerPhase1Tests.swift)

B-1 と B-2 は CompletionGraph に集中しているため、一括で修正すると整合性が取りやすい。
