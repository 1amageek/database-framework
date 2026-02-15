# Adaptive Join Strategy 提案

## 概要

database-framework の SPARQL 実行エンジン（`SPARQLQueryExecutor.evaluateBasic`）に **3段階の Adaptive Join Strategy** を導入し、大規模データセットでの BGP 評価パフォーマンスを改善する。

## 背景

### 現状の Nested Loop Join with Variable Substitution

`evaluateBasic`（L368-450）は BGP の各トリプルパターンを順番に評価し、Nested Loop Join で結合する。

```
for binding in currentBindings:             // 左側: N 件
    substituted = pattern.substitute(binding)  // 変数を具体値に置換
    matches = executePattern(substituted)       // FDB prefix scan × N 回
    for match in matches:
        merged = binding.merged(match)
```

**変数置換の効果**: `(?person, "knows", ?target)` に `{?person → "Alice"}` を代入すると `("Alice", "knows", ?target)` になり、hexastore SPO インデックスで `("Alice", "knows", *)` のプレフィックススキャンに落ちる。これは非常に選択的で、少数の結果しか返さない。

### 問題

中間結果セットが大きくなると FDB ラウンドトリップが累積する。

```
N bindings × 0.3ms/lookup = N × 0.3ms（1 パターンあたり）

N=100  → 30ms   ← 問題なし
N=1000 → 300ms  ← 許容限界
N=5000 → 1.5s   ← 問題
```

### 単純な Hash Join の問題点

右パターンを変数置換なしで実行する Hash Join には致命的な問題がある。

```
パターン: (?person, "knows", ?target)
変数置換なし → predicate のみ bound → PSO インデックスで ("knows", *, *)
→ "knows" エッジ 50,000 件をフルスキャン
```

Nested Loop なら 1,000 回のプレフィックススキャン（各数件）で合計 ~3,000 件の読み取り。
Hash Join だと 1 回のスキャンで 50,000 件の読み取り。**Hash Join の方が遅い可能性がある。**

## 対象ファイル

```
Sources/GraphIndex/SPARQL/SPARQLQueryExecutor.swift  (evaluateBasic: L368-450)
Sources/GraphIndex/SPARQL/VariableBinding.swift
Sources/GraphIndex/SPARQL/TriplePattern.swift
```

## 提案: 3段階 Adaptive Join Strategy

### 戦略選択フロー

```
                  currentBindings.count
                         │
                    ≤ NL_THRESHOLD (64)?
                    ┌────┴────┐
                   YES       NO
                    │         │
              Nested Loop   結合変数あり？
              (現行どおり)   ┌────┴────┐
                           NO       YES
                            │         │
                      Nested Loop   右パターンの静的 bound 数を評価
                      (フォールバック)    │
                                    静的 bound ≥ 2?
                                   ┌────┴────┐
                                  YES       NO
                                   │         │
                              Hash Join   Batched NLJ
                              (右側十分選択的) (プレフィックス共有でバッチ化)
```

### Strategy 1: Nested Loop Join（現行）

左側が小さい場合（≤ 64 件）または結合変数がない場合。現行のまま。

### Strategy 2: Batched Nested Loop Join（新規・主要最適化）

**核心的アイデア**: 変数置換の選択性を維持しつつ、FDB ラウンドトリップを削減する。

左バインディングが同じプレフィックスを生成する場合、1 回のレンジスキャンで複数バインディングの結果を取得できる。

```
左バインディング:
  {?person → "Alice", ?age → 30}
  {?person → "Alice", ?age → 25}
  {?person → "Bob",   ?age → 40}

パターン: (?person, "knows", ?target)

置換後のプレフィックス:
  "Alice" → ("Alice", "knows", *)  ← 2バインディングが同じスキャン
  "Bob"   → ("Bob", "knows", *)

FDB アクセス: 2回（N=3 → 重複排除で 2回に削減）
```

**一般化**: 結合変数が複数ある場合、置換後のパターンの bound 部分が一致するバインディングをグループ化する。

```swift
/// Batched Nested Loop Join
///
/// 左バインディングを置換後のスキャンプレフィックスでグループ化し、
/// 同一プレフィックスのバインディングを 1 回の FDB スキャンで処理する。
///
/// Reference: "Sideways Information Passing" in query optimization literature
private func evaluateBatchedNLJ(
    pattern: ExecutionTriple,
    leftBindings: [VariableBinding],
    indexSubspace: Subspace,
    strategy: GraphIndexStrategy,
    transaction: any TransactionProtocol,
    filter: FilterExpression?
) async throws -> ([VariableBinding], ExecutionStatistics) {
    var stats = ExecutionStatistics()
    stats.joinOperations += 1

    // 1. 各バインディングを置換し、スキャンプレフィックスでグループ化
    var prefixGroups: [ScanPrefix: [VariableBinding]] = [:]
    for binding in leftBindings {
        let substituted = pattern.substitute(binding)
        let prefix = ScanPrefix(pattern: substituted, strategy: strategy)
        prefixGroups[prefix, default: []].append(binding)
    }

    // 2. ユニークなプレフィックスごとに 1 回だけ FDB スキャン
    var results: [VariableBinding] = []
    for (prefix, groupBindings) in prefixGroups {
        let (matches, scanStats) = try await executePattern(
            prefix.substitutedPattern,
            indexSubspace: indexSubspace,
            strategy: strategy,
            transaction: transaction,
            filter: nil
        )
        stats.indexScans += scanStats.indexScans

        // 3. グループ内の各バインディングと結果をマージ
        for match in matches {
            for leftBinding in groupBindings {
                if let merged = leftBinding.merged(with: match) {
                    if let f = filter {
                        guard f.evaluate(merged) else { continue }
                    }
                    results.append(merged)
                }
            }
        }
    }

    stats.intermediateResults += results.count
    return (results, stats)
}
```

**ScanPrefix**: 置換後パターンの bound 部分をキーとする。

```swift
/// 置換後パターンのスキャンプレフィックスをキーとする
///
/// 同じ bound 値を持つ置換パターンは同一の FDB レンジスキャンになるため、
/// グループ化して 1 回のスキャンで処理する。
struct ScanPrefix: Hashable {
    /// 置換後の bound 値（subject, predicate, object の順）
    let boundValues: [FieldValue?]

    /// グループの代表パターン（FDB スキャン用）
    let substitutedPattern: ExecutionTriple

    init(pattern: ExecutionTriple, strategy: GraphIndexStrategy) {
        self.boundValues = [
            pattern.subject.literalValue,
            pattern.predicate.literalValue,
            pattern.object.literalValue
        ]
        self.substitutedPattern = pattern
    }

    // Hashable: boundValues のみで比較（substitutedPattern は除外）
    static func == (lhs: ScanPrefix, rhs: ScanPrefix) -> Bool {
        lhs.boundValues == rhs.boundValues
    }

    func hash(into hasher: inout Hasher) {
        for value in boundValues {
            hasher.combine(value)
        }
    }
}
```

**効果の見積もり**:

| 条件 | Nested Loop | Batched NLJ | 削減率 |
|------|-------------|-------------|--------|
| 1,000 バインディング、10 ユニーク person | FDB 1,000 回 | FDB 10 回 | 99% |
| 1,000 バインディング、500 ユニーク person | FDB 1,000 回 | FDB 500 回 | 50% |
| 1,000 バインディング、1,000 ユニーク（全て異なる） | FDB 1,000 回 | FDB 1,000 回 | 0% |

**重要**: 変数置換の選択性を完全に維持するため、右側フルスキャンのリスクがない。

### Strategy 3: Hash Join（選択的な場合のみ）

右パターンが静的に十分な bound 項を持つ場合のみ使用する。

**採用条件**（全て満たす場合のみ）:
1. 左側が大きい（> NL_THRESHOLD）
2. 結合変数がある
3. 右パターンの**静的 bound 数** ≥ 2（変数置換前の時点で bound な項）

**「静的 bound」とは**: 結合変数ではなく、パターン自体に含まれる定数値。

```
パターン: (?person, "knows", "Alice")
  静的 bound: predicate="knows", object="Alice" → 2
  結合変数: ?person → 左バインディングから来る

パターン: (?person, "knows", ?target)
  静的 bound: predicate="knows" → 1
  結合変数: ?person → 左バインディングから来る

パターン: (?s, ?p, ?o)
  静的 bound: なし → 0
```

静的 bound ≥ 2 の場合、変数置換なしでも hexastore のプレフィックススキャンが十分に狭い（例: SPO で subject と predicate が bound なら、その組み合わせのエッジのみスキャン）。

```swift
/// Hash Join（右パターンが十分に選択的な場合のみ使用）
///
/// 左バインディングをハッシュテーブルに格納し、
/// 右パターンを 1 回のスキャンで実行して結合する。
///
/// Reference: "Classic Hash Join", Garcia-Molina et al.,
///            "Database Systems: The Complete Book", Chapter 15.5
private func evaluateHashJoin(
    pattern: ExecutionTriple,
    leftBindings: [VariableBinding],
    joinVariables: Set<String>,
    indexSubspace: Subspace,
    strategy: GraphIndexStrategy,
    transaction: any TransactionProtocol,
    filter: FilterExpression?
) async throws -> ([VariableBinding], ExecutionStatistics) {
    let sortedJoinVars = joinVariables.sorted()

    // Build Phase: 左側をハッシュテーブルに格納
    var hashTable: [JoinKey: [VariableBinding]] = [:]
    hashTable.reserveCapacity(leftBindings.count)
    for binding in leftBindings {
        let key = JoinKey(binding: binding, variables: sortedJoinVars)
        hashTable[key, default: []].append(binding)
    }

    // Probe Phase: 右パターンを 1 回実行（変数置換なし）
    // 静的 bound ≥ 2 が保証されているため、スキャン範囲は制限される
    let (rightMatches, scanStats) = try await executePattern(
        pattern,
        indexSubspace: indexSubspace,
        strategy: strategy,
        transaction: transaction,
        filter: nil
    )

    // 結合
    var results: [VariableBinding] = []
    for match in rightMatches {
        let probeKey = JoinKey(binding: match, variables: sortedJoinVars)
        guard let leftGroup = hashTable[probeKey] else { continue }
        for leftBinding in leftGroup {
            if let merged = leftBinding.merged(with: match) {
                if let f = filter {
                    guard f.evaluate(merged) else { continue }
                }
                results.append(merged)
            }
        }
    }

    return (results, scanStats)
}
```

### JoinKey

旧提案の `compactMap` バグを修正。unbound 変数は `.null` にマッピングして配列長を保持する。

```swift
/// Hash Join のキー（結合変数の値の組み合わせ）
///
/// 結合変数の値を配列として保持する。unbound 変数は .null として扱い、
/// 配列長を常に一定に保つ。compactMap で nil をスキップすると
/// 異なる変数の組み合わせで衝突が発生するため禁止。
struct JoinKey: Hashable {
    let values: [FieldValue]

    init(binding: VariableBinding, variables: [String]) {
        // .null で unbound を表現（配列長を保持）
        self.values = variables.map { binding[$0] ?? .null }
    }
}
```

### evaluateBasic の変更

```swift
// evaluateBasic 内のループ（L405-428 を置換）
for (index, pattern) in orderedPatterns.enumerated() {
    stats.joinOperations += 1
    var newBindings: [VariableBinding] = []

    // フィルタ適用判定（既存ロジック維持）
    var boundVariablesAfterPattern = Set<String>()
    for i in 0...index {
        boundVariablesAfterPattern.formUnion(orderedPatterns[i].variables)
    }
    let canApplyFilter = filterVariables.isSubset(of: boundVariablesAfterPattern)
    let patternFilter = canApplyFilter ? filter : nil

    // --- 戦略選択 ---
    let joinVars = pattern.variables.intersection(
        currentBindings.first?.boundVariables ?? []
    )

    if currentBindings.count <= nlThreshold || joinVars.isEmpty {
        // Strategy 1: Nested Loop Join（現行）
        for binding in currentBindings {
            let substituted = pattern.substitute(binding)
            let (matches, scanStats) = try await executePattern(
                substituted,
                indexSubspace: indexSubspace,
                strategy: strategy,
                transaction: transaction,
                filter: nil
            )
            stats.indexScans += scanStats.indexScans
            for match in matches {
                if let merged = binding.merged(with: match) {
                    if let f = patternFilter {
                        guard f.evaluate(merged) else { continue }
                    }
                    newBindings.append(merged)
                }
            }
        }
    } else if pattern.staticBoundCount >= 2 {
        // Strategy 3: Hash Join（右パターンが十分に選択的）
        let (joined, joinStats) = try await evaluateHashJoin(
            pattern: pattern,
            leftBindings: currentBindings,
            joinVariables: joinVars,
            indexSubspace: indexSubspace,
            strategy: strategy,
            transaction: transaction,
            filter: patternFilter
        )
        newBindings = joined
        stats = stats.merged(with: joinStats)
    } else {
        // Strategy 2: Batched NLJ（デフォルト）
        let (batched, batchStats) = try await evaluateBatchedNLJ(
            pattern: pattern,
            leftBindings: currentBindings,
            indexSubspace: indexSubspace,
            strategy: strategy,
            transaction: transaction,
            filter: patternFilter
        )
        newBindings = batched
        stats = stats.merged(with: batchStats)
    }

    currentBindings = newBindings
    stats.intermediateResults += currentBindings.count

    if currentBindings.isEmpty { break }
}
```

### ExecutionTriple への追加プロパティ

```swift
extension ExecutionTriple {
    /// 変数置換に依存しない、パターン自体の bound 項の数
    ///
    /// 結合変数（左バインディングから来る変数）ではなく、
    /// パターン定義時の定数値の数。
    /// Hash Join の右側スキャンの選択性を判断するために使用。
    var staticBoundCount: Int {
        var count = 0
        if subject.isBound { count += 1 }
        if predicate.isBound { count += 1 }
        if object.isBound { count += 1 }
        return count
    }
}
```

注: `staticBoundCount` は既存の `boundCount` と同じ実装だが、意味論が異なる。`evaluateBasic` のループ内では、`pattern` は `orderedPatterns` から取り出した**置換前**のパターンであり、`substitute()` はまだ呼ばれていない。したがって `boundCount` がそのまま「静的 bound 数」を表す。明示的な名前で意図を明確にする。

## 戦略比較

### コスト分析

```
N = 左バインディング数
U = ユニークプレフィックス数（Batched NLJ）
R = 右パターンの結果数（Hash Join）
k = 1回のスキャンの平均結果数

Nested Loop:   FDB I/O = N,  メモリ = O(k)
Batched NLJ:   FDB I/O = U,  メモリ = O(N + k)
Hash Join:     FDB I/O = 1,  メモリ = O(N + R)
```

### 各戦略が最適なケース

| 戦略 | 最適条件 | FDB I/O | リスク |
|------|---------|---------|--------|
| Nested Loop | N ≤ 64, 結合変数なし | N | なし（実績あり） |
| Batched NLJ | N > 64, U << N | U | U ≈ N の場合は効果なし |
| Hash Join | N > 64, staticBound ≥ 2 | 1 | R >> N の場合にメモリ圧迫 |

### クロスオーバーポイント

```
Nested Loop vs Batched NLJ:
  N × 0.3ms  vs  U × 0.3ms + バッチ構築コスト
  → U < 0.8N なら Batched NLJ が有利（20% 以上の重複がある場合）

Batched NLJ vs Hash Join:
  U × 0.3ms  vs  1 × scan_time(R)
  → scan_time(R) < U × 0.3ms なら Hash Join が有利
  → R が小さい（staticBound ≥ 2 で保証）場合に成立
```

## OPTIONAL への拡張

`evaluateOptional`（L485-531）は Left Outer Join であり、Batched NLJ を自然に適用できる。

```swift
// evaluateOptional 内でも同じバッチ化が可能
private func evaluateOptionalBatched(
    leftBindings: [VariableBinding],
    rightPattern: ExecutionPattern,
    ...
) async throws -> ([VariableBinding], ExecutionStatistics) {
    // Batched NLJ と同じプレフィックスグループ化
    // 違い: 右側にマッチしなかった左バインディングを保持する

    var matched = Set<Int>()  // マッチした左バインディングのインデックス

    for (prefix, groupBindings) in prefixGroups {
        let rightResult = try await evaluate(...)
        for (idx, leftBinding) in groupBindings {
            if rightResult.bindings.isEmpty {
                results.append(leftBinding)
            } else {
                // ... merge logic（マッチしたら matched に追加）
            }
        }
    }

    // マッチしなかった左バインディングを保持
    for (idx, binding) in leftBindings.enumerated() where !matched.contains(idx) {
        results.append(binding)
    }
}
```

## 定数

```swift
extension SPARQLQueryExecutor {
    /// Nested Loop Join の上限閾値
    ///
    /// 左バインディング数がこの値以下なら Nested Loop Join を使用する。
    /// 変数置換によるポイントルックアップが最も効率的な範囲。
    ///
    /// 根拠: FDB の単一ラウンドトリップ ≈ 0.3ms
    ///       64 × 0.3ms = 19.2ms（十分高速）
    private static let nlThreshold: Int = 64

    /// Hash Join の右パターン最小静的 bound 数
    ///
    /// 右パターンの変数置換前の bound 項数がこの値以上なら Hash Join を許可。
    /// hexastore では 2 項が bound なら十分に選択的なプレフィックススキャンになる。
    private static let hashJoinMinStaticBound: Int = 2
}
```

## テスト方針

### 機能テスト

1. **戦略選択の検証**
   - N ≤ 64 → Nested Loop が選択される
   - N > 64, staticBound < 2, 重複あり → Batched NLJ が選択される
   - N > 64, staticBound ≥ 2 → Hash Join が選択される

2. **正確性の検証**
   - 結合変数あり / なしの BGP クエリ
   - OPTIONAL + Batched NLJ（Left Outer Join の保持）
   - UNION + 各戦略の組み合わせ
   - FILTER 付き JOIN
   - 空の左側 / 右側
   - unbound 変数を含む JoinKey（.null マッピングの検証）

3. **エッジケース**
   - 全バインディングが同一プレフィックス（Batched NLJ: 1 グループ）
   - 全バインディングがユニーク（Batched NLJ: 効果なし → 正しくフォールバック）
   - 結合変数が unbound のバインディングが混在

### パフォーマンステスト

```
データセット: 10K, 50K, 100K, 500K トリプル
クエリパターン:
  A. 高重複率 BGP（Batched NLJ が有利）
     SELECT ?x ?y WHERE { ?person :knows ?x . ?person :likes ?y }
  B. 高選択性 BGP（Hash Join が有利）
     SELECT ?person WHERE { ?person :worksAt "Google" . ?person :knows "Alice" }
  C. 低選択性 BGP（Nested Loop が有利）
     SELECT ?s ?p ?o WHERE { ?s ?p ?o }
測定: 実行時間、FDB ラウンドトリップ数、メモリ使用量
比較: Nested Loop vs Batched NLJ vs Hash Join vs Adaptive
```

### 閾値チューニング

NL_THRESHOLD を 32, 64, 128, 256 で変化させ、各データサイズでのクロスオーバーポイントを特定する。

## 実装順序

1. **Phase 1: Batched NLJ**（リスク最小、効果最大）
   - `ScanPrefix` 型の追加
   - `evaluateBatchedNLJ` メソッドの実装
   - `evaluateBasic` に閾値による切り替えを追加
   - テスト追加

2. **Phase 2: Hash Join**（選択的なケースのみ）
   - `JoinKey` 型の追加
   - `evaluateHashJoin` メソッドの実装
   - `staticBoundCount` による採用条件の追加
   - テスト追加

3. **Phase 3: OPTIONAL 対応**
   - `evaluateOptional` への Batched NLJ 適用
   - Left Outer Join セマンティクスの検証

4. **Phase 4: 計測と閾値調整**
   - ベンチマークスイートの作成
   - 閾値のプロファイリングベース調整
