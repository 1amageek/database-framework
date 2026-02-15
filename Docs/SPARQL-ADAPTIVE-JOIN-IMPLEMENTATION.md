# SPARQL Adaptive Join 実装記録

## 目的

`SPARQLQueryExecutor.evaluateBasic` の JOIN 実行を、データ分布に応じて切り替えることで、
不要なスキャンとラウンドトリップを削減する。

この文書は「提案」ではなく、現在の実装仕様を記録する。

## 実装済み範囲

### 1. BGP の戦略切替（Adaptive）

`evaluateBasic` の各ステップで、以下の順序で戦略を選択する。

1. `Nested Loop`
2. `Hash Join`（条件を満たす場合）
3. `Batched Nested Loop`（Hash Join 非採用時のデフォルト）

実装定数（現状）:

- `nestedLoopThreshold = 64`
- `hashJoinMinStaticBound = 2`
- `hashJoinRightSideScanCap = 64`
- `hashJoinEnabled = true`

### 2. ScanSignature ベースのバッチ化

スキャン再利用キーは `ScanSignature` を使用する。

- `orderingKey`
- `prefixValues`
- `graphConstraint`（`none / bound / variable / wildcard`）

これにより、`subject/predicate/object` が同じでも graph 条件が異なるケースで
誤ったキャッシュ共有を防ぐ。

### 3. OPTIONAL（単一トリプル右辺）のバッチ化

`OPTIONAL` で右辺が `basic([singleTriple])` の場合、左バインディングを
`ScanSignature` でグループ化して評価する。

セマンティクス:

- 右辺と互換マージできた左行はマージ結果を返す
- 互換マージが 0 件の左行は、最終フェーズで 1 回だけ左行を返す

これにより未マッチ左行の二重追加を防ぐ。

### 4. Hash Join のガード付き実行

Hash Join 実行時は、右辺を `resultLimit = cap + 1` で評価し、
`cap` を超えた場合は Hash Join を採用しない。

## フォールバック方針（重要）

本実装における「フォールバック」は**最適化戦略の切替**であり、
**実行エラーの握りつぶしではない**。

### フォールバックしてよい条件

- 非エラー条件で Hash Join 前提が崩れる場合
  - 例: 右辺件数が `hashJoinRightSideScanCap` を超過

### フォールバックしてはいけない条件

- 例外が発生した場合
  - FDB 例外
  - デコード失敗
  - その他 `throw` される実行時エラー

実装上、`catch { fallback }` は採用していない。例外は呼び出し側へ伝播する。

## 統計と可観測性

`ExecutionStatistics` に以下を追加。

- `joinStrategies: [JoinExecutionStrategy]`
  - `nestedLoop`
  - `batchedNestedLoop`
  - `hashJoin`
- `joinFallbackReasons: [JoinFallbackReason]`
  - `hashJoinRightSideExceededCap`

`merged(with:)` / `mergedStats(with:)` 経路で、これらの配列は結合される。

## 追加テスト

### 戦略選択・フォールバック

- `SPARQLIntegrationTests/testHashJoinStrategySelected`
  - Hash Join が選択されること
- `SPARQLIntegrationTests/testHashJoinFallbackReasonRecorded`
  - cap 超過時に Batched NLJ へ切替し、理由が記録されること

### 既存回帰

- `SPARQLIntegrationTests/testOptionalPattern`
- `SPARQLIntegrationTests/testOptionalBatchedNoDuplicateUnmatchedRows`
- `NamedGraphSPARQLTests/testJoinGraphBoundSubstitutionRespectsGraphConstraint`

## 関連ファイル

- `Sources/GraphIndex/SPARQL/SPARQLQueryExecutor.swift`
- `Sources/GraphIndex/SPARQL/SPARQLResult.swift`
- `Tests/GraphIndexTests/SPARQLIntegrationTests.swift`
- `Tests/GraphIndexTests/NamedGraphSPARQLTests.swift`

