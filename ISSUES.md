# database-framework: Known Issues

全モジュール横断レビューで発見された問題の一覧。重大度順に整理。

発見日: 2026-01-27
最終更新: 2026-01-27

---

## ステータスサマリ

| 重大度 | 合計 | 解決 | Close | 保留 | Open |
|--------|------|------|-------|------|------|
| Critical | 4 | 2 | 2 (検証済正常) | 0 | 0 |
| High | 7 | 7 | 0 | 0 | 0 |
| Medium | 13 | 8 | 3 (false positive) + 1 (意図的設計) | 1 | 0 |
| Low | 3 | 0 | 0 | 3 | 0 |
| SPARQL | 21 | 21 | 0 | 0 | 0 |
| **合計** | **48** | **38** | **6** | **4** | **0** |

---

## Open Issues

なし — 全件対応済み（解決・Close・保留）

---

## Deferred Issues

### M9: LeaderboardIndex — Score ジェネリック型パラメータが実質未使用

**問題**: `TimeWindowLeaderboardIndexMaintainer<Item, Score>` の `Score` 型パラメータは
受け取るが内部では全て `Int64` に変換。型パラメータが誤解を招く。

**保留理由**: LeaderboardIndex 全体のリファクタが必要。影響範囲が大きいため後日対応。

---

### L1: DatabaseEngine — `@unchecked Sendable` の使用

**ファイル**: `Sources/DatabaseEngine/QueryPlanner/PlanExecutor.swift:44`,
`Sources/DatabaseEngine/QueryPlanner/AggregationPlanExecutor.swift:56`

**問題**: `@unchecked Sendable` が使用されている。実害はないが、
CLAUDE.md ガイドラインでは Mutex パターンを推奨。

**保留理由**: Mutex 移行は大規模。現状実害なし。

---

### L2: VectorIndex — `computeIndexKeys()` 内の `transaction: nil`

**ファイル**: `Sources/VectorIndex/HNSWIndexMaintainer.swift:188`

**問題**: `getLabelForPrimaryKey(primaryKey: id, transaction: nil)` で
transaction を nil で呼び出し。設計上の疑問。

**保留理由**: 設計上の疑問だが動作に問題なし。

---

### L3: RelationshipIndex — JSON round-trip による FK nullification

**ファイル**: `Sources/RelationshipIndex/RelationshipMaintainer.swift:347-372`

**問題**: FK nullification で JSON エンコード → dict 変更 → JSON デコードの
round-trip を使用。動作するが非効率。

**保留理由**: 動作するが非効率。優先度低。

---

## 横断的パターン問題

| パターン | 違反モジュール | 状態 |
|---------|---------------|------|
| `String(describing:)` フォールバック | GraphIndex, ScalarIndex, BitmapIndex, AggregationIndex | ✅ H1 で解決 |
| 手動型変換（TypeConversion 未使用） | RankIndex, LeaderboardIndex, RelationshipIndex | ✅ H2 で解決 |
| 不要ラッパーメソッド | LeaderboardIndex, AggregationIndex, RankIndex, ScalarIndex, BitmapIndex, DatabaseEngine | ✅ H7 で解決 |
| `try?` エラー握りつぶし | SpatialIndex | ✅ H5/H6 で解決 |
| コード重複 | BitmapIndex | ✅ M5 で解決 |
| Vector 型変換の重複実装 | VectorIndex, DatabaseEngine | ✅ M13 で解決 |

---

## 模範的モジュール

以下のモジュールは問題なし。他モジュールの改善時に参照実装として活用できる。

| モジュール | 特筆事項 |
|-----------|---------|
| **DatabaseEngine** | Container/Context パターン完全準拠、3層評価アーキテクチャ正確、TypeConversion の統一実装 |
| **FullTextIndex** | BM25 実装正確、TupleEncoder 使用、エラー伝搬、命名規約全準拠、テストカバレッジ充実 |

---

## Resolved Issues (Medium)

### ~~M1: VectorIndex — 未使用の Heap 実装（Dead Code）~~ [RESOLVED]

**修正**: `CandidateHeap`, `ResultHeap` を削除。`BinaryHeap` は `MinHeap` が依存するため残存。

---

### ~~M2: AggregationIndex — `evaluateIndexFields()` の不統一~~ [RESOLVED]

**修正**: `AverageIndexMaintainer.swift` の `scanItem()` と `computeIndexKeys()` で
直接 `DataAccess.evaluateIndexFields(from:keyPaths:expression:)` を呼んでいた箇所を
`SubspaceIndexMaintainer` プロトコル拡張の `evaluateIndexFields(from:)` に統一。

---

### ~~M3: AggregationIndex — Average の null ハンドリング不統一~~ [CLOSED: intentional]

**理由**: `getAverage()` は単一 API → 空で throw。`getAllAverages()` は一覧 API → 空スキップ。
PostgreSQL の `AVG()` (NULL) vs `GROUP BY AVG()` (行なし) と同等の意図的設計。

---

### ~~M4: RankIndex — `TopKHeap` コード重複~~ [CLOSED: false positive]

**理由**: `TopKHeap` は `RankIndexMaintainer.swift` に1箇所のみ。
`RankQuery.swift` には存在しない。

---

### ~~M5: BitmapIndex — クエリビルダーのコード重複~~ [RESOLVED]

**修正**: `execute()`, `count()`, `getBitmap()` の共通処理を
`withResolvedBitmap<R: Sendable>` に抽出。
guard + indexName 生成 + typeSubspace/indexSubspace + transaction + maintainer + operation switch を
一本化し、各メソッドは bitmap 取得後の処理のみ記述。

---

### ~~M6: BitmapIndex — Bitmap 集合演算の非効率実装~~ [RESOLVED]

**修正**: 2つの問題を修正。
1. `Container.intersection` の array-array ケースが O(n\*m) → `Set` 使用で O(n+m)
2. `-` 演算子が Container 抽象を無視して全コンテナを `toArray()` → `filter` で処理していた。
   `Container.difference(_:_:)` static メソッドを新設し、array-array, array-bitmap,
   bitmap-array, bitmap-bitmap の4ケースで最適化。

---

### ~~M7: SpatialIndex — 距離単位の不統一~~ [CLOSED: false positive]

**理由**: `SpatialQuery` もメートル、`Fusion/Nearby` もメートル。単位は統一済み。

---

### ~~M8: LeaderboardIndex — README と実装の矛盾~~ [RESOLVED]

**修正**: README.md の Implementation Status を更新。
Bottom-K, Percentile, Dense ranking を「✅ Implemented (internal)」に変更。
public query API 未公開である旨を注記。

---

### ~~M10: Package.swift — 不要な依存関係~~ [RESOLVED]

**修正**: DatabaseEngine から `"Relationship"` 依存を削除、
RelationshipIndex から `"ScalarIndex"` 依存を削除。

---

### ~~M11: QueryAST — `GraphPattern.variables` の projection 分析が不完全~~ [RESOLVED]

**修正**: `.subquery` ケースで `.items` のみ処理していた箇所を全 `Projection` ケースに対応。
- `.distinctItems`: `.items` と同一処理
- `.all`: SPARQL 1.1 §18.2.4.1 に従い、`query.source` の `GraphPattern` から
  `collectVariables(into:)` で再帰的に全変数を抽出
- `.allFrom`: SPARQL 未使用のため `break`

---

### ~~M12: QueryAST — `DataType.sqlName` の参照~~ [CLOSED: false positive]

**理由**: `DataType.sqlName` は `MatchPattern.swift` 内に extension として定義済み。

---

### ~~M13: VectorIndex — `TupleElement → Float` 変換の重複実装~~ [RESOLVED]

**修正**: `TypeConversion.asFloat(_ value: Any) -> Float?` を追加し、
既存の `asInt64()`, `asDouble()`, `asString()` パターンに統一。
4ファイルの個別型スイッチを `TypeConversion.asFloat()` に置換:
- `VectorConversion.tupleToVector()`
- `Similar.searchVectors()`
- `IndexSearcher` vector decode
- `StatisticsProvider.parseVectorForStats()`

---

## Resolved Issues (High)

### ~~H1: `String(describing:)` フォールバック（4モジュール横断・6箇所）~~ [RESOLVED]

**修正**: 3パターンに分類して対応。
- 到達不能コード (SPARQLQueryExecutor, AggregationQuery): `assertionFailure` + フォールバック維持
- 型制約違反 (GraphEdgeScanner×3, GraphQuery): `throw GraphIndexError.unexpectedElementType`
- 値破損リスク (ScalarIndexMaintainer): `throw TupleEncodingError.unsupportedType`
- 設計変更 (Bitmap): `convertToTupleElement` を `throws` に変更、呼び出し元に `try` 伝搬

**根本原因修正** (深層監査):
`FieldValue(tupleElement:)` が Float, UUID, Date を未サポートだったため、
H1a/H1f の `assertionFailure` に到達する可能性があった。
`FieldValue+TupleElement.swift` に3 case を追加し、根本原因を解消。

---

### ~~H2: 手動型変換（TypeConversion 未使用）（3モジュール・5箇所）~~ [RESOLVED]

**修正**: 全5箇所を `TypeConversion` の統一APIに置き換え。
- `Rank.swift`: 12-case switch → `TypeConversion.asDouble()`
- `RankQuery.swift` parseIndexKey: 2-type if-let → `TypeConversion.double(from:)`
- `RankQuery.swift` extractNumericValue: 4-type if-let → `TypeConversion.asDouble()`
- `TimeWindowLeaderboardIndexMaintainer.swift`: 5-case Score.self switch → `TypeConversion.int64(from:)`
- `RelationshipIndexMaintainer.swift`: FieldValue + TupleElement → `TypeConversion.toTupleElement()`

**リグレッション修正** (深層監査):
H2d で `TypeConversion.int64(from:)` に置き換えたが、`TupleDecoder.decodeInt64()` が
Double→Int64 変換を未サポートだったため Float/Double スコアで throw していた。
2段階で修正:
1. `TupleDecoder.decodeInt64()` に Double/Float からの exact 整数変換を追加（基盤修正）
2. `extractScoreValue` を Double 経由で Int64 に切り捨て変換する方式に変更（ドメイン固有）

---

### ~~H3: ScalarIndex — `@unchecked Sendable` 使用~~ [RESOLVED]

**修正**: `FilterPredicate` enum から `@unchecked` を削除。
全 associated values が `Sendable` 準拠のため不要。

---

### ~~H4: ScalarIndex — 文字列比較フォールバック~~ [RESOLVED]

**修正**: `"\(fieldValue)" == "\(value)"` を `TypeConversion.toFieldValue()` による
`FieldValue` 比較に変更。クロス型対応（`.int64(42) == .double(42.0)` → `true`）。

---

### ~~H5: SpatialIndex — `try?` によるエラー握りつぶし~~ [RESOLVED]

**修正**: `SpatialCellScanner.swift` の3箇所で `try?` → `try` に変更。
全メソッドは既に `async throws`。

---

### ~~H6: SpatialIndex — 冗長な unpack-pack-unpack サイクル~~ [RESOLVED]

**修正**: `Nearby.swift` の `searchSpatial()` で:
- 冗長な `unpack(key)` → `pack()` → `Tuple.unpack(from:)` を除去、`keyTuple` を直接使用
- `try?` → `try` に変更（H5 と同様）
- `seenIds` を `Set<String>` (base64) → `Set<Data>` に変更（効率化）
- `elementsToStableKey()` ヘルパーを削除

---

### ~~H7: 不要ラッパーメソッド（8モジュール横断・13メソッド）~~ [RESOLVED]

**修正**: `TypeConversion` / `TupleEncoder` / `TupleDecoder` を呼ぶだけの private メソッドを
全て削除し、呼出元で直接使用するよう変更。

| 削除メソッド | モジュール | 置換 |
|-------------|-----------|------|
| `extractScoreValue` | LeaderboardIndex | `TypeConversion.int64(from:)` |
| `extractInt64` (9呼出) | LeaderboardIndex | `TypeConversion.int64(from:)` |
| `extractNumericValue` | PercentileIndex | `TypeConversion.double(from:)` |
| `extractNumericValue` | AggregationExecution | `TypeConversion.asDouble()` |
| `extractNumericValue` | AggregationQuery | `TypeConversion.asDouble()` |
| `extractNumericValue` | RankQuery | `TypeConversion.asDouble()` |
| `convertToTupleElement` | Filter | `TupleEncoder.encode()` |
| `convertToTupleElement` | Bitmap | `TupleEncoder.encode()` |
| `extractScore` | RankIndexMaintainer | `TupleDecoder.decode()` |
| `convertScoreToTupleElement` | RankIndexMaintainer | `TupleEncoder.encode()` |
| `NumericValueExtractor.extractInt64()` | NumericAggregationSupport | `TypeConversion.int64(from:)` |
| `NumericValueExtractor.extractDouble()` | NumericAggregationSupport | `TypeConversion.double(from:)` |
| `ComparableValueExtractor.extract()` | NumericAggregationSupport | `TupleDecoder.decode()` |

**再発防止**: CLAUDE.md に「ラッパーメソッド禁止（直接呼出の原則）」を追記。

---

## Resolved Issues (Critical)

### ~~C1: GraphIndex — BFS オリジン未伝搬（object が variable のケース）~~ [RESOLVED]

**修正**: `evaluateTransitivePath` の object-is-variable 分岐で `resultBinding` を構築し、
`subject.variableName` が binding に存在しない場合（depth 2+）に `bindingOrigin` から復元。
bound-object ケース（既に修正済み）と同一のオリジン追跡パターンを適用。
テスト: `PropertyPathAdvancedTests` に2テスト追加 (linear chain, branching)。

---

### ~~C2: VectorIndex — DotProduct メトリック変換~~ [VERIFIED CORRECT]

**検証結果**: swift-hnsw ライブラリの `.innerProduct` は内部で `1.0f - dotProduct` を計算し、
距離（低い=良い）に変換済み。`.dotProduct → .innerProduct` のマッピングは正しい。
符号反転の二重適用は発生しない。

---

### ~~C3: PermutedIndex — executeWithFields() の PrimaryKey 不一致~~ [RESOLVED]

**修正**: `PermutedQuery.executeWithFields()` の item マッチングを
`dynamicMember("id")` + `TupleEncoder.encodeOrNil()` から `DataAccess.extractId(from:using:)` に変更。
`PermutedIndexMaintainer.buildPermutedKey()` と同一のメソッドを使用し、
ストレージパスとクエリパスの一貫性を保証。

---

### ~~C4: VersionIndex — `at()` の比較ロジック~~ [VERIFIED CORRECT]

**検証結果**: `execute()` は newest-first で返す。`at(version)` は最初の `v <= version` を返す。
newest-first 順のため「requested 以下で最新のバージョン」が正しく返される。
Point-in-time セマンティクスとして正確。

---

## Resolved Issues (SPARQL Layer)

SPARQL レイヤー固有の解決済み問題。詳細は `Sources/GraphIndex/SPARQL/ISSUES.md` を参照。

### Phase 1-6 で解決済み (21件)

| ID | 重大度 | 問題 | 修正内容 |
|----|--------|------|---------|
| H1 | High | `inferFieldValue` が曖昧な型推論 | Phase 2: `SPARQLTerm.value(FieldValue)` に変更 |
| H2 | High | FieldValue Equatable がクロス型非対応 | Phase 1: 手動 Equatable/Hashable 実装 |
| H3 | High | BFS bound object で多ホップ不可 | Phase 3: exploration variable 導入 |
| H4 | High | `inverse(sequence)` 正規化不完全 | Phase 4: 正規化ルール追加 |
| H5 | High | OPTIONAL で左バインディング消失 | Phase 5: `anyMerged` フラグ追加 |
| M1 | Medium | 非互換型比較の typeOrder フォールバック | `FieldValue.compare(to:)` に変更 |
| M2 | Medium | Property path 中間値の String 変換破損 | Phase 2: FieldValue ベースに変更 |
| M3 | Medium | COUNT DISTINCT クロス型問題 | Phase 1: Hashable 実装で自動解決 |
| M4 | Medium | SUM の Int64.max 付近 crash | `Int64(exactly:)` に変更 |
| M5 | Medium | filter convenience の `inferFieldValue` 依存 | Phase 2: 直接 `.string()` 使用 |
| M6 | Medium | `normalized()` alternative フラットニング no-op | 右結合に変更 |
| L1 | Low | `.null` の SPARQL セマンティクス違反 | Phase 6: `hasNull` ガード追加 (24テスト) |
| L2 | Low | `GroupValue.stringValue` とのロジック重複 | `FieldValue.displayString` 抽出 |
| L3 | Low | `zeroOrOne` の `isRecursive` 誤り | `false` を返すよう修正 |
| L4 | Low | `maxDepth` のドキュメント不一致 | doc comment 修正 |
| L5 | Low | `firstNumericAggregate` の dead code | String パース fallback 削除 |
| L6 | Low | `parseKeyToBinding` の `String(describing:)` | `FieldValue(tupleElement:)` に変更 |
| T1 | Test | テスト名と内容の不一致 | `testTransitiveClosureLinearChain` に改名 |
| T2 | Test | assertion 不足 | N3, N4 の assertion 追加 |
| T3 | Test | assertion が弱い | exact 値に変更 |
| T4 | Test | alternative フラットニングのテストなし | 正規化テストケース追加 |
| BFS | Known | Unbound subject + bound object のオリジン | Phase 6: frontier にオリジン追跡追加 (4テスト) |
