# SPARQL Layer: Known Issues

レビューで発見された問題の一覧。重大度順に整理。

---

## Open Issues

(なし — 全件解決済み)

---

## Resolved Issues

### ~~H1: `inferFieldValue` が曖昧な型推論をする~~ [RESOLVED]

**修正**: Phase 2 で `SPARQLTerm.value(String)` → `SPARQLTerm.value(FieldValue)` に変更。
`parseKeyToBinding` が `FieldValue(tupleElement:)` で TupleElement → FieldValue を直接変換するようになり、
String 経由の型消失が排除された。`inferFieldValue` は削除済み。

---

### ~~H2: FieldValue の Equatable がクロス型非対応~~ [RESOLVED]

**修正**: Phase 1 で `database-kit/Sources/Core/FieldValue.swift` に手動 Equatable/Hashable を実装。
`.int64(42) == .double(42.0)` → `true`。Hashable も一貫性を保証。

---

### ~~H3: BFS transitive path で object が bound のとき多ホップ探索が機能しない~~ [RESOLVED]

**修正**: Phase 3 で BFS に exploration variable を導入。
bound object の場合、一時的な unbound variable (`?_bfs_explore_`) を使用して全中間ノードを探索し、
到達後に bound object と照合してフィルタリング。

Phase 6 で frontier にオリジン追跡を追加し、subject が unbound variable のケースも完全修正
（下記 BFS Known Limitation の解決を参照）。

---

### ~~H4: `inverse(sequence(p1, p2))` の正規化が不完全~~ [RESOLVED]

**修正**: Phase 4 で `evaluatePropertyPath` の `.inverse` ハンドラと
`PropertyPath.normalized()` に以下のルールを追加:
- `^(p1/p2)` = `(^p2)/(^p1)` (SPARQL 1.1 Section 18.4)
- `^(p1|p2)` = `(^p1)|(^p2)`
- `^^p` = `p` (double inverse cancels)

---

### ~~H5: OPTIONAL (LEFT JOIN) で左バインディングが消失する~~ [RESOLVED]

**修正**: Phase 5 で `evaluateOptional` に `anyMerged` フラグを追加。
右パターンが非空だが左バインディングと全て非互換の場合、左バインディングを保持する。

---

### ~~L1: `.null` 値の SPARQL セマンティクス違反~~ [RESOLVED]

**修正**: `FilterExpression.evaluate()` に `hasNull` ガードを追加。
SPARQL 三値論理 (Section 17.2) に従い、NULL を含む比較は全て `false` を返す。
8箇所の比較 case (equals, notEquals, lessThan, lessThanOrEqual, greaterThan,
greaterThanOrEqual, variableEquals, variableNotEquals) に `if Self.hasNull(lhs, rhs) { return false }` を挿入。
FieldValue の Equatable はシステム全体で使用されるため変更せず、SPARQL レイヤーのみで対応。
テスト: `NullSemanticsTests` (24テスト)。

---

### ~~BFS Transitive Path: Unbound Subject + Bound Object~~ [RESOLVED]

**修正**: `evaluateTransitivePath` の frontier を `[SPARQLTerm]` から
`[(node: SPARQLTerm, origin: FieldValue?)]` に変更。
各 frontier エントリがオリジンノード（depth 1 で発見した起点）を保持し、
depth 2+ でも起点情報が失われないようにした。

- subject が unbound variable の場合: per-origin サイクル検出
  (`visitedPerOrigin: [FieldValue: Set<FieldValue>]`) を使用。
  同一ノードに異なるオリジンから到達可能。
- subject が bound の場合: グローバル `visitedNodes` を維持（パフォーマンス優先）。

テスト: `PropertyPathAdvancedTests` に4テスト追加
(linear chain, branching graph, bound subject regression x2)。

---

### ~~M1: 非互換型の比較が typeOrder フォールバックで意味のない結果を返す~~ [RESOLVED]

**修正**: `FilterExpression` の ordering 比較 (lessThan, lessThanOrEqual, greaterThan, greaterThanOrEqual)
を直接演算子 (`<`, `>`) から `FieldValue.compare(to:)` に変更。
非互換型の比較は `nil` → `false` を返す（SPARQL 仕様準拠）。

---

### ~~M2: Property path sequence の中間値が String 変換を経由して破損する~~ [RESOLVED]

**修正**: Phase 2 で中間値を `binding.string(var)` → `binding[var]` (FieldValue) に変更。
BFS frontier も `FieldValue` ベースに変更。String 経由の変換を排除。

---

### ~~M3: COUNT DISTINCT でクロス型数値が別値扱い~~ [RESOLVED]

**修正**: Phase 1 で FieldValue の Hashable を手動実装し、
`.int64(42)` と `.double(42.0)` が同一ハッシュを持つように変更。H2 の修正で自動的に解決。

---

### ~~M4: SUM が Int64.max 付近で crash する可能性~~ [RESOLVED]

**修正**: `Int64(sum)` → `Int64(exactly: sum)` に変更。
`Int64(exactly:)` は範囲外・非整数で `nil` を返すため、trap が発生しない。
事前の `sum.rounded()` チェックと範囲リテラル比較も不要になり削除。

---

### ~~M5: filter convenience メソッドが `inferFieldValue` に依存~~ [RESOLVED]

**修正**: Phase 2 で convenience メソッドが `inferFieldValue` ではなく
`.string(value)` を直接使用するように変更。`inferFieldValue` は削除済み。

---

### ~~M6: `normalized()` の alternative フラットニングが実質 no-op~~ [RESOLVED]

**修正**: `dropFirst().reduce(first!)` (左結合) → `dropLast().reversed().reduce(last!)` (右結合) に変更。
`(a|b)|c` → `a|(b|c)` に正しく正規化される。
テストカバレッジも追加済み (PropertyPathAdvancedTests の `testPropertyPathNormalization`)。

---

### ~~L2: `GroupValue.stringValue` と `VariableBinding.string()` のロジック重複~~ [RESOLVED]

**修正**: `FieldValue.displayString` プロパティを抽出し、
`VariableBinding.string()` と `GroupValue.stringValue` の両方から共有利用するよう変更。

---

### ~~L3: `zeroOrOne` が `isRecursive = true`~~ [RESOLVED]

**修正**: `.zeroOrOne` が `isRecursive` で `false` を返すよう修正済み。
`.zeroOrMore`, `.oneOrMore` のみが `true` を返す。

---

### ~~L4: `maxDepth` のドキュメントと実装の不一致~~ [RESOLVED]

**修正**: doc comment を `Default is 100` に修正済み。実装値と一致。

---

### ~~L5: `firstNumericAggregate` に String パース fallback (dead code)~~ [RESOLVED]

**修正**: String パース fallback を削除。`value.int64Value` による直接変換のみ使用。

---

### ~~L6: `parseKeyToBinding` で非 String TupleElement が `String(describing:)` で変換される~~ [RESOLVED]

**修正**: Phase 2 で `FieldValue(tupleElement:)` を使用した直接変換に変更。
`String(describing:)` フォールバックは残存するが、正常なパスでは到達しない。

---

### ~~T1: `testRangeQuantifier` が range quantifier をテストしていない~~ [RESOLVED]

**修正**: テスト名を `testTransitiveClosureLinearChain` に変更し、
oneOrMore のリニアチェーン探索を正確にテストするよう修正。
到達ノード数の exact assertion (`count == 5`) を追加。

---

### ~~T2: `testComplexQuantifierSequenceOneOrMore` の assertion が不十分~~ [RESOLVED]

**修正**: sequence + oneOrMore の結果 (N3, N4) に対する assertion を追加。
zero-hop (N1) だけでなく、core functionality の検証を追加。

---

### ~~T3: `testPropertyPathComplexity` の assertion が弱い~~ [RESOLVED]

**修正**: 全 PropertyPath ケース (iri, negatedPropertySet, inverse, sequence,
alternative, oneOrMore, zeroOrMore, zeroOrOne) に対する exact assertion に変更。
複合パスも `== 10200` の exact 値で検証。

---

### ~~T4: alternative フラットニングのテストカバレッジがゼロ~~ [RESOLVED]

**修正**: `testPropertyPathNormalization` に以下のテストケースを追加:
- 左結合 → 右結合の正規化 (`(a|b)|c` → `a|(b|c)`)
- ネストされた alternative のフラットニング (`((a|b)|(c|d))` → `a|(b|(c|d))`)
- 既に右結合のパスが不変であることの検証
- inverse over sequence / alternative の正規化
