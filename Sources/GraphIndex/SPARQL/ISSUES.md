# SPARQL Layer: Known Issues

レビューで発見された問題の一覧。重大度順に整理。

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

**既知の制限**: subject が unbound variable で object が bound のとき、
depth > 1 の結果で subject 変数が中間ノードにバインドされる場合がある（起点ノードではなく）。
完全な修正には frontier にオリジン情報を伝搬する設計変更が必要。
Phase 3 以前はそもそも多ホップ探索が不可能だったため、退行ではない。

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

### ~~M5: filter convenience メソッドが `inferFieldValue` に依存~~ [RESOLVED]

**修正**: Phase 2 で convenience メソッドが `inferFieldValue` ではなく
`.string(value)` を直接使用するように変更。`inferFieldValue` は削除済み。

---

### ~~L6: `parseKeyToBinding` で非 String TupleElement が `String(describing:)` で変換される~~ [RESOLVED]

**修正**: Phase 2 で `FieldValue(tupleElement:)` を使用した直接変換に変更。
`String(describing:)` フォールバックは残存するが、正常なパスでは到達しない。

---

## Open Issues

### Medium Severity

### ~~M4: SUM が Int64.max 付近で crash する可能性~~ [RESOLVED]

**修正**: `Int64(sum)` → `Int64(exactly: sum)` に変更。
`Int64(exactly:)` は範囲外・非整数で `nil` を返すため、trap が発生しない。
事前の `sum.rounded()` チェックと範囲リテラル比較も不要になり削除。

---

### M6: `normalized()` の alternative フラットニングが実質 no-op

**ファイル**: `PropertyPath.swift:216-219`

```swift
// コメント: "Rebuild as right-associative chain"
return alternatives.dropFirst().reduce(alternatives.first!) { acc, next in
    PropertyPath.alternative(acc, next)
}
```

**問題**: `reduce` は左結合で構築する。左結合入力 `(a|b)|c` をフラットニングして `[a, b, c]` にした後、同じ左結合 `(a|b)|c` を再構築する。正規化が何も変更しない。

テストカバレッジもゼロ (PropertyPathAdvancedTests.swift:706-719)。

---

### Low Severity

### L1: `.null` 値の SPARQL セマンティクス違反

**ファイル**: `VariableBinding.swift:193-206, 215-224`

**問題**: FieldValue の Equatable で `.null == .null` → `true`。SPARQL 三値論理では NULL 比較は error (false でも true でもない)。

現在 `.null` が binding に入ることはないが、API 上は `binding("?x", to: .null)` が可能。

関連: `GroupValue(from: .null)` が `.bound(.null)` になり `.unbound` と区別される (L305-311)。

---

### L2: `GroupValue.stringValue` と `VariableBinding.string()` のロジック重複

**ファイル**: `VariableBinding.swift:326-338` vs `VariableBinding.swift:84-94`

**問題**: 同じ FieldValue → String 変換ロジックが2箇所に存在。一方を変更しても他方に反映されない。

---

### L3: `zeroOrOne` が `isRecursive = true`

**ファイル**: `PropertyPath.swift:121-123`

**問題**: `zeroOrOne` (path?) は 0 回または 1 回の有界評価。反復/再帰は不要だが、`isRecursive` が `true` を返す。クエリプランナーが不要な再帰評価パスを選択する可能性。

---

### L4: `maxDepth` のドキュメントと実装の不一致

**ファイル**: `PropertyPath.swift:332`

**問題**: doc comment が `Default is 10` と記載しているが実際のデフォルト値は `100` (L347)。

---

### L5: `firstNumericAggregate` に String パース fallback (dead code)

**ファイル**: `SPARQLGroupedQueryBuilder.swift:367-372`

```swift
if let s = value.stringValue { return Int(s) }  // ← dead code
```

**問題**: FieldValue 移行が完了しているため、aggregate 結果は常に型付き (`.int64`, `.double`)。String パース fallback は到達しない。バグのマスク源。

---

## Test Issues

### T1: `testRangeQuantifier` が range quantifier をテストしていない

**ファイル**: `PropertyPathAdvancedTests.swift:242-280`

テスト名は `{2,4}` を主張するが、実際は `oneOrMore` (`+`) をテスト。

---

### T2: `testComplexQuantifierSequenceOneOrMore` の assertion が不十分

**ファイル**: `PropertyPathAdvancedTests.swift:203-240`

`(a/b+)*` テストが zero-hop (自身の返却) のみ assert。core functionality (sequence + oneOrMore の組合せ) は検証されていない。

---

### T3: `testPropertyPathComplexity` の assertion が弱い

**ファイル**: `PropertyPathAdvancedTests.swift:683-704`

`>= 2`, `> 10` 等の弱い不等式で、実装が壊れても通過する。

---

### T4: alternative フラットニングのテストカバレッジがゼロ

**ファイル**: `PropertyPathAdvancedTests.swift:706-719`

`normalized()` の最も複雑なロジック (alternative flattening) がテストされていない。

---

## Known Limitations

### BFS Transitive Path: Unbound Subject + Bound Object

**条件**: subject が unbound variable、object が bound value、depth > 1

**例**: エッジ `A→B→C` で `?person (knows)+ C` を評価する場合:
1. ホップ1: frontier = `[A, B, C, ...]` (全ノード)。`A→B` を発見 → `?person=A`
2. ホップ2: frontier = `[B]`。`B→C` を発見 → `?person=B` (中間ノード)
3. 結果: `?person=B` が返される（`?person=A` が正しい）

**原因**: BFS が各ホップで subject 変数を再バインドするため、
最後のホップの subject が結果に反映される。正確にはオリジンノードからの
フルパスを追跡する必要がある。

**影響**: Phase 3 以前はそもそも multi-hop + bound object が機能しなかったため退行ではない。
完全修正には frontier にオリジンバインディングを伝搬する大規模な再設計が必要。
