# SQL / SPARQL 統合設計

## 概要

database-framework は SQL と SPARQL の両方を同一ストレージ基盤（FoundationDB）上で実行する。本文書では、統合アーキテクチャの設計判断とその根拠、既知の意味論ギャップ、および今後の実装方針を記録する。

---

## 1. アーキテクチャ選択: 共通IR + 分離実行（アプローチC）

SQL と SPARQL を統合するアプローチは大きく3つ存在する。

| アプローチ | 概要 | 評価 |
|-----------|------|------|
| **A: RDF-first** | SQL を RDF/SPARQL に変換して実行 | SQL の集合演算（JOIN, GROUP BY）の表現力が失われる |
| **B: SQL-first** | SPARQL を SQL に変換して実行 | Open World Assumption, OPTIONAL の意味論が SQL に写像できない |
| **C: 共通IR + 分離実行** | 共通 AST に変換し、実行時にバックエンドを切り替え | **本プロジェクトの採用アプローチ** |

### 採用理由

1. **FoundationDB は RDBMS でも Triple Store でもない** — Key-Value ストアの上にプラガブルなインデックス（Scalar, Vector, FullText, Spatial, Graph, ...）を構築する設計であり、特定のデータモデルに縛られない
2. **共通 IR は既に存在する** — `Expression.swift` が "Unified expression representation for SQL and SPARQL" として設計されている
3. **実行パスは自然に分離される** — GraphIndex が SPARQL を、ScalarIndex が SQL を実行し、モジュール間に依存関係がない

```
                  ┌──────────────┐
                  │  QueryAST    │    ← 共通IR（Expression, SelectQuery）
                  │  (Parser)    │
                  ├──────┬───────┤
                  │ SQL  │SPARQL │    ← 個別パーサー
                  │Parser│Parser │
                  └──┬───┴───┬───┘
                     │       │
              ┌──────▼──┐ ┌──▼───────┐
              │ Scalar  │ │  Graph   │   ← 分離実行
              │ Index   │ │  Index   │
              └─────────┘ └──────────┘
                     │       │
                  ┌──▼───────▼──┐
                  │ FoundationDB │   ← 共通ストレージ
                  └─────────────┘
```

---

## 2. 共通IR の設計方針

### 2.1 IR は言語中立を維持する

`Expression` enum に dialect タグ（`SourceDialect`）を持たせる案があるが、これは採用しない。

**理由**:
- dialect タグは IR を "2つの tagged union" に変え、共通IR の意味を失わせる
- パーサーが既に意味的な選択を AST ノードに埋め込んでいる（例: `.bound()` は SPARQL 専用, `.like()` は SQL 専用）
- 実行パスが分離されているため、評価器が dialect-specific なセマンティクスを適用できる

**代わりに**: 同名で意味が異なる関数（SPARQL `UCASE` vs SQL `UPPER`）は、評価器レベルで区別する。IR 上は正規化された関数名（`FunctionCall.name`）を使い、評価器がコンテキストに応じた意味論を適用する。

### 2.2 共通化する構造

以下は SQL/SPARQL 間で共通化済みの IR 構造:

| IR ノード | SQL での意味 | SPARQL での意味 |
|----------|-------------|----------------|
| `Expression.equal` | `=` 比較 | `=` 比較（RDF term equality） |
| `Expression.and`/`.or`/`.not` | 論理演算 | 論理演算 |
| `Expression.aggregate` | COUNT, SUM, AVG, MIN, MAX | COUNT, SUM, AVG, MIN, MAX, SAMPLE, GROUP_CONCAT |
| `Expression.function` | 関数呼び出し | Built-in call / IRI function call |
| `Expression.coalesce` | COALESCE | COALESCE |
| `SelectQuery` | SELECT 文 | SELECT クエリ |

### 2.3 言語固有の IR ノード

以下は一方の言語に固有であり、もう一方では使用されない:

| IR ノード | 所属 | 備考 |
|----------|------|------|
| `.column(ColumnRef)` | SQL | テーブル.カラム参照 |
| `.variable(Variable)` | SPARQL | ?var 参照 |
| `.bound(Variable)` | SPARQL | BOUND(?var) |
| `.like(pattern:)` | SQL | LIKE パターン |
| `.regex(pattern:flags:)` | SPARQL | REGEX() — リテラル引数限定 |
| `.triple`, `.isTriple`, `.subject`, `.predicate`, `.object` | SPARQL | RDF-star 操作 |
| `.inSubquery`, `.cast`, `.nullIf` | SQL | SQL 固有操作 |

これらは IR に共存するが、パーサーとエ評価器が正しい組み合わせを保証する。

---

## 3. SQL と SPARQL の意味論ギャップ

共通 IR で構文を統一しても、**評価規則（意味論）**は SQL と SPARQL で一致しない領域がある。これらは IR ではなく評価器レベルで吸収する。

### 3.1 世界仮説

| | SQL | SPARQL |
|--|-----|--------|
| **世界仮説** | Closed World Assumption | Open World Assumption |
| **不在の意味** | 値は NULL（既知だが不明） | 変数は unbound（情報が存在しない） |
| **IR 表現** | `.isNull(expr)` | `.bound(variable)` |

**設計方針**: IR レベルでは `.isNull` と `.bound` を別ノードとして維持する。評価器がそれぞれの世界仮説に基づいて解釈する。

### 3.2 エラー伝播規則

SPARQL は演算子ごとに固有のエラー伝播規則を持つ（W3C SPARQL 1.1 §17.2）。

| コンテキスト | SQL の動作 | SPARQL の動作 | W3C 参照 |
|-------------|-----------|--------------|---------|
| **FILTER 内の型エラー** | エラー伝播（クエリ失敗） | 偽扱い（行が除外される） | §17.2 |
| **BIND 内のエラー** | エラー伝播 | 変数が unbound になる | §18.5 |
| **比較時の型不一致** | 型変換ルール適用 or エラー | type error → 偽 | §17.3 |
| **NULL/unbound の比較** | 3値論理（TRUE/FALSE/UNKNOWN） | 2値に近い（error → false） | §17.2 |

**設計方針**: 評価器に `EvaluationSemantics` プロトコルを導入し、エラー伝播規則を実行コンテキストに応じて切り替える。IR は変更しない。

### 3.3 EBV（Effective Boolean Value）

SPARQL は独自のブーリアン変換規則を持つ（W3C SPARQL 1.1 §17.2.2）:

- 数値型: 0 / NaN → false, それ以外 → true
- 文字列型: 空文字列 → false, それ以外 → true
- ブーリアン型: そのまま
- それ以外: type error

SQL の暗黙的ブーリアン変換とは異なる規則であり、SPARQL 評価器で個別に実装する必要がある。

### 3.4 集合 vs バッグ

| | SQL | SPARQL |
|--|-----|--------|
| **デフォルト** | bag（重複許可） | sequence（重複許可） |
| **重複排除** | SELECT DISTINCT | SELECT DISTINCT |
| **UNION** | UNION ALL = bag, UNION = set | UNION = set (重複排除) |

**設計方針**: `SelectQuery` に `.distinct` フラグがあり、SPARQL パーサーが UNION 時に適切に設定する。

### 3.5 照合順序と文字正規化

SPARQL の文字列関数は Unicode 正規化の扱いが SQL と異なりうる。例えば:
- SPARQL `UCASE`: Unicode Case Folding（W3C XPath/XQuery Functions）
- SQL `UPPER`: データベース固有の照合順序

**設計方針**: `FunctionCall.name` は正規化された名前を保持し、評価器が実行時の照合順序を決定する。IR に照合順序情報は埋め込まない。

---

## 4. FunctionCall の設計

### 4.1 現在の構造

```swift
public struct FunctionCall: Sendable, Equatable, Hashable {
    public let name: String           // 正規化された関数名
    public let arguments: [Expression]
    public let distinct: Bool         // DISTINCT 修飾子
}
```

### 4.2 dialect タグを入れない理由

`FunctionCall` に `SourceDialect` を追加する案:

```swift
// 不採用
public struct FunctionCall {
    public let name: String
    public let arguments: [Expression]
    public let distinct: Bool
    public let dialect: SourceDialect  // ← 不採用
}
```

**不採用の理由**:

1. **IR の中立性が壊れる** — 共通 IR は「どの言語から来たか」を知らないのが原則
2. **評価器が既にコンテキストを知っている** — GraphIndex の SPARQL evaluator は SPARQL セマンティクスを適用し、ScalarIndex は SQL セマンティクスを適用する
3. **SPARQL 専用関数は既に区別されている** — `.bound()`, `.regex()` など専用 AST ノードが存在する
4. **将来のクロスクエリ最適化を妨げる** — dialect タグがあると、異なる dialect の式を組み合わせる際に不整合が生じる

### 4.3 同名異義語の扱い

SPARQL `UCASE` と SQL `UPPER` のように、名前が異なるが機能が類似する関数については:

- パーサーが**各言語のオリジナル名**を `FunctionCall.name` に設定する（`"UCASE"`, `"UPPER"`）
- 評価器が名前に基づいて適切なセマンティクスを適用する
- 必要に応じて `FunctionCatalog`（後述）が名前の正規化と意味論のマッピングを提供する

---

## 5. 今後の実装ロードマップ

### Phase 1: パーサー準拠 (**完了**)

W3C SPARQL 1.1 §19.8 EBNF に準拠した built-in function パーシング。

- [x] `BuiltInCall` [121] 全関数のパーサー実装
- [x] `Aggregate` [127] パーサー実装
- [x] `iriOrFunction` [128] パーサー実装
- [x] `Constraint` [69] の `FunctionCall` [70] 対応
- [x] テスト 368 件通過

### Phase 2: FunctionCatalog

SPARQL built-in function のメタデータカタログ。

```
FunctionCatalog
├── name: String (正規化名)
├── argumentTypes: [TypeConstraint]
├── returnType: TypeConstraint
├── errorConditions: [ErrorCondition]
└── evaluator: (args) -> Result
```

- [ ] 全 SPARQL 1.1 built-in function の引数型・戻り型定義
- [ ] SQL 組み込み関数との対応表
- [ ] 型チェック（静的解析）

### Phase 3: SPARQL 式評価器

GraphIndex 内の SPARQL expression evaluator にエラー伝播・EBV を実装。

- [ ] EBV（Effective Boolean Value）W3C §17.2.2
- [ ] FILTER 内エラー → false 変換（§17.2）
- [ ] BIND 内エラー → unbound 変換（§18.5）
- [ ] RDF term 型比較規則（§17.3）
- [ ] XSD 型プロモーション

### Phase 4: W3C テストスイート

W3C SPARQL 1.1 Test Suite（式・関数・フィルタ部分）を回す。

- [ ] 文字列関数テスト
- [ ] 数値関数テスト
- [ ] 日時関数テスト
- [ ] RDF term 関数テスト
- [ ] 集約関数テスト

### Phase 5: クロスインデックスクエリプランナー

異なるインデックス間の JOIN 最適化。

- [ ] セマンティック境界変換（NULL ↔ unbound, bag ↔ set）
- [ ] クロスインデックス結合戦略
- [ ] コスト推定

---

## 6. クロスインデックス JOIN の将来課題

現在は GraphIndex と ScalarIndex の実行パスが分離されているため、意味論の衝突は発生しない。しかし、以下のケースで再燃する:

1. **SPARQL → SQL**: SPARQL の変数束縛結果を ScalarIndex にフィードする
2. **SQL → SPARQL**: SQL の行集合を GraphIndex のトラバーサルの起点にする

これらを実現するには、**クエリプランナーが境界変換を挿入する**必要がある:

```
SPARQL 結果 (unbound = 情報なし)
    │
    ▼  [境界変換: unbound → NULL]
    │
SQL Index (NULL = 既知だが不明)
```

```
SQL 結果 (NULL, bag)
    │
    ▼  [境界変換: NULL → 除外, bag → set]
    │
Graph Index (Open World, set)
```

この変換ロジックはクエリプランナー層に置き、IR と評価器は変更しない。

---

## 7. 参考仕様

| 仕様 | 用途 |
|------|------|
| [W3C SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/) | SPARQL 構文・意味論 |
| [W3C SPARQL 1.1 §17](https://www.w3.org/TR/sparql11-query/#expressions) | 式の評価規則、EBV |
| [W3C SPARQL 1.1 §19.8](https://www.w3.org/TR/sparql11-query/#sparqlGrammar) | EBNF 文法 |
| [ISO/IEC 9075:2023](https://www.iso.org/standard/76583.html) | SQL 標準 |
| [XPath/XQuery Functions 3.1](https://www.w3.org/TR/xpath-functions-31/) | SPARQL が参照する関数仕様 |
