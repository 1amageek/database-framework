# Ontology System Review TODO

2026-02-22 実施のレビューで発見された全問題のトラッキングドキュメント。
付け焼き刃ではなく本質的な修正を行うこと。

---

## A. OntologyStore 内部実装

### A-1. `listOntologies()` の O(n^2) 重複排除 [HIGH]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologyStore.swift:109`

```swift
if !ontologies.contains(ontologyIRI) {  // O(n) per iteration
    ontologies.append(ontologyIRI)
}
```

**問題**: `Array.contains()` は O(n)。全体で O(n^2)。
**修正**: `Set<String>` を使用し、最後に `Array` に変換。

---

### A-2. axiom 上書きバグ（再ロード時に古い axiom が残る） [CRITICAL]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologyStore.swift:661`

`loadOntology()` は `saveAxioms()` を呼ぶが、axiom サブスペースを事前にクリアしない。
`FDBContext+Ontology.swift:103` で `deleteOntology()` → `loadOntology()` の順で呼ばれるため
通常フローでは問題ないが、`loadOntology()` 単体で呼ばれた場合や同一トランザクション内で
2回呼ばれた場合に古い axiom（インデックスが大きいもの）が残る。

**修正**: `loadOntology()` 冒頭で axiom サブスペースを `clearRange()` する。

---

### A-3. `computeTransitiveClosure()` の暗黙的自己ループ除去 [MEDIUM]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologyStore.swift:770`

```swift
visited.remove(start)  // 最後に自己参照を除去
```

循環階層（A equiv B equiv C）で `start` が BFS 中に `visited` に追加され、
最後の `remove(start)` で偶然正しくなる。ロジックが脆弱。

**修正**: `start` を初期 `visited` に含め、`reachable` セットを別に管理する。

---

### A-4. プロパティ階層マテリアライズのデータ源不整合 [MEDIUM]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologyStore.swift:719-730`

プロパティ階層は axiom からのみ構築されるが、`StoredPropertyDefinition` は
`OWLObjectProperty.superProperties` からも独立に `directSuperProperties` を持つ。
2つの truth source が存在し、乖離する可能性がある。

**修正**: axiom を唯一の truth source とし、`StoredPropertyDefinition.directSuperProperties` は
axiom から導出するか、明確にドキュメント化する。

---

### A-5. データプロパティ階層のマテリアライズ漏れ [MEDIUM]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologyStore.swift:719-730`

`materializePropertyHierarchy()` は `.subObjectPropertyOf` と `.equivalentObjectProperties` のみ処理。
`.subDataPropertyOf` / `.equivalentDataProperties` が未処理。

**修正**: データプロパティ axiom も同様にマテリアライズする。

---

### A-6. `try?` によるエラー握りつぶし（8箇所） [MEDIUM]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologyStore.swift`

| 行 | メソッド |
|----|---------|
| 107 | `listOntologies()` |
| 335 | `getSuperClasses()` |
| 360 | `getSubClasses()` |
| 401 | `getSuperProperties()` |
| 426 | `getSubProperties()` |
| 502 | `getTransitiveProperties()` |
| 535 | `addPropertyChain()` |
| 585 | `getAllPropertyChains()` |

CLAUDE.md の `try?` 禁止ルールに違反。

**修正**: `do-catch` で明示的にエラーハンドリングするか、`try` で伝播する。

---

### A-7. sameAs Union-Find が未実装 [LOW]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologySubspace.swift:60, 73-82, 179-263`

サブスペース定義（parent, rank, members）は完備だが、`OntologyStore` に
Union-Find 操作メソッドが一切ない。

**修正**: 実装するか、明確に deferred とドキュメント化する。

---

## B. OWL Reasoner

### B-1. ノミナルマージの禁止漏れ [CRITICAL]

**ファイル**:
- `Sources/GraphIndex/Reasoning/ExpansionRules.swift:492-501`
- `Sources/GraphIndex/Reasoning/CompletionGraph.swift:399-500`

`applyMaxCardinalityRule()` でノミナルノードをマージ可能にしている。
ノミナルは名前付き個体であり、Unique Name Assumption に基づきマージ不可。

また `CompletionGraph.mergeNodes()` にもノミナルチェックがない。

**修正**: ノミナルのマージ要求時は clash を返す。`mergeNodes()` 冒頭に guard を追加。

**参照**: Horrocks & Sattler, 2007 — SHOIN(D) semantics

---

### B-2. `undoAction()` でハッシュシグネチャが復元されない [HIGH]

**ファイル**: `Sources/GraphIndex/Reasoning/CompletionGraph.swift:639-656`

`.addedConcept` の undo で `concepts.remove()` と `complementClashes.remove()` は行うが、
`conceptSignature` のビットは復元しない。Bloom filter は要素削除を本質的にサポートしないため、
バックトラック後にブロッキング判定が不正確になる。

**修正**: バックトラック時にシグネチャを再計算するか、Bloom filter に代わるデータ構造を使用。

---

### ~~B-3. `minExclusive` / `maxExclusive` のファセット検証ロジック~~ [NOT A BUG]

**ステータス**: ✅ 検証済み — ロジックは正しい

`comparison = value.compare(facet)` の結果:
- `.orderedDescending` = value > facet → `!= .orderedDescending` は value ≤ facet を reject → minExclusive 正しい
- `.orderedAscending` = value < facet → `!= .orderedAscending` は value ≥ facet を reject → maxExclusive 正しい

---

### ~~B-4. `classify()` での等価クラス重複挿入~~ [MINOR — 冗長だが正しい]

**ステータス**: ⚠️ 冗長だが正しい — ClassHierarchy は Set ベースのため冪等

`addSubsumption()` + `addEquivalence()` の両方が呼ばれるが、
ClassHierarchy 内部は `Set<String>` で管理されているため重複は自動排除される。
パフォーマンス上の懸念は negligible。

---

### ~~B-5. `types()` が disjoint クラスをフィルタしない~~ [NOT A BUG]

**ステータス**: ✅ 正しい OWL 標準動作

`types()` は個体の所属クラスを返す API。disjoint 制約を持つクラスに
同時に所属する個体は **不整合** であり、それは `isConsistent()` が検出する責務。
`types()` が disjoint フィルタリングする設計は OWL 標準から逸脱する。

---

### B-6. `maxInferenceDepth` が OWL2RLMaterializer で未強制 [HIGH]

**ファイル**: `Sources/GraphIndex/OWLReasoning/OWL2RLMaterializer.swift:50`

Configuration に `maxInferenceDepth` が定義されているが、
マテリアライゼーション中にこの制限が一度も参照されない。
推移的プロパティチェーンなどで無限推論が発生する可能性。

**修正**: forward chaining ループに depth トラッキングを追加。

---

### ~~B-7. `subsumes()` が unknown を false として返す~~ [NOT A BUG — sound behavior]

**ステータス**: ✅ 健全な動作

タイムアウト時に `false`（subsumption なし）を返すのは **sound** な判断。
subsumption が証明できなかっただけで、否定された訳ではない。
OWL 推論器の標準的な保守的動作。コメントで明文化済み。

---

### ~~B-8. ClassHierarchy の循環等価クラス未統合~~ [MINOR — DFS で処理済み]

**ステータス**: ⚠️ 最適ではないが正しい

`computeClosuresIfNeeded()` は Kahn's topological sort を試行し、
循環がある場合は DFS フォールバックで正しく closure を計算する。
SCC 統合は性能改善になるが、正確性には影響しない。

---

### B-9. `reachableIndividuals()` がプロパティチェーンを処理しない [MEDIUM]

**ファイル**: `Sources/GraphIndex/Reasoning/OWLReasoner.swift:623-689`

inverse, symmetric, sub-properties, transitive closure は処理するが、
`owl:propertyChainAxiom` による到達可能性が欠落。

**修正**: `roleHierarchy.propertyChains()` を参照し、チェーン経由の個体も収集。

---

### B-10. `updateBlocking()` の無条件アンブロック [LOW]

**ファイル**: `Sources/GraphIndex/Reasoning/CompletionGraph.swift:505-529`

毎回全ノードをアンブロックしてから再計算する。
trail に O(steps x nodes) のアンブロックアクションが蓄積。

**修正**: 差分のみ trail に記録する。

---

### B-11. RoleHierarchy のプロパティチェーン重複排除が O(n^2) [LOW]

**ファイル**: `Sources/GraphIndex/Reasoning/RoleHierarchy.swift:448-454`

ユーザコード側で重複排除している。RoleHierarchy 内部で `Set` を使用すべき。

---

### B-12. `isValidDate()` の ISO 8601 不完全サポート [LOW]

**ファイル**: `Sources/GraphIndex/Reasoning/OWLDatatypeValidator.swift:484-488`

XSD `xsd:date` はタイムゾーンサフィックス（`2024-01-15Z`, `2024-01-15+02:00`）を許容するが、
`ISO8601DateFormatter` の `.withFullDate` のみでは不十分な場合がある。

---

## C. OWL 型システム（database-kit）

### C-1. OntologyIndex に `dataPropertySignature` キャッシュがない [LOW]

**ファイル**: `database-kit/Sources/Graph/Schema/OntologyIndex.swift:80-87`

`classSignature`, `objectPropertySignature`, `individualSignature` は存在するが
`dataPropertySignature` がない。

**修正**: `dataPropertySignature: Set<String>` を追加。

---

### C-2. ネガティブアサーションがインデックスされていない [LOW]

**ファイル**: `database-kit/Sources/Graph/Schema/OntologyIndex.swift:24-104`

`negativeObjectPropertyAssertion` / `negativeDataPropertyAssertion` に対する
高速ルックアップインデックスがない。線形スキャンが必要。

**修正**: サブジェクト別のインデックスを追加。

---

## D. マクロシステム（database-kit）

### D-1. ブリッジプロトコルの命名非一貫性 [LOW]

**ファイル**: `database-kit/Sources/Core/OntologyBridge.swift:6-19`

- `_OntologyClassIRIProvider`（旧名ベース）
- `_ObjectPropertyIRIProvider`（OWL 接頭辞なし）

public プロトコルは `OWLClassEntity`, `OWLObjectPropertyEntity` に統一済み。

**修正**: `_OWLClassIRIProvider`, `_OWLObjectPropertyIRIProvider` にリネーム。
影響箇所: OWLClassEntity.swift, OWLObjectPropertyEntity.swift, Schema.swift

---

## E. バリデーション・テストカバレッジ

### E-1. `persistableType == nil` 時にデータプロパティ検証がスキップされる [HIGH]

**ファイル**: `Sources/GraphIndex/OntologyStorage/FDBContext+Ontology.swift:402-409`

```swift
if let type = entity.persistableType {
    // ... validate data properties ...
} // else: silently skip
```

JSON デシリアライズ後の Schema.Entity では `persistableType` が nil になる。
この場合、データプロパティ IRI の検証が完全にスキップされる。

**本質的修正**: Schema.Entity に `dataPropertyIRIs: [String]?` を追加し、
wire format でも検証可能にする。ブリッジプロトコル `_DataPropertyIRIsProvider` を
Core に追加し、`persistableType` 経由のランタイムキャストに依存しない設計にする。

---

### E-2. エラーメッセージが常に `@OWLObjectProperty` と表示 [MEDIUM]

**ファイル**: `Sources/GraphIndex/OntologyStorage/OntologyIRIValidator.swift:149`

```swift
"Ensure the property is defined in the OntologyStore before referencing it with @OWLObjectProperty."
```

`propertyNotFound` エラーが DataProperty 由来でもこのメッセージが表示される。

**修正**: エラーにコンテキスト情報（どのマクロ由来か）を追加するか、
汎用メッセージに変更する。

---

### E-3. 非 OntologyValidationError 例外でエラー集約が中断 [MEDIUM]

**ファイル**: `Sources/GraphIndex/OntologyStorage/FDBContext+Ontology.swift:379-425`

`validator.validateClass()` が `OntologyValidationError` 以外の例外（例: FDB 接続エラー）を
投げた場合、`catch let error as OntologyValidationError` で捕捉されず、
トランザクション全体が中断される。残りの検証が実行されない。

**修正**: `catch` ブロックを拡張し、非バリデーションエラーも適切に処理する。

---

### E-4. テストカバレッジ不足 [MEDIUM]

**ファイル**: `Tests/GraphIndexTests/OntologyIRIValidationTests.swift`

以下のシナリオが未テスト:

| シナリオ | 重要度 |
|---------|--------|
| `persistableType == nil` の Schema.Entity | 高 |
| 複数エンティティが同一 IRI を参照 | 中 |
| 空オントロジー（クラス・プロパティ0件）に対する検証 | 中 |
| @OWLObjectProperty エンティティ内の @OWLDataProperty 検証 | 中 |
| 特殊文字・Unicode を含む IRI | 低 |

---

## F. SPARQL-オントロジー統合

### F-1. SPARQL がプロパティ階層を参照しない [CRITICAL]

**ファイル**: `Sources/GraphIndex/SPARQL/SPARQLQueryExecutor.swift:1537-1546`

`.iri(predicate)` パス評価で、述語を文字列リテラルとして直接トリプル検索する。
OntologyStore のプロパティ階層（subPropertyOf）を一切参照しない。

**影響**: `ex:knows` のクエリで、そのサブプロパティ `ex:isFriendOf` 経由の結果が欠落。

---

### F-2. 逆プロパティの自動展開なし [CRITICAL]

**ファイル**: `Sources/GraphIndex/SPARQL/SPARQLQueryExecutor.swift:1548-1602`

inverse path は SPARQL 構文変換（subject/object のスワップ）のみ。
`owl:inverseOf` 宣言をオントロジーから参照しない。

---

### F-3. 推移的プロパティの最適化なし [HIGH]

**ファイル**: `Sources/GraphIndex/SPARQL/SPARQLQueryExecutor.swift:1604-1646`

sequence path はナイーブにチェーンする。OWL の `TransitiveProperty` 宣言や
`propertyChainAxiom` を活用した最適化がない。

---

### F-4. カーディナリティ推定がハードコード定数 [HIGH]

**ファイル**: `Sources/GraphIndex/SPARQL/SPARQLQueryOptimizer.swift:517-536`

```swift
let baseCardinality: Double = 10000
```

全ての推定がハードコード定数。オントロジー統計（述語別トリプル数、
functional property のヒント、クラス階層深度）を一切使用しない。

`QueryStatistics` パラメータ（70-78行目）は存在するが、ほぼ未使用。

---

### F-5. `ReasoningGraphQueryBuilder.execute()` が未完成 [CRITICAL]

**ファイル**: `Sources/GraphIndex/Reasoning/ReasoningGraphQueryBuilder.swift:160-173`

```swift
return results  // ← BASE RESULTS UNCHANGED
```

`includeInferred = true` でも推論が適用されない。
設定フラグ（`expandTransitive`, `includeInverse`, `includeSubProperties` 47-60行目）も未使用。

---

### F-6. GraphQuery エントリポイントにオントロジーパラメータがない [MEDIUM]

**ファイル**: `Sources/GraphIndex/GraphQuery.swift:1-60`

`GraphEntryPoint` / `GraphQueryBuilder` がオントロジーコンテキストを受け取る手段がない。

---

### F-7. PropertyPath の `allIRIs` がリテラル抽出のみ [MEDIUM]

**ファイル**: `Sources/GraphIndex/SPARQL/PropertyPath.swift:146-162`

パス中の IRI をリテラルに抽出するだけで、オントロジー知識に基づく展開がない。

---

### F-8. SHACL バリデーションがオントロジー非対応 [MEDIUM]

SHACL target resolution は SPARQL を使用するが（唯一の双方向統合）、
制約評価自体がオントロジーの推論型を使用しない。

---

## 優先度マトリクス（訂正済み 2026-02-22）

**訂正**: B-3, B-5, B-7 は NOT A BUG。B-4, B-8 は MINOR（正確性に影響なし）。

```
        Impact
        HIGH ──────────────────────────────
        │ B-1(CRITICAL) A-2(CRITICAL)    │
        │ B-2(HIGH)  B-6(HIGH)           │
        │ F-1(CRITICAL) F-2(CRITICAL)    │
        │ F-5(CRITICAL) E-1(HIGH)        │
        │ F-3(HIGH) F-4(HIGH)            │
        ├──────────────────────────────────
        │ A-4  A-5  B-9  E-2  E-3  E-4  │
  MED   │ F-6  F-7  F-8                  │
        ├──────────────────────────────────
        │ A-1  A-3  A-6  A-7  B-10      │
  LOW   │ B-11 B-12 C-1  C-2  D-1       │
        └──────────────────────────────────
              LOW         MED        HIGH
                     Effort
```

**除外**: ~~B-3~~, ~~B-4~~, ~~B-5~~, ~~B-7~~, ~~B-8~~ — NOT A BUG または MINOR

## 推奨対応順序

### Phase 1: Reasoner の正確性（B-1, B-2, B-6, B-9） ✅ DONE
推論の健全性に関わるため最優先。不正な推論結果は下流の全てに波及する。

### Phase 2: OntologyStore のデータ整合性（A 系列） ✅ DONE
永続化層のバグは蓄積するため早期修正が必要。
- A-1: listOntologies() O(n²) → Set<String> に修正
- A-2: loadOntology() 冪等化（冒頭で deleteOntology）
- A-3: computeTransitiveClosure() BFS 堅牢化（start 初期 visited）
- A-4: プロパティ階層 truth source 一元化（axioms + property struct）
- A-5: データプロパティ階層マテリアライズ追加
- A-6: try? 8箇所を try に修正
- A-7: sameAs Union-Find — 未実装のまま（deferred）

### Phase 3: バリデーション完全性（E 系列） ✅ DONE
Schema 検証の信頼性確保。
- E-1: Schema.Entity に `dataPropertyIRIs: [String]?` 追加。ブリッジプロトコル `_DataPropertyIRIsProvider`。
- E-2: エラーメッセージをコンテキスト対応（@OWLDataProperty / @OWLObjectProperty 判別）
- E-3: 非 OntologyValidationError 例外は即時伝播確認。データプロパティ IRI の2ソース構造化。

### Phase 4: SPARQL-Ontology 統合（F 系列） ✅ DONE
アーキテクチャ変更を伴う大規模対応。
- F-1: `.iri(predicate)` が OntologyContext 経由でサブプロパティ展開
- F-2: `.inverse(.iri(p))` が `owl:inverseOf` を参照し、逆プロパティも UNION
- F-3: OntologyContext が推移的プロパティの検出・BFS 最適化を提供
- F-4: SPARQLQueryOptimizer にオントロジーコンテキスト追加。functional property ヒント。統計ベース baseCardinality。
- F-5: ReasoningGraphQueryBuilder.execute() 完全実装（3フェーズ: sub-property, inverse, transitive BFS）
- F-6: GraphEntryPoint / GraphQueryBuilder に `.withOntology()` API 追加
- F-7: ExecutionPropertyPath に `expandedIRIs(using:)` メソッド追加
- F-8: SHACL validate() が OWL entailment 時に OntologyContext を SPARQLQueryExecutor に伝播

### Phase 5: 型システム・マクロ・低優先度（C, D, 残りの B 系列 LOW） ✅ DONE
機能的影響が小さいため最後。
- B-10: CompletionGraph.updateBlocking() 差分 trail 記録
- B-11: RoleHierarchy.isSimple() デッドループコード削除
- B-12: isValidDate() regex + ISO8601DateFormatter 組み合わせ（タイムゾーンサフィックス対応）
- C-1: OntologyIndex に `dataPropertySignature: Set<String>` 追加
- C-2: ネガティブアサーションインデックス追加（negativeObjectPropertyAssertionsBySubject/negativeDataPropertyAssertionsBySubject）
- D-1: ブリッジプロトコル `_OWLClassIRIProvider`, `_OWLObjectPropertyIRIProvider` にリネーム（旧名は deprecated typealias）
