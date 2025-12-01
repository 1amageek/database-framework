# Index Test Design

各インデックスタイプに必要なテストの設計と実装状況。

## テスト構造

```
Tests/
├── DatabaseEngineTests/      # コアエンジンのテスト
├── ScalarIndexTests/         # ScalarIndex
├── VectorIndexTests/         # VectorIndex
├── GraphIndexTests/          # GraphIndex
├── AggregationIndexTests/    # Count, Sum, Min, Max
├── VersionIndexTests/        # VersionIndex
├── SpatialIndexTests/        # SpatialIndex ✅
├── RankIndexTests/           # RankIndex ✅
├── FullTextIndexTests/       # FullTextIndex ✅
├── PermutedIndexTests/       # PermutedIndex ✅
└── Shared/                   # TestSupport
```

## テストカテゴリ

### 1. Metadata Tests (FDB不要)
- `identifier` の正確性
- `subspaceStructure` の正確性
- `validateTypes()` の検証
- `Codable` 準拠
- `Hashable` 準拠

### 2. Behavior Tests (FDB必要)
- **Insert**: 新規アイテム挿入時のインデックス更新
- **Update**: アイテム更新時のインデックス更新（値変更あり/なし）
- **Delete**: アイテム削除時のインデックス削除
- **Scan**: バッチインデックス構築

### 3. Query Tests (FDB必要)
- 範囲クエリ
- 等価クエリ
- 集計クエリ（該当するインデックスのみ）

### 4. Edge Cases
- 空の値
- NULL値
- 重複値
- 境界値

---

## インデックス別テスト要件

### ScalarIndex ✅
**ファイル**: `ScalarIndexTests/`

| テスト | 説明 | 状態 |
|--------|------|------|
| identifier | 識別子が正しい | ✅ |
| subspaceStructure | サブスペース構造が正しい | ✅ |
| validateTypes | 型検証が正しい | ✅ |
| Codable | エンコード/デコード | ✅ |
| Hashable | ハッシュ可能 | ✅ |
| Insert creates index entry | 挿入時にインデックスエントリが作成される | ✅ |
| Update with same value | 同じ値での更新はインデックス変更なし | ✅ |
| Update with different value | 異なる値での更新は古いエントリ削除+新規作成 | ✅ |
| Delete removes index entry | 削除時にインデックスエントリが削除される | ✅ |
| Composite index ordering | 複合インデックスの順序が正しい | ✅ |
| ScanItem creates entry | バッチスキャンでエントリ作成 | ✅ |

### CountIndex ✅
**ファイル**: `AggregationIndexTests/`

| テスト | 説明 | 状態 |
|--------|------|------|
| identifier | 識別子が正しい | ✅ |
| subspaceStructure | サブスペース構造が正しい | ✅ |
| validateTypes | 型検証が正しい | ✅ |
| Insert increments count | 挿入でカウントが+1 | ✅ |
| Delete decrements count | 削除でカウントが-1 | ✅ |
| Update same group no change | 同グループ更新でカウント不変 | ✅ |
| Update different group | 異グループ更新で旧-1、新+1 | ✅ |
| Multiple groups tracked | 複数グループが独立してカウント | ✅ |
| getCount returns correct value | クエリが正しいカウントを返す | ✅ |
| getAllCounts returns all groups | 全グループの一覧取得 | ✅ |
| ScanItem increments count | バッチスキャンでカウント増加 | ✅ |
| Composite grouping | 複合グルーピング | ✅ |

### SumIndex ✅
**ファイル**: `AggregationIndexTests/`

| テスト | 説明 | 状態 |
|--------|------|------|
| identifier | 識別子が正しい | ✅ |
| subspaceStructure | サブスペース構造が正しい | ✅ |
| validateTypes | 型検証が正しい | ✅ |
| Insert adds value | 挿入で値が加算される | ✅ |
| Delete subtracts value | 削除で値が減算される | ✅ |
| Update same group | 同グループ更新で差分が反映 | ✅ |
| Update different group | 異グループ更新で旧減算、新加算 | ✅ |
| Negative values supported | 負の値がサポートされる | ✅ |
| Float precision | 浮動小数点の精度 | ✅ |
| getAllSums returns all groups | 全グループの一覧取得 | ✅ |
| ScanItem adds to sum | バッチスキャンで合計に加算 | ✅ |
| Composite grouping | 複合グルーピング | ✅ |

### MinIndex ✅
**ファイル**: `AggregationIndexTests/MinIndexBehaviorTests.swift`

| テスト | 説明 | 状態 |
|--------|------|------|
| identifier | 識別子が正しい | ✅ |
| subspaceStructure | サブスペース構造が正しい | ✅ |
| validateTypes | 型検証が正しい | ✅ |
| Insert adds to sorted set | 挿入でソート済みセットに追加 | ✅ |
| Delete removes from sorted set | 削除でソート済みセットから削除 | ✅ |
| Update changes position | 更新で位置が変わる | ✅ |
| getMin returns minimum | 最小値が正しく取得できる | ✅ |
| Multiple groups independent | 複数グループが独立 | ✅ |
| Min updates on minimum delete | 最小値削除時に次の最小値に更新 | ✅ |
| ScanItem adds to sorted set | バッチスキャンでソート済みセットに追加 | ✅ |

### MaxIndex ✅
**ファイル**: `AggregationIndexTests/MaxIndexBehaviorTests.swift`

| テスト | 説明 | 状態 |
|--------|------|------|
| identifier | 識別子が正しい | ✅ |
| subspaceStructure | サブスペース構造が正しい | ✅ |
| validateTypes | 型検証が正しい | ✅ |
| Insert adds to sorted set | 挿入でソート済みセットに追加 | ✅ |
| Delete removes from sorted set | 削除でソート済みセットから削除 | ✅ |
| Update changes position | 更新で位置が変わる | ✅ |
| getMax returns maximum | 最大値が正しく取得できる | ✅ |
| Multiple groups independent | 複数グループが独立 | ✅ |
| Max updates on maximum delete | 最大値削除時に次の最大値に更新 | ✅ |
| ScanItem adds to sorted set | バッチスキャンでソート済みセットに追加 | ✅ |

### VectorIndex (Flat) ✅
**ファイル**: `VectorIndexTests/`

| テスト | 説明 | 状態 |
|--------|------|------|
| Insert stores vector | ベクトルが正しく保存される | ✅ |
| Delete removes vector | ベクトルが正しく削除される | ✅ |
| Update replaces vector | ベクトルが正しく更新される | ✅ |
| Cosine similarity search | コサイン類似度検索 | ✅ |
| Euclidean distance search | ユークリッド距離検索 | ✅ |
| Dimension mismatch error | 次元不一致でエラー | ✅ |
| Top-K results | 上位K件が正しく返る | ✅ |
| Invalid k throws error | 無効なkでエラー | ✅ |
| Empty index search | 空のインデックス検索 | ✅ |
| ScanItem stores vector | バッチスキャンでベクトル保存 | ✅ |

### GraphIndex (Adjacency) ✅
**ファイル**: `GraphIndexTests/`

| テスト | 説明 | 状態 |
|--------|------|------|
| Insert creates outgoing edge | 挿入で出方向エッジ作成 | ✅ |
| Insert creates incoming edge (bidirectional) | 双方向で入方向エッジも作成 | ✅ |
| Insert unidirectional no incoming | 片方向では入方向エッジなし | ✅ |
| Delete removes outgoing edge | 削除で出方向エッジ削除 | ✅ |
| Delete removes incoming edge (bidirectional) | 双方向で入方向エッジも削除 | ✅ |
| Update edge target | エッジのターゲット更新 | ✅ |
| Different labels create separate edges | ラベルでフィルタリング | ✅ |
| Multiple edges from same source | 同じソースから複数エッジ | ✅ |
| Multiple edges to same target | 同じターゲットへ複数エッジ | ✅ |
| Self-loop edge | 自己ループエッジ | ✅ |
| ScanItem creates entries | バッチスキャンでエッジ作成 | ✅ |

### VersionIndex ✅
**ファイル**: `VersionIndexTests/VersionIndexBehaviorTests.swift`

| テスト | 説明 | 状態 |
|--------|------|------|
| identifier | 識別子が正しい | ✅ |
| subspaceStructure | サブスペース構造が正しい | ✅ |
| validateTypes | 型検証が正しい | ✅ |
| Insert creates version entry | 挿入でバージョンエントリ作成 | ✅ |
| Multiple updates create versions | 複数更新で複数バージョン作成 | ✅ |
| Delete creates deletion marker | 削除で削除マーカー作成 | ✅ |
| getVersionHistory returns all | バージョン履歴取得 | ✅ |
| getVersionHistory with limit | 制限付きバージョン履歴取得 | ✅ |
| getLatestVersion | 最新バージョン取得 | ✅ |
| keepLast strategy limits versions | keepLast保持戦略 | ✅ |
| ScanItem creates version entry | バッチスキャンでバージョン作成 | ✅ |
| Different documents separate histories | ドキュメント毎に独立した履歴 | ✅ |
| Versions ordered by versionstamp | versionstampによる順序付け | ✅ |

### SpatialIndex ✅
**ファイル**: `SpatialIndexTests/SpatialIndexBehaviorTests.swift`

| テスト | 説明 | 状態 |
|--------|------|------|
| Insert stores location | 位置が正しく保存される | ✅ |
| Multiple locations indexed | 複数の位置がインデックスされる | ✅ |
| Delete removes location | 位置が正しく削除される | ✅ |
| Update changes location | 位置が正しく更新される | ✅ |
| Radius search finds nearby | 半径検索で近くの場所を検出 | ✅ |
| ScanItem stores location | バッチスキャンで位置保存 | ✅ |
| Morton encoding works | Mortonエンコーディングが動作 | ✅ |

### RankIndex ✅
**ファイル**: `RankIndexTests/RankIndexBehaviorTests.swift`

| テスト | 説明 | 状態 |
|--------|------|------|
| Insert adds to ranking | 挿入でランキングに追加 | ✅ |
| Multiple inserts create leaderboard | 複数挿入でリーダーボード作成 | ✅ |
| Delete removes from ranking | 削除でランキングから削除 | ✅ |
| Update changes rank | 更新でランクが変わる | ✅ |
| getTopK returns top items | 上位K件を返す | ✅ |
| getRank returns correct position | 正しいランク位置を返す | ✅ |
| Ties handled correctly | 同点の処理が正しい | ✅ |
| ScanItem adds to ranking | バッチスキャンでランキングに追加 | ✅ |

### FullTextIndex ✅
**ファイル**: `FullTextIndexTests/FullTextIndexBehaviorTests.swift`

| テスト | 説明 | 状態 |
|--------|------|------|
| Insert tokenizes and indexes | 挿入でトークン化してインデックス | ✅ |
| Multiple documents indexed | 複数ドキュメントがインデックスされる | ✅ |
| Delete removes all tokens | 削除で全トークン削除 | ✅ |
| Update re-tokenizes | 更新で再トークン化 | ✅ |
| Simple term search | 単純な語句検索 | ✅ |
| Boolean AND query | AND検索 | ✅ |
| Boolean OR query | OR検索 | ✅ |
| Stemming tokenizer | ステミングトークナイザー | ✅ |
| Case insensitive search | 大文字小文字を区別しない検索 | ✅ |
| ScanItem tokenizes and indexes | バッチスキャンでトークン化・インデックス | ✅ |

### PermutedIndex ✅
**ファイル**: `PermutedIndexTests/PermutedIndexBehaviorTests.swift`

| テスト | 説明 | 状態 |
|--------|------|------|
| Insert creates permuted entry | 挿入で順列エントリ作成 | ✅ |
| Multiple inserts create entries | 複数挿入でエントリ作成 | ✅ |
| Delete removes permuted entry | 削除で順列エントリ削除 | ✅ |
| Update changes permuted key | 更新で順列キー変更 | ✅ |
| scanByPrefix finds by permuted prefix | 順列プレフィックスで検索 | ✅ |
| scanByPrefix empty returns all | 空プレフィックスで全件取得 | ✅ |
| scanByExactMatch finds entries | 完全一致検索 | ✅ |
| scanByExactMatch throws for wrong count | フィールド数不一致でエラー | ✅ |
| scanAll returns all entries | 全エントリ取得 | ✅ |
| toOriginalOrder converts back | 元の順序に変換 | ✅ |
| Different permutation orders fields | 異なる順列で順序変更 | ✅ |
| Identity permutation maintains order | 恒等順列で順序維持 | ✅ |
| ScanItem adds permuted entry | バッチスキャンで順列エントリ追加 | ✅ |

---

## 実装状況サマリー

| インデックス | Metadata | Behavior | 完了 |
|-------------|----------|----------|------|
| ScalarIndex | ✅ | ✅ | ✅ |
| CountIndex | ✅ | ✅ | ✅ |
| SumIndex | ✅ | ✅ | ✅ |
| MinIndex | ✅ | ✅ | ✅ |
| MaxIndex | ✅ | ✅ | ✅ |
| VectorIndex | ✅ | ✅ | ✅ |
| GraphIndex | ✅ | ✅ | ✅ |
| VersionIndex | ✅ | ✅ | ✅ |
| SpatialIndex | - | ✅ | ✅ |
| RankIndex | - | ✅ | ✅ |
| FullTextIndex | - | ✅ | ✅ |
| PermutedIndex | - | ✅ | ✅ |

**Note**: 全てのインデックスタイプのBehaviorテストが完了しました。
