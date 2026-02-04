# Performance Optimization TODO

このドキュメントは、database-frameworkの全インデックス実装における高速化の機会をまとめたものです。

**最終更新日**: 2026-02-04

---

## 優先度の定義

| 優先度 | 基準 | タイムライン |
|--------|------|------------|
| **P0 - Critical** | 既存実装の公開、多数のユースケースで劇的改善 | 即座に実装 |
| **P1 - High** | 大幅なパフォーマンス改善、スケーラビリティ向上 | 次のマイルストーン |
| **P2 - Medium** | 新機能追加、特定ユースケースでの改善 | 将来のリリース |
| **P3 - Low** | 開発者体験向上、エッジケース最適化 | バックログ |

---

## 1. ScalarIndex（基本インデックス）

### 現状
- 標準的なB-tree実装、パフォーマンスは良好
- Covering index未実装（README:194）

### TODO

#### P0: Covering Index実装
- [ ] storedFieldsの値をインデックスキーまたはバリューに格納
- [ ] Index-only scanの実装（プライマリルックアップ回避）
- [ ] FDBQueryBuilderでcovering index検出ロジック追加
- [ ] パフォーマンステスト追加

**推定効果**: 50-80%のレイテンシ削減（covering queryの場合）

**参考資料**:
- [PostgreSQL Covering Indexes](https://www.postgresql.org/docs/current/indexes-index-only-scans.html)
- [FDB Record Layer Covering Indexes](https://github.com/FoundationDB/fdb-record-layer)

**ファイル**:
- `Sources/ScalarIndex/ScalarIndexMaintainer.swift`
- `Sources/DatabaseEngine/Fetch/FDBQueryBuilder.swift`

---

## 2. VectorIndex（ベクトル検索）

### 現状
- HNSW実装は良好（swift-hnsw使用）
- Flat scanは線形スキャン
- Batch query部分実装（README:212）

### TODO

#### P1: IVF (Inverted File Index)実装
- [ ] IVFIndexMaintainerの実装
- [ ] クラスタリングアルゴリズム（k-means）
- [ ] クラスタ割り当てロジック
- [ ] nprobe パラメータサポート（探索クラスタ数）
- [ ] パフォーマンスベンチマーク（100万ベクトル以上）

**推定効果**: 10-100倍のスループット向上（100万ベクトル超）

**参考資料**:
- [FAISS IVF Implementation](https://github.com/facebookresearch/faiss/wiki/Faiss-indexes#cell-probe-methods-IndexIVF-indexes)
- Jégou et al., "Product Quantization for Nearest Neighbor Search", TPAMI 2011

#### P1: Product Quantization (PQ)実装
- [ ] PQエンコーダ/デコーダ実装
- [ ] サブベクトル分割ロジック
- [ ] コードブック生成（k-means）
- [ ] 距離計算の高速化（ルックアップテーブル）
- [ ] メモリ使用量ベンチマーク

**推定効果**: メモリ使用量80-90%削減

#### P2: バッチクエリ最適化
- [ ] 複数クエリベクトルの同時処理
- [ ] HNSW graphの共有アクセス最適化
- [ ] 並列距離計算

**推定効果**: 2-5倍のスループット向上（バッチサイズに依存）

#### P2: SIMD最適化
- [ ] Accelerate frameworkの活用
- [ ] ベクトル内積のSIMD実装
- [ ] ユークリッド距離のSIMD実装
- [ ] コサイン類似度のSIMD実装

**推定効果**: 距離計算2-4倍高速化

**ファイル**:
- `Sources/VectorIndex/IVFIndexMaintainer.swift` (新規)
- `Sources/VectorIndex/ProductQuantization.swift` (新規)
- `Sources/VectorIndex/HNSWIndexMaintainer.swift`

---

## 3. FullTextIndex（全文検索）

### 現状
- 倒置インデックス実装済み
- BM25スコアリング実装済み
- Autocomplete部分実装（README:298）

### TODO

#### P2: ファセット検索実装
- [ ] Term aggregation（カテゴリ別カウント）
- [ ] ファセットクエリAPI
- [ ] トップNファセット取得
- [ ] ファセットフィルタリング

**推定効果**: 新機能（現在未実装）

**参考資料**:
- [Elasticsearch Aggregations](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html)
- [Lucene Faceting](https://lucene.apache.org/core/9_0_0/facet/org/apache/lucene/facet/package-summary.html)

#### P2: 並列トークン化
- [ ] 大規模ドキュメントの並列トークン化
- [ ] チャンクベース処理
- [ ] TaskGroupを使った並列実行

**推定効果**: 30-50%の indexing throughput 向上

#### P3: tf-idfキャッシング
- [ ] 頻繁に検索されるtermのスコアキャッシュ
- [ ] LRUキャッシュ実装
- [ ] キャッシュ無効化戦略

**推定効果**: 10-20%の検索レイテンシ削減

**ファイル**:
- `Sources/FullTextIndex/FullTextIndexMaintainer.swift`
- `Sources/FullTextIndex/FacetAggregation.swift` (新規)
- `Sources/FullTextIndex/ParallelTokenizer.swift` (新規)

---

## 4. GraphIndex（グラフトラバーサル）

### 現状
- 複数ストラテジー実装済み
- 基本的なアルゴリズム実装済み
- OWL推論エンジン実装済み

### TODO

#### P1: GraphTraverserのバッチ並列化
- [ ] 複数開始点からの並列トラバーサル
- [ ] TaskGroupを使った並列BFS
- [ ] 結果のマージロジック
- [ ] デッドロック回避

**推定効果**: 3-10倍（複数クエリ時）

#### P1: 増分PageRank実装
- [ ] 差分更新アルゴリズム
- [ ] 影響を受けるノードの特定
- [ ] 部分的な再計算
- [ ] 収束判定の最適化

**推定効果**: 10-100倍（更新頻度に依存）

**参考資料**:
- Bahmani et al., "Fast Incremental and Personalized PageRank", VLDB 2010

#### P2: トラバーサル結果のメモライゼーション
- [ ] トラバーサルパターンのキャッシュ
- [ ] キャッシュキー生成（開始点、深度、ラベル）
- [ ] TTLベースの無効化
- [ ] メモリ制限の実装

**推定効果**: 5-20倍（同じパターンの再利用時）

#### P3: Labeled Property Graph (LPG)拡張
- [ ] エッジプロパティのネイティブサポート
- [ ] プロパティフィルタリング
- [ ] プロパティ射影
- [ ] Cypherライクなクエリ言語

**推定効果**: 開発者体験向上

**ファイル**:
- `Sources/GraphIndex/GraphTraverser.swift`
- `Sources/GraphIndex/IncrementalPageRank.swift` (新規)
- `Sources/GraphIndex/TraversalCache.swift` (新規)

---

## 5. SpatialIndex（空間インデックス）

### 現状
- S2とMortonコード実装済み
- カバリングセルアルゴリズム実装済み
- KNN未実装（README:274）

### TODO

#### P2: K-nearest neighbors実装
- [ ] 距離ソートによるKNN検索
- [ ] ヒープベースのトップK管理
- [ ] 早期終了の最適化
- [ ] KNNクエリAPI

**推定効果**: 新機能

#### P2: ポリゴンクエリ実装
- [ ] ポリゴン内包判定（Ray casting）
- [ ] S2ポリゴンサポート
- [ ] 複雑領域のカバリングセル生成
- [ ] ポリゴンクエリAPI

**推定効果**: 新機能

#### P3: 適応的セルレベル
- [ ] データ密度の分析
- [ ] レベル自動調整アルゴリズム
- [ ] 動的リバランシング
- [ ] パフォーマンスモニタリング

**推定効果**: 20-40%のスキャン削減

#### P3: 空間結合最適化
- [ ] 2つの空間データセットの効率的結合
- [ ] セル重複検出の最適化
- [ ] 並列結合処理

**推定効果**: 2-5倍（結合クエリ）

**ファイル**:
- `Sources/SpatialIndex/SpatialIndexMaintainer.swift`
- `Sources/SpatialIndex/KNNQuery.swift` (新規)
- `Sources/SpatialIndex/PolygonQuery.swift` (新規)

---

## 6. RankIndex（ランキング）

### 現状
- TopKHeap実装（O(n log k)）
- アトミックカウンタ実装済み
- Full Range Tree未実装（README:269）

### TODO

#### P0: Full Range Tree実装
- [ ] 階層的バケット構造
- [ ] O(log n)ランクルックアップ
- [ ] O(log n + k) top-K クエリ
- [ ] バケットのリバランシング
- [ ] パフォーマンステスト（10万エントリ以上）

**推定効果**: 100倍以上（大規模データセット、10万エントリ超）

**参考資料**:
- [FoundationDB Record Layer RankedSet](https://github.com/FoundationDB/fdb-record-layer/blob/main/docs/RankedSets.md)

#### P1: Reverse iteration実装
- [ ] fdb-swift-bindingsへのPR作成
- [ ] 逆順イテレーションAPI追加
- [ ] RankIndexMaintainerでの活用
- [ ] bottom-Kクエリの最適化

**推定効果**: bottom-Kクエリの高速化

#### P3: パーセンタイル事前計算
- [ ] 特定パーセンタイル（p50, p90, p95, p99）の定期更新
- [ ] バックグラウンド更新ジョブ
- [ ] キャッシュ管理

**推定効果**: 10-50倍（頻繁なパーセンタイルクエリ）

**ファイル**:
- `Sources/RankIndex/RangeTree.swift` (新規)
- `Sources/RankIndex/RankIndexMaintainer.swift`

---

## 7. PermutedIndex（複合インデックス順序変更）

### 現状
- 基本機能実装済み
- ストレージ効率良好

### TODO

#### P2: 複数置換の同時クエリ
- [ ] 異なる置換インデックスの並列クエリ
- [ ] 結果のマージ
- [ ] クエリプランナーの最適化

**推定効果**: 2-3倍（複数置換使用時）

#### P3: 動的置換選択
- [ ] クエリパターンの分析
- [ ] 最適置換の自動選択
- [ ] コストベース最適化
- [ ] 統計情報の収集

**推定効果**: 開発者体験向上

#### P3: 置換ヒントアノテーション
- [ ] よく使われるクエリパターンの事前定義
- [ ] @PermutedIndex マクロの拡張
- [ ] ヒントに基づく自動インデックス選択

**推定効果**: 開発者体験向上

**ファイル**:
- `Sources/PermutedIndex/PermutedIndexMaintainer.swift`
- `Sources/PermutedIndex/QueryOptimizer.swift` (新規)

---

## 8. AggregationIndex（集約）

### 現状
- COUNT/SUM/AVG実装済み
- DISTINCT (HyperLogLog++)実装済み
- PERCENTILE (t-digest)実装済み
- MIN/MAX: バッチAPI未実装（README:705）

### TODO

#### P0: MIN/MAXバッチAPI実装
- [ ] `getAllMins()` / `getAllMaxs()` の公開API
- [ ] 複数グループの並列取得
- [ ] ストレージレイアウトの最適化（必要に応じて）
- [ ] パフォーマンステスト

**推定効果**: 10-50倍（複数グループ時）

**Note**: 内部実装は存在、公開APIのみ必要

#### P2: スケッチの増分更新
- [ ] HyperLogLog++の差分更新
- [ ] t-digestの差分更新
- [ ] 削除操作のサポート検討
- [ ] 精度の検証

**推定効果**: 削除を含む更新の正確性向上

#### P2: 複数集約の並列実行
- [ ] COUNT/SUM/AVGの並列計算
- [ ] TaskGroupを使った並列実行
- [ ] 結果のマージ

**推定効果**: 2-3倍

#### P3: 集約キャッシュ層
- [ ] 頻繁にアクセスされる集約結果のメモリキャッシュ
- [ ] LRUキャッシュ実装
- [ ] TTLベースの無効化
- [ ] キャッシュヒット率のモニタリング

**推定効果**: 10-100倍（キャッシュヒット時）

**ファイル**:
- `Sources/AggregationIndex/MinMaxIndexMaintainer.swift`
- `Sources/AggregationIndex/IncrementalSketch.swift` (新規)
- `Sources/AggregationIndex/AggregationCache.swift` (新規)

---

## 9. VersionIndex（バージョン管理）

### 現状
- FDBバージョンスタンプ使用
- 基本的な履歴クエリ実装済み
- ブランチング/マージング未実装（README:322）

### TODO

#### P3: ブランチング/マージング実装
- [ ] 複数バージョンブランチのサポート
- [ ] ブランチメタデータ管理
- [ ] マージアルゴリズム（3-way merge）
- [ ] コンフリクト検出と解決

**推定効果**: 新機能

#### P2: 差分ストレージ
- [ ] 完全データではなく差分のみ保存
- [ ] 差分計算アルゴリズム
- [ ] 差分適用による復元
- [ ] ストレージ使用量の削減

**推定効果**: 70-90%のストレージ削減

**参考資料**:
- Git diff algorithm
- [Myers Diff Algorithm](http://www.xmailserver.org/diff2.pdf)

#### P2: バージョン圧縮
- [ ] 古いバージョンの圧縮
- [ ] 圧縮ポリシー（時間ベース、カウントベース）
- [ ] バックグラウンド圧縮ジョブ
- [ ] 圧縮されたバージョンの復元

**推定効果**: 50-70%のストレージ削減

#### P3: Cross-item point-in-time最適化
- [ ] 複数アイテムの同時点検索の最適化
- [ ] バッチリカバリAPI
- [ ] スナップショットの効率的な生成

**推定効果**: 5-10倍（複数アイテム復元時）

**ファイル**:
- `Sources/VersionIndex/VersionBranching.swift` (新規)
- `Sources/VersionIndex/DeltaCompression.swift` (新規)
- `Sources/VersionIndex/VersionIndexMaintainer.swift`

---

## 10. BitmapIndex（ビットマップ）

### 現状
- Roaring Bitmap実装済み
- AND/OR/ANDNOT実装済み
- XOR未実装（README:354）

### TODO

#### P1: バイナリシリアライゼーション
- [ ] JSONではなくバイナリ形式
- [ ] Roaring公式フォーマットのサポート
- [ ] エンコード/デコードの実装
- [ ] マイグレーションパス

**推定効果**: 50-70%のシリアライゼーション高速化

**参考資料**:
- [Roaring Bitmap Format Spec](https://github.com/RoaringBitmap/RoaringFormatSpec)

#### P2: XOR演算実装
- [ ] 対称差分の実装
- [ ] XOR最適化（container-level）
- [ ] XORクエリAPI

**推定効果**: 新機能

#### P2: NOT演算の完全実装
- [ ] ユニバースセットの管理
- [ ] ANDNOTによるNOT実装
- [ ] NOTクエリAPI

**推定効果**: 新機能

#### P2: 並列ビットマップ演算
- [ ] 複数ビットマップの並列AND/OR
- [ ] TaskGroupを使った並列実行
- [ ] Containerレベルの並列化

**推定効果**: 2-4倍（複数ビットマップ時）

**ファイル**:
- `Sources/BitmapIndex/RoaringBitmap.swift`
- `Sources/BitmapIndex/BinarySerializer.swift` (新規)
- `Sources/BitmapIndex/ParallelOperations.swift` (新規)

---

## 11. LeaderboardIndex（リーダーボード）

### 現状
- タイムウィンドウ実装済み
- スコア反転で降順ソート実装済み
- Bottom-K/Percentile/Dense ranking実装済み（内部のみ、README:336-338）

### TODO

#### P0: 公開API追加
- [ ] Bottom-Kクエリの公開API
- [ ] パーセンタイルクエリの公開API
- [ ] Dense rankingの公開API
- [ ] ドキュメント更新

**推定効果**: 新機能（内部実装は存在）

#### P3: ウィンドウ集約
- [ ] 複数ウィンドウの集約統計
- [ ] 週次/月次サマリー
- [ ] トレンド分析

**推定効果**: 新機能

**ファイル**:
- `Sources/LeaderboardIndex/LeaderboardQuery.swift`
- `Sources/LeaderboardIndex/TimeWindowLeaderboardIndexMaintainer.swift`

---

## 12. RelationshipIndex（関係性管理）

### 現状
- FK管理実装済み
- eager loading実装済み
- delete rule実装済み
- Inverse relationships未実装（README:441）

### TODO

#### P2: バッチFK解決
- [ ] 複数FK値の一括解決
- [ ] 重複FK値の除外
- [ ] 並列フェッチ
- [ ] パフォーマンステスト

**推定効果**: 3-5倍（多数の関係を持つ場合）

#### P3: Inverse relationships
- [ ] 双方向関係の自動同期
- [ ] @InverseRelationship マクロ
- [ ] 整合性保証
- [ ] トランザクション内同期

**推定効果**: 開発者体験向上

#### P3: Cascade cycle detection
- [ ] 循環カスケード削除の検出
- [ ] サイクル検出アルゴリズム（DFS）
- [ ] エラーメッセージの改善
- [ ] デバッグサポート

**推定効果**: データ整合性向上

#### P3: FK制約チェックのキャッシュ
- [ ] 参照存在チェックのキャッシュ
- [ ] LRUキャッシュ実装
- [ ] トランザクション内キャッシュ

**推定効果**: 10-30%（多数のFK検証時）

**ファイル**:
- `Sources/RelationshipIndex/BatchResolver.swift` (新規)
- `Sources/RelationshipIndex/InverseRelationships.swift` (新規)
- `Sources/RelationshipIndex/CycleDetector.swift` (新規)

---

## 共通高速化戦略

### トランザクション最適化

#### P1: Batch操作の並列化
- [ ] 独立した操作の並列実行
- [ ] TaskGroupの活用
- [ ] エラーハンドリングの改善
- [ ] すべてのIndexMaintainerに適用

**推定効果**: 2-5倍（バッチ操作時）

**ファイル**:
- `Sources/DatabaseEngine/Internal/TransactionRunner.swift`

#### P2: Read-your-writes最適化
- [ ] トランザクション内キャッシュ
- [ ] 読み取り結果のキャッシュ
- [ ] キャッシュ無効化戦略

**推定効果**: 20-50%（同一トランザクション内の再読み取り）

#### P2: Pipelined operations
- [ ] FDBの並列リクエスト
- [ ] 非同期操作のパイプライン化
- [ ] レイテンシ削減

**推定効果**: 30-60%のレイテンシ削減

**ファイル**:
- `Sources/DatabaseEngine/Core/FDBContext.swift`
- `Sources/DatabaseEngine/Internal/FDBDataStore.swift`

---

### キャッシング戦略

#### P2: Query result cache
- [ ] 頻繁なクエリ結果のキャッシュ
- [ ] LRUキャッシュ実装
- [ ] TTLベースの無効化
- [ ] キャッシュヒット率のモニタリング

**推定効果**: 10-100倍（キャッシュヒット時）

**ファイル**:
- `Sources/DatabaseEngine/Cache/QueryCache.swift` (新規)

#### P2: Index metadata cache
- [ ] インデックスメタデータのメモリキャッシュ
- [ ] SchemaRegistryとの統合
- [ ] 起動時のウォームアップ

**推定効果**: 起動時間の短縮

**ファイル**:
- `Sources/DatabaseEngine/Registry/SchemaRegistry.swift`

#### P3: Negative cache
- [ ] 存在しないキーのキャッシュ
- [ ] Bloom filterの活用
- [ ] メモリ効率の最適化

**推定効果**: 10-30%（存在しないキーへのアクセスが多い場合）

**ファイル**:
- `Sources/DatabaseEngine/Cache/NegativeCache.swift` (新規)

---

### プロファイリング/監視

#### P2: Performance metrics
- [ ] 各インデックスのメトリクス収集
- [ ] レイテンシ、スループット、エラー率
- [ ] OpenTelemetry統合
- [ ] メトリクスエクスポート

**推定効果**: 可観測性向上

**ファイル**:
- `Sources/DatabaseEngine/Metrics/PerformanceMetrics.swift` (新規)

#### P2: Slow query log
- [ ] 遅いクエリの自動記録
- [ ] 閾値の設定（例: 100ms）
- [ ] クエリプランの記録
- [ ] ログ出力

**推定効果**: デバッグ効率向上

**ファイル**:
- `Sources/DatabaseEngine/Metrics/SlowQueryLog.swift` (新規)

#### P3: Index usage statistics
- [ ] インデックス使用状況の追跡
- [ ] 使用頻度の記録
- [ ] 未使用インデックスの検出
- [ ] 統計レポート

**推定効果**: インデックス設計の最適化

**ファイル**:
- `Sources/DatabaseEngine/Metrics/IndexUsageStats.swift` (新規)

---

## 外部依存の改善

### fdb-swift-bindings

#### P1: Reverse iteration サポート
- [ ] 逆順イテレーションAPIの追加
- [ ] PRを本家にマージ
- [ ] database-frameworkへの統合

**推定効果**: RankIndexのbottom-Kクエリ最適化

**リポジトリ**: https://github.com/1amageek/fdb-swift-bindings

---

## 実装ロードマップ

### Phase 1: Quick Wins（即座に実装）
- [ ] ScalarIndex: Covering Index
- [ ] RankIndex: Full Range Tree
- [ ] AggregationIndex: MIN/MAX バッチAPI
- [ ] BitmapIndex: バイナリシリアライゼーション
- [ ] LeaderboardIndex: 公開API追加

**期間**: 2-3週間
**推定効果**: 既存クエリの大幅な高速化

### Phase 2: Scalability（次のマイルストーン）
- [ ] VectorIndex: IVF実装
- [ ] VectorIndex: Product Quantization
- [ ] GraphIndex: 増分PageRank
- [ ] GraphIndex: バッチ並列トラバーサル
- [ ] Batch操作の並列化

**期間**: 1-2ヶ月
**推定効果**: 大規模データセットでのスケーラビリティ向上

### Phase 3: New Features（将来のリリース）
- [ ] FullTextIndex: ファセット検索
- [ ] SpatialIndex: KNN実装
- [ ] SpatialIndex: ポリゴンクエリ
- [ ] RelationshipIndex: バッチFK解決
- [ ] Query result cache

**期間**: 2-3ヶ月
**推定効果**: 新機能追加、特定ユースケースの改善

### Phase 4: Polish（バックログ）
- [ ] VersionIndex: 差分ストレージ
- [ ] RelationshipIndex: Inverse relationships
- [ ] Performance metrics
- [ ] Slow query log
- [ ] すべてのP3項目

**期間**: 継続的
**推定効果**: 開発者体験向上、エッジケース最適化

---

## 測定とベンチマーク

### ベンチマーク追加が必要な項目
- [ ] Covering Index vs 非Covering Index
- [ ] Full Range Tree vs TopKHeap
- [ ] IVF vs HNSW vs Flat scan
- [ ] バイナリ vs JSON シリアライゼーション
- [ ] 並列 vs シーケンシャル batch操作

### パフォーマンスターゲット
- [ ] 各インデックスでp50/p95/p99レイテンシを定義
- [ ] スループット目標の設定（ops/sec）
- [ ] メモリ使用量の上限設定
- [ ] 継続的ベンチマークのCI統合

---

## 参考資料

### 学術論文
- Malkov & Yashunin, "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs", 2016
- Weiss et al., "Hexastore: sextuple indexing for semantic web data management", VLDB 2008
- Lemire et al., "Roaring Bitmaps: Implementation of an Optimized Software Library", 2016
- Bahmani et al., "Fast Incremental and Personalized PageRank", VLDB 2010
- Jégou et al., "Product Quantization for Nearest Neighbor Search", TPAMI 2011

### 実装参考
- [FoundationDB Record Layer](https://github.com/FoundationDB/fdb-record-layer)
- [PostgreSQL Indexes](https://www.postgresql.org/docs/current/indexes.html)
- [FAISS](https://github.com/facebookresearch/faiss)
- [Apache Lucene](https://lucene.apache.org/)
- [CRoaring](https://github.com/RoaringBitmap/CRoaring)

---

## 貢献ガイドライン

このTODOに取り組む際は：

1. **優先度を尊重**: P0 → P1 → P2 → P3 の順で実装
2. **学術的根拠**: アルゴリズムは論文・教科書に基づく
3. **参照実装の調査**: PostgreSQL, FDB Record Layer, FAISS等を参照
4. **包括的テスト**: 新機能には必ずユニットテスト追加
5. **ベンチマーク**: 推定効果を検証するベンチマーク追加
6. **ドキュメント更新**: README.mdとCLAUDE.mdを更新

詳細は `CLAUDE.md` の「実装品質ガイドライン」を参照してください。
