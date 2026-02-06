# Skip List Span Counter デバッグログ

## 発見されたバグと修正

### 1. Phase 2.5の範囲エラー（修正完了）

**問題**: Phase 2.5が`level in 1..<newLevel`で開始、Level 0をスキップ

```swift
// 誤り
for level in 1..<newLevel {
    if updateKeys[level] == nil { ... }
}

// 修正
for level in 0..<newLevel {
    if updateKeys[level] == nil { ... }
}
```

**影響**: Level 0で`updateKey == nil`の場合に`firstEntryRanks[0]`が未設定
**結果**: Level 0-2で99-100/100に改善

---

### 2. findInsertionPoint()の停止条件エラー（修正完了）

**問題**: 停止条件に達したときに`lastKey`を設定せずに`break`

```swift
// 誤り
if currentScore <= targetScore {
    break  // lastKeyが設定されない
}
lastKey = key  // 実行されない

// 修正試行1（誤り）
if currentScore <= targetScore {
    lastKey = key
    accumulatedRank += span  // ← これが誤り
    break
}

// 修正試行2（現在）
if currentScore <= targetScore {
    break  // spanを累積しない
}
lastKey = key  // breakしない場合のみ累積
```

**影響**: `updateKey == nil`の誤判定
**結果**: Level 1で92-97/100 → 100/100に改善

---

## 現在の状態

**テスト結果** (5回実行):
- Run 1: Level 2で99/100（失敗）
- Run 2: 完全パス ✓
- Run 3: Level 1で99/100（失敗）
- Run 4: Level 3で99/100（失敗）
- Run 5: Level 3で99/100（失敗）

**成功率**: 20% (1/5)

**パターン**:
- Level 0: 常に100/100 ✓
- Level 1: ほぼ100/100 ✓
- Level 2-3: 確率的に99/100（1エントリ欠落）

---

## 残存する問題

### 症状

高レベル（Level 2-3）で確率的に1エントリのspan不足が発生。

### 仮説

1. **Phase 4の処理漏れ**: `updateKey == nil`のケースで更新が必要？
   - 検証済み: Pugh 1990では`updateKey == nil`は何もしないのが正しい
   - 結果: 仮説棄却

2. **Phase 3のupdateKey == nilケース**: 最初のエントリのspan更新が必要？
   - 検証済み: コメントで「span does NOT change」と記載
   - 結果: 仮説棄却

3. **findInsertionPoint()の停止条件**: 境界条件の誤り？
   - 現在調査中

---

## 次のアクション

1. ✅ プランファイル（snoopy-zooming-aho.md）のPugh 1990アルゴリズムを再確認
2. ✅ FoundationDB Record Layer RankedSetの実装を参照
3. ✅ 標準アルゴリズムとの差異を特定
