# Skip List Span Counter テスト統計

## 現在の状態

**日時**: 2026-02-05

**テスト**: `testSpanCounterAccuracy` (100エントリ挿入)

**結果** (10回実行):
- 成功: 1/10
- 失敗: 9/10
- **成功率: 10%**

## 失敗パターン

失敗時の典型的なエラー：
```
Span counter mismatch at level X:
  Expected: 100, Got: 99
  Entries at level: N
```

- Level 0: 常に100/100 ✓
- Level 1: ほぼ100/100
- Level 2-3: 99/100（1エントリ欠落）

## 実装済みの修正

### 1. Phase 2.5の範囲（修正完了）

```swift
// Before
for level in 1..<newLevel { ... }

// After
for level in 0..<newLevel { ... }
```

**影響**: Level 0の`updateKey == nil`ケースをカバー

### 2. findInsertionPoint()の停止条件（現在のロジック）

```swift
if currentScore <= targetScore {
    if currentScore == targetScore {
        let currentPK = extractPrimaryKey(from: suffix)
        if compareTuples(currentPK, targetPrimaryKey) != .orderedDescending {
            break  // spanを累積しない
        }
    } else {
        break  // spanを累積しない
    }
}

// breakしなかった場合のみ
let span = try SpanValue.decode(value)
accumulatedRank += span.count
lastKey = key
```

**問題**: 停止時に`lastKey`が設定されない
→ しかし、Pugh 1990アルゴリズムでは停止条件に達したエントリは累積しないのが正しい

## プランファイルとの比較

### プランファイル（Line 704-711）

```
while next = readNext(level, currentKey):
  if next.score > score OR (next.score == score AND next.pk > primaryKey):
    rank[level] += next.span
    currentKey = next.key
  else:
    break

update[level] = currentKey  # ループ後のcurrentKey
```

### 現在の実装

```swift
for try await (key, value) in sequence {
    if currentScore <= targetScore {
        // 停止条件
        break
    }

    // 累積
    accumulatedRank += span.count
    lastKey = key
}

return (accumulatedRank, lastKey)
```

## 問題の可能性

1. **lastKeyの設定タイミング**: ループを抜けた時点の`lastKey`が正しいか？
   - 標準アルゴリズム：ループ後の`currentKey`
   - 現在の実装：最後に累積したエントリの`key`

2. **停止条件に達したエントリの扱い**:
   - 標準：累積せず、`update[level]`にも含めない（NIL）
   - 現在：累積せず、`lastKey`にも含めない（nil）

## 次のアクション

プランファイルのアルゴリズムと完全に一致させる必要がある。

**重要な違い**:
標準アルゴリズムでは`update[level] = currentKey`（ループ後）
現在の実装では`lastKey`は最後に累積したエントリ

→ この違いが1エントリ欠落の原因か？

## 発見された問題（2026-02-05）

### レベルが空の場合のspan計算の矛盾

**プランファイル** (Line 732-733):
```
else:
  newNodeSpan = 1  # レベルが空
```

**現在の実装** (SkipListInsertion.swift Line 183-184):
```swift
// Level is empty, span = distance to end
newSpan = (totalCountBefore + 1) - rank[0]
```

**問題**:
- プランファイル: span = 1 (固定)
- 現在の実装: span = (total + 1) - rank （可変）

もし rank[0]=10, totalCountBefore=100 なら:
- プランファイル: span = 1
- 現在の実装: span = 101 - 10 = 91

**修正試行** (2026-02-05 15:30):
Line 186を `newSpan = 1` に変更。

**結果**: ❌ 失敗
- 1回目: ✅ パス
- 2-6回目: ❌ Level 1で97/100（修正前は99/100）
- **悪化**: 99/100 → 97/100

**原因分析**:
プランファイルLine 732-733の`newNodeSpan = 1`は、文脈が異なる可能性がある。
- Level 0では常にspan=1（正しい）
- Level 1+で「レベルが空」の場合、span=1は誤り

**元に戻した**: `newSpan = (totalCountBefore + 1) - rank[0]`

**次のアプローチ**: 別の角度から問題を分析する必要がある。

## 修正2: 条件分岐の簡略化（2026-02-05 15:45）

**問題**: プランファイルとの不一致
- プランファイルLine 1177: `if let firstRank = firstEntryRanks[level]`
- 現在の実装: `if let firstEntry..., let firstRank..., let oldFirstSpan...`

**修正内容**:
```swift
// Before
if let firstEntry = firstEntriesAtInsertionTime[level],
   let firstRank = firstEntryRanks[level],
   let oldFirstSpan = firstEntrySpans[level] {

// After
if let firstRank = firstEntryRanks[level] {
```

**結果**: 成功率 2/10 (20%)
- 改善なし（修正前と同程度）
- ただし、プランファイルとの一致性は向上

**結論**: 条件分岐の問題ではない。根本原因は別の場所。
