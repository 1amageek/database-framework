# 降順スキャンの停止条件分析

## 標準Pugh 1990（昇順: low → high）

```
while next.key < searchKey do
  advance
```

停止: `next.key >= searchKey`

## プランファイルの降順実装（high → low）

Line 704-709:
```
while next = readNext(level, currentKey):
  if next.score > score OR (next.score == score AND next.pk > primaryKey):
    rank[level] += next.span
    currentKey = next.key
  else:
    break
```

**条件**:
- `next.score > score` → 進む
- `next.score == score AND next.pk > primaryKey` → 進む
- **それ以外** → 停止

**停止するケース**:
- `next.score < score` → 停止（次のエントリは新ノードより低いスコア）
- `next.score == score AND next.pk <= primaryKey` → 停止（同じスコアで次のPKが小さいか等しい）

## 現在の実装

SkipListInsertion.swift Line 344-358:
```swift
if currentScore <= targetScore {
    if currentScore == targetScore {
        let currentPK = extractPrimaryKey(from: suffix)
        if compareTuples(currentPK, targetPrimaryKey) != .orderedDescending {
            // Current key <= target, stop here
            break
        }
    } else {
        // Current score is lower than target, stop here
        break
    }
}

// currentScore > targetScore (or same score with higher PK), accumulate span
let span = try SpanValue.decode(value)
accumulatedRank += span.count
lastKey = key
```

**停止するケース**:
- `currentScore < targetScore` → 停止
- `currentScore == targetScore AND compareTuples(currentPK, targetPK) != .orderedDescending` → 停止
  - compareTuples != .orderedDescending = .orderedAscending OR .orderedSame
  - つまり: currentPK < targetPK OR currentPK == targetPK

**累積するケース**:
- `currentScore > targetScore` → 累積
- `currentScore == targetScore AND compareTuples(currentPK, targetPK) == .orderedDescending` → 累積
  - compareTuples == .orderedDescending
  - つまり: currentPK > targetPK

## 比較

### プラン:
- 進む: `next.score > score` OR (`next.score == score` AND `next.pk > primaryKey`)
- 停止: `next.score < score` OR (`next.score == score` AND `next.pk <= primaryKey`)

### 現在:
- 累積: `currentScore > targetScore` OR (`currentScore == targetScore` AND `currentPK > targetPK`)
- 停止: `currentScore < targetScore` OR (`currentScore == targetScore` AND `currentPK <= targetPK`)

**結論**: ロジックは同じ！停止条件は正しい。

## 問題の可能性

停止条件が正しいなら、問題は別の場所にある：

1. **Phase 2.5のfirstRank計算**: 正しいか？
2. **Phase 4の処理**: updateKey == nilのケースで何か必要？
3. **トランザクション競合**: snapshot: trueだが、整合性が保たれているか？

## 次のアクション

デバッグログを最小限に絞って、以下を確認：
1. Phase 1の各レベルでのrank配列
2. Phase 2.5のfirstRank計算結果
3. Phase 3の実際のnewSpan値
4. 挿入後の各レベルのspan合計

1つの挿入を完全にトレースする。
