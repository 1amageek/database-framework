# Pugh 1990 アルゴリズムと現在の実装の比較

## Phase 3: updateKey == nil ケースのspan計算

### プランファイル（正しいアルゴリズム）

Line 727-735:
```
else:
  # レベルの先頭に挿入（updateKey == nil）
  nextEntry = readFirstEntry(level)
  if nextEntry != NIL:
    nextRank = getRank(nextEntry.score, nextEntry.pk)
    newNodeSpan = nextRank - rank[0] + 1  # ← +1に注目！
  else:
    newNodeSpan = 1
```

### 現在の実装

SkipListInsertion.swift Line 186-189:
```swift
// First entry's new rank (after insertion) = firstRank + 1
// newSpan = first entry's new rank - new node's rank
newSpan = (firstRank + 1) - rank[0]
```

## 数式の比較

### プラン:
```
newNodeSpan = nextRank - rank[0] + 1
```

### 現在:
```
newSpan = (firstRank + 1) - rank[0]
      = firstRank + 1 - rank[0]
      = firstRank - rank[0] + 1
```

**結論**: 数式は同じ！

## 問題の可能性

1. **firstRankの計算方法が誤り？**
   - Phase 2.5で`findInsertionPoint(level: 0, ...)`を呼ぶ
   - これはLevel 0でのrankを返す
   - プランでは`getRank(nextEntry.score, nextEntry.pk)`

2. **findInsertionPoint()とgetRank()の違い**

### findInsertionPoint():
- 特定のレベルで挿入位置を探す
- そのレベルでの累積rankを返す

### getRank():
- **全レベルをトラバースして** Level 0のrankを返す
- 複数レベルの情報を使う

**重要**: Phase 2.5で`findInsertionPoint(level: 0, ...)`を呼んでいるが、
これは**Level 0のみ**のスキャン。getRank()は**複数レベル**をトラバース。

## 修正案

Phase 2.5の`findInsertionPoint(level: 0, ...)`は正しいか？

→ プランではgetRank()を呼ぶべき
→ しかし、getRank()は全Skip List構造を使うため、挿入中には使えない

**結論**: Phase 2.5のアプローチ自体は正しい（Level 0でのrankを直接計算）
