# RankIndex

`RankIndex` executes numeric rank declarations.

```swift
#Index(.rank(
    name: "players_by_score",
    score: \Player.score
))
```

The declaration produces a descending score key. The module owns maintenance
and rank reads and accepts only `IndexType.rank`.
