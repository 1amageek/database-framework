# LeaderboardIndex

`LeaderboardIndex` executes time-window leaderboard declarations.

```swift
#Index(.leaderboard(
    name: "scores_by_region",
    groupBy: [.ascending(\Score.region)],
    score: \Score.value,
    window: .daily,
    windowCount: 7
))
```

Grouping keys, score, window, and retained window count are logical semantics.
The module owns windowed entry maintenance and leaderboard reads and accepts
only `IndexType.leaderboard`.
