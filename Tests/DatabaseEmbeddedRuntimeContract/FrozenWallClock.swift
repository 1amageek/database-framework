import DatabaseEngine
import DatabaseTypes

/// Deterministic fixture time; this suite makes no elapsed-time assertion.
struct FrozenWallClock: WallClock {
    var now: Timestamp { Timestamp(secondsSinceUnixEpoch: 0) }
}
