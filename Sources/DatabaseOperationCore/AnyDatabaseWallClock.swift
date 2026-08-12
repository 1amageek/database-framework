import DatabaseEngine
import DatabaseTypes

/// Type-erased wall clock used by long-lived database runtimes.
public final class AnyDatabaseWallClock: WallClock, Sendable {
    private let clock: any WallClock

    public init(_ clock: any WallClock) {
        self.clock = clock
    }

    public var now: Timestamp {
        clock.now
    }
}
