import DatabaseEngine
import DatabaseTypes

/// Type-erased wall clock used by long-lived database runtimes.
public final class AnyDatabaseWallClock: WallClock, Sendable {
    private let readTimestamp: @Sendable () -> Timestamp

    public init<Clock: WallClock>(_ clock: Clock) {
        self.readTimestamp = {
            clock.now
        }
    }

    public var now: Timestamp {
        readTimestamp()
    }
}
