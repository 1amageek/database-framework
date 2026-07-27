import DatabaseTypes

/// Type-erased wall clock used by long-lived database runtimes.
public final class AnyDatabaseWallClock: DatabaseWallClock, Sendable {
    private let readTimestamp: @Sendable () -> Timestamp

    public init<Clock: DatabaseWallClock>(_ clock: Clock) {
        self.readTimestamp = {
            clock.now()
        }
    }

    public func now() -> Timestamp {
        readTimestamp()
    }
}
