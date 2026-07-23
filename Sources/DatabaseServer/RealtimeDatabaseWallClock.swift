import DatabaseValue
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public struct RealtimeDatabaseWallClock: DatabaseWallClock {
    public init() {}

    public func now() -> DatabaseTimestamp {
        let interval = Date().timeIntervalSince1970
        let seconds = interval.rounded(.down)
        let nanoseconds = (interval - seconds) * 1_000_000_000
        return DatabaseTimestamp(
            secondsSinceUnixEpoch: Int64(seconds),
            nanoseconds: UInt32(nanoseconds.rounded(.down))
        )
    }
}
