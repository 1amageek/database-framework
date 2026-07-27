import DatabaseTypes
import DatabaseTypesFoundation
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public struct RealtimeDatabaseWallClock: DatabaseWallClock {
    public init() {}

    public func now() -> Timestamp {
        do {
            return try Timestamp(Date())
        } catch {
            preconditionFailure(
                "The platform clock produced an invalid timestamp: \(error)"
            )
        }
    }
}
