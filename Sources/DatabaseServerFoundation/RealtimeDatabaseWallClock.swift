import DatabaseServer
import DatabaseTypes
import DatabaseTypesFoundation
import Foundation

public struct RealtimeDatabaseWallClock: DatabaseWallClock {
    public init() {}

    public func now() -> Timestamp {
        do {
            return try Timestamp(Foundation.Date())
        } catch {
            preconditionFailure(
                "The platform clock produced an invalid timestamp: \(error)"
            )
        }
    }
}
