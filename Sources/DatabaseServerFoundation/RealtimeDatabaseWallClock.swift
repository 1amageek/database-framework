import DatabaseServer
import DatabaseEngine
import DatabaseTypes
import DatabaseTypesFoundation
import Foundation

public struct RealtimeDatabaseWallClock: WallClock {
    public init() {}

    public var now: Timestamp {
        do {
            return try Timestamp(Foundation.Date())
        } catch {
            preconditionFailure(
                "The platform clock produced an invalid timestamp: \(error)"
            )
        }
    }
}
