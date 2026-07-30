import DatabaseServer
import DatabaseTypes
import Foundation

public struct RandomDatabaseUUIDGenerator: DatabaseUUIDGenerator {
    public init() {}

    public func generate() -> DatabaseTypes.UUID {
        let bytes = Foundation.UUID().uuid
        return DatabaseTypes.UUID(
            high: UInt64(bytes.0) << 56
                | UInt64(bytes.1) << 48
                | UInt64(bytes.2) << 40
                | UInt64(bytes.3) << 32
                | UInt64(bytes.4) << 24
                | UInt64(bytes.5) << 16
                | UInt64(bytes.6) << 8
                | UInt64(bytes.7),
            low: UInt64(bytes.8) << 56
                | UInt64(bytes.9) << 48
                | UInt64(bytes.10) << 40
                | UInt64(bytes.11) << 32
                | UInt64(bytes.12) << 24
                | UInt64(bytes.13) << 16
                | UInt64(bytes.14) << 8
                | UInt64(bytes.15)
        )
    }
}
