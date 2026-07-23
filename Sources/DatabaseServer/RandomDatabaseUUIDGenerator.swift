import DatabaseValue
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public struct RandomDatabaseUUIDGenerator: DatabaseUUIDGenerator {
    public init() {}

    public func generate() -> DatabaseUUID {
        let bytes = UUID().uuid
        return DatabaseUUID(
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
