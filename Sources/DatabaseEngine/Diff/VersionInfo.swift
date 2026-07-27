#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Version metadata associated with one side of a model diff.
public struct VersionInfo: Sendable, Hashable {
    public let versionID: String
    public let timestamp: Date?

    public init(versionID: String, timestamp: Date?) {
        self.versionID = versionID
        self.timestamp = timestamp
    }
}
