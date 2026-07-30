
import DatabaseTypes

/// Version metadata associated with one side of a model diff.
public struct VersionInfo: Sendable, Hashable {
    public let versionID: String
    public let timestamp: Timestamp?

    public init(versionID: String, timestamp: Timestamp?) {
        self.versionID = versionID
        self.timestamp = timestamp
    }
}
