import DatabaseValue
import DatabaseWire

public struct DatabaseRuntimeDescriptor: Sendable, Hashable {
    public let runtimeVersion: String
    public let schemaVersion: DatabaseSchemaVersion
    public let features: [CapabilitiesDescribeOperation.Feature]

    public init(
        runtimeVersion: String,
        schemaVersion: DatabaseSchemaVersion,
        features: [CapabilitiesDescribeOperation.Feature]
    ) {
        self.runtimeVersion = runtimeVersion
        self.schemaVersion = schemaVersion
        self.features = features
    }
}
