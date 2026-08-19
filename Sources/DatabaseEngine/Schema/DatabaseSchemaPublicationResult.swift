import DatabaseKit
import DatabaseTypes

@_spi(DatabaseExecution)
public struct DatabaseSchemaPublicationResult: Sendable {
    public let previousFingerprint: SchemaFingerprint?
    public let fingerprint: SchemaFingerprint
    public let schemaVersion: Schema.Version
    public let generation: UInt64
    public let indexPhysicalFingerprint: ByteString
    public let executionRuntimeFingerprint: ByteString
}
