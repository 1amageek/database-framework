import DatabaseKit
@_spi(DatabaseExecution) import DatabaseWire

@_spi(DatabaseExecution)
public struct DatabaseSchemaPublicationResult: Sendable {
    public let previousFingerprint: SchemaFingerprint?
    public let fingerprint: SchemaFingerprint
    public let schemaVersion: Schema.Version
    public let generation: UInt64
}
