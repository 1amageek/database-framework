import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire

package struct DatabaseSchemaPublicationResult: Sendable {
    package let previousFingerprint: SchemaFingerprint?
    package let fingerprint: SchemaFingerprint
    package let schemaVersion: Schema.Version
    package let generation: UInt64
}
