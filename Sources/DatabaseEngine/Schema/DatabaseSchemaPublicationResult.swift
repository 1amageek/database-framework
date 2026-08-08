import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire

package struct DatabaseSchemaPublicationResult: Sendable {
    package let previousFingerprint: SchemaFingerprint?
    package let fingerprint: SchemaFingerprint
    package let schemaVersion: Schema.Version
    package let generation: UInt64
    package let job: JobIdentity?
}
