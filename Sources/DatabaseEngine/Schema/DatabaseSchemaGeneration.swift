import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire

/// One immutable database execution generation.
///
/// Requests retain a generation through `DatabaseSchemaLease`, so publishing a
/// replacement never changes the schema, runtime registry, or authorization
/// policy observed by work that is already in flight.
struct DatabaseSchemaGeneration: Sendable {
    let identifier: UInt64
    let fingerprint: SchemaFingerprint
    let schema: Schema
    let runtimeConfiguration: DatabaseRuntimeConfiguration
    let securityDelegate: (any DataStoreSecurityDelegate)?
}
