import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire

struct DatabaseRestoredSchemaState: Sendable {
    let schema: Schema
    let fingerprint: SchemaFingerprint
    let generation: UInt64
}
