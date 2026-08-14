import DatabaseKit
@_spi(DatabaseExecution) import DatabaseWire

struct DatabaseRestoredSchemaState: Sendable {
    let schema: Schema
    let fingerprint: SchemaFingerprint
    let generation: UInt64
}
