import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire

struct DatabaseRestoredSchemaState: Sendable {
    let schema: Schema
    let fingerprint: SchemaFingerprint
    let generation: UInt64
}
