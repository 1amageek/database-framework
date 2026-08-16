import DatabaseKit

struct DatabaseRestoredSchemaState: Sendable {
    let schema: Schema
    let fingerprint: SchemaFingerprint
    let generation: UInt64
}
