import DatabaseKit
import DatabaseTypes

struct DatabaseRestoredSchemaState: Sendable {
    let schema: Schema
    let fingerprint: SchemaFingerprint
    let generation: UInt64
    let indexPhysicalFingerprint: ByteString?
    let executionRuntimeFingerprint: ByteString?
    let indexLayoutFingerprints: [String: ByteString]
}
