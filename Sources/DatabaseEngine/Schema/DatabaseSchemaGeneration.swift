import DatabaseKit
import DatabaseTypes

/// One immutable database execution generation.
///
/// Requests retain a generation through `DatabaseSchemaLease`, so publishing a
/// replacement never changes the schema, runtime registry, or authorization
/// policy observed by work that is already in flight.
struct DatabaseSchemaGeneration: Sendable {
    let identifier: UInt64
    let fingerprint: SchemaFingerprint
    let indexPhysicalFingerprint: ByteString
    let executionRuntimeFingerprint: ByteString
    let schema: Schema
    let runtimeConfiguration: DatabaseRuntimeConfiguration
    let indexPhysicalLayouts: [String: IndexPhysicalLayout]
    /// Layer of every Directory node position `schema` declares.
    ///
    /// The derivation is a property of the whole schema rather than of one
    /// declaration, so it belongs to the generation that fixes that schema.
    /// Deriving it here also rejects a schema whose declarations disagree about
    /// a position before that schema can be published.
    let directoryLayers: DirectoryLayerTagMap
    let securityDelegate: (any DataStoreSecurityDelegate)?
}
