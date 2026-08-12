import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire

/// A retained snapshot of one immutable schema execution generation.
public final class DatabaseSchemaLease: Sendable {
    private let value: DatabaseSchemaGeneration

    init(_ value: DatabaseSchemaGeneration) {
        self.value = value
    }

    /// Monotonically increasing identifier persisted with schema publication.
    public var generation: UInt64 { value.identifier }

    /// Canonical DatabaseWire fingerprint of `schema`.
    public var fingerprint: SchemaFingerprint { value.fingerprint }

    /// Schema used for the complete lifetime of the request holding this lease.
    public var schema: Schema { value.schema }

    /// Runtime registrations paired with `schema` in this generation.
    public var runtimeConfiguration: DatabaseRuntimeConfiguration {
        value.runtimeConfiguration
    }

    var securityDelegate: (any DataStoreSecurityDelegate)? {
        value.securityDelegate
    }
}
