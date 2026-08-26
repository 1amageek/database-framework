import DatabaseKit
import DatabaseTypes
import Synchronization

/// A retained snapshot of one immutable schema execution generation.
public final class DatabaseSchemaLease: Sendable {
    private let value: DatabaseSchemaGeneration
    private let token: DatabaseSchemaLeaseToken

    init(_ value: DatabaseSchemaGeneration,
        token: DatabaseSchemaLeaseToken
    ) {
        self.value = value
        self.token = token
    }

    /// Monotonically increasing identifier persisted with schema publication.
    public var generation: UInt64 { value.identifier }

    /// Canonical fingerprint of `schema`.
    public var fingerprint: SchemaFingerprint { value.fingerprint }

    /// Canonical identity of every executable behavior in this lease.
    @_spi(DatabaseExecution)
    public var executionRuntimeFingerprint: ByteString {
        value.executionRuntimeFingerprint
    }

    /// Canonical identity of the schema and physical index layouts.
    @_spi(DatabaseExecution)
    public var indexPhysicalFingerprint: ByteString {
        value.indexPhysicalFingerprint
    }

    /// Schema used for the complete lifetime of the request holding this lease.
    public var schema: Schema { value.schema }

    /// Runtime registrations paired with `schema` in this generation.
    package var runtimeConfiguration: DatabaseRuntimeConfiguration {
        value.runtimeConfiguration
    }

    package var indexPhysicalLayouts: [String: IndexPhysicalLayout] {
        value.indexPhysicalLayouts
    }

    var securityDelegate: (any DataStoreSecurityDelegate)? {
        value.securityDelegate
    }
}

final class DatabaseSchemaLeaseToken: Sendable {
    private let didFinish = Mutex(false)
    private let finishOperation: @Sendable () -> Void

    init(finishOperation: @escaping @Sendable () -> Void) {
        self.finishOperation = finishOperation
    }

    func finish() {
        let shouldFinish = didFinish.withLock { didFinish in
            guard !didFinish else { return false }
            didFinish = true
            return true
        }
        if shouldFinish {
            finishOperation()
        }
    }

    deinit {
        finish()
    }
}
