import DatabaseKit
import DatabaseTypes
import Synchronization

/// A retained snapshot of one immutable schema execution generation.
public final class DatabaseSchemaLease: Sendable {
    private let value: DatabaseSchemaGeneration
    private let token: DatabaseSchemaLeaseToken
    let publicationSource: DatabaseSchemaLease?

    init(_ value: DatabaseSchemaGeneration,
        token: DatabaseSchemaLeaseToken,
        publicationSource: DatabaseSchemaLease? = nil
    ) {
        self.value = value
        self.token = token
        self.publicationSource = publicationSource
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

    var sqlCatalog: SQLCatalogSnapshot { value.sqlCatalog }

    /// Runtime registrations paired with `schema` in this generation.
    package var runtimeConfiguration: DatabaseRuntimeConfiguration {
        value.runtimeConfiguration
    }

    package var indexPhysicalLayouts: [String: IndexPhysicalLayout] {
        value.indexPhysicalLayouts
    }

    /// Layer of every Directory node position this generation's schema declares.
    package var directoryLayers: DirectoryLayerTagMap {
        value.directoryLayers
    }

    var securityDelegate: (any DataStoreSecurityDelegate)? {
        value.securityDelegate
    }

    /// Composes an application declaration with this generation's SQL-owned
    /// entities, as normal open does. Omitted SQL-owned entities are retained;
    /// a declaration that redefines one throws
    /// `SQLCatalogBindingError.sqlOwnedEntityRedeclared`.
    @_spi(DatabaseExecution)
    public func applicationSchema(declaring declared: Schema) throws -> Schema {
        let catalog = value.sqlCatalog
        return try SQLCatalogStoredState(catalog: catalog.catalog,
            physicalSchemaFingerprint: catalog.physicalSchemaFingerprint,
            protectedContracts: catalog.protectedContracts, digest: catalog.digest,
            sqlOwnedEntityNames: catalog.sqlOwnedEntityNames)
            .applicationSchema(physicalSchema: value.schema, declaredSchema: declared)
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
