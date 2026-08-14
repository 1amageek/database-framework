#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import StorageKit

/// Read-only selector for one named Base Composition.
public struct CompositionDataSource: Sendable {
    public let id: Base.Composition.ID
    package let container: DBContainer
    package let authorization: AuthorizationContext

    package init(
        id: Base.Composition.ID,
        container: DBContainer,
        authorization: AuthorizationContext
    ) {
        self.id = id
        self.container = container
        self.authorization = authorization
    }

    public func query<Model: Persistable>(
        _ type: Model.Type
    ) -> CompositionQueryExecutor<Model> {
        CompositionQueryExecutor(
            source: self,
            query: Query<Model>()
        )
    }

    /// Acquires the immutable Composition definition and every member Base
    /// lease. Holding all leases prevents a member from retiring while the
    /// caller authorizes or executes the federated operation.
    private func acquireLease() async throws -> DatabaseCompositionLease {
        let record: DatabaseCompositionRecord
        record = try await container.withControlMetadataTransaction(
            configuration: .readOnly
        ) { transaction in
            guard let record = try await container.compositionCatalog.load(
                id,
                transaction: transaction.storageAccess
            ) else {
                throw DatabaseCompositionAccessError.unavailable(id)
            }
            return record
        }
        var memberLeases: [DatabaseBaseLease] = []
        memberLeases.reserveCapacity(record.composition.bases.count)
        do {
            for baseID in record.composition.bases {
                memberLeases.append(try container.acquireBaseLease(baseID))
            }
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw DatabaseCompositionAccessError.unavailable(id)
        }
        return DatabaseCompositionLease(
            record: record,
            members: memberLeases
        )
    }

    /// Acquires and authorizes every member for metadata-only resolution.
    @_spi(DatabaseExecution)
    public func acquireReadLease() async throws -> DatabaseCompositionLease {
        let lease = try await acquireLease()
        do {
            for member in lease.members {
                try await container.withBaseLease(member) {
                    let context = container.session(
                        authorization: authorization
                    ).base(member.baseID).newContext()
                    try await context.withTransaction(
                        requiredAccess: .read,
                        configuration: .readOnly
                    ) { _ in () }
                }
            }
            return lease
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw DatabaseCompositionAccessError.unavailable(id)
        }
    }

    /// Resolves a Composition for metadata operations while preserving the
    /// same all-member authorization contract as execution.
    @_spi(DatabaseExecution)
    public func resolve() async throws -> DatabaseCompositionRecord {
        try await acquireReadLease().record
    }

    /// Opens one read transaction per physical domain and keeps all of them
    /// alive until the federated operation finishes. Every member Grant is
    /// checked before `operation` can observe data.
    @_spi(DatabaseExecution)
    public func withReadSnapshot<Result: Sendable>(
        _ operation: @escaping @Sendable (
            DatabaseCompositionReadSnapshot
        ) async throws -> Result
    ) async throws -> Result {
        try await container.withOperationSchemaLease { _ in
            try await withBoundSchemaReadSnapshot(operation)
        }
    }

    private func withBoundSchemaReadSnapshot<Result: Sendable>(
        _ operation: @escaping @Sendable (
            DatabaseCompositionReadSnapshot
        ) async throws -> Result
    ) async throws -> Result {
        // Authorization is performed exactly once against the same domain
        // transactions that provide the federated read snapshot.
        let lease = try await acquireLease()
        var domains: [String: DatabaseStorageDomainRuntime] = [:]
        for member in lease.members {
            domains[member.domainID] = member.generation.domain
        }
        let domainIDs = domains.keys.sorted()
        var owned: [(id: String, transaction: any Transaction)] = []
        owned.reserveCapacity(domainIDs.count)
        var committedCount = 0
        do {
            for domainID in domainIDs {
                guard let domain = domains[domainID] else {
                    throw DatabaseCompositionAccessError.unavailable(id)
                }
                let transaction = try domain.transactionExecutor
                    .createOwnedTransaction()
                _ = try TransactionConfiguration.readOnly.apply(
                    to: transaction
                )
                owned.append((id: domainID, transaction: transaction))
            }

            var transactions: [String: any TransactionAccess] = [:]
            transactions.reserveCapacity(owned.count)
            var readPoints: [DomainReadPoint] = []
            readPoints.reserveCapacity(owned.count)
            for entry in owned {
                // Capture each backend read point before any member Grant can
                // expose data. This fixes the federated snapshot boundary
                // without nesting transaction runners.
                guard let domain = domains[entry.id] else {
                    throw DatabaseCompositionAccessError.unavailable(id)
                }
                let position: DomainReadPoint.Position
                if domain.transactionCapabilities.readVersion {
                    guard let version = UInt64(
                        exactly: try await entry.transaction.getReadVersion()
                    ) else {
                        throw DatabaseCompositionAccessError.unavailable(id)
                    }
                    position = .version(version)
                } else {
                    // This identifier denotes the lifetime of this exact
                    // transaction snapshot. The durable result spool is the
                    // ownership boundary that keeps later pages stable after
                    // the transaction closes.
                    position = .opaque(Self.makeOpaqueReadPoint())
                }
                transactions[entry.id] = entry.transaction
                readPoints.append(
                    try DomainReadPoint(
                        domainID: entry.id,
                        position: position
                    )
                )
            }
            for member in lease.members {
                guard let transaction = transactions[member.domainID] else {
                    throw DatabaseCompositionAccessError.unavailable(id)
                }
                try await DatabaseGrantStore(
                    resource: .base(member.baseID),
                    root: member.root
                ).require(
                    .read,
                    authorization: authorization,
                    transaction: transaction
                )
            }
            let result = try await operation(
                DatabaseCompositionReadSnapshot(
                    lease: lease,
                    transactions: transactions,
                    readPoints: readPoints
                )
            )
            for entry in owned {
                try await entry.transaction.commit()
                committedCount += 1
            }
            return result
        } catch {
            let operationError = error
            var cleanupError: (any Error)?
            if committedCount < owned.count {
                for entry in owned[committedCount...].reversed() {
                    do {
                        try await entry.transaction.cancel()
                    } catch {
                        if cleanupError == nil { cleanupError = error }
                    }
                }
            }
            if let cleanupError {
                throw StorageTransactionCleanupError(
                    operationError: operationError,
                    cancellationError: cleanupError
                )
            }
            if operationError is CancellationError {
                throw CancellationError()
            }
            if operationError is DatabaseGrantAuthorizationError {
                throw DatabaseCompositionAccessError.unavailable(id)
            }
            throw operationError
        }
    }

    /// Executes one member-local read against the transaction captured by the
    /// federated snapshot. The caller receives neither a container nor a
    /// transaction capable of resolving a different Base.
    @_spi(DatabaseExecution)
    public func withMemberContext<Result: Sendable>(
        _ member: DatabaseBaseLease,
        in snapshot: DatabaseCompositionReadSnapshot,
        _ operation: @Sendable @escaping (
            DatabaseContext,
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard snapshot.lease.record.composition.id == id,
              snapshot.lease.members.contains(where: { $0 === member }) else {
            throw DatabaseCompositionAccessError.unavailable(id)
        }
        let transaction = try snapshot.transaction(for: member)
        return try await container.withBaseLease(member) {
            let context = container.session(
                authorization: authorization
            ).base(member.baseID).newContext()
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                try await operation(context, transaction)
            }
        }
    }

    private static func makeOpaqueReadPoint() -> ByteString {
        var generator = SystemRandomNumberGenerator()
        return ByteString((0..<32).map { _ in
            UInt8.random(in: .min ... .max, using: &generator)
        })
    }
}

#endif
