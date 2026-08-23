#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

private actor CompositionRowResultCollector {
    private var metadata: CompositionQueryMetadata?
    private var values: [CompositionResult<QueryRow>] = []

    func receive(
        _ event: CompositionQueryEvent,
        workMeter: DatabaseWorkMeter
    ) throws {
        switch event {
        case .began(let value):
            metadata = value
        case .row(let value):
            guard let metadata else {
                throw CompositionQueryError.workspaceCorrupted
            }
            try workMeter.recordOutputRows()
            values.append(
                CompositionResult(
                    composition: metadata.composition,
                    origin: value.origin,
                    value: value.row
                )
            )
        }
    }

    func result() -> [CompositionResult<QueryRow>] {
        values
    }
}

/// Read-only selector for one named or request-scoped Base Composition.
public struct CompositionDataSource: Sendable {
    public let selection: CompositionSelection
    package let container: DBContainer
    package let authorization: AuthorizationContext
    package let sourceIdentity: DatabaseCompositionSourceIdentity

    package init(
        selection: CompositionSelection,
        container: DBContainer,
        authorization: AuthorizationContext
    ) {
        self.selection = selection
        self.container = container
        self.authorization = authorization
        self.sourceIdentity = DatabaseCompositionSourceIdentity()
    }

    public func query<Model: Persistable>(
        _ type: Model.Type
    ) -> CompositionQueryExecutor<Model> {
        CompositionQueryExecutor(
            source: self,
            query: Query<Model>()
        )
    }

    /// Executes canonical relational QueryIR in-process through the same
    /// semantic planner used by remote adapters.
    public func execute(
        _ query: SelectQuery,
        options: ReadExecutionOptions = .default
    ) async throws -> [CompositionResult<QueryRow>] {
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: container.monotonicClock
        )
        let collector = CompositionRowResultCollector()
        try await CompositionQueryPlanner(
            structuralLimits: execution.queryStructuralLimits
        ).execute(
            query,
            source: self,
            options: CompositionQueryExecutionOptions(
                pageSize: try Self.plannerPageSize(options: options),
                readContext: execution
            )
        ) { event in
            try await collector.receive(
                event,
                workMeter: execution.workMeter
            )
            return true
        }
        return await collector.result()
    }

    /// Acquires the immutable Composition definition and every member Base
    /// lease. Holding all leases prevents a member from retiring while the
    /// caller authorizes or executes the federated operation.
    package func acquireLease() async throws -> DatabaseCompositionLease {
        let namedRecord: DatabaseCompositionRecord?
        let resolution: CompositionResolution
        switch selection.kind {
        case .named:
            guard let id = selection.namedID else {
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
            let record = try await container.withControlMetadataTransaction(
                configuration: .readOnly
            ) { transaction in
                guard let record = try await container.compositionCatalog.load(
                    id,
                    transaction: transaction.storageAccess
                ) else {
                    throw DatabaseCompositionAccessError.unavailable(selection)
                }
                return record
            }
            namedRecord = record
            do {
                resolution = try .named(
                    id: record.composition.id,
                    generation: record.generation,
                    bases: record.composition.bases
                )
            } catch {
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
        case .derived:
            guard let bases = selection.bases else {
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
            namedRecord = nil
            do { resolution = try .derived(bases) }
            catch {
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
        }
        var memberLeases: [DatabaseBaseLease] = []
        memberLeases.reserveCapacity(resolution.bases.count)
        do {
            for baseID in resolution.bases {
                memberLeases.append(try container.acquireBaseLease(baseID))
            }
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw DatabaseCompositionAccessError.unavailable(selection)
        }
        return DatabaseCompositionLease(
            selection: selection,
            resolution: resolution,
            namedRecord: namedRecord,
            members: memberLeases
        )
    }

    /// Acquires and authorizes every member for metadata-only resolution.
    package func acquireReadLease() async throws -> DatabaseCompositionLease {
        let lease = try await acquireLease()
        do {
            for member in lease.members {
                try await container.withBaseLease(member) {
                    let context = container.session(
                        authorization: authorization
                    ).base(member.baseID).newContext()
                    try await context.withReadStorageAccess(
                        configuration: .readOnly
                    ) { _ in () }
                }
            }
            return lease
        } catch is CancellationError {
            throw CancellationError()
        } catch is DatabaseGrantAuthorizationError {
            throw DatabaseCompositionAccessError.unavailable(selection)
        } catch {
            throw error
        }
    }

    /// Resolves a Composition for metadata operations while preserving the
    /// same all-member authorization contract as execution.
    @_spi(DatabaseExecution)
    public func resolve() async throws -> CompositionResolution {
        try await acquireReadLease().resolution
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
                    throw DatabaseCompositionAccessError.unavailable(selection)
                }
                let transaction = try domain.transactionExecutor
                    .createOwnedTransaction()
                // Register lifecycle ownership before configuration because
                // backend option application may fail synchronously.
                owned.append((id: domainID, transaction: transaction))
                _ = try TransactionConfiguration.readOnly.apply(
                    to: transaction
                )
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
                    throw DatabaseCompositionAccessError.unavailable(selection)
                }
                let position: DomainReadPoint.Position
                if domain.transactionCapabilities.readVersion {
                    guard let version = UInt64(
                        exactly: try await entry.transaction.getReadVersion()
                    ) else {
                        throw DatabaseCompositionAccessError.unavailable(selection)
                    }
                    position = .version(version)
                } else {
                    // This identifier is valid only for the lifetime of this
                    // exact in-process transaction snapshot. A host adapter
                    // must own any durable result paging after it closes.
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
                    throw DatabaseCompositionAccessError.unavailable(selection)
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
            let snapshot = try DatabaseCompositionReadSnapshot(
                lease: lease,
                sourceIdentity: sourceIdentity,
                authorization: authorization,
                transactions: transactions,
                readPoints: readPoints
            )
            let result: Result
            do {
                result = try await operation(snapshot)
            } catch {
                let operationError = error
                do {
                    try await snapshot.close()
                } catch let cleanupError as DatabaseReadScopeCleanupError {
                    throw cleanupError.preserving(
                        operationError: operationError
                    )
                }
                throw operationError
            }
            try await snapshot.close()
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
                throw DatabaseCompositionAccessError.unavailable(selection)
            }
            throw operationError
        }
    }

    /// Executes one member-local read against the transaction captured by the
    /// federated snapshot. The caller receives neither a container nor a
    /// transaction capable of resolving a different Base.
    @_spi(DatabaseExecution)
    public func withMemberContext<Result: Sendable>(
        _ member: DatabaseCompositionReadMember,
        in snapshot: DatabaseCompositionReadSnapshot,
        _ operation: @Sendable @escaping (
            DatabaseContext,
            any TransactionReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard snapshot.sourceIdentity === sourceIdentity,
              snapshot.selection == selection else {
            throw DatabaseCompositionAccessError.unavailable(selection)
        }
        let operationLease = try snapshot.operationScope.beginRead()
        defer { operationLease.finish() }
        let admittedMember = try snapshot.admittedMember(for: member)
        return try await container.withBaseLease(admittedMember.lease) {
            let context = container.session(
                authorization: snapshot.authorization
            ).base(member.baseID).newContext()
            let executionBinding = DatabaseTransactionExecutionBinding(
                identity: try DatabaseTransactionExecutionIdentity(
                    context: context
                ),
                transaction: admittedMember.transaction,
                grantedAccess: .read,
                accessMode: .readOnly,
                operationScope: snapshot.operationScope,
                resource: context.resource,
                authorization: snapshot.authorization,
                databaseTransaction: nil
            )
            return try await RequestAuthorization.$context.withValue(
                snapshot.authorization
            ) {
                try await ActiveDatabaseTransactionContext.$binding.withValue(
                    executionBinding
                ) {
                    try await operation(
                        context,
                        admittedMember.transaction.readProjection()
                    )
                }
            }
        }
    }

    private static func makeOpaqueReadPoint() -> ByteString {
        var generator = SystemRandomNumberGenerator()
        return ByteString((0..<32).map { _ in
            UInt8.random(in: .min ... .max, using: &generator)
        })
    }

    package static func plannerPageSize(
        options: ReadExecutionOptions
    ) throws -> Int {
        guard let maximumRows = Int(exactly: options.budget.maximumRows),
              let maximumIntermediateRows = Int(
                  exactly: options.budget.maximumIntermediateRows
              ) else {
            throw CompositionQueryError.invalidExecutionConfiguration(
                "row limits exceed the current runtime range"
            )
        }
        return max(1, min(maximumRows, maximumIntermediateRows / 4, 256))
    }
}

#endif
