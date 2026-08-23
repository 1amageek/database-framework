#if DATABASE_MULTI_BASE
import DatabaseKit
import StorageKit

public enum CompositionDecisionError: Error, Sendable, Equatable {
    case baseNotMember(Base.ID)
    case multipleStorageDomains
}

/// One same-domain decision transaction: reads may select any member Base,
/// while every mutation remains bound to the explicitly selected writer Base.
public final class CompositionDecisionTransaction: Sendable {
    public let composition: CompositionResolution
    public let writerBaseID: Base.ID

    private let container: DBContainer
    private let authorization: AuthorizationContext
    private let lease: DatabaseCompositionLease
    private let domainStorageAccess: any TransactionAccess
    private let writerTransaction: DatabaseTransaction
    private let operationGate = TransactionOperationGate()

    package init(
        container: DBContainer,
        authorization: AuthorizationContext,
        lease: DatabaseCompositionLease,
        writerBaseID: Base.ID,
        domainStorageAccess: any TransactionAccess,
        writerTransaction: DatabaseTransaction
    ) {
        self.composition = lease.resolution
        self.writerBaseID = writerBaseID
        self.container = container
        self.authorization = authorization
        self.lease = lease
        self.domainStorageAccess = domainStorageAccess
        self.writerTransaction = writerTransaction
    }

    /// Reads one member through the same physical transaction used by writes.
    public func fetch<Model: Persistable>(
        _ query: Query<Model>,
        from baseID: Base.ID
    ) async throws -> [Model] {
        try operationGate.enter()
        defer { operationGate.leave() }
        guard let member = lease.member(identifiedBy: baseID) else {
            throw CompositionDecisionError.baseNotMember(baseID)
        }
        return try await container.withBaseLease(member) {
            let context = container.session(
                authorization: authorization
            ).base(baseID).newContext()
            guard let baseBinding = ActiveDatabaseBaseContext.binding else {
                throw DatabaseCompositionAccessError.unavailable(
                    lease.selection
                )
            }
            let memberAccess = DataRootTransactionAccess.admitted(
                domainStorageAccess,
                dataRoot: member.root,
                accessMode: .readOnly,
                readScope: baseBinding.operationScope
            )
            defer { memberAccess.revoke() }
            let executionBinding = DatabaseTransactionExecutionBinding(
                identity: try DatabaseTransactionExecutionIdentity(
                    context: context
                ),
                transaction: memberAccess,
                grantedAccess: .read,
                accessMode: .readOnly,
                operationScope: baseBinding.operationScope,
                resource: context.resource,
                authorization: authorization,
                databaseTransaction: nil
            )
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                try await ActiveDatabaseTransactionContext.$binding.withValue(
                    executionBinding
                ) {
                    try await context.fetch(
                        query,
                        transaction: memberAccess.readProjection()
                    )
                }
            }
        }
    }

    public func save<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition = .none
    ) async throws {
        try operationGate.enter()
        defer { operationGate.leave() }
        try await writerTransaction.save(model, precondition: precondition)
    }

    public func delete<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition = .exists
    ) async throws {
        try operationGate.enter()
        defer { operationGate.leave() }
        try await writerTransaction.delete(model, precondition: precondition)
    }

    package func close() async {
        await operationGate.closeAndWait()
    }
}

public extension CompositionDataSource {
    /// Executes a read-decide-write operation only when all selected Bases and
    /// the writer share one physical transaction domain.
    func withDecisionTransaction<Result: Sendable>(
        writingTo writerBaseID: Base.ID,
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            CompositionDecisionTransaction
        ) async throws -> Result
    ) async throws -> Result {
        try await container.withOperationSchemaLease { _ in
            let lease = try await acquireLease()
            guard let writer = lease.member(identifiedBy: writerBaseID) else {
                throw CompositionDecisionError.baseNotMember(writerBaseID)
            }
            guard lease.members.allSatisfy({
                $0.domainID == writer.domainID
            }) else {
                throw CompositionDecisionError.multipleStorageDomains
            }
            let runner = TransactionRunner(
                transactionExecutor: writer.transactionExecutor,
                clock: container.monotonicClock,
                logging: container.configuration.logging,
                metrics: container.configuration.metrics
            )
            return try await runner.run(
                configuration: configuration,
                operationDescription: "Composition decision transaction"
            ) { domainStorageAccess in
                for member in lease.members {
                    try await DatabaseGrantStore(
                        resource: .base(member.baseID),
                        root: member.root
                    ).require(
                        .read,
                        authorization: authorization,
                        transaction: domainStorageAccess
                    )
                }
                try await DatabaseGrantStore(
                    resource: .base(writer.baseID),
                    root: writer.root
                ).require(
                    .write,
                    authorization: authorization,
                    transaction: domainStorageAccess
                )
                return try await container.withBaseLease(writer) {
                    try await RequestAuthorization.$context.withValue(
                        authorization
                    ) {
                        try await container.withRootScopedDatabaseTransaction(
                            storageAccess: domainStorageAccess,
                            dataRoot: writer.root,
                            accessMode: .readWrite
                        ) { writerTransaction in
                            let decision = CompositionDecisionTransaction(
                                container: container,
                                authorization: authorization,
                                lease: lease,
                                writerBaseID: writerBaseID,
                                domainStorageAccess: domainStorageAccess,
                                writerTransaction: writerTransaction
                            )
                            do {
                                let result = try await operation(decision)
                                await decision.close()
                                return result
                            } catch {
                                await decision.close()
                                throw error
                            }
                        }
                    }
                }
            }
        }
    }
}

#endif
