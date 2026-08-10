import DatabaseEngine
import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

/// Executes persisted database and Base Grant operations.
public struct GrantExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = GrantExecuteOperation

    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let timeoutMilliseconds: UInt32

    public init(
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseRuntimeLimits = .default
    ) {
        self.coordinator = coordinator
        self.timeoutMilliseconds = runtimeLimits.maximumTimeoutMilliseconds
    }

    public func requirement(
        for request: GrantExecuteOperation.Request
    ) throws -> DatabaseOperationRequirement {
        _ = request
        return DatabaseOperationRequirement(
            acceptedTargets: [.database, .base],
            access: .administer,
            transaction: .write,
            baseAdmission: .administration
        )
    }

    public func invoke(
        request: GrantExecuteOperation.Request,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) async throws -> DatabaseOperationResult {
        switch request.invocation {
        case .direct(let subject):
            let grants = try await withAuthorizedTransaction(
                context: context,
                configuration: .readOnly
            ) { store, transaction in
                try await store.direct(
                    subject: subject,
                    transaction: transaction.storageAccess
                )
            }
            return DatabaseOperationResult(
                GrantExecuteOperation.self,
                response: .direct(
                    GrantExecuteOperation.DirectGrantSet(
                        revision: grants.revision,
                        grants: grants.grants
                    )
                )
            )

        case .effective(let subject):
            let effective = try await withAuthorizedTransaction(
                context: context,
                configuration: .readOnly
            ) { store, transaction in
                switch subject {
                case .principal(let identifier):
                    return try await store.effective(
                        principal: Principal(identifier: identifier),
                        transaction: transaction.storageAccess
                    )
                case .principalRole(_):
                    let direct = try await store.direct(
                        subject: subject,
                        transaction: transaction.storageAccess
                    )
                    return DatabaseEffectiveGrant(
                        access: direct.grants.reduce(into: Security.Access()) {
                            $0.formUnion($1.access)
                        },
                        contributors: direct.grants
                    )
                }
            }
            return DatabaseOperationResult(
                GrantExecuteOperation.self,
                response: .effective(
                    GrantExecuteOperation.EffectiveGrantSet(
                        access: effective.access,
                        contributors: effective.contributors
                    )
                )
            )

        case .grant(
            let grant,
            let expectedRevision,
            let idempotencyKey
        ):
            try requireResource(grant.resource, context: context)
            try requireIdempotencyKey(idempotencyKey, context: context)
            return try await coordinator.execute(
                operation: .grantExecute,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: timeoutMilliseconds
            ) { transaction in
                try await store(context: context).grant(
                    grant,
                    expectedRevision: expectedRevision,
                    transaction: transaction.storageAccess
                )
            } makeResponse: { revision, _ in
                DatabaseOperationResponseEncoder(
                    GrantExecuteOperation.self,
                    response: .mutated(revision: revision)
                )
            }.result

        case .revoke(
            let grant,
            let expectedRevision,
            let idempotencyKey
        ):
            try requireResource(grant.resource, context: context)
            try requireIdempotencyKey(idempotencyKey, context: context)
            return try await coordinator.execute(
                operation: .grantExecute,
                requestPayload: context.requestPayload,
                context: context,
                timeoutMilliseconds: timeoutMilliseconds
            ) { transaction in
                try await store(context: context).revoke(
                    grant,
                    expectedRevision: expectedRevision,
                    transaction: transaction.storageAccess
                )
            } makeResponse: { revision, _ in
                DatabaseOperationResponseEncoder(
                    GrantExecuteOperation.self,
                    response: .mutated(revision: revision)
                )
            }.result
        }
    }

    private func withAuthorizedTransaction<Result: Sendable>(
        context: DatabaseOperationContext,
        configuration: TransactionConfiguration,
        _ operation: @Sendable @escaping (
            DatabaseGrantStore,
            DatabaseTransaction
        ) async throws -> Result
    ) async throws -> Result {
        switch context.target {
        case .database:
            let executor = try context.requireControlExecutor()
            return try await executor.withTransaction(
                requiredAccess: .administer,
                configuration: configuration
            ) { transaction in
                try await operation(
                    executor.grantStore,
                    transaction
                )
            }
        case .base:
            let executor = try context.requireBaseExecutor()
            return try await executor.withAdministrationTransaction(
                requiredAccess: .administer,
                configuration: configuration
            ) { transaction in
                try await operation(try executor.grantStore(), transaction)
            }
        case .composition:
            throw DatabaseAdministrationError.targetMismatch(context.target)
        }
    }

    private func store(
        context: DatabaseOperationContext
    ) throws -> DatabaseGrantStore {
        switch context.target {
        case .database:
            return try context.requireControlExecutor().grantStore
        case .base:
            return try context.requireBaseExecutor().grantStore()
        case .composition:
            throw DatabaseAdministrationError.targetMismatch(context.target)
        }
    }

    private func requireResource(
        _ actual: Security.Resource,
        context: DatabaseOperationContext
    ) throws {
        let expected: Security.Resource
        switch context.target {
        case .database:
            expected = .database
        case .base(let id):
            expected = .base(id)
        case .composition:
            throw DatabaseAdministrationError.targetMismatch(context.target)
        }
        guard actual == expected else {
            throw DatabaseAdministrationError.grantResourceMismatch(
                expected: expected,
                actual: actual
            )
        }
    }

    private func requireIdempotencyKey(
        _ key: String,
        context: DatabaseOperationContext
    ) throws {
        guard context.metadata.idempotencyKey == key else {
            throw DatabaseAdministrationError.idempotencyKeyMismatch
        }
    }
}
