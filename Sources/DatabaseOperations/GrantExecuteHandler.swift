import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseEngine
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire
import StorageKit

/// Executes persisted database and Base Grant operations.
public struct GrantExecuteHandler: DatabaseOperationEndpointHandler {
    public typealias Operation = GrantExecuteOperation

    private let coordinator: DatabaseTransactionalOperationCoordinator
    private let timeoutMilliseconds: UInt32

    public init(
        coordinator: DatabaseTransactionalOperationCoordinator,
        runtimeLimits: DatabaseOperationLimits = .default
    ) {
        self.coordinator = coordinator
        self.timeoutMilliseconds = runtimeLimits.maximumTimeoutMilliseconds
    }

    public func requirement(
        for request: GrantExecuteOperation.Request
    ) throws -> DatabaseOperationRequirement {
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        let acceptedTargets: DatabaseOperationTargetKinds = [
            .database,
            .base,
        ]
        #else
        let acceptedTargets: DatabaseOperationTargetKinds = .database
        #endif
        let access: Security.Access
        let transaction: DatabaseOperationTransactionKind
        switch request.invocation {
        case .effective:
            access = .read
            transaction = .read
        case .direct:
            access = .administer
            transaction = .read
        case .grant, .revoke:
            access = .administer
            transaction = .write
        }
        return DatabaseOperationRequirement(
            acceptedTargets: acceptedTargets,
            access: access,
            transaction: transaction,
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
                requiredAccess: .administer,
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

        case .effective:
            guard let principal = context.authorization.principal else {
                throw DatabaseGrantAuthorizationError.unauthenticated
            }
            let effective = try await withAuthorizedTransaction(
                context: context,
                requiredAccess: .read,
                configuration: .readOnly
            ) { store, transaction in
                try await store.effective(
                    principal: principal,
                    transaction: transaction.storageAccess
                )
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
        requiredAccess: Security.Access,
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
                requiredAccess: requiredAccess,
                configuration: configuration
            ) { transaction in
                try await operation(
                    executor.grantStore,
                    transaction
                )
            }
        case .base:
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            let executor = try context.requireBaseExecutor()
            return try await executor.withAdministrationTransaction(
                requiredAccess: requiredAccess,
                configuration: configuration
            ) { transaction in
                try await operation(try executor.grantStore(), transaction)
            }
            #else
            throw DatabaseAdministrationError.targetMismatch(context.target)
            #endif
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
            #if DATABASE_OPERATIONS_MULTIPLE_BASES
            return try context.requireBaseExecutor().grantStore()
            #else
            throw DatabaseAdministrationError.targetMismatch(context.target)
            #endif
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
