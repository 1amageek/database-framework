import Core
import DatabaseValue
import StorageKit

/// Persistence capabilities available while maintaining derived transaction
/// state for a model mutation.
///
/// Nested mutations re-enter the owning `DatabaseTransaction` with the same
/// operation identity, so they participate in the same lifecycle, validation,
/// and physical transaction.
public struct PersistableMutationContext: ~Copyable, Sendable {
    public let schema: Schema

    private let transaction: DatabaseTransaction
    private let operationID: UInt64
    private let scope: DatabaseTransactionScope
    package let storageAccess: any TransactionAccess

    package init(
        schema: Schema,
        transaction: DatabaseTransaction,
        operationID: UInt64,
        storageAccess: any TransactionAccess
    ) {
        self.schema = schema
        self.transaction = transaction
        self.operationID = operationID
        self.scope = DatabaseTransactionScope()
        self.storageAccess = storageAccess
    }

    deinit {}

    public func fetch(
        _ identity: PersistableIdentity
    ) async throws -> (any Persistable)? {
        try await perform {
            try await transaction.fetchPersistedModel(
                identifiedBy: identity,
                within: operationID
            )
        }
    }

    public func save(
        _ model: any Persistable,
        precondition: WritePrecondition = .none
    ) async throws {
        try await perform {
            try await transaction.savePersistedModel(
                model,
                precondition: precondition,
                within: operationID
            )
        }
    }

    public func delete(
        _ model: any Persistable,
        precondition: WritePrecondition = .exists
    ) async throws {
        try await perform {
            try await transaction.deletePersistedModel(
                model,
                precondition: precondition,
                within: operationID
            )
        }
    }

    public func isDeletionScheduled(
        for identity: PersistableIdentity
    ) async throws -> Bool {
        try await perform {
            try await transaction.isDeletionScheduled(
                for: identity,
                within: operationID
            )
        }
    }

    package func closeAndWait() async {
        await scope.closeAndWait()
    }

    private func perform<Value: Sendable>(
        _ operation: () async throws -> Value
    ) async throws -> Value {
        try await scope.enter()
        do {
            try Task.checkCancellation()
            let value = try await operation()
            await scope.leave()
            return value
        } catch {
            await scope.leave()
            throw error
        }
    }
}
