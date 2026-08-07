import DatabaseKit
import DatabaseTypes
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
    private let operationGate: TransactionOperationGate
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
        self.operationGate = TransactionOperationGate()
        self.storageAccess = storageAccess
    }

    public func fetch(
        _ identity: EntityReference
    ) async throws -> PersistedModel? {
        try await perform {
            try await transaction.fetchPersistedModel(
                identifiedBy: identity,
                within: operationID
            )
        }
    }

    public func save(
        _ model: PersistedModel,
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

    /// Saves a statically typed model through the same canonical transaction
    /// boundary used by heterogeneous mutation maintainers.
    public func save<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition = .none
    ) async throws {
        try await save(
            PersistedModel(model),
            precondition: precondition
        )
    }

    public func delete(
        _ model: PersistedModel,
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

    /// Deletes a statically typed model through the same canonical transaction
    /// boundary used by heterogeneous mutation maintainers.
    public func delete<Model: Persistable>(
        _ model: Model,
        precondition: WritePrecondition = .exists
    ) async throws {
        try await delete(
            PersistedModel(model),
            precondition: precondition
        )
    }

    public func isDeletionScheduled(
        for identity: EntityReference
    ) async throws -> Bool {
        try await perform {
            try await transaction.isDeletionScheduled(
                for: identity,
                within: operationID
            )
        }
    }

    package func closeAndWait() async {
        await operationGate.closeAndWait()
    }

    private func perform<Value: Sendable>(
        _ operation: () async throws -> Value
    ) async throws -> Value {
        try operationGate.enter()
        do {
            try ensureDatabaseTaskIsActive()
            let value = try await operation()
            operationGate.leave()
            return value
        } catch {
            operationGate.leave()
            throw error
        }
    }
}
