import Core
import DatabaseEngine
import Relationship

extension FDBContext {
    /// Creates a typed reference containing the model's complete persisted identity.
    public func reference<Target: Persistable>(
        to target: Target
    ) throws -> DatabaseReference<Target> {
        try DatabaseReference(
            identity: PersistableIdentityEncoder.encode(target)
        )
    }

    /// Loads a typed reference in a new transaction.
    public func model<Target: Persistable>(
        for reference: DatabaseReference<Target>
    ) async throws -> Target? {
        try await withTransaction { transaction in
            try await self.load(reference, transaction: transaction)
        }
    }

    package func load<Target: Persistable>(
        _ reference: DatabaseReference<Target>,
        transaction: DatabaseTransaction
    ) async throws -> Target? {
        guard let model = try await transaction.fetchPersistedModel(
            identifiedBy: reference.identity
        ) else {
            return nil
        }
        guard let target = model as? Target else {
            throw RelationshipReferenceError.loadedTypeMismatch(
                expected: Target.persistableType,
                actual: type(of: model).persistableType
            )
        }
        return target
    }
}
