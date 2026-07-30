import DatabaseKit
import DatabaseEngine
import DatabaseTypes

extension DatabaseContext {
    /// Creates a statically typed reference containing the model's complete persisted identity.
    public func reference<Target: Persistable>(
        to target: Target
    ) throws -> PersistableReference<Target> {
        try PersistableReference(
            identity: EntityReferenceEncoder.encode(target)
        )
    }

    /// Loads a reference as the explicitly selected model type.
    public func model<Target: Persistable>(
        for reference: EntityReference,
        as targetType: Target.Type
    ) async throws -> Target? {
        try await withTransaction { transaction in
            try await self.load(
                reference,
                as: targetType,
                transaction: transaction
            )
        }
    }

    /// Loads a statically typed persistable reference in a new transaction.
    public func model<Target: Persistable>(
        for reference: PersistableReference<Target>
    ) async throws -> Target? {
        try await model(
            for: reference.persistableIdentity,
            as: Target.self
        )
    }

    package func load<Target: Persistable>(
        _ reference: EntityReference,
        as targetType: Target.Type,
        transaction: DatabaseTransaction
    ) async throws -> Target? {
        guard let model = try await transaction.fetchPersistedModel(
            identifiedBy: reference
        ) else {
            return nil
        }
        guard model.entity == Target.persistableType else {
            throw RelationshipReferenceError.loadedTypeMismatch(
                expected: Target.persistableType,
                actual: model.entity
            )
        }
        return try model.decode(as: Target.self)
    }

    package func load<Target: Persistable>(
        _ reference: PersistableReference<Target>,
        transaction: DatabaseTransaction
    ) async throws -> Target? {
        try await load(
            reference.persistableIdentity,
            as: Target.self,
            transaction: transaction
        )
    }
}
