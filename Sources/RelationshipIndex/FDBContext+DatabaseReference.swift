import Core
import DatabaseEngine
import Relationship
import StorageKit

extension FDBContext {
    /// Creates a typed reference containing the model's complete persisted identity.
    public func reference<Target: Persistable>(
        to target: Target
    ) throws -> DatabaseReference<Target> {
        try DatabaseReference(
            identity: DatabaseRecordIdentityEncoder.encode(target)
        )
    }

    /// Loads a typed reference in a new transaction.
    public func model<Target: Persistable>(
        for reference: DatabaseReference<Target>
    ) async throws -> Target? {
        let handler = makePersistenceHandler()
        return try await container.engine.withTransaction { transaction in
            try await load(
                reference,
                transaction: transaction,
                handler: handler
            )
        }
    }

    package func load<Target: Persistable>(
        _ reference: DatabaseReference<Target>,
        transaction: any Transaction,
        handler: ModelPersistenceHandler
    ) async throws -> Target? {
        let resolved = try CanonicalRelationshipIdentity.resolve(
            reference.identity,
            container: container
        )
        guard let model = try await handler.load(
            reference.identity.entity,
            id: resolved.id,
            partition: resolved.partition,
            transaction: transaction
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
