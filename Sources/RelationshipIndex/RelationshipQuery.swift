import Core
import DatabaseEngine
import Relationship

extension FDBContext {
    /// Loads a record snapshot by its complete typed identity.
    public func get<Target: Persistable>(
        _ reference: DatabaseReference<Target>
    ) async throws -> Snapshot<Target>? {
        guard let model = try await model(for: reference) else {
            return nil
        }
        return Snapshot(item: model)
    }

    /// Loads an owner and an optional to-one relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: DatabaseReference<Owner>,
        joining keyPath: KeyPath<Owner, DatabaseReference<Related>?>
    ) async throws -> Snapshot<Owner>? {
        let handler = makePersistenceHandler()
        return try await container.engine.withTransaction { transaction in
            guard let owner = try await load(
                reference,
                transaction: transaction,
                handler: handler
            ) else {
                return nil
            }
            guard let relatedReference = owner[keyPath: keyPath] else {
                return Snapshot(item: owner)
            }
            let related = try await load(
                relatedReference,
                transaction: transaction,
                handler: handler
            )
            return Snapshot(item: owner).with(keyPath, loadedAs: related)
        }
    }

    /// Loads an owner and a required to-one relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: DatabaseReference<Owner>,
        joining keyPath: KeyPath<Owner, DatabaseReference<Related>>
    ) async throws -> Snapshot<Owner>? {
        let handler = makePersistenceHandler()
        return try await container.engine.withTransaction { transaction in
            guard let owner = try await load(
                reference,
                transaction: transaction,
                handler: handler
            ) else {
                return nil
            }
            let related = try await load(
                owner[keyPath: keyPath],
                transaction: transaction,
                handler: handler
            )
            return Snapshot(item: owner).with(keyPath, loadedAs: related)
        }
    }

    /// Loads an owner and a to-many relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: DatabaseReference<Owner>,
        joining keyPath: KeyPath<Owner, [DatabaseReference<Related>]>
    ) async throws -> Snapshot<Owner>? {
        let handler = makePersistenceHandler()
        return try await container.engine.withTransaction { transaction in
            guard let owner = try await load(
                reference,
                transaction: transaction,
                handler: handler
            ) else {
                return nil
            }
            var related: [Related] = []
            for relatedReference in owner[keyPath: keyPath] {
                if let model = try await load(
                    relatedReference,
                    transaction: transaction,
                    handler: handler
                ) {
                    related.append(model)
                }
            }
            return Snapshot(item: owner).with(keyPath, loadedAs: related)
        }
    }

    public func related<Owner: Persistable, Related: Persistable>(
        _ owner: Owner,
        _ keyPath: KeyPath<Owner, DatabaseReference<Related>?>
    ) async throws -> Related? {
        guard let reference = owner[keyPath: keyPath] else {
            return nil
        }
        return try await model(for: reference)
    }

    public func related<Owner: Persistable, Related: Persistable>(
        _ owner: Owner,
        _ keyPath: KeyPath<Owner, DatabaseReference<Related>>
    ) async throws -> Related? {
        try await model(for: owner[keyPath: keyPath])
    }

    public func related<Owner: Persistable, Related: Persistable>(
        _ owner: Owner,
        _ keyPath: KeyPath<Owner, [DatabaseReference<Related>]>
    ) async throws -> [Related] {
        let handler = makePersistenceHandler()
        return try await container.engine.withTransaction { transaction in
            var models: [Related] = []
            for reference in owner[keyPath: keyPath] {
                if let model = try await load(
                    reference,
                    transaction: transaction,
                    handler: handler
                ) {
                    models.append(model)
                }
            }
            return models
        }
    }
}
