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
        let fieldName = Owner.fieldName(for: keyPath)
        let loaded: (owner: Owner, related: Related?)? =
            try await withTransaction { transaction in
            guard let owner = try await self.load(
                reference,
                transaction: transaction
            ) else {
                return nil
            }
            guard let relatedReference = owner[
                dynamicMember: fieldName
            ] as? DatabaseReference<Related> else {
                return (owner, nil)
            }
            let related = try await self.load(
                relatedReference,
                transaction: transaction
            )
            return (owner, related)
        }
        guard let loaded else {
            return nil
        }
        return Snapshot(item: loaded.owner).with(
            keyPath,
            loadedAs: loaded.related
        )
    }

    /// Loads an owner and a required to-one relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: DatabaseReference<Owner>,
        joining keyPath: KeyPath<Owner, DatabaseReference<Related>>
    ) async throws -> Snapshot<Owner>? {
        let fieldName = Owner.fieldName(for: keyPath)
        let loaded: (owner: Owner, related: Related?)? =
            try await withTransaction { transaction in
            guard let owner = try await self.load(
                reference,
                transaction: transaction
            ) else {
                return nil
            }
            guard let relatedReference = owner[
                dynamicMember: fieldName
            ] as? DatabaseReference<Related> else {
                throw RelationshipReferenceError.invalidRelationshipValue(
                    entity: Owner.persistableType,
                    field: fieldName
                )
            }
            let related = try await self.load(
                relatedReference,
                transaction: transaction
            )
            return (owner, related)
        }
        guard let loaded else {
            return nil
        }
        return Snapshot(item: loaded.owner).with(
            keyPath,
            loadedAs: loaded.related
        )
    }

    /// Loads an owner and a to-many relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: DatabaseReference<Owner>,
        joining keyPath: KeyPath<Owner, [DatabaseReference<Related>]>
    ) async throws -> Snapshot<Owner>? {
        let fieldName = Owner.fieldName(for: keyPath)
        let loaded: (owner: Owner, related: [Related])? =
            try await withTransaction { transaction in
            guard let owner = try await self.load(
                reference,
                transaction: transaction
            ) else {
                return nil
            }
            guard let references = owner[
                dynamicMember: fieldName
            ] as? [DatabaseReference<Related>] else {
                throw RelationshipReferenceError.invalidRelationshipValue(
                    entity: Owner.persistableType,
                    field: fieldName
                )
            }
            var related: [Related] = []
            for relatedReference in references {
                if let model = try await self.load(
                    relatedReference,
                    transaction: transaction
                ) {
                    related.append(model)
                }
            }
            return (owner, related)
        }
        guard let loaded else {
            return nil
        }
        return Snapshot(item: loaded.owner).with(
            keyPath,
            loadedAs: loaded.related
        )
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
        let references = owner[keyPath: keyPath]
        return try await withTransaction { transaction in
            var models: [Related] = []
            for reference in references {
                if let model = try await self.load(
                    reference,
                    transaction: transaction
                ) {
                    models.append(model)
                }
            }
            return models
        }
    }
}
