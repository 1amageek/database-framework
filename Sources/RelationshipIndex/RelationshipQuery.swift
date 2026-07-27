import DatabaseKit
import DatabaseEngine
import DatabaseTypes

extension DatabaseContext {
    /// Loads an entity snapshot by its complete typed identity.
    public func get<Target: Persistable>(
        _ reference: PersistableReference<Target>
    ) async throws -> RelationshipSnapshot<Target>? {
        guard let model = try await model(for: reference) else {
            return nil
        }
        return RelationshipSnapshot(item: model)
    }

    /// Loads an owner and an optional to-one relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: PersistableReference<Owner>,
        joining field: Field<Owner, PersistableReference<Related>?>
    ) async throws -> RelationshipSnapshot<Owner>? {
        let loaded: (owner: Owner, related: Related?)? =
            try await withTransaction { transaction in
            guard let owner = try await self.load(
                reference,
                transaction: transaction
            ) else {
                return nil
            }
            guard let relatedReference = try optionalReference(
                from: owner,
                field: field
            ) else {
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
        return RelationshipSnapshot(item: loaded.owner).with(
            field,
            loadedAs: loaded.related
        )
    }

    /// Loads an owner and a required to-one relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: PersistableReference<Owner>,
        joining field: Field<Owner, PersistableReference<Related>>
    ) async throws -> RelationshipSnapshot<Owner>? {
        let loaded: (owner: Owner, related: Related?)? =
            try await withTransaction { transaction in
            guard let owner = try await self.load(
                reference,
                transaction: transaction
            ) else {
                return nil
            }
            let relatedReference = try requiredReference(
                from: owner,
                field: field
            )
            let related = try await self.load(
                relatedReference,
                transaction: transaction
            )
            return (owner, related)
        }
        guard let loaded else {
            return nil
        }
        return RelationshipSnapshot(item: loaded.owner).with(
            field,
            loadedAs: loaded.related
        )
    }

    /// Loads an owner and a to-many relationship at one read version.
    public func get<Owner: Persistable, Related: Persistable>(
        _ reference: PersistableReference<Owner>,
        joining field: Field<Owner, [PersistableReference<Related>]>
    ) async throws -> RelationshipSnapshot<Owner>? {
        let loaded: (owner: Owner, related: [Related])? =
            try await withTransaction { transaction in
            guard let owner = try await self.load(
                reference,
                transaction: transaction
            ) else {
                return nil
            }
            let references = try referenceArray(
                from: owner,
                field: field
            )
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
        return RelationshipSnapshot(item: loaded.owner).with(
            field,
            loadedAs: loaded.related
        )
    }

    public func related<Owner: Persistable, Related: Persistable>(
        _ owner: Owner,
        _ field: Field<Owner, PersistableReference<Related>?>
    ) async throws -> Related? {
        guard let reference = try optionalReference(
            from: owner,
            field: field
        ) else {
            return nil
        }
        return try await model(for: reference)
    }

    public func related<Owner: Persistable, Related: Persistable>(
        _ owner: Owner,
        _ field: Field<Owner, PersistableReference<Related>>
    ) async throws -> Related? {
        try await model(
            for: requiredReference(from: owner, field: field)
        )
    }

    public func related<Owner: Persistable, Related: Persistable>(
        _ owner: Owner,
        _ field: Field<Owner, [PersistableReference<Related>]>
    ) async throws -> [Related] {
        let references = try referenceArray(from: owner, field: field)
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

private func optionalReference<Owner: Persistable, Related: Persistable>(
    from owner: Owner,
    field: Field<Owner, PersistableReference<Related>?>
) throws -> PersistableReference<Related>? {
    guard let value = try owner.persistedFieldValue(for: field.identity) else {
        throw RelationshipReferenceError.missingRelationshipField(
            entity: Owner.persistableType,
            field: field.name
        )
    }
    if value == .null {
        return nil
    }
    return try decodedReference(
        from: value,
        owner: Owner.persistableType,
        field: field.name
    )
}

private func requiredReference<Owner: Persistable, Related: Persistable>(
    from owner: Owner,
    field: Field<Owner, PersistableReference<Related>>
) throws -> PersistableReference<Related> {
    guard let value = try owner.persistedFieldValue(for: field.identity) else {
        throw RelationshipReferenceError.missingRelationshipField(
            entity: Owner.persistableType,
            field: field.name
        )
    }
    return try decodedReference(
        from: value,
        owner: Owner.persistableType,
        field: field.name
    )
}

private func referenceArray<Owner: Persistable, Related: Persistable>(
    from owner: Owner,
    field: Field<Owner, [PersistableReference<Related>]>
) throws -> [PersistableReference<Related>] {
    guard let value = try owner.persistedFieldValue(for: field.identity) else {
        throw RelationshipReferenceError.missingRelationshipField(
            entity: Owner.persistableType,
            field: field.name
        )
    }
    guard case .array(let values) = value else {
        throw RelationshipReferenceError.invalidRelationshipValue(
            entity: Owner.persistableType,
            field: field.name
        )
    }
    var references: [PersistableReference<Related>] = []
    references.reserveCapacity(values.count)
    for value in values {
        references.append(
            try decodedReference(
                from: value,
                owner: Owner.persistableType,
                field: field.name
            )
        )
    }
    return references
}

private func decodedReference<Related: Persistable>(
    from value: FieldValue,
    owner: String,
    field: String
) throws -> PersistableReference<Related> {
    guard case .reference(let identity) = value else {
        throw RelationshipReferenceError.invalidRelationshipValue(
            entity: owner,
            field: field
        )
    }
    return try PersistableReference(identity: identity)
}
