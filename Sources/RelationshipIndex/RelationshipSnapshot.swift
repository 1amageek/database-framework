import DatabaseKit

/// A persisted model and the relationships loaded at the same read version.
@dynamicMemberLookup
public struct RelationshipSnapshot<Model: Persistable>: Sendable {
    public let item: Model
    var loadedRelationships: [String: LoadedRelationship]

    public init(item: Model) {
        self.item = item
        self.loadedRelationships = [:]
    }

    init(
        item: Model,
        loadedRelationships: [String: LoadedRelationship]
    ) {
        self.item = item
        self.loadedRelationships = loadedRelationships
    }

    public subscript<Value>(dynamicMember keyPath: KeyPath<Model, Value>) -> Value {
        item[keyPath: keyPath]
    }

    public func ref<Related: Persistable>(
        _ field: Field<Model, PersistableReference<Related>?>
    ) throws(RelationshipSnapshotError) -> Related? {
        try loadedToOne(
            fieldName: field.name,
            as: Related.self,
            expectedCardinality: .optionalToOne
        )
    }

    public func ref<Related: Persistable>(
        _ field: Field<Model, PersistableReference<Related>>
    ) throws(RelationshipSnapshotError) -> Related? {
        try loadedToOne(
            fieldName: field.name,
            as: Related.self,
            expectedCardinality: .requiredToOne
        )
    }

    public func refs<Related: Persistable>(
        _ field: Field<Model, [PersistableReference<Related>]>
    ) throws(RelationshipSnapshotError) -> [Related] {
        let fieldName = field.name
        guard let loaded = loadedRelationships[fieldName] else {
            throw .relationNotLoaded(
                owner: Model.persistableType,
                field: fieldName
            )
        }
        guard case let .toMany(value) = loaded else {
            throw .cardinalityMismatch(
                owner: Model.persistableType,
                field: fieldName,
                expected: .toMany
            )
        }
        guard let related = value as? [Related] else {
            throw .relatedTypeMismatch(
                owner: Model.persistableType,
                field: fieldName,
                expected: Related.persistableType,
                actual: loaded.relatedTypeName
            )
        }
        return related
    }

    func with<Related: Persistable>(
        _ field: Field<Model, PersistableReference<Related>?>,
        loadedAs value: Related?
    ) -> RelationshipSnapshot<Model> {
        replacingRelationship(
            named: field.name,
            with: value.map(LoadedRelationship.toOne) ?? .absentToOne
        )
    }

    func with<Related: Persistable>(
        _ field: Field<Model, PersistableReference<Related>>,
        loadedAs value: Related?
    ) -> RelationshipSnapshot<Model> {
        replacingRelationship(
            named: field.name,
            with: value.map(LoadedRelationship.toOne) ?? .absentToOne
        )
    }

    func with<Related: Persistable>(
        _ field: Field<Model, [PersistableReference<Related>]>,
        loadedAs value: [Related]
    ) -> RelationshipSnapshot<Model> {
        replacingRelationship(
            named: field.name,
            with: .toMany(value)
        )
    }

    private func loadedToOne<Related: Persistable>(
        fieldName: String,
        as relatedType: Related.Type,
        expectedCardinality: RelationshipCardinality
    ) throws(RelationshipSnapshotError) -> Related? {
        guard let loaded = loadedRelationships[fieldName] else {
            throw .relationNotLoaded(
                owner: Model.persistableType,
                field: fieldName
            )
        }
        switch loaded {
        case .absentToOne:
            return nil
        case let .toOne(value):
            guard let related = value as? Related else {
                throw .relatedTypeMismatch(
                    owner: Model.persistableType,
                    field: fieldName,
                    expected: Related.persistableType,
                    actual: type(of: value).persistableType
                )
            }
            return related
        case .toMany:
            throw .cardinalityMismatch(
                owner: Model.persistableType,
                field: fieldName,
                expected: expectedCardinality
            )
        }
    }

    private func replacingRelationship(
        named fieldName: String,
        with value: LoadedRelationship
    ) -> RelationshipSnapshot<Model> {
        var updated = loadedRelationships
        updated[fieldName] = value
        return RelationshipSnapshot(
            item: item,
            loadedRelationships: updated
        )
    }
}

extension RelationshipSnapshot: Identifiable where Model: Identifiable {
    public var id: Model.ID {
        item.id
    }
}

extension RelationshipSnapshot: CustomStringConvertible {
    public var description: String {
        let relationSummary = loadedRelationships.isEmpty
            ? ""
            : ", \(loadedRelationships.count) relation(s) loaded"
        return "RelationshipSnapshot<\(Model.persistableType)>(\(item.id)\(relationSummary))"
    }
}

extension RelationshipSnapshot: CustomDebugStringConvertible {
    public var debugDescription: String {
        var lines = ["RelationshipSnapshot<\(Model.persistableType)>", "  item: \(item)"]
        if !loadedRelationships.isEmpty {
            lines.append("  relationships:")
            for (name, value) in loadedRelationships {
                lines.append("    \(name): \(value)")
            }
        }
        return lines.joined(separator: "\n")
    }
}

enum LoadedRelationship: Sendable, CustomStringConvertible {
    case absentToOne
    case toOne(any Persistable)
    case toMany(any Sendable)

    var relatedTypeName: String {
        switch self {
        case .absentToOne:
            return "none"
        case let .toOne(value):
            return type(of: value).persistableType
        case .toMany:
            return "collection"
        }
    }

    var description: String {
        switch self {
        case .absentToOne:
            return "absent"
        case .toOne:
            return "loaded entity"
        case .toMany:
            return "loaded collection"
        }
    }
}
