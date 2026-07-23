import Core
import DatabaseValue
import Relationship

/// Applies delete-rule projections to compiled entities through the canonical entity codec.
public struct RelationshipEntityEditor: Sendable {
    private let resolver: RelationshipReferenceResolver

    public init(schema: Schema) {
        self.resolver = RelationshipReferenceResolver(schema: schema)
    }

    public func removingReference(
        to target: PersistableIdentity,
        from model: any Persistable,
        descriptor: RelationshipDescriptor
    ) throws -> any Persistable {
        let modelType = type(of: model)
        var fields = try PersistableFieldEncoder.encode(model)
        guard let fieldIndex = fields.firstIndex(where: {
            $0.name == descriptor.propertyName
        }), let fieldSchema = modelType.fieldSchemas.first(where: {
            $0.name == descriptor.propertyName
        }) else {
            throw RelationshipReferenceError.missingRelationshipField(
                entity: modelType.persistableType,
                field: descriptor.propertyName
            )
        }

        let replacement: DatabaseValue
        if descriptor.isToMany {
            guard case .array(let values) = fields[fieldIndex].value else {
                throw RelationshipReferenceError.invalidRelationshipValue(
                    entity: modelType.persistableType,
                    field: descriptor.propertyName
                )
            }
            replacement = .array(
                try values.filter { value in
                    try resolver.identity(
                        from: value,
                        descriptor: descriptor
                    ) != target
                }
            )
        } else {
            guard fieldSchema.isOptional else {
                throw RelationshipReferenceError.nullifyRequiresOptionalField(
                    entity: modelType.persistableType,
                    field: descriptor.propertyName
                )
            }
            replacement = .null
        }

        fields[fieldIndex] = DatabaseObjectField(
            number: fields[fieldIndex].number,
            name: fields[fieldIndex].name,
            value: replacement
        )
        do {
            return try modelType.decodePersistedFields(fields)
        } catch {
            throw RelationshipReferenceError.entityDecodingFailed(
                entity: modelType.persistableType,
                reason: String(describing: error)
            )
        }
    }
}
