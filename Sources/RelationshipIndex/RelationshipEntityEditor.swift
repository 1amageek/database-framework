import DatabaseKit
import DatabaseTypes

/// Applies delete-rule projections to compiled entities through the canonical entity codec.
public struct RelationshipEntityEditor: Sendable {
    private let resolver: RelationshipReferenceResolver

    public init(schema: Schema) {
        self.resolver = RelationshipReferenceResolver(schema: schema)
    }

    public func removingReference(
        to target: EntityReference,
        from model: PersistedModel,
        descriptor: RelationshipDescriptor
    ) throws -> PersistedModel {
        var fields = model.fields
        guard let entity = resolver.entity(named: model.entity) else {
            throw RelationshipReferenceError.invalidOwnerIdentity(
                entity: model.entity
            )
        }
        guard let fieldIndex = fields.firstIndex(where: {
            $0.name == descriptor.propertyName
        }), let fieldSchema = entity.fields.first(where: {
            $0.name == descriptor.propertyName
        }) else {
            throw RelationshipReferenceError.missingRelationshipField(
                entity: model.entity,
                field: descriptor.propertyName
            )
        }

        let replacement: FieldValue
        if descriptor.isToMany {
            guard case .array(let values) = fields[fieldIndex].value else {
                throw RelationshipReferenceError.invalidRelationshipValue(
                    entity: model.entity,
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
                    entity: model.entity,
                    field: descriptor.propertyName
                )
            }
            replacement = .null
        }

        fields[fieldIndex] = try PersistableField(
            number: fields[fieldIndex].number,
            name: fields[fieldIndex].name,
            value: replacement
        )
        do {
            return try PersistedModel(
                entity: model.entity,
                fields: fields
            )
        } catch {
            throw RelationshipReferenceError.entityDecodingFailed(
                entity: model.entity,
                reason: "updated fields do not form a canonical persisted model"
            )
        }
    }
}
