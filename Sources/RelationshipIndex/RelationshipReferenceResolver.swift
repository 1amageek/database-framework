import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Resolves typed relationship fields to complete entity identities.
public struct RelationshipReferenceResolver: Sendable {
    private let schema: Schema

    public init(schema: Schema) {
        self.schema = schema
    }

    package func entity(named name: String) -> Schema.Entity? {
        schema.entity(named: name)
    }

    public func references(
        from model: PersistedModel,
        descriptor: RelationshipDescriptor
    ) throws -> Set<EntityReference> {
        Set(try orderedReferences(from: model, descriptor: descriptor))
    }

    public func orderedReferences(
        from model: PersistedModel,
        descriptor: RelationshipDescriptor
    ) throws -> [EntityReference] {
        guard descriptor.ownerTypeName == model.entity else {
            throw RelationshipReferenceError.descriptorMismatch(
                owner: model.entity,
                field: descriptor.propertyName
            )
        }
        guard let entity = schema.entity(named: model.entity),
              let fieldSchema = entity.fields.first(where: {
            $0.name == descriptor.propertyName
        }), fieldSchema.fieldNumber == Int(descriptor.propertyFieldNumber) else {
            throw RelationshipReferenceError.missingRelationshipField(
                entity: model.entity,
                field: descriptor.propertyName
            )
        }

        guard let relationshipField = model.fields.first(where: {
            $0.number == descriptor.propertyFieldNumber
                && $0.name == descriptor.propertyName
        }) else {
            throw RelationshipReferenceError.missingRelationshipField(
                entity: model.entity,
                field: descriptor.propertyName
            )
        }

        let values: [FieldValue]
        switch descriptor.cardinality {
        case .requiredToOne:
            guard relationshipField.value != .null else {
                throw RelationshipReferenceError.invalidRelationshipValue(
                    entity: model.entity,
                    field: descriptor.propertyName
                )
            }
            values = [relationshipField.value]
        case .optionalToOne:
            values = relationshipField.value == .null
                ? []
                : [relationshipField.value]
        case .toMany:
            guard case .array(let elements) = relationshipField.value else {
                throw RelationshipReferenceError.invalidRelationshipValue(
                    entity: model.entity,
                    field: descriptor.propertyName
                )
            }
            values = elements
        }

        return try values.map {
            try identity(from: $0, descriptor: descriptor)
        }
    }

    package func identity(
        from value: FieldValue,
        descriptor: RelationshipDescriptor
    ) throws -> EntityReference {
        guard case .reference(let identity) = value else {
            throw RelationshipReferenceError.invalidRelationshipValue(
                entity: descriptor.ownerTypeName,
                field: descriptor.propertyName
            )
        }
        guard identity.entity == descriptor.relatedTypeName else {
            throw RelationshipReferenceError.invalidReferenceEntity(
                expected: descriptor.relatedTypeName,
                actual: identity.entity
            )
        }
        try validatePartition(identity)
        return identity
    }

    private func validatePartition(_ identity: EntityReference) throws {
        guard let entity = schema.entity(named: identity.entity) else {
            throw RelationshipReferenceError.unknownRelatedEntity(identity.entity)
        }
        do {
            try PersistableIdentifierKeyCodec.validate(
                identity.id,
                expectedType: entity.identifierType
            )
        } catch let error {
            throw RelationshipReferenceError.invalidTargetIdentifier(
                entity: identity.entity,
                reason: error
            )
        }
        do {
            try CanonicalPartitionBinding.validate(
                identity.partitions,
                for: entity
            )
        } catch {
            throw RelationshipReferenceError.invalidTargetPartition(
                entity: identity.entity,
                reason: "partition does not match the compiled entity schema"
            )
        }
    }
}
