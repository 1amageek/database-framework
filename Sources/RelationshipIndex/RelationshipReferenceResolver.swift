import Core
import DatabaseEngine
import DatabaseValue
import Relationship

/// Resolves typed relationship fields to complete entity identities.
public struct RelationshipReferenceResolver: Sendable {
    private let schema: Schema

    public init(schema: Schema) {
        self.schema = schema
    }

    public func references(
        from model: any Persistable,
        descriptor: RelationshipDescriptor
    ) throws -> Set<PersistableIdentity> {
        Set(try orderedReferences(from: model, descriptor: descriptor))
    }

    public func orderedReferences(
        from model: any Persistable,
        descriptor: RelationshipDescriptor
    ) throws -> [PersistableIdentity] {
        let ownerType = type(of: model)
        guard descriptor.ownerTypeName == ownerType.persistableType else {
            throw RelationshipReferenceError.descriptorMismatch(
                owner: ownerType.persistableType,
                field: descriptor.propertyName
            )
        }
        guard let fieldSchema = ownerType.fieldSchemas.first(where: {
            $0.name == descriptor.propertyName
        }), fieldSchema.fieldNumber == Int(descriptor.propertyFieldNumber) else {
            throw RelationshipReferenceError.missingRelationshipField(
                entity: ownerType.persistableType,
                field: descriptor.propertyName
            )
        }

        let fields = try PersistableFieldEncoder.encode(model)
        guard let relationshipField = fields.first(where: {
            $0.number == descriptor.propertyFieldNumber
                && $0.name == descriptor.propertyName
        }) else {
            throw RelationshipReferenceError.missingRelationshipField(
                entity: ownerType.persistableType,
                field: descriptor.propertyName
            )
        }

        let values: [DatabaseValue]
        switch descriptor.cardinality {
        case .requiredToOne:
            guard relationshipField.value != .null else {
                throw RelationshipReferenceError.invalidRelationshipValue(
                    entity: ownerType.persistableType,
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
                    entity: ownerType.persistableType,
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
        from value: DatabaseValue,
        descriptor: RelationshipDescriptor
    ) throws -> PersistableIdentity {
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

    private func validatePartition(_ identity: PersistableIdentity) throws {
        guard let entity = schema.entity(named: identity.entity) else {
            throw RelationshipReferenceError.unknownRelatedEntity(identity.entity)
        }
        guard let targetType = entity.persistableType else {
            throw RelationshipReferenceError.relatedEntityHasNoCompiledType(
                identity.entity
            )
        }
        do {
            try PersistableIdentifierValidator.validate(
                identity.id,
                as: targetType.persistableIdentifierType
            )
        } catch let error {
            throw RelationshipReferenceError.invalidTargetIdentifier(
                entity: identity.entity,
                reason: error
            )
        }
        do {
            _ = try CanonicalPartitionBinding.makeAnyBinding(
                for: targetType,
                partitions: identity.partitions
            )
        } catch {
            throw RelationshipReferenceError.invalidTargetPartition(
                entity: identity.entity,
                reason: String(describing: error)
            )
        }
    }
}
