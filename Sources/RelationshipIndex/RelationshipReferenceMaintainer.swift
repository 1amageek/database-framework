import Core
import DatabaseEngine
import DatabaseValue
import Relationship
import StorageKit

/// Maintains the canonical inverse-reference catalog for every relationship field.
public struct RelationshipReferenceMaintainer: RecordMutationMaintainer {
    public let identifier = "relationship.reference"

    public init() {}

    public func validate(schema: Schema) throws {
        var descriptorNames = Set<String>()
        var ownerFields = Set<String>()
        for entity in schema.entities {
            guard let ownerType = entity.persistableType else { continue }
            for descriptor in ownerType.relationshipDescriptors {
                guard descriptorNames.insert(descriptor.name).inserted else {
                    throw RelationshipSchemaError.duplicateDescriptorName(
                        descriptor.name
                    )
                }
                let ownerFieldKey = "\(descriptor.ownerTypeName).\(descriptor.propertyName)"
                guard ownerFields.insert(ownerFieldKey).inserted else {
                    throw RelationshipSchemaError.duplicateRelationshipField(
                        owner: descriptor.ownerTypeName,
                        field: descriptor.propertyName
                    )
                }
                guard descriptor.ownerTypeName == ownerType.persistableType else {
                    throw RelationshipSchemaError.ownerMismatch(
                        expected: ownerType.persistableType,
                        actual: descriptor.ownerTypeName
                    )
                }
                guard let field = ownerType.fieldSchemas.first(where: {
                    $0.name == descriptor.propertyName
                }) else {
                    throw RelationshipSchemaError.missingField(
                        owner: descriptor.ownerTypeName,
                        field: descriptor.propertyName
                    )
                }
                guard field.fieldNumber == Int(descriptor.propertyFieldNumber),
                      descriptor.propertyFieldNumber > 0 else {
                    throw RelationshipSchemaError.fieldNumberMismatch(
                        owner: descriptor.ownerTypeName,
                        field: descriptor.propertyName
                    )
                }
                guard field.type == .reference else {
                    throw RelationshipSchemaError.fieldTypeMismatch(
                        owner: descriptor.ownerTypeName,
                        field: descriptor.propertyName
                    )
                }
                guard field.referenceTargetEntity == descriptor.relatedTypeName else {
                    throw RelationshipSchemaError.referenceTargetMismatch(
                        owner: descriptor.ownerTypeName,
                        field: descriptor.propertyName,
                        expected: descriptor.relatedTypeName,
                        actual: field.referenceTargetEntity
                    )
                }
                guard let target = schema.entity(named: descriptor.relatedTypeName) else {
                    throw RelationshipSchemaError.unknownTarget(
                        descriptor.relatedTypeName
                    )
                }
                guard target.persistableType != nil else {
                    throw RelationshipSchemaError.targetHasNoCompiledType(
                        descriptor.relatedTypeName
                    )
                }
                let cardinalityIsValid: Bool
                switch descriptor.cardinality {
                case .requiredToOne:
                    cardinalityIsValid = !field.isOptional && !field.isArray
                case .optionalToOne:
                    cardinalityIsValid = field.isOptional && !field.isArray
                case .toMany:
                    cardinalityIsValid = !field.isOptional && field.isArray
                }
                guard cardinalityIsValid else {
                    throw RelationshipSchemaError.cardinalityMismatch(
                        owner: descriptor.ownerTypeName,
                        field: descriptor.propertyName
                    )
                }
                if descriptor.deleteRule == .nullify,
                   descriptor.cardinality == .requiredToOne {
                    throw RelationshipSchemaError.nullifyRequiresNullableCardinality(
                        owner: descriptor.ownerTypeName,
                        field: descriptor.propertyName
                    )
                }
            }
        }
    }

    public func update(
        oldModel: (any Persistable)?,
        newModel: (any Persistable)?,
        container: DBContainer,
        transaction: any Transaction
    ) async throws {
        if let oldModel, let newModel {
            guard type(of: oldModel).persistableType == type(of: newModel).persistableType else {
                throw RelationshipReferenceError.invalidOwnerIdentity(
                    entity: type(of: newModel).persistableType
                )
            }
        }

        if let oldModel, newModel == nil {
            let handler = container.newContext().makePersistenceHandler()
            try await RelationshipMaintainer(
                container: container,
                schema: container.schema
            ).enforceDeleteRules(
                for: oldModel,
                transaction: transaction,
                handler: handler
            )
        }

        let resolver = RelationshipReferenceResolver(schema: container.schema)
        if let oldModel {
            let owner = try DatabaseRecordIdentityEncoder.encode(oldModel)
            for descriptor in type(of: oldModel).relationshipDescriptors {
                for target in try resolver.references(
                    from: oldModel,
                    descriptor: descriptor
                ) {
                    try RelationshipReferenceCatalog.clear(
                        target: target,
                        owner: owner,
                        descriptor: descriptor,
                        transaction: transaction
                    )
                }
            }
        }

        if let newModel {
            let owner = try DatabaseRecordIdentityEncoder.encode(newModel)
            for descriptor in type(of: newModel).relationshipDescriptors {
                for target in try resolver.references(
                    from: newModel,
                    descriptor: descriptor
                ) {
                    try RelationshipReferenceCatalog.set(
                        target: target,
                        owner: owner,
                        descriptor: descriptor,
                        transaction: transaction
                    )
                }
            }
        }
    }

    public func validateFinalState(
        of models: [any Persistable],
        container: DBContainer,
        transaction: any Transaction
    ) async throws {
        let resolver = RelationshipReferenceResolver(schema: container.schema)
        let handler = container.newContext().makePersistenceHandler()
        var validated = Set<RecordIdentity>()

        for model in models {
            for descriptor in type(of: model).relationshipDescriptors {
                for target in try resolver.references(
                    from: model,
                    descriptor: descriptor
                ) where validated.insert(target).inserted {
                    let resolved = try CanonicalRelationshipIdentity.resolve(
                        target,
                        container: container
                    )
                    guard try await handler.load(
                        target.entity,
                        id: resolved.id,
                        partition: resolved.partition,
                        transaction: transaction
                    ) != nil else {
                        throw RelationshipReferenceError.targetRecordMissing(target)
                    }
                }
            }
        }
    }
}
