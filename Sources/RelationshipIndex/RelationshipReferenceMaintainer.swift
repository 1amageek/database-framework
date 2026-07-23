import Core
import DatabaseEngine
import DatabaseValue
import Relationship

/// Maintains the canonical inverse-reference catalog for every relationship field.
public struct RelationshipReferenceMaintainer: PersistableMutationMaintainer {
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
        context: borrowing PersistableMutationContext
    ) async throws {
        if let oldModel, let newModel {
            guard type(of: oldModel).persistableType == type(of: newModel).persistableType else {
                throw RelationshipReferenceError.invalidOwnerIdentity(
                    entity: type(of: newModel).persistableType
                )
            }
        }

        if let oldModel, newModel == nil {
            try await RelationshipMaintainer(
                schema: context.schema
            ).enforceDeleteRules(
                for: oldModel,
                context: context
            )
        }

        let resolver = RelationshipReferenceResolver(schema: context.schema)
        if let oldModel {
            let owner = try PersistableIdentityEncoder.encode(oldModel)
            for descriptor in type(of: oldModel).relationshipDescriptors {
                for target in try resolver.references(
                    from: oldModel,
                    descriptor: descriptor
                ) {
                    try RelationshipReferenceCatalog.clear(
                        target: target,
                        owner: owner,
                        descriptor: descriptor,
                        transaction: context.storageAccess
                    )
                }
            }
        }

        if let newModel {
            let owner = try PersistableIdentityEncoder.encode(newModel)
            for descriptor in type(of: newModel).relationshipDescriptors {
                for target in try resolver.references(
                    from: newModel,
                    descriptor: descriptor
                ) {
                    try RelationshipReferenceCatalog.set(
                        target: target,
                        owner: owner,
                        descriptor: descriptor,
                        transaction: context.storageAccess
                    )
                }
            }
        }
    }

    public func validateFinalState(
        of models: [any Persistable],
        context: borrowing PersistableValidationContext
    ) async throws {
        let resolver = RelationshipReferenceResolver(schema: context.schema)
        var validated = Set<RecordIdentity>()

        for model in models {
            for descriptor in type(of: model).relationshipDescriptors {
                for target in try resolver.references(
                    from: model,
                    descriptor: descriptor
                ) where validated.insert(target).inserted {
                    guard try await context.fetch(target) != nil else {
                        throw RelationshipReferenceError.targetRecordMissing(target)
                    }
                }
            }
        }
    }
}
