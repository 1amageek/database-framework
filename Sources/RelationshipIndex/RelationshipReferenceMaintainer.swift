import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Maintains the canonical inverse-reference catalog for every relationship field.
public struct RelationshipReferenceMaintainer: PersistableMutationMaintainer {
    public let identifier = "relationship.reference"

    public init() {}

    public func validate(schema: Schema) throws {
        var descriptorNames = Set<String>()
        var ownerFields = Set<String>()
        for entity in schema.entities {
            for descriptor in entity.relationships {
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
                guard descriptor.ownerTypeName == entity.name else {
                    throw RelationshipSchemaError.ownerMismatch(
                        expected: entity.name,
                        actual: descriptor.ownerTypeName
                    )
                }
                guard let field = entity.fields.first(where: {
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
                guard schema.entity(named: descriptor.relatedTypeName) != nil else {
                    throw RelationshipSchemaError.unknownTarget(
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
        identity: EntityReference,
        oldModel: PersistedModel?,
        newModel: PersistedModel?,
        context: borrowing PersistableMutationContext
    ) async throws {
        if let oldModel, let newModel {
            guard oldModel.entity == newModel.entity else {
                throw RelationshipReferenceError.invalidOwnerIdentity(
                    entity: newModel.entity
                )
            }
        }

        if let oldModel, newModel == nil {
            try await RelationshipMaintainer(
                schema: context.schema
            ).enforceDeleteRules(
                for: oldModel,
                identity: identity,
                context: context
            )
        }

        let resolver = RelationshipReferenceResolver(schema: context.schema)
        if let oldModel {
            let descriptors = try relationships(
                for: oldModel,
                schema: context.schema
            )
            for descriptor in descriptors {
                for target in try resolver.references(
                    from: oldModel,
                    descriptor: descriptor
                ) {
                    try RelationshipReferenceCatalog.clear(
                        target: target,
                        owner: identity,
                        descriptor: descriptor,
                        transaction: context.storageAccess
                    )
                }
            }
        }

        if let newModel {
            let descriptors = try relationships(
                for: newModel,
                schema: context.schema
            )
            for descriptor in descriptors {
                for target in try resolver.references(
                    from: newModel,
                    descriptor: descriptor
                ) {
                    try RelationshipReferenceCatalog.set(
                        target: target,
                        owner: identity,
                        descriptor: descriptor,
                        transaction: context.storageAccess
                    )
                }
            }
        }
    }

    public func validateFinalState(
        of models: [PersistedModel],
        context: borrowing PersistableValidationContext
    ) async throws {
        let resolver = RelationshipReferenceResolver(schema: context.schema)
        var validated = Set<EntityReference>()

        for model in models {
            for descriptor in try relationships(
                for: model,
                schema: context.schema
            ) {
                for target in try resolver.references(
                    from: model,
                    descriptor: descriptor
                ) where validated.insert(target).inserted {
                    guard try await context.fetch(target) != nil else {
                        throw RelationshipReferenceError.targetEntityMissing(target)
                    }
                }
            }
        }
    }

    private func relationships(
        for model: PersistedModel,
        schema: Schema
    ) throws -> [RelationshipDescriptor] {
        guard let entity = schema.entity(named: model.entity) else {
            throw RelationshipReferenceError.invalidOwnerIdentity(
                entity: model.entity
            )
        }
        return entity.relationships
    }
}
