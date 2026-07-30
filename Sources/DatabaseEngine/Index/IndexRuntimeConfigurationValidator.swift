import DatabaseKit

/// Validates deployment-specific index policy against the compiled schema.
enum IndexRuntimeConfigurationValidator {
    static func validate(
        _ configurations: [any IndexRuntimeConfiguration],
        schema: Schema,
        entityRuntimes: EntityRuntimeRegistry
    ) throws(IndexRuntimeConfigurationError) {
        guard !configurations.isEmpty else {
            return
        }

        var descriptorsByName: [String: [IndexDescriptor]] = [:]
        for descriptor in schema.indexDescriptors {
            descriptorsByName[descriptor.name, default: []].append(descriptor)
        }
        for group in schema.polymorphicGroups {
            for memberTypeName in group.memberTypeNames {
                guard let memberType = entityRuntimes.modelType(
                    named: memberTypeName
                ) else {
                    throw .invalidConfiguration(
                        indexName: group.identifier,
                        reason: "compiled polymorphic member '\(memberTypeName)' is unavailable"
                    )
                }
                for descriptor in schema.polymorphicIndexDescriptors(
                    identifier: group.identifier,
                    memberTypeName: memberType.persistableType
                ) {
                    descriptorsByName[
                        descriptor.name,
                        default: []
                    ].append(descriptor)
                }
            }
        }

        for configuration in configurations {
            guard let namedDescriptors = descriptorsByName[
                configuration.indexName
            ] else {
                throw .unknownIndex(indexName: configuration.indexName)
            }
            guard let descriptor = namedDescriptors.first(where: {
                $0.entityName == configuration.entityName
            }) else {
                throw .invalidConfiguration(
                    indexName: configuration.indexName,
                    reason: "entity '\(configuration.entityName)' does not own this index"
                )
            }
            guard descriptor.kindIdentifier
                    == configuration.kindIdentifier else {
                throw .indexKindMismatch(
                    indexName: configuration.indexName,
                    expected: descriptor.kindIdentifier,
                    actual: configuration.kindIdentifier
                )
            }
            guard descriptor.fieldNames.contains(
                configuration.fieldName
            ) else {
                throw .invalidConfiguration(
                    indexName: configuration.indexName,
                    reason: "field '\(configuration.fieldName)' is not selected by the compiled index"
                )
            }
            if let subspaceKey = configuration.subspaceKey,
               subspaceKey.isEmpty {
                throw .invalidConfiguration(
                    indexName: configuration.indexName,
                    reason: "subspace key must not be empty"
                )
            }
        }
    }
}
