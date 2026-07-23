import Core
import DatabaseValue

/// Immutable, container-scoped composition of runtime extension points.
public struct DatabaseRuntimeConfiguration: Sendable {
    public let indexMaintainerProviders: IndexMaintainerProviderRegistry
    public let readExecutors: ReadExecutorRegistry
    public let logicalSourceExecutors: LogicalSourceExecutorRegistry
    public let persistableMutationMaintainers: [any PersistableMutationMaintainer]

    public init(
        indexMaintainerProviders: [any IndexMaintainerProvider] = [],
        indexReadExecutors: [any IndexReadExecutor] = [],
        polymorphicIndexReadExecutors: [any PolymorphicIndexReadExecutor] = [],
        fusionReadExecutors: [any FusionReadExecutor] = [],
        graphTableSourceExecutor: (any GraphTableSourceExecutor)? = nil,
        sparqlSourceExecutor: (any SPARQLSourceExecutor)? = nil,
        persistableMutationMaintainers: [any PersistableMutationMaintainer] = []
    ) throws(DatabaseRuntimeConfigurationError) {
        self.indexMaintainerProviders = try IndexMaintainerProviderRegistry(
            providers: indexMaintainerProviders
        )
        self.readExecutors = try ReadExecutorRegistry(
            indexExecutors: indexReadExecutors,
            polymorphicIndexExecutors: polymorphicIndexReadExecutors,
            fusionExecutors: fusionReadExecutors
        )
        self.logicalSourceExecutors = LogicalSourceExecutorRegistry(
            graphTableExecutor: graphTableSourceExecutor,
            sparqlExecutor: sparqlSourceExecutor
        )
        var maintainerIdentifiers = Set<String>()
        for maintainer in persistableMutationMaintainers {
            guard maintainerIdentifiers.insert(maintainer.identifier).inserted else {
                throw .duplicatePersistableMutationMaintainer(
                    identifier: maintainer.identifier
                )
            }
        }
        self.persistableMutationMaintainers = persistableMutationMaintainers
    }

    public func validate(
        schema: Schema
    ) throws(DatabaseRuntimeConfigurationError) {
        for entity in schema.entities {
            guard let persistableType = entity.persistableType else {
                throw .missingCompiledEntityType(entityName: entity.name)
            }
            do {
                try PersistableIdentifierValidator.validate(
                    persistableType.persistableIdentifierType
                )
            } catch let error {
                throw .invalidPersistableIdentifierType(
                    entityName: entity.name,
                    reason: error
                )
            }
            try validateMaintainerProviders(
                source: .entity(entity.name),
                descriptors: entity.indexDescriptors
            )
            for descriptor in persistableType.descriptors {
                guard let maintained = descriptor as? any RuntimeMaintainedDescriptor else {
                    continue
                }
                guard persistableMutationMaintainers.contains(where: {
                    $0.identifier == maintained.runtimeMaintainerIdentifier
                }) else {
                    throw .missingPersistableMutationMaintainer(
                        entityName: entity.name,
                        descriptorName: descriptor.name,
                        identifier: maintained.runtimeMaintainerIdentifier
                    )
                }
            }
        }
        for group in schema.polymorphicGroups {
            for memberTypeName in group.memberTypeNames {
                guard let memberType = schema.entity(
                    named: memberTypeName
                )?.persistableType else {
                    throw .missingCompiledPolymorphicMemberType(
                        groupIdentifier: group.identifier,
                        memberTypeName: memberTypeName
                    )
                }
                try validateMaintainerProviders(
                    source: .polymorphicGroup(group.identifier),
                    descriptors: schema.polymorphicIndexDescriptors(
                        identifier: group.identifier,
                        memberType: memberType
                    )
                )
            }
            try validateMaintainerProviders(
                source: .polymorphicGroup(group.identifier),
                descriptors: group.indexes
            )
        }
        for maintainer in persistableMutationMaintainers {
            do {
                try maintainer.validate(schema: schema)
            } catch {
                throw .invalidPersistableMutationMaintainerSchema(
                    identifier: maintainer.identifier,
                    reason: String(describing: error)
                )
            }
        }
    }

    private func validateMaintainerProviders(
        source: DatabaseRuntimeIndexRequirementSource,
        descriptors: [IndexDescriptor]
    ) throws(DatabaseRuntimeConfigurationError) {
        for descriptor in descriptors {
            guard let requirements = indexMaintainerProviders.runtimeRequirements(
                for: descriptor.kindIdentifier
            ) else {
                throw .missingIndexMaintainerProvider(
                    source: source,
                    indexName: descriptor.name,
                    kindIdentifier: descriptor.kindIdentifier
                )
            }
            try validateProvider(
                source: source,
                indexName: descriptor.name,
                kindIdentifier: descriptor.kindIdentifier,
                requirements: requirements
            )
        }
    }

    private func validateMaintainerProviders(
        source: DatabaseRuntimeIndexRequirementSource,
        descriptors: [IndexDescriptorMetadata]
    ) throws(DatabaseRuntimeConfigurationError) {
        for descriptor in descriptors {
            guard let requirements = indexMaintainerProviders.runtimeRequirements(
                for: descriptor.kindIdentifier
            ) else {
                throw .missingIndexMaintainerProvider(
                    source: source,
                    indexName: descriptor.name,
                    kindIdentifier: descriptor.kindIdentifier
                )
            }
            try validateProvider(
                source: source,
                indexName: descriptor.name,
                kindIdentifier: descriptor.kindIdentifier,
                requirements: requirements
            )
        }
    }

    private func validateProvider(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        kindIdentifier: String,
        requirements: IndexRuntimeRequirements
    ) throws(DatabaseRuntimeConfigurationError) {
        guard indexMaintainerProviders.contains(
            kindIdentifier: kindIdentifier
        ) else {
            throw .missingIndexMaintainerProvider(
                source: source,
                indexName: indexName,
                kindIdentifier: kindIdentifier
            )
        }
        switch source {
        case .entity:
            if requirements.requiresEntityReadExecutor,
               readExecutors.indexExecutor(for: kindIdentifier) == nil {
                throw .missingIndexReadExecutor(
                    source: source,
                    indexName: indexName,
                    kindIdentifier: kindIdentifier
                )
            }
        case .polymorphicGroup:
            if requirements.requiresPolymorphicReadExecutor,
               readExecutors.polymorphicIndexExecutor(
                for: kindIdentifier
               ) == nil {
                throw .missingPolymorphicIndexReadExecutor(
                    source: source,
                    indexName: indexName,
                    kindIdentifier: kindIdentifier
                )
            }
        }

        if requirements.logicalSourceExecutors.contains(.graphTable),
           logicalSourceExecutors.graphTableExecutor == nil {
            throw .missingGraphTableSourceExecutor
        }
        if requirements.logicalSourceExecutors.contains(.sparql),
           logicalSourceExecutors.sparqlExecutor == nil {
            throw .missingSPARQLSourceExecutor
        }
    }
}
