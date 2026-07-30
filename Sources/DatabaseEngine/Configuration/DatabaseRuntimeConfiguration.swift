import DatabaseKit
import DatabaseTypes

/// Immutable, container-scoped composition of runtime extension points.
public struct DatabaseRuntimeConfiguration: Sendable {
    public let indexMaintainerProviders: IndexMaintainerProviderRegistry
    public let readExecutors: ReadExecutorRegistry
    public let logicalSourceExecutors: LogicalSourceExecutorRegistry
    public let persistableMutationMaintainers: [any PersistableMutationMaintainer]
    public let authorizationPolicies: AuthorizationPolicyRegistry
    public let entityRuntimes: EntityRuntimeRegistry

    public init(
        indexMaintainerProviderDescriptors: [
            IndexMaintainerProviderDescriptor
        ] = [],
        polymorphicIndexReadExecutors: [any PolymorphicIndexReadExecutor] = [],
        graphTableSourceExecutor: (any GraphTableSourceExecutor)? = nil,
        sparqlSourceExecutor: (any SPARQLSourceExecutor)? = nil,
        persistableMutationMaintainers: [any PersistableMutationMaintainer] = [],
        entityRuntimes: [EntityRuntimeRegistration] = [],
        authorizationPolicies: [AuthorizationPolicyHandler] = []
    ) throws(DatabaseRuntimeConfigurationError) {
        self.indexMaintainerProviders = try IndexMaintainerProviderRegistry(
            descriptors: indexMaintainerProviderDescriptors
        )
        self.readExecutors = try ReadExecutorRegistry(
            polymorphicIndexExecutors: polymorphicIndexReadExecutors
        )
        self.logicalSourceExecutors = LogicalSourceExecutorRegistry(
            graphTableExecutor: graphTableSourceExecutor,
            sparqlExecutor: sparqlSourceExecutor
        )
        self.authorizationPolicies = try AuthorizationPolicyRegistry(
            handlers: authorizationPolicies
        )
        self.entityRuntimes = try EntityRuntimeRegistry(
            registrations: entityRuntimes
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
            guard let entityRuntime = entityRuntimes.registration(named: entity.name) else {
                throw .missingCompiledEntityType(entityName: entity.name)
            }
            try validateMaintainerProviders(
                source: .entity(entity.name),
                descriptors: entity.indexDescriptors,
                entityRuntime: entityRuntime
            )
            for maintained in entity.relationships {
                guard persistableMutationMaintainers.contains(where: {
                    $0.identifier == maintained.runtimeMaintainerIdentifier
                }) else {
                    throw .missingPersistableMutationMaintainer(
                        entityName: entity.name,
                        descriptorName: maintained.name,
                        identifier: maintained.runtimeMaintainerIdentifier
                    )
                }
            }
        }
        for group in schema.polymorphicGroups {
            for memberTypeName in group.memberTypeNames {
                guard let memberRuntime = entityRuntimes.registration(
                    named: memberTypeName
                ) else {
                    throw .missingCompiledPolymorphicMemberType(
                        groupIdentifier: group.identifier,
                        memberTypeName: memberTypeName
                    )
                }
                try validateMaintainerProviders(
                    source: .polymorphicGroup(group.identifier),
                    descriptors: schema.polymorphicIndexDescriptors(
                        identifier: group.identifier,
                        memberTypeName: memberRuntime.entity.name
                    ),
                    entityRuntime: memberRuntime
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
                    reason: "maintainer schema validation failed"
                )
            }
        }
    }

    private func validateMaintainerProviders(
        source: DatabaseRuntimeIndexRequirementSource,
        descriptors: [IndexDescriptor],
        entityRuntime: EntityRuntimeRegistration? = nil
    ) throws(DatabaseRuntimeConfigurationError) {
        for descriptor in descriptors {
            guard let requirements = entityRuntime?.runtimeRequirements(
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
                requiresUniqueness: descriptor.isUnique,
                requirements: requirements,
                entityRuntime: entityRuntime
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
                requiresUniqueness: descriptor.unique,
                requirements: requirements
            )
        }
    }

    private func validateMaintainerProviders(
        source: DatabaseRuntimeIndexRequirementSource,
        descriptors: [PolymorphicIndexMetadata]
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
                requiresUniqueness: descriptor.commonOptions.unique,
                requirements: requirements
            )
        }
    }

    private func validateProvider(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        kindIdentifier: String,
        requiresUniqueness: Bool,
        requirements: IndexRuntimeRequirements,
        entityRuntime: EntityRuntimeRegistration? = nil
    ) throws(DatabaseRuntimeConfigurationError) {
        guard entityRuntime != nil || indexMaintainerProviders.contains(
            kindIdentifier: kindIdentifier
        ) else {
            throw .missingIndexMaintainerProvider(
                source: source,
                indexName: indexName,
                kindIdentifier: kindIdentifier
            )
        }
        if let entityRuntime,
           !entityRuntime.hasIndexProvider(for: kindIdentifier) {
            throw .missingIndexMaintainerProvider(
                source: source,
                indexName: indexName,
                kindIdentifier: kindIdentifier
            )
        }
        let supportsUniqueness = entityRuntime?
            .supportsUniquenessConstraints(for: kindIdentifier)
            ?? indexMaintainerProviders.supportsUniquenessConstraints(
                for: kindIdentifier
            )
        if requiresUniqueness, supportsUniqueness != true {
            throw .missingIndexUniquenessSupport(
                source: source,
                indexName: indexName,
                kindIdentifier: kindIdentifier
            )
        }
        switch source {
        case .entity:
            if requirements.requiresEntityReadExecutor,
               entityRuntime?.hasIndexReader(for: kindIdentifier) != true {
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
