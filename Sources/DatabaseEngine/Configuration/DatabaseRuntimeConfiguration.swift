import DatabaseKit
import DatabaseTypes
import StorageKit

/// Immutable composition of runtime extension points for one schema generation.
public struct DatabaseRuntimeConfiguration: Sendable {
    public let executionIdentity: DatabaseExecutionRuntimeIdentity
    public let indexMaintainerProviders: IndexMaintainerProviderRegistry
    public let readExecutors: ReadExecutorRegistry
    let fusionReadExecutors: FusionReadExecutorRegistry
    public let logicalSourceExecutors: LogicalSourceExecutorRegistry
    public let persistableMutationMaintainers: [any PersistableMutationMaintainer]
    public let authorizationPolicies: AuthorizationPolicyRegistry
    public let entityRuntimes: EntityRuntimeRegistry
    public let indexConfigurations: [any IndexRuntimeConfiguration]
    private let indexConfigurationsByName: [String: [any IndexRuntimeConfiguration]]

    public init(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        indexMaintainerProviderDescriptors: [
            IndexMaintainerProviderDescriptor
        ] = [],
        polymorphicIndexReadExecutors: [any PolymorphicIndexReadExecutor] = [],
        graphTableSourceExecutor: (any GraphTableSourceExecutor)? = nil,
        sparqlSourceExecutor: (any SPARQLSourceExecutor)? = nil,
        persistableMutationMaintainers: [any PersistableMutationMaintainer] = [],
        entityRuntimes: [EntityRuntimeRegistration] = [],
        authorizationPolicies: [AuthorizationPolicyHandler] = [],
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) throws(DatabaseRuntimeConfigurationError) {
        try self.init(
            executionIdentity: executionIdentity,
            indexMaintainerProviderDescriptors:
                indexMaintainerProviderDescriptors,
            polymorphicIndexReadExecutors: polymorphicIndexReadExecutors,
            fusionIndexReadExecutors: [],
            graphTableSourceExecutor: graphTableSourceExecutor,
            sparqlSourceExecutor: sparqlSourceExecutor,
            persistableMutationMaintainers: persistableMutationMaintainers,
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations
        )
    }

    package init(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        indexMaintainerProviderDescriptors: [
            IndexMaintainerProviderDescriptor
        ] = [],
        polymorphicIndexReadExecutors: [any PolymorphicIndexReadExecutor] = [],
        fusionIndexReadExecutors: [any FusionIndexReadExecutor] = [],
        graphTableSourceExecutor: (any GraphTableSourceExecutor)? = nil,
        sparqlSourceExecutor: (any SPARQLSourceExecutor)? = nil,
        persistableMutationMaintainers: [any PersistableMutationMaintainer] = [],
        entityRuntimes: [EntityRuntimeRegistration] = [],
        authorizationPolicies: [AuthorizationPolicyHandler] = [],
        indexConfigurations: [any IndexRuntimeConfiguration] = []
    ) throws(DatabaseRuntimeConfigurationError) {
        guard !executionIdentity.identifier.isEmpty,
            executionIdentity.identifier.utf8.count
                <= DatabaseExecutionRuntimeIdentity
                .maximumIdentifierUTF8ByteCount
        else {
            throw .invalidExecutionIdentityIdentifier
        }
        guard executionIdentity.revision > 0 else {
            throw .invalidExecutionIdentityRevision
        }
        self.executionIdentity = executionIdentity
        self.indexMaintainerProviders = try IndexMaintainerProviderRegistry(
            descriptors: indexMaintainerProviderDescriptors
        )
        self.readExecutors = try ReadExecutorRegistry(
            polymorphicIndexExecutors: polymorphicIndexReadExecutors
        )
        self.fusionReadExecutors = try FusionReadExecutorRegistry(
            indexExecutors: fusionIndexReadExecutors
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
        self.indexConfigurations = indexConfigurations
        var configurationsByName: [String: [any IndexRuntimeConfiguration]] = [:]
        configurationsByName.reserveCapacity(indexConfigurations.count)
        for configuration in indexConfigurations {
            configurationsByName[configuration.indexName, default: []]
                .append(configuration)
        }
        self.indexConfigurationsByName = configurationsByName
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

    /// Returns the deployment policy paired with one declared index in this
    /// immutable runtime generation.
    public func indexConfigurations(
        named indexName: String
    ) -> [any IndexRuntimeConfiguration] {
        indexConfigurationsByName[indexName] ?? []
    }

    public func validate(
        schema: Schema
    ) throws(DatabaseRuntimeConfigurationError) {
        for entity in schema.entities {
            guard let entityRuntime = entityRuntimes.registration(named: entity.name) else {
                throw .missingCompiledEntityType(entityName: entity.name)
            }
            guard entityRuntime.entity == entity else {
                throw .entitySchemaMismatch(
                    entityName: entity.name,
                    schemaEntity: entity,
                    runtimeEntity: entityRuntime.entity
                )
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

    /// Validates requirements supplied by the selected storage backend before
    /// a schema generation can become observable.
    public func validateStorageRequirements(
        schema: Schema,
        transactionCapabilities: TransactionCapabilities
    ) throws(DatabaseRuntimeConfigurationError) {
        for entity in schema.entities {
            guard let entityRuntime = entityRuntimes.registration(
                named: entity.name
            ) else {
                throw .missingCompiledEntityType(entityName: entity.name)
            }
            for descriptor in entity.indexDescriptors {
                guard let requirements = entityRuntime.runtimeRequirements(
                    for: descriptor.type
                    )
                else {
                    throw .missingIndexMaintainerProvider(
                        source: .entity(entity.name),
                        indexName: descriptor.name,
                        indexType: descriptor.type
                    )
                }
                try validateStorageRequirements(
                    source: .entity(entity.name),
                    indexName: descriptor.name,
                    indexType: descriptor.type,
                    requirements: requirements,
                    transactionCapabilities: transactionCapabilities
                )
            }
        }
        for group in schema.polymorphicGroups {
            for descriptor in group.indexes {
                guard let requirements = indexMaintainerProviders
                    .runtimeRequirements(for: descriptor.definition.type)
                else {
                    throw .missingIndexMaintainerProvider(
                        source: .polymorphicGroup(group.identifier),
                        indexName: descriptor.name,
                        indexType: descriptor.definition.type
                    )
                }
                try validateStorageRequirements(
                    source: .polymorphicGroup(group.identifier),
                    indexName: descriptor.name,
                    indexType: descriptor.definition.type,
                    requirements: requirements,
                    transactionCapabilities: transactionCapabilities
                )
            }
        }
    }

    private func validateStorageRequirements(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        indexType: IndexType,
        requirements: IndexRuntimeRequirements,
        transactionCapabilities: TransactionCapabilities
    ) throws(DatabaseRuntimeConfigurationError) {
        if requirements.requiresVersionstampedMutations,
           !transactionCapabilities.versionstampedMutations {
            throw .unsupportedStorageCapability(
                source: source,
                indexName: indexName,
                indexType: indexType,
                capability: .versionstampedMutations
            )
        }
    }

    private func validateMaintainerProviders(
        source: DatabaseRuntimeIndexRequirementSource,
        descriptors: [IndexDescriptor],
        entityRuntime: EntityRuntimeRegistration? = nil
    ) throws(DatabaseRuntimeConfigurationError) {
        for descriptor in descriptors {
            guard let requirements = entityRuntime?.runtimeRequirements(
                for: descriptor.type
                )
            else {
                throw .missingIndexMaintainerProvider(
                    source: source,
                    indexName: descriptor.name,
                    indexType: descriptor.type
                )
            }
            try validateProvider(
                source: source,
                indexName: descriptor.name,
                indexType: descriptor.type,
                requiresUniqueness: descriptor.isUnique,
                requirements: requirements,
                entityRuntime: entityRuntime
            )
        }
    }

    private func validateMaintainerProviders(
        source: DatabaseRuntimeIndexRequirementSource,
        descriptors: [IndexDeclaration<String>]
    ) throws(DatabaseRuntimeConfigurationError) {
        for descriptor in descriptors {
            guard let requirements = indexMaintainerProviders.runtimeRequirements(
                for: descriptor.definition.type
                )
            else {
                throw .missingIndexMaintainerProvider(
                    source: source,
                    indexName: descriptor.name,
                    indexType: descriptor.definition.type
                )
            }
            try validateProvider(
                source: source,
                indexName: descriptor.name,
                indexType: descriptor.definition.type,
                requiresUniqueness: descriptor.definition.isUnique,
                requirements: requirements
            )
        }
    }

    private func validateProvider(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        indexType: IndexType,
        requiresUniqueness: Bool,
        requirements: IndexRuntimeRequirements,
        entityRuntime: EntityRuntimeRegistration? = nil
    ) throws(DatabaseRuntimeConfigurationError) {
        guard entityRuntime != nil || indexMaintainerProviders.contains(
                    indexType: indexType
                )
        else {
            throw .missingIndexMaintainerProvider(
                source: source,
                indexName: indexName,
                indexType: indexType
            )
        }
        if let entityRuntime,
           !entityRuntime.hasIndexProvider(for: indexType)
        {
            throw .missingIndexMaintainerProvider(
                source: source,
                indexName: indexName,
                indexType: indexType
            )
        }
        let supportsUniqueness = entityRuntime?
            .supportsUniquenessConstraints(for: indexType)
            ?? indexMaintainerProviders.supportsUniquenessConstraints(
                for: indexType
            )
        if requiresUniqueness, supportsUniqueness != true {
            throw .missingIndexUniquenessSupport(
                source: source,
                indexName: indexName,
                indexType: indexType
            )
        }
        switch source {
        case .entity:
            if requirements.requiresEntityReadExecutor,
               entityRuntime?.hasIndexReader(for: indexType) != true {
                throw .missingIndexReadExecutor(
                    source: source,
                    indexName: indexName,
                    indexType: indexType
                )
            }
        case .polymorphicGroup:
            if requirements.requiresPolymorphicReadExecutor,
               readExecutors.polymorphicIndexExecutor(
                    for: indexType
                ) == nil {
                throw .missingPolymorphicIndexReadExecutor(
                    source: source,
                    indexName: indexName,
                    indexType: indexType
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
