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
    private let schemaDrivenAuthorizationPolicyFactory: (@Sendable (Schema.Entity) -> AuthorizationPolicyHandler?)?
    private let sqlAuthorizationPolicyFactory: (@Sendable (Schema.Entity, SQLNamespace, String) -> AuthorizationPolicyHandler?)?
    private let sqlOwnedPolicyEntityNames: Set<SQLCatalogName>
    private let schemaDrivenEntityFactory: (@Sendable (Schema.Entity) throws -> EntityRuntimeRegistration)?
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
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        schemaDrivenEntityFactory: (@Sendable (Schema.Entity) throws -> EntityRuntimeRegistration)? = nil,
        schemaDrivenAuthorizationPolicyFactory: (@Sendable (Schema.Entity) -> AuthorizationPolicyHandler?)? = nil,
        sqlAuthorizationPolicyFactory: (@Sendable (Schema.Entity, SQLNamespace, String) -> AuthorizationPolicyHandler?)? = nil
    ) throws(DatabaseRuntimeConfigurationError) {
        try self.init(
            executionIdentity: executionIdentity,
            indexMaintainerProviderDescriptors:
                indexMaintainerProviderDescriptors,
            polymorphicIndexReadExecutors: polymorphicIndexReadExecutors,
            fusionIndexReadExecutors: [],
            fusionConnectedReadExecutors: [],
            graphTableSourceExecutor: graphTableSourceExecutor,
            sparqlSourceExecutor: sparqlSourceExecutor,
            persistableMutationMaintainers: persistableMutationMaintainers,
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies,
            indexConfigurations: indexConfigurations,
            schemaDrivenEntityFactory: schemaDrivenEntityFactory,
            schemaDrivenAuthorizationPolicyFactory: schemaDrivenAuthorizationPolicyFactory,
            sqlAuthorizationPolicyFactory: sqlAuthorizationPolicyFactory
        )
    }

    package init(
        executionIdentity: DatabaseExecutionRuntimeIdentity,
        indexMaintainerProviderDescriptors: [
            IndexMaintainerProviderDescriptor
        ] = [],
        polymorphicIndexReadExecutors: [any PolymorphicIndexReadExecutor] = [],
        fusionIndexReadExecutors: [any FusionIndexReadExecutor] = [],
        fusionConnectedReadExecutors: [
            any FusionConnectedReadExecutor
        ] = [],
        graphTableSourceExecutor: (any GraphTableSourceExecutor)? = nil,
        sparqlSourceExecutor: (any SPARQLSourceExecutor)? = nil,
        persistableMutationMaintainers: [any PersistableMutationMaintainer] = [],
        entityRuntimes: [EntityRuntimeRegistration] = [],
        authorizationPolicies: [AuthorizationPolicyHandler] = [],
        indexConfigurations: [any IndexRuntimeConfiguration] = [],
        schemaDrivenEntityFactory: (@Sendable (Schema.Entity) throws -> EntityRuntimeRegistration)? = nil,
        schemaDrivenAuthorizationPolicyFactory: (@Sendable (Schema.Entity) -> AuthorizationPolicyHandler?)? = nil,
        sqlAuthorizationPolicyFactory: (@Sendable (Schema.Entity, SQLNamespace, String) -> AuthorizationPolicyHandler?)? = nil
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
        self.schemaDrivenAuthorizationPolicyFactory = schemaDrivenAuthorizationPolicyFactory
        self.sqlAuthorizationPolicyFactory = sqlAuthorizationPolicyFactory
        self.sqlOwnedPolicyEntityNames = []
        self.schemaDrivenEntityFactory = schemaDrivenEntityFactory
        self.executionIdentity = executionIdentity
        self.indexMaintainerProviders = try IndexMaintainerProviderRegistry(
            descriptors: indexMaintainerProviderDescriptors
        )
        self.readExecutors = try ReadExecutorRegistry(
            polymorphicIndexExecutors: polymorphicIndexReadExecutors
        )
        self.fusionReadExecutors = try FusionReadExecutorRegistry(
            indexExecutors: fusionIndexReadExecutors,
            connectedExecutors: fusionConnectedReadExecutors
        )
        self.logicalSourceExecutors = LogicalSourceExecutorRegistry(
            graphTableExecutor: graphTableSourceExecutor,
            sparqlExecutor: sparqlSourceExecutor
        )
        var admittedPolicies = authorizationPolicies
        for registration in entityRuntimes where registration.compiledModelContract == nil
            && !authorizationPolicies.contains(where: { $0.entityName == registration.entity.name }) {
            if let policy = schemaDrivenAuthorizationPolicyFactory?(registration.entity) {
                guard policy.entityName == registration.entity.name else {
                    throw .authorizationPolicyEntityMismatch(expected: registration.entity.name, actual: policy.entityName)
                }
                admittedPolicies.append(policy)
            }
        }
        self.authorizationPolicies = try AuthorizationPolicyRegistry(handlers: admittedPolicies)
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

    /// Retains the installed capabilities while changing dynamic entity contracts.
    package func replacingPhysicalSchema(_ schema: Schema) throws -> Self {
        var registrations: [EntityRuntimeRegistration] = []
        for entity in schema.entities {
            if let current = entityRuntimes.registration(named: entity.name), current.entity == entity {
                registrations.append(current)
            } else if let factory = schemaDrivenEntityFactory {
                registrations.append(try factory(entity))
            } else {
                registrations.append(EntityRuntimeDefinition(schemaDriven: entity).registration())
            }
        }
        var policies: [AuthorizationPolicyHandler] = []
        for entity in schema.entities where entityRuntimes.registration(named: entity.name) == nil
            && authorizationPolicies.handler(for: entity.name) == nil {
            if let policy = schemaDrivenAuthorizationPolicyFactory?(entity) {
                guard policy.entityName == entity.name else {
                    throw DatabaseRuntimeConfigurationError.authorizationPolicyEntityMismatch(
                        expected: entity.name, actual: policy.entityName)
                }
                policies.append(policy)
            }
        }
        return try Self(source: self, registrations: registrations, addedPolicies: policies,
            indexNames: schema.allIndexNames)
    }

    private init(source: Self, registrations: [EntityRuntimeRegistration]?,
        addedPolicies: [AuthorizationPolicyHandler], indexNames: Set<String>,
        authorizationPolicies: AuthorizationPolicyRegistry? = nil,
        sqlOwnedPolicyEntityNames: Set<SQLCatalogName>? = nil
    ) throws {
        self.executionIdentity = source.executionIdentity
        self.indexMaintainerProviders = source.indexMaintainerProviders
        self.readExecutors = source.readExecutors
        self.fusionReadExecutors = source.fusionReadExecutors
        self.logicalSourceExecutors = source.logicalSourceExecutors
        self.persistableMutationMaintainers = source.persistableMutationMaintainers
        self.authorizationPolicies = try authorizationPolicies ?? source.authorizationPolicies.adding(addedPolicies)
        self.entityRuntimes = try registrations.map { try EntityRuntimeRegistry(registrations: $0) } ?? source.entityRuntimes
        self.indexConfigurations = source.indexConfigurations.filter { indexNames.contains($0.indexName) }
        self.indexConfigurationsByName = source.indexConfigurationsByName.filter { indexNames.contains($0.key) }
        self.schemaDrivenEntityFactory = source.schemaDrivenEntityFactory
        self.sqlAuthorizationPolicyFactory = source.sqlAuthorizationPolicyFactory
        self.sqlOwnedPolicyEntityNames = sqlOwnedPolicyEntityNames ?? source.sqlOwnedPolicyEntityNames
        self.schemaDrivenAuthorizationPolicyFactory = source.schemaDrivenAuthorizationPolicyFactory
    }

    /// Rebinds SQL-owned policies only from the validated logical catalog.
    func rebindingSQLAuthorization(to snapshot: SQLCatalogSnapshot, previousSQLOwnedEntityNames: [String] = []) throws -> Self {
        guard let factory = sqlAuthorizationPolicyFactory else { return self }
        let ownedNames = Set(snapshot.sqlOwnedEntityNames.map(SQLCatalogName.init))
        let namespaces = Dictionary(uniqueKeysWithValues: snapshot.catalog.namespaces.map {
            (SQLCatalogName($0.id), $0)
        })
        let materializedNames = Dictionary(uniqueKeysWithValues: snapshot.catalog.materializedViews.map {
            (SQLCatalogName($0.backingRelationID), $0.name)
        })
        var policies: [AuthorizationPolicyHandler] = []
        for relation in snapshot.catalog.relations where ownedNames.contains(SQLCatalogName(relation.entityName)) {
            guard let entity = entityRuntimes.registration(named: relation.entityName)?.entity,
                let namespace = namespaces[SQLCatalogName(relation.namespaceID)] else {
                throw SQLCatalogBindingError.missingEntity(relation.entityName)
            }
            let name = materializedNames[SQLCatalogName(relation.id)] ?? relation.name
            if let policy = factory(entity, namespace, name) {
                guard policy.entityName.utf8.elementsEqual(entity.name.utf8) else {
                    throw DatabaseRuntimeConfigurationError.authorizationPolicyEntityMismatch(
                        expected: entity.name, actual: policy.entityName)
                }
                policies.append(policy)
            }
        }
        let registry = try authorizationPolicies.replacing(
            entityNames: ownedNames.union(sqlOwnedPolicyEntityNames)
                .union(previousSQLOwnedEntityNames.map(SQLCatalogName.init)), with: policies)
        return try Self(source: self, registrations: nil,
            addedPolicies: [], indexNames: Set(indexConfigurationsByName.keys),
            authorizationPolicies: registry, sqlOwnedPolicyEntityNames: ownedNames)
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
