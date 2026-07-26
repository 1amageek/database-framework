import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import RelationshipIndex
import ScalarIndex
import Testing
import VectorIndex

@Suite("Database Runtime Configuration Validation")
struct DatabaseRuntimeConfigurationValidationTests {
    @Test("Schema validation rejects an unregistered compiled entity type")
    func missingCompiledEntityTypeFailsValidation() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: RuntimeConfigurationScalarEntity.self)
            ]
        )
        let configuration = try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [ScalarIndexMaintainerProvider()]
        )

        #expect(
            throws: DatabaseRuntimeConfigurationError.missingCompiledEntityType(
                entityName: RuntimeConfigurationScalarEntity.persistableType
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Runtime configuration rejects duplicate compiled entity types")
    func duplicateCompiledEntityTypeFailsInitialization() throws {
        #expect(
            throws: DatabaseRuntimeConfigurationError.duplicatePersistableType(
                entityName: RuntimeConfigurationScalarEntity.persistableType
            )
        ) {
            try DatabaseRuntimeConfiguration(
                persistableTypes: [
                    RuntimeConfigurationScalarEntity.self,
                    RuntimeConfigurationScalarEntity.self,
                ]
            )
        }
    }

    @Test("Schema validation rejects a missing maintainer provider")
    func missingMaintainerProviderFailsValidation() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: RuntimeConfigurationScalarEntity.self)
            ]
        )
        let configuration = try DatabaseRuntimeConfiguration(
            persistableTypes: [RuntimeConfigurationScalarEntity.self]
        )
        let indexName = try RuntimeConfigurationScalarEntity
            .indexDescriptors[0].name

        #expect(
            throws: DatabaseRuntimeConfigurationError.missingIndexMaintainerProvider(
                source: .entity(RuntimeConfigurationScalarEntity.persistableType),
                indexName: indexName,
                kindIdentifier: "scalar"
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Builtin runtime satisfies compiled schema maintainers")
    func builtinRuntimeSatisfiesSchema() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: RuntimeConfigurationScalarEntity.self)
            ]
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            persistableTypes: [RuntimeConfigurationScalarEntity.self]
        )

        try configuration.validate(schema: schema)
    }

    @Test("Schema validation rejects a missing required read executor")
    func missingReadExecutorFailsValidation() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: RuntimeConfigurationVectorEntity.self)
            ]
        )
        let configuration = try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [
                VectorIndexMaintainerProvider()
            ],
            persistableTypes: [RuntimeConfigurationVectorEntity.self]
        )
        let indexName = try RuntimeConfigurationVectorEntity
            .indexDescriptors[0].name

        #expect(
            throws: DatabaseRuntimeConfigurationError.missingIndexReadExecutor(
                source: .entity(RuntimeConfigurationVectorEntity.persistableType),
                indexName: indexName,
                kindIdentifier: "vector"
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Schema validation rejects a missing entity mutation maintainer")
    func missingPersistableMutationMaintainerFailsValidation() throws {
        let descriptor = RuntimeConfigurationRelationshipOwner.relationshipDescriptors[0]
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: RuntimeConfigurationRelationshipTarget.self
                ),
                try Schema.Entity(
                    from: RuntimeConfigurationRelationshipOwner.self
                ),
            ]
        )
        let configuration = try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [
                ScalarIndexMaintainerProvider()
            ],
            persistableTypes: [
                RuntimeConfigurationRelationshipTarget.self,
                RuntimeConfigurationRelationshipOwner.self,
            ]
        )

        #expect(
            throws: DatabaseRuntimeConfigurationError.missingPersistableMutationMaintainer(
                entityName: RuntimeConfigurationRelationshipOwner.persistableType,
                descriptorName: descriptor.name,
                identifier: descriptor.runtimeMaintainerIdentifier
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Runtime configuration rejects duplicate entity mutation maintainers")
    func duplicatePersistableMutationMaintainerFailsInitialization() throws {
        let maintainer = RelationshipReferenceMaintainer()

        #expect(
            throws: DatabaseRuntimeConfigurationError.duplicatePersistableMutationMaintainer(
                identifier: maintainer.identifier
            )
        ) {
            try DatabaseRuntimeConfiguration(
                persistableMutationMaintainers: [maintainer, maintainer]
            )
        }
    }

    @Test("Builtin runtime satisfies relationship invariants")
    func builtinRuntimeSatisfiesRelationshipSchema() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: RuntimeConfigurationRelationshipTarget.self
                ),
                try Schema.Entity(
                    from: RuntimeConfigurationRelationshipOwner.self
                ),
            ]
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            persistableTypes: [
                RuntimeConfigurationRelationshipTarget.self,
                RuntimeConfigurationRelationshipOwner.self,
            ]
        )

        try configuration.validate(schema: schema)
    }
}
