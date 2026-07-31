import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import RelationshipIndex
import ScalarIndex
import StorageKit
import TestSupport
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
            indexMaintainerProviderDescriptors: [
                .init(describing: ScalarIndexMaintainerProvider())
            ]
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
                entityRuntimes: [
                    try EntityRuntimeDefinition(
                        RuntimeConfigurationScalarEntity.self
                    ).registration(),
                    try EntityRuntimeDefinition(
                        RuntimeConfigurationScalarEntity.self
                    ).registration(),
                ]
            )
        }
    }

    @Test("Schema validation rejects a compiled entity with different schema")
    func mismatchedCompiledEntitySchemaFailsValidation() throws {
        let additionalIndex = try IndexDescriptor(
            name: "RuntimeConfigurationScalarEntity_schema_only",
            definition: .scalar,
            fields: [
                RuntimeConfigurationScalarEntity.fields.name.ascending
            ]
        )
        let schemaEntity = try Schema.Entity(
            from: RuntimeConfigurationScalarEntity.self,
            including: [additionalIndex]
        )
        let runtimeEntity = try Schema.Entity(
            from: RuntimeConfigurationScalarEntity.self
        )
        let schema = try Schema(entities: [schemaEntity])
        let configuration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationScalarEntity.self
                )
            ]
        )

        #expect(
            throws: DatabaseRuntimeConfigurationError.entitySchemaMismatch(
                entityName: RuntimeConfigurationScalarEntity.persistableType,
                schemaEntity: schemaEntity,
                runtimeEntity: runtimeEntity
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Runtime accepts additional indexes compiled with the schema")
    func matchingAdditionalIndexesPassValidation() throws {
        let additionalIndex = try IndexDescriptor(
            name: "RuntimeConfigurationScalarEntity_additional",
            definition: .scalar,
            fields: [
                RuntimeConfigurationScalarEntity.fields.name.ascending
            ]
        )
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: RuntimeConfigurationScalarEntity.self,
                    including: [additionalIndex]
                )
            ]
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationScalarEntity.self,
                    including: [additionalIndex]
                )
            ]
        )

        try configuration.validate(schema: schema)
    }

    @Test("OWL runtime retains its provider with additional indexes")
    func owlRuntimeSupportsAdditionalIndexes() throws {
        let additionalIndex = try IndexDescriptor(
            name: "RuntimeConfigurationOWLEntity_additional",
            definition: .scalar,
            fields: [
                RuntimeConfigurationOWLEntity.fields.name.ascending
            ]
        )
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: RuntimeConfigurationOWLEntity.self,
                    including: [additionalIndex]
                )
            ]
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationOWLEntity.self,
                    including: [additionalIndex]
                )
            ]
        )

        try configuration.validate(schema: schema)
    }

    @Test("Container open rejects a mismatched entity before initialization")
    func containerOpenRejectsMismatchedEntitySchema() async throws {
        let additionalIndex = try IndexDescriptor(
            name: "RuntimeConfigurationScalarEntity_schema_only",
            definition: .scalar,
            fields: [
                RuntimeConfigurationScalarEntity.fields.name.ascending
            ]
        )
        let schemaEntity = try Schema.Entity(
            from: RuntimeConfigurationScalarEntity.self,
            including: [additionalIndex]
        )
        let runtimeEntity = try Schema.Entity(
            from: RuntimeConfigurationScalarEntity.self
        )
        let schema = try Schema(entities: [schemaEntity])
        let configuration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationScalarEntity.self
                )
            ]
        )

        await #expect(
            throws: DatabaseRuntimeConfigurationError.entitySchemaMismatch(
                entityName: RuntimeConfigurationScalarEntity.persistableType,
                schemaEntity: schemaEntity,
                runtimeEntity: runtimeEntity
            )
        ) {
            _ = try await DBContainer.open(
                for: schema,
                configuration: .testing(
                    storageEngine: InMemoryEngine()
                ),
                runtimeConfiguration: configuration,
                security: .disabled
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
            entityRuntimes: [
                try EntityRuntimeDefinition(
                    RuntimeConfigurationScalarEntity.self
                ).registration()
            ]
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
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationScalarEntity.self
                )
            ]
        )

        try configuration.validate(schema: schema)
    }

    @Test("Builtin runtime validates polymorphic member maintainers")
    func builtinRuntimeValidatesPolymorphicMemberMaintainers() throws {
        let schema = try Schema(
            entities: [
                try RuntimeConfigurationPolymorphicVectorEntity.schemaEntity
            ]
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationPolymorphicVectorEntity.self
                )
            ]
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
        var entityRuntime = try EntityRuntimeDefinition(
            RuntimeConfigurationVectorEntity.self
        )
        try entityRuntime.register(VectorIndexMaintainerProvider())
        let configuration = try DatabaseRuntimeConfiguration(
            indexMaintainerProviderDescriptors: [
                .init(describing: VectorIndexMaintainerProvider())
            ],
            entityRuntimes: [entityRuntime.registration()]
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

    @Test("Schema validation rejects uniqueness without maintainer support")
    func missingUniquenessSupportFailsValidation() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: RuntimeConfigurationUniqueVectorEntity.self
                )
            ]
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationUniqueVectorEntity.self
                )
            ]
        )
        let descriptor = try RuntimeConfigurationUniqueVectorEntity
            .indexDescriptors[0]

        #expect(
            throws: DatabaseRuntimeConfigurationError
                .missingIndexUniquenessSupport(
                    source: .entity(
                        RuntimeConfigurationUniqueVectorEntity.persistableType
                    ),
                    indexName: descriptor.name,
                    kindIdentifier: descriptor.kindIdentifier
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
            indexMaintainerProviderDescriptors: [
                .init(describing: ScalarIndexMaintainerProvider())
            ],
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationRelationshipTarget.self
                ),
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationRelationshipOwner.self
                ),
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
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationRelationshipTarget.self
                ),
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationRelationshipOwner.self
                ),
            ]
        )

        try configuration.validate(schema: schema)
    }
}
