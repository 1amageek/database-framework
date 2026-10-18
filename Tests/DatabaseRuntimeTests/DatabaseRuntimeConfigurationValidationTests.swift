import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import RelationshipIndex
import ScalarIndex
import StorageKit
import TestSupport
import Testing
import VectorIndex

@Suite("Database Runtime Configuration Validation")
struct DatabaseRuntimeConfigurationValidationTests {
    @Test("Runtime configuration rejects an empty execution identity")
    func emptyExecutionIdentityFailsInitialization() {
        #expect(
            throws: DatabaseRuntimeConfigurationError
                .invalidExecutionIdentityIdentifier
        ) {
            _ = try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "",
                    revision: 1
                )
            )
        }
    }

    @Test("Runtime configuration rejects a zero execution revision")
    func zeroExecutionRevisionFailsInitialization() {
        #expect(
            throws: DatabaseRuntimeConfigurationError
                .invalidExecutionIdentityRevision
        ) {
            _ = try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "application",
                    revision: 0
                )
            )
        }
    }

    @Test("Runtime configuration rejects an oversized execution identity")
    func oversizedExecutionIdentityFailsInitialization() {
        #expect(
            throws: DatabaseRuntimeConfigurationError
                .invalidExecutionIdentityIdentifier
        ) {
            _ = try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: String(
                        repeating: "a",
                        count: DatabaseExecutionRuntimeIdentity
                            .maximumIdentifierUTF8ByteCount + 1
                    ),
                    revision: 1
                )
            )
        }
    }

    @Test("Schema validation rejects an unregistered compiled entity type")
    func missingCompiledEntityTypeFailsValidation() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: RuntimeConfigurationScalarEntity.self)
            ]
        )
        let configuration = try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
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
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
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
            entityName: RuntimeConfigurationScalarEntity.persistableType,
            declaration: .ordered(
                name: "RuntimeConfigurationScalarEntity_schema_only",
                keys: [
                    .ascending(
                        RuntimeConfigurationScalarEntity.fields.name.identity
                    )
                ]
            ),
            fieldSchemas: try RuntimeConfigurationScalarEntity.fieldSchemas
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
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

    #if ScalarIndexes
    @Test("Runtime accepts additional indexes compiled with the schema")
    func matchingAdditionalIndexesPassValidation() throws {
        let additionalIndex = try IndexDescriptor(
            entityName: RuntimeConfigurationScalarEntity.persistableType,
            declaration: .ordered(
                name: "RuntimeConfigurationScalarEntity_additional",
                keys: [
                    .ascending(
                        RuntimeConfigurationScalarEntity.fields.name.identity
                    )
                ]
            ),
            fieldSchemas: try RuntimeConfigurationScalarEntity.fieldSchemas
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationScalarEntity.self,
                    including: [additionalIndex]
                )
            ]
        )

        try configuration.validate(schema: schema)
    }
    #endif

    #if GraphIndexes
    @Test("OWL runtime retains its provider with additional indexes")
    func owlRuntimeSupportsAdditionalIndexes() throws {
        let additionalIndex = try IndexDescriptor(
            entityName: RuntimeConfigurationOWLEntity.persistableType,
            declaration: .ordered(
                name: "RuntimeConfigurationOWLEntity_additional",
                keys: [
                    .ascending(
                        RuntimeConfigurationOWLEntity.fields.name.identity
                    )
                ]
            ),
            fieldSchemas: try RuntimeConfigurationOWLEntity.fieldSchemas
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationOWLEntity.self,
                    including: [additionalIndex]
                )
            ]
        )

        try configuration.validate(schema: schema)
    }
    #endif

    @Test("Container open rejects a mismatched entity before initialization")
    func containerOpenRejectsMismatchedEntitySchema() async throws {
        let additionalIndex = try IndexDescriptor(
            entityName: RuntimeConfigurationScalarEntity.persistableType,
            declaration: .ordered(
                name: "RuntimeConfigurationScalarEntity_schema_only",
                keys: [
                    .ascending(
                        RuntimeConfigurationScalarEntity.fields.name.identity
                    )
                ]
            ),
            fieldSchemas: try RuntimeConfigurationScalarEntity.fieldSchemas
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
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
                security: .testingDisabled
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
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
                indexType: .ordered
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    #if ScalarIndexes
    @Test("Builtin runtime satisfies compiled schema maintainers")
    func builtinRuntimeSatisfiesSchema() throws {
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: RuntimeConfigurationScalarEntity.self)
            ]
        )
        let configuration = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationScalarEntity.self
                )
            ]
        )

        try configuration.validate(schema: schema)
    }
    #endif

    #if VectorIndexes
    @Test("Builtin runtime validates polymorphic member maintainers")
    func builtinRuntimeValidatesPolymorphicMemberMaintainers() throws {
        let schema = try Schema(
            entities: [
                try RuntimeConfigurationPolymorphicVectorEntity.schemaEntity
            ]
        )
        let indexName = try #require(schema.polymorphicGroups.first?.indexes.first?.name)
        let deployment = VectorIndexConfiguration(indexName: indexName, algorithm: .flat)
        let configuration = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    RuntimeConfigurationPolymorphicVectorEntity.self
                )
            ],
            indexConfigurations: [deployment]
        )

        try configuration.validate(schema: schema)
        let added = try Schema.Entity(name: "RuntimeAddedSQLItem", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)],
            directoryComponents: [.staticPath("runtime-added-sql-item")])
        let composedSchema = try Schema(entities: schema.entities + [added], version: schema.version)
        let cloned = try configuration.replacingPhysicalSchema(composedSchema)
        try cloned.validate(schema: composedSchema)
        let retained = try #require(cloned.indexConfigurations(named: indexName).first)
        #expect(retained.indexName == deployment.indexName)
        #expect(try retained.executionOptions == deployment.executionOptions)
    }
    #endif

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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
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
                indexType: .vector
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
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
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                persistableMutationMaintainers: [maintainer, maintainer]
            )
        }
    }

    #if Relationships
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
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
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
    #endif
}
