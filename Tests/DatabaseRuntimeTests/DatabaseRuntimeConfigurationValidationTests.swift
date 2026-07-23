import Core
import DatabaseEngine
import DatabaseRuntime
import RelationshipIndex
import ScalarIndex
import Testing
import VectorIndex

@Suite("Database Runtime Configuration Validation")
struct DatabaseRuntimeConfigurationValidationTests {
    @Test("Schema validation rejects a missing maintainer provider")
    func missingMaintainerProviderFailsValidation() throws {
        let schema = Schema([RuntimeConfigurationScalarRecord.self])
        let configuration = try DatabaseRuntimeConfiguration()

        #expect(
            throws: DatabaseRuntimeConfigurationError.missingIndexMaintainerProvider(
                source: .entity(RuntimeConfigurationScalarRecord.persistableType),
                indexName: RuntimeConfigurationScalarRecord.indexDescriptors[0].name,
                kindIdentifier: "scalar"
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Builtin runtime satisfies compiled schema maintainers")
    func builtinRuntimeSatisfiesSchema() throws {
        let schema = Schema([RuntimeConfigurationScalarRecord.self])
        let configuration = try DatabaseFrameworkRuntime.configuration()

        try configuration.validate(schema: schema)
    }

    @Test("Schema validation rejects a missing required read executor")
    func missingReadExecutorFailsValidation() throws {
        let schema = Schema([RuntimeConfigurationVectorRecord.self])
        let configuration = try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [
                VectorIndexMaintainerProvider()
            ]
        )

        #expect(
            throws: DatabaseRuntimeConfigurationError.missingIndexReadExecutor(
                source: .entity(RuntimeConfigurationVectorRecord.persistableType),
                indexName: RuntimeConfigurationVectorRecord.indexDescriptors[0].name,
                kindIdentifier: "vector"
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Schema validation rejects a missing record mutation maintainer")
    func missingRecordMutationMaintainerFailsValidation() throws {
        let descriptor = RuntimeConfigurationRelationshipOwner.relationshipDescriptors[0]
        let schema = Schema([
            RuntimeConfigurationRelationshipTarget.self,
            RuntimeConfigurationRelationshipOwner.self,
        ])
        let configuration = try DatabaseRuntimeConfiguration(
            indexMaintainerProviders: [
                ScalarIndexMaintainerProvider()
            ]
        )

        #expect(
            throws: DatabaseRuntimeConfigurationError.missingRecordMutationMaintainer(
                entityName: RuntimeConfigurationRelationshipOwner.persistableType,
                descriptorName: descriptor.name,
                identifier: descriptor.runtimeMaintainerIdentifier
            )
        ) {
            try configuration.validate(schema: schema)
        }
    }

    @Test("Runtime configuration rejects duplicate record mutation maintainers")
    func duplicateRecordMutationMaintainerFailsInitialization() throws {
        let maintainer = RelationshipReferenceMaintainer()

        #expect(
            throws: DatabaseRuntimeConfigurationError.duplicateRecordMutationMaintainer(
                identifier: maintainer.identifier
            )
        ) {
            try DatabaseRuntimeConfiguration(
                recordMutationMaintainers: [maintainer, maintainer]
            )
        }
    }

    @Test("Builtin runtime satisfies relationship invariants")
    func builtinRuntimeSatisfiesRelationshipSchema() throws {
        let schema = Schema([
            RuntimeConfigurationRelationshipTarget.self,
            RuntimeConfigurationRelationshipOwner.self,
        ])
        let configuration = try DatabaseFrameworkRuntime.configuration()

        try configuration.validate(schema: schema)
    }
}
