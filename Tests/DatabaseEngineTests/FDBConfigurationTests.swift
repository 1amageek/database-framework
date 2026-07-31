#if !os(WASI)
#if FOUNDATION_DB
// DBConfigurationTests.swift
// Tests for DBConfiguration and runtime index configuration.

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import Logging
import Synchronization
import TestSupport
import DatabaseTypes
@testable import DatabaseKit
@testable import DatabaseEngine
import DatabaseRuntime

/// Tests for DBConfiguration and runtime index configuration.
@Suite("DBConfiguration Tests", .foundationDBScenario, .serialized, .heartbeat)
struct DBConfigurationTests {

    // MARK: - Test Models

    @Persistable
    struct IndexConfigurationUser {
        #Directory<IndexConfigurationUser>("config_tests", "users")
        #Index(
            .fullText(),
            fields: [\IndexConfigurationUser.name],
            name: "IndexConfigurationUser_name"
        )
        #Index(
            .vector(dimensions: 3),
            embedding: \IndexConfigurationUser.embedding,
            name: "IndexConfigurationUser_embedding"
        )

        var id: String = ""
        var name: String = ""
        var embedding: Vector = Vector(int8: [])
    }

    // MARK: - Single Configuration API Tests

    @Test("DBContainer accepts indexConfigurations")
    func singleConfigurationAPI() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        let container = try await DBContainer.open(
            testing: schema,
            configuration: .init(
                storageEngine: database,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(),
                indexConfigurations: [
                    ContainerEmbeddingConfiguration(
                        fieldName: "embedding",
                        entityName: "IndexConfigurationUser",
                        profileIdentifier: "single-config-test"
                    )
                ]
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)]),
            security: .disabled,
        )

        #expect(container.indexConfigurations.count == 1)
        #expect(container.indexConfigurations["IndexConfigurationUser_embedding"] != nil)
        #expect(container.indexConfigurations["IndexConfigurationUser_embedding"]?.count == 1)
    }

    @Test("DBContainer groups multiple configurations by indexName")
    func multipleConfigurationsGroupedByIndexName() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        let container = try await DBContainer.open(
            testing: schema,
            configuration: .init(
                storageEngine: database,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(),
                indexConfigurations: [
                    ContainerLocalizedTextConfiguration(fieldName: "name", entityName: "IndexConfigurationUser", language: "en"),
                    ContainerLocalizedTextConfiguration(fieldName: "name", entityName: "IndexConfigurationUser", language: "ja"),
                    ContainerLocalizedTextConfiguration(fieldName: "name", entityName: "IndexConfigurationUser", language: "zh"),
                    ContainerEmbeddingConfiguration(fieldName: "embedding", entityName: "IndexConfigurationUser", profileIdentifier: "test")
                ]
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)]),
            security: .disabled,
        )

        #expect(container.indexConfigurations.count == 2)
        #expect(container.indexConfigurations["IndexConfigurationUser_name"]?.count == 3)
        #expect(container.indexConfigurations["IndexConfigurationUser_embedding"]?.count == 1)
    }

    @Test("DBContainer with empty indexConfigurations")
    func emptyIndexConfigurations() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)]),
            security: .disabled,
        )

        #expect(container.indexConfigurations.isEmpty)
    }

    @Test("Runtime configuration rejects an index kind mismatch")
    func configurationKindMismatch() throws {
        let schema = try Schema(
            entities: [try IndexConfigurationUser.schemaEntity]
        )
        let entityRuntimes = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)
            ]
        ).entityRuntimes
        let configuration = ContainerLocalizedTextConfiguration(
            fieldName: "embedding",
            entityName: "IndexConfigurationUser",
            language: "en"
        )

        #expect(
            throws: IndexRuntimeConfigurationError.indexKindMismatch(
                indexName: "IndexConfigurationUser_embedding",
                expected: "vector",
                actual: "fulltext"
            )
        ) {
            try IndexRuntimeConfigurationValidator.validate(
                [configuration],
                schema: schema,
                entityRuntimes: entityRuntimes
            )
        }
    }

    @Test("Runtime configuration rejects an unknown index")
    func unknownConfiguredIndex() throws {
        let schema = try Schema(
            entities: [try IndexConfigurationUser.schemaEntity]
        )
        let entityRuntimes = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)
            ]
        ).entityRuntimes
        let configuration = ContainerEmbeddingConfiguration(
            fieldName: "missing",
            entityName: "IndexConfigurationUser",
            profileIdentifier: "unknown-index"
        )

        #expect(
            throws: IndexRuntimeConfigurationError.unknownIndex(
                indexName: "IndexConfigurationUser_missing"
            )
        ) {
            try IndexRuntimeConfigurationValidator.validate(
                [configuration],
                schema: schema,
                entityRuntimes: entityRuntimes
            )
        }
    }

    // MARK: - Configuration Access Helper Tests

    @Test("Grouped index configuration preserves its concrete policy type")
    func groupedIndexConfigurationPreservesPolicyType() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        let container = try await DBContainer.open(
            testing: schema,
            configuration: .init(
                storageEngine: database,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(),
                indexConfigurations: [
                    ContainerEmbeddingConfiguration(
                        fieldName: "embedding",
                        entityName: "IndexConfigurationUser",
                        profileIdentifier: "typed-access"
                    )
                ]
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)]),
            security: .disabled,
        )

        let vectorConfig = container.indexConfigurations[
            "IndexConfigurationUser_embedding"
        ]?.first as? ContainerEmbeddingConfiguration

        #expect(vectorConfig != nil)
        #expect(vectorConfig?.profileIdentifier == "typed-access")
    }

    @Test("Grouped index configurations preserve every matching policy")
    func groupedIndexConfigurationsPreserveMatchingPolicies() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        let container = try await DBContainer.open(
            testing: schema,
            configuration: .init(
                storageEngine: database,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(),
                indexConfigurations: [
                    ContainerLocalizedTextConfiguration(fieldName: "name", entityName: "IndexConfigurationUser", language: "en"),
                    ContainerLocalizedTextConfiguration(fieldName: "name", entityName: "IndexConfigurationUser", language: "ja")
                ]
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)]),
            security: .disabled,
        )

        let ftConfigs = container.indexConfigurations[
            "IndexConfigurationUser_name"
        ]?.compactMap { configuration in
            configuration as? ContainerLocalizedTextConfiguration
        } ?? []

        #expect(ftConfigs.count == 2)
        let languages = Set(ftConfigs.map { $0.language })
        #expect(languages.contains("en"))
        #expect(languages.contains("ja"))
    }
}

// MARK: - Test Runtime Configurations

struct ContainerEmbeddingConfiguration: IndexRuntimeConfiguration, Sendable {
    static var kindIdentifier: String { IndexDefinition.vector(dimensions: 1).identifier }

    let fieldName: String
    let entityName: String

    let profileIdentifier: String

    init(fieldName: String, entityName: String, profileIdentifier: String) {
        self.fieldName = fieldName
        self.entityName = entityName
        self.profileIdentifier = profileIdentifier
    }
}

struct ContainerLocalizedTextConfiguration: IndexRuntimeConfiguration, Sendable {
    static var kindIdentifier: String { IndexDefinition.fullText().identifier }

    let fieldName: String
    let entityName: String

    let language: String

    init(fieldName: String, entityName: String, language: String) {
        self.fieldName = fieldName
        self.entityName = entityName
        self.language = language
    }
}
#endif

#endif
