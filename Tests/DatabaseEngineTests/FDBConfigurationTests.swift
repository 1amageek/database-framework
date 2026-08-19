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
import VectorIndex

/// Tests for DBConfiguration and runtime index configuration.
@Suite("DBConfiguration Tests", .foundationDBScenario, .serialized, .heartbeat)
struct DBConfigurationTests {

    // MARK: - Test Models

    @Persistable
    struct IndexConfigurationUser {
        #Directory<IndexConfigurationUser>("config_tests", "users")
        #Index(
            .text(
                name: "IndexConfigurationUser_name", fields: [\IndexConfigurationUser.name],
                mode: .fullText(
                    tokenizer: .simple, storePositions: true, ngramSize: 3,
                    minimumTermLength: 2)))
        #Index(
            .vector(
                name: "IndexConfigurationUser_embedding",
                embedding: \IndexConfigurationUser.embedding,
                dimensions: 3
            ))

        var id: String = ""
        var name: String = ""
        var embedding: Vector = Vector(int8: [])
    }

    // MARK: - Single Configuration API Tests

    @Test("Runtime generation retains indexConfigurations")
    func singleConfigurationAPI() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        let container = try await DBContainer.open(
            testing: schema,
            configuration: try .testing(
                storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        IndexConfigurationUser.self
                    )
                ],
                indexConfigurations: [
                    VectorIndexConfiguration(
                        indexName: "IndexConfigurationUser_embedding",
                        algorithm: .flat
                    )
                ]),
            security: .testingDisabled,
        )

        #expect(container.runtimeConfiguration.indexConfigurations.count == 1)
        #expect(
            container.runtimeConfiguration.indexConfigurations(
                named: "IndexConfigurationUser_embedding"
            ).count == 1)
    }

    @Test("Provider preflight rejects duplicate exclusive configurations")
    func duplicateConfigurationsFailBeforeInitialization() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        await #expect(throws: IndexRuntimeConfigurationError.self) {
            _ = try await DBContainer.open(
            testing: schema,
            configuration: try .testing(
                storageEngine: database),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [
                        try DatabaseFrameworkRuntime.entity(
                            IndexConfigurationUser.self
                        )
                    ],
                    indexConfigurations: [
                        VectorIndexConfiguration(
                            indexName: "IndexConfigurationUser_embedding",
                            algorithm: .flat
                        ),
                        VectorIndexConfiguration(
                            indexName: "IndexConfigurationUser_embedding",
                            algorithm: .hnsw(.default)
                        ),
                    ]
                ),
            security: .testingDisabled
            )
        }
    }

    @Test("DBContainer with empty indexConfigurations")
    func emptyIndexConfigurations() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)]),
            security: .testingDisabled,
        )

        #expect(container.runtimeConfiguration.indexConfigurations.isEmpty)
    }

    @Test("Runtime configuration rejects an index type mismatch")
    func configurationKindMismatch() throws {
        let schema = try Schema(
            entities: [try IndexConfigurationUser.schemaEntity]
        )
        let configuration = ContainerLocalizedTextConfiguration(
            indexName: "IndexConfigurationUser_embedding",
            language: "en"
        )
        let runtimeConfiguration = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)
            ],
            indexConfigurations: [configuration]
        )

        #expect(
            throws: IndexRuntimeConfigurationError.indexTypeMismatch(
                indexName: "IndexConfigurationUser_embedding",
                expected: .vector,
                actual: .text(.fullText)
            )
        ) {
            try IndexRuntimeConfigurationValidator.validate(
                schema: schema,
                runtimeConfiguration: runtimeConfiguration
            )
        }
    }

    @Test("Runtime configuration rejects an unknown index")
    func unknownConfiguredIndex() throws {
        let schema = try Schema(
            entities: [try IndexConfigurationUser.schemaEntity]
        )
        let configuration = ContainerEmbeddingConfiguration(
            indexName: "IndexConfigurationUser_missing",
            profileIdentifier: "unknown-index"
        )
        let runtimeConfiguration = try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(IndexConfigurationUser.self)
            ],
            indexConfigurations: [configuration]
        )

        #expect(
            throws: IndexRuntimeConfigurationError.unknownIndex(
                indexName: "IndexConfigurationUser_missing"
            )
        ) {
            try IndexRuntimeConfigurationValidator.validate(
                schema: schema,
                runtimeConfiguration: runtimeConfiguration
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
            configuration: try .testing(
                storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        IndexConfigurationUser.self
                    )
                ],
                indexConfigurations: [
                    VectorIndexConfiguration(
                        indexName: "IndexConfigurationUser_embedding",
                        algorithm: .hnsw(.default)
                    )
                ]),
            security: .testingDisabled,
        )

        let vectorConfig =
            container.runtimeConfiguration
            .indexConfigurations(named: "IndexConfigurationUser_embedding")
            .first as? VectorIndexConfiguration

        #expect(vectorConfig != nil)
        guard let vectorConfig, case .hnsw = vectorConfig.algorithm else {
            Issue.record("Expected the retained HNSW runtime policy")
            return
        }
    }

    @Test("A provider must explicitly accept runtime configuration")
    func unhandledProviderConfigurationFailsPreflight() async throws {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()

        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(entities: [try IndexConfigurationUser.schemaEntity])

        await #expect(throws: IndexRuntimeConfigurationError.self) {
            _ = try await DBContainer.open(
            testing: schema,
            configuration: try .testing(
                storageEngine: database),
                runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                    executionIdentity: DatabaseExecutionRuntimeIdentity(
                        identifier: "database-tests",
                        revision: 1
                    ),
                    entityRuntimes: [
                        try DatabaseFrameworkRuntime.entity(
                            IndexConfigurationUser.self
                        )
                    ],
                    indexConfigurations: [
                        ContainerLocalizedTextConfiguration(
                            indexName: "IndexConfigurationUser_name",
                            language: "en"
                        )
                    ]
                ),
            security: .testingDisabled
            )
        }
    }
}

// MARK: - Test Runtime Configurations

struct ContainerEmbeddingConfiguration: IndexRuntimeConfiguration, Sendable {
    static let indexType: IndexType = .vector

    let indexName: String

    let profileIdentifier: String

    init(
        indexName: String,
        profileIdentifier: String
    ) {
        self.indexName = indexName
        self.profileIdentifier = profileIdentifier
    }
}

struct ContainerLocalizedTextConfiguration: IndexRuntimeConfiguration, Sendable {
    static let indexType: IndexType = .text(.fullText)

    let indexName: String

    let language: String

    init(
        indexName: String,
        language: String
    ) {
        self.indexName = indexName
        self.language = language
    }
}
#endif

#endif
