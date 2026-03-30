#if FOUNDATION_DB
// DBConfigurationTests.swift
// Tests for DBConfiguration and IndexConfiguration API

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import Logging
import Synchronization
@testable import Core
@testable import DatabaseEngine

/// Tests for DBConfiguration and IndexConfiguration API
@Suite("DBConfiguration Tests", .heartbeat)
struct DBConfigurationTests {

    // MARK: - Test Models

    @Persistable
    struct ConfigTestUser {
        #Directory<ConfigTestUser>("test", "config", "users")
        #Index(ScalarIndexKind<ConfigTestUser>(fields: [\.name]))
        #Index(ScalarIndexKind<ConfigTestUser>(fields: [\.embedding]))

        var name: String = ""
        var embedding: [Float] = []
    }

    // MARK: - Single Configuration API Tests

    @Test("DBContainer accepts indexConfigurations")
    func singleConfigurationAPI() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try await FDBStorageEngine(configuration: .init())
        let schema = Schema([ConfigTestUser.self])

        let container = try await DBContainer(
            for: schema,
            configuration: .init(
                backend: .custom(database),
                indexConfigurations: [
                    TestVectorConfig(
                        fieldName: "embedding",
                        modelTypeName: "ConfigTestUser",
                        dimensions: 512,
                        testValue: "single-config-test"
                    )
                ]
            ),
            security: .disabled
        )

        #expect(container.indexConfigurations.count == 1)
        #expect(container.indexConfigurations["ConfigTestUser_embedding"] != nil)
        #expect(container.indexConfigurations["ConfigTestUser_embedding"]?.count == 1)
    }

    @Test("DBContainer groups multiple configurations by indexName")
    func multipleConfigurationsGroupedByIndexName() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try await FDBStorageEngine(configuration: .init())
        let schema = Schema([ConfigTestUser.self])

        let container = try await DBContainer(
            for: schema,
            configuration: .init(
                backend: .custom(database),
                indexConfigurations: [
                    TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "en"),
                    TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "ja"),
                    TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "zh"),
                    TestVectorConfig(fieldName: "embedding", modelTypeName: "ConfigTestUser", dimensions: 256, testValue: "test")
                ]
            ),
            security: .disabled
        )

        #expect(container.indexConfigurations.count == 2)
        #expect(container.indexConfigurations["ConfigTestUser_name"]?.count == 3)
        #expect(container.indexConfigurations["ConfigTestUser_embedding"]?.count == 1)
    }

    @Test("DBContainer with empty indexConfigurations")
    func emptyIndexConfigurations() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try await FDBStorageEngine(configuration: .init())
        let schema = Schema([ConfigTestUser.self])

        let container = try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(database)),
            security: .disabled
        )

        #expect(container.indexConfigurations.isEmpty)
    }

    // MARK: - Configuration Access Helper Tests

    @Test("indexConfiguration(for:as:) returns correct typed configuration")
    func indexConfigurationTypedAccess() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try await FDBStorageEngine(configuration: .init())
        let schema = Schema([ConfigTestUser.self])

        let container = try await DBContainer(
            for: schema,
            configuration: .init(
                backend: .custom(database),
                indexConfigurations: [
                    TestVectorConfig(
                        fieldName: "embedding",
                        modelTypeName: "ConfigTestUser",
                        dimensions: 768,
                        testValue: "typed-access"
                    )
                ]
            ),
            security: .disabled
        )

        let vectorConfig = container.indexConfiguration(
            for: "ConfigTestUser_embedding",
            as: TestVectorConfig.self
        )

        #expect(vectorConfig != nil)
        #expect(vectorConfig?.dimensions == 768)
        #expect(vectorConfig?.testValue == "typed-access")
    }

    @Test("indexConfigurations(for:as:) returns all matching typed configurations")
    func indexConfigurationsTypedAccess() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try await FDBStorageEngine(configuration: .init())
        let schema = Schema([ConfigTestUser.self])

        let container = try await DBContainer(
            for: schema,
            configuration: .init(
                backend: .custom(database),
                indexConfigurations: [
                    TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "en"),
                    TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "ja")
                ]
            ),
            security: .disabled
        )

        let ftConfigs = container.indexConfigurations(
            for: "ConfigTestUser_name",
            as: TestFullTextConfig.self
        )

        #expect(ftConfigs.count == 2)
        let languages = Set(ftConfigs.map { $0.language })
        #expect(languages.contains("en"))
        #expect(languages.contains("ja"))
    }
}

// MARK: - DBConfiguration Properties Tests

@Suite("DBConfiguration Properties Tests", .heartbeat)
struct DBConfigurationPropertiesTests {

    @Persistable
    struct ConfigTestUser {
        #Directory<ConfigTestUser>("test", "config", "users")
        var name: String = ""
        var embedding: [Float] = []
    }

    @Test("DBConfiguration stores all properties correctly")
    func allPropertiesStored() {
        let configs: [any IndexConfiguration] = [
            TestVectorConfig(fieldName: "embedding", modelTypeName: "ConfigTestUser", dimensions: 128, testValue: "test")
        ]

        let config = DBConfiguration(
            name: "test-config",
            indexConfigurations: configs
        )

        #expect(config.name == "test-config")
        #expect(config.indexConfigurations.count == 1)
    }

    @Test("DBConfiguration convenience initializer sets defaults")
    func convenienceInitializerDefaults() {
        let config = DBConfiguration()

        #expect(config.name == nil)
        #expect(config.indexConfigurations.isEmpty)
    }

    @Test("DBConfiguration debugDescription includes all info")
    func debugDescriptionComplete() {
        let config = DBConfiguration(
            name: "debug-test",
            indexConfigurations: [
                TestVectorConfig(fieldName: "embedding", modelTypeName: "ConfigTestUser", dimensions: 64, testValue: "test")
            ]
        )

        let desc = config.debugDescription
        #expect(desc.contains("debug-test"))
        #expect(desc.contains("indexConfigs: 1"))
    }
}

// MARK: - Test IndexConfiguration Implementations

struct TestVectorConfig: IndexConfiguration, Sendable {
    static var kindIdentifier: String { "scalar" }

    let fieldName: String
    let _modelTypeName: String
    var modelTypeName: String { _modelTypeName }
    var keyPath: AnyKeyPath { \DBConfigurationTests.ConfigTestUser.embedding }
    var indexName: String { "\(_modelTypeName)_\(fieldName)" }

    let dimensions: Int
    let testValue: String

    init(fieldName: String, modelTypeName: String, dimensions: Int, testValue: String) {
        self.fieldName = fieldName
        self._modelTypeName = modelTypeName
        self.dimensions = dimensions
        self.testValue = testValue
    }
}

struct TestFullTextConfig: IndexConfiguration, Sendable {
    static var kindIdentifier: String { "scalar" }

    let fieldName: String
    let _modelTypeName: String
    var modelTypeName: String { _modelTypeName }
    var keyPath: AnyKeyPath { \DBConfigurationTests.ConfigTestUser.name }
    var indexName: String { "\(_modelTypeName)_\(fieldName)" }

    let language: String

    init(fieldName: String, modelTypeName: String, language: String) {
        self.fieldName = fieldName
        self._modelTypeName = modelTypeName
        self.language = language
    }
}
#endif
