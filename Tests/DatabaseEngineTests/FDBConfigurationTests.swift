// FDBConfigurationTests.swift
// Tests for FDBConfiguration and IndexConfiguration API

import Testing
import Foundation
import FoundationDB
import Logging
import Synchronization
@testable import Core
@testable import DatabaseEngine

/// Tests for FDBConfiguration and IndexConfiguration API
@Suite("FDBConfiguration Tests")
struct FDBConfigurationTests {

    // MARK: - Test Models

    @Persistable
    struct ConfigTestUser {
        #Directory<ConfigTestUser>("test", "config", "users")
        #Index<ConfigTestUser>([\.name], type: ScalarIndexKind())
        #Index<ConfigTestUser>([\.embedding], type: ScalarIndexKind())

        var name: String = ""
        var embedding: [Float] = []
    }

    // MARK: - Single Configuration API Tests

    @Test("FDBContainer accepts indexConfigurations")
    func singleConfigurationAPI() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try FDBClient.openDatabase()
        let schema = Schema([ConfigTestUser.self])

        let container = FDBContainer(
            database: database,
            schema: schema,
            indexConfigurations: [
                TestVectorConfig(
                    fieldName: "embedding",
                    modelTypeName: "ConfigTestUser",
                    dimensions: 512,
                    testValue: "single-config-test"
                )
            ]
        )

        #expect(container.indexConfigurations.count == 1)
        #expect(container.indexConfigurations["ConfigTestUser_embedding"] != nil)
        #expect(container.indexConfigurations["ConfigTestUser_embedding"]?.count == 1)
    }

    @Test("FDBContainer groups multiple configurations by indexName")
    func multipleConfigurationsGroupedByIndexName() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try FDBClient.openDatabase()
        let schema = Schema([ConfigTestUser.self])

        let container = FDBContainer(
            database: database,
            schema: schema,
            indexConfigurations: [
                TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "en"),
                TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "ja"),
                TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "zh"),
                TestVectorConfig(fieldName: "embedding", modelTypeName: "ConfigTestUser", dimensions: 256, testValue: "test")
            ]
        )

        #expect(container.indexConfigurations.count == 2)
        #expect(container.indexConfigurations["ConfigTestUser_name"]?.count == 3)
        #expect(container.indexConfigurations["ConfigTestUser_embedding"]?.count == 1)
    }

    @Test("FDBContainer with empty indexConfigurations")
    func emptyIndexConfigurations() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try FDBClient.openDatabase()
        let schema = Schema([ConfigTestUser.self])

        let container = FDBContainer(
            database: database,
            schema: schema,
            indexConfigurations: []
        )

        #expect(container.indexConfigurations.isEmpty)
    }

    // MARK: - Configuration Access Helper Tests

    @Test("indexConfiguration(for:as:) returns correct typed configuration")
    func indexConfigurationTypedAccess() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        let database = try FDBClient.openDatabase()
        let schema = Schema([ConfigTestUser.self])

        let container = FDBContainer(
            database: database,
            schema: schema,
            indexConfigurations: [
                TestVectorConfig(
                    fieldName: "embedding",
                    modelTypeName: "ConfigTestUser",
                    dimensions: 768,
                    testValue: "typed-access"
                )
            ]
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

        let database = try FDBClient.openDatabase()
        let schema = Schema([ConfigTestUser.self])

        let container = FDBContainer(
            database: database,
            schema: schema,
            indexConfigurations: [
                TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "en"),
                TestFullTextConfig(fieldName: "name", modelTypeName: "ConfigTestUser", language: "ja")
            ]
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

// MARK: - FDBConfiguration Properties Tests

@Suite("FDBConfiguration Properties Tests")
struct FDBConfigurationPropertiesTests {

    @Persistable
    struct ConfigTestUser {
        #Directory<ConfigTestUser>("test", "config", "users")
        var name: String = ""
        var embedding: [Float] = []
    }

    @Test("FDBConfiguration stores all properties correctly")
    func allPropertiesStored() {
        let schema = Schema([ConfigTestUser.self])
        let url = URL(filePath: "/custom/path/fdb.cluster")
        let configs: [any IndexConfiguration] = [
            TestVectorConfig(fieldName: "embedding", modelTypeName: "ConfigTestUser", dimensions: 128, testValue: "test")
        ]

        let config = FDBConfiguration(
            name: "test-config",
            schema: schema,
            apiVersion: 710,
            url: url,
            indexConfigurations: configs
        )

        #expect(config.name == "test-config")
        #expect(config.schema != nil)
        #expect(config.apiVersion == 710)
        #expect(config.url?.path == "/custom/path/fdb.cluster")
        #expect(config.indexConfigurations.count == 1)
    }

    @Test("FDBConfiguration convenience initializer sets defaults")
    func convenienceInitializerDefaults() {
        let schema = Schema([ConfigTestUser.self])
        let config = FDBConfiguration(schema: schema)

        #expect(config.name == nil)
        #expect(config.schema != nil)
        #expect(config.apiVersion == nil)
        #expect(config.url == nil)
        #expect(config.indexConfigurations.isEmpty)
    }

    @Test("FDBConfiguration debugDescription includes all info")
    func debugDescriptionComplete() {
        let schema = Schema([ConfigTestUser.self])
        let config = FDBConfiguration(
            name: "debug-test",
            schema: schema,
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
    var keyPath: AnyKeyPath { \FDBConfigurationTests.ConfigTestUser.embedding }
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
    var keyPath: AnyKeyPath { \FDBConfigurationTests.ConfigTestUser.name }
    var indexName: String { "\(_modelTypeName)_\(fieldName)" }

    let language: String

    init(fieldName: String, modelTypeName: String, language: String) {
        self.fieldName = fieldName
        self._modelTypeName = modelTypeName
        self.language = language
    }
}
