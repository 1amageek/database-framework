// IndexConfigurationTests.swift
// FDBIndexingTests - Tests for IndexConfiguration propagation and application

import Testing
import Foundation
import FoundationDB
@testable import Core
@testable import Core
@testable import DatabaseEngine

/// Tests for IndexConfiguration functionality
@Suite("IndexConfiguration Tests")
struct IndexConfigurationTests {

    // MARK: - IndexConfigurationApplicable Tests

    @Test("IndexConfigurationApplicable apply is called with correct configuration")
    func indexConfigurationApplicableApply() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        // Create mock configuration
        let mockConfig = MockVectorIndexConfig(
            fieldName: "embedding",
            modelTypeName: "ConfigTestItem",
            dimensions: 384,
            testParameter: "test-value"
        )

        // Create index
        let index = Index(
            name: "ConfigTestItem_embedding",
            kind: MockConfigurableIndexKind(dimensions: 384),
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "ConfigTestItem_embedding"
        )

        // Cast to IndexKindMaintainable and create maintainer
        guard let maintainable = index.kind as? any IndexKindMaintainable else {
            Issue.record("IndexKind does not conform to IndexKindMaintainable")
            return
        }

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")

        let maintainer: any IndexMaintainer<ConfigTestItem> = maintainable.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: [mockConfig]
        )

        // Verify the configuration was applied
        if let mockMaintainer = maintainer as? MockConfigurableIndexMaintainer<ConfigTestItem> {
            #expect(mockMaintainer.appliedDimensions == 384)
            #expect(mockMaintainer.appliedTestParameter == "test-value")
            #expect(mockMaintainer.configurationApplied == true)
        } else {
            Issue.record("Expected MockConfigurableIndexMaintainer but got \(type(of: maintainer))")
        }
    }

    @Test("MultiIndexConfigurationApplicable apply is called with multiple configurations")
    func multiIndexConfigurationApplicableApply() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        // Create multiple configurations (simulating multi-language full-text)
        let configs: [any IndexConfiguration] = [
            MockMultiLanguageIndexConfig(fieldName: "content", modelTypeName: "ConfigTestItem", language: "en"),
            MockMultiLanguageIndexConfig(fieldName: "content", modelTypeName: "ConfigTestItem", language: "ja"),
            MockMultiLanguageIndexConfig(fieldName: "content", modelTypeName: "ConfigTestItem", language: "zh")
        ]

        let index = Index(
            name: "ConfigTestItem_content",
            kind: MockMultiConfigIndexKind(),
            rootExpression: FieldKeyExpression(fieldName: "content"),
            subspaceKey: "ConfigTestItem_content"
        )

        guard let maintainable = index.kind as? any IndexKindMaintainable else {
            Issue.record("IndexKind does not conform to IndexKindMaintainable")
            return
        }

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")

        let maintainer: any IndexMaintainer<ConfigTestItem> = maintainable.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configs
        )

        // Verify all configurations were applied
        if let mockMaintainer = maintainer as? MockMultiConfigIndexMaintainer<ConfigTestItem> {
            #expect(mockMaintainer.appliedLanguages.count == 3)
            #expect(mockMaintainer.appliedLanguages.contains("en"))
            #expect(mockMaintainer.appliedLanguages.contains("ja"))
            #expect(mockMaintainer.appliedLanguages.contains("zh"))
        } else {
            Issue.record("Expected MockMultiConfigIndexMaintainer but got \(type(of: maintainer))")
        }
    }

    @Test("Configuration not applied when index name doesn't match")
    func configurationNotAppliedForMismatchedIndex() async throws {
        try await FDBTestEnvironment.shared.ensureInitialized()

        // Create configuration for a DIFFERENT index
        let mockConfig = MockVectorIndexConfig(
            fieldName: "embedding",
            modelTypeName: "ConfigTestItem",
            dimensions: 768,
            testParameter: "wrong-index"
        )
        // Note: This config will have indexName "ConfigTestItem_embedding" but we're creating
        // a different index

        let index = Index(
            name: "ConfigTestItem_otherField",  // Different index name
            kind: MockConfigurableIndexKind(dimensions: 128),
            rootExpression: FieldKeyExpression(fieldName: "otherField"),
            subspaceKey: "ConfigTestItem_otherField"
        )

        guard let maintainable = index.kind as? any IndexKindMaintainable else {
            Issue.record("IndexKind does not conform to IndexKindMaintainable")
            return
        }

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")

        let maintainer: any IndexMaintainer<ConfigTestItem> = maintainable.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: [mockConfig]  // Config for different index
        )

        // Verify configuration was NOT applied (uses defaults)
        if let mockMaintainer = maintainer as? MockConfigurableIndexMaintainer<ConfigTestItem> {
            #expect(mockMaintainer.configurationApplied == false)
            // Default values should be used
            #expect(mockMaintainer.appliedDimensions == 128)  // From IndexKind
        } else {
            Issue.record("Expected MockConfigurableIndexMaintainer but got \(type(of: maintainer))")
        }
    }

    // MARK: - Configuration Name Matching Tests

    @Test("IndexConfiguration indexName is computed correctly")
    func indexConfigurationIndexName() {
        let config = MockVectorIndexConfig(
            fieldName: "embedding",
            modelTypeName: "ConfigTestItem",
            dimensions: 384,
            testParameter: "test"
        )

        // indexName should be "{modelTypeName}_{fieldName}"
        #expect(config.indexName == "ConfigTestItem_embedding")
    }

    @Test("IndexConfiguration kindIdentifier matches expected kind")
    func indexConfigurationKindIdentifier() {
        #expect(MockVectorIndexConfig.kindIdentifier == "mock-configurable")
        #expect(MockMultiLanguageIndexConfig.kindIdentifier == "mock-multi-config")
    }
}

// MARK: - Test Fixtures

/// Test item for configuration tests
@Persistable
struct ConfigTestItem {
    var content: String = ""
    var embedding: [Float] = []
    var otherField: String = ""
}

// MARK: - Mock IndexConfiguration

/// Mock IndexConfiguration for testing single configuration application (no generic KeyPath)
struct MockVectorIndexConfig: IndexConfiguration, Sendable {
    static var kindIdentifier: String { "mock-configurable" }

    let fieldName: String
    let _modelTypeName: String
    var modelTypeName: String { _modelTypeName }

    // Dummy keyPath for protocol conformance
    var keyPath: AnyKeyPath { \ConfigTestItem.embedding }

    var indexName: String { "\(_modelTypeName)_\(fieldName)" }

    let dimensions: Int
    let testParameter: String

    init(fieldName: String, modelTypeName: String, dimensions: Int, testParameter: String) {
        self.fieldName = fieldName
        self._modelTypeName = modelTypeName
        self.dimensions = dimensions
        self.testParameter = testParameter
    }
}

/// Mock IndexConfiguration for testing multi-configuration application
struct MockMultiLanguageIndexConfig: IndexConfiguration, Sendable {
    static var kindIdentifier: String { "mock-multi-config" }

    let fieldName: String
    let _modelTypeName: String
    var modelTypeName: String { _modelTypeName }

    // Dummy keyPath for protocol conformance
    var keyPath: AnyKeyPath { \ConfigTestItem.content }

    var indexName: String { "\(_modelTypeName)_\(fieldName)" }

    let language: String

    init(fieldName: String, modelTypeName: String, language: String) {
        self.fieldName = fieldName
        self._modelTypeName = modelTypeName
        self.language = language
    }
}

// MARK: - Mock IndexKind

/// Mock IndexKind that creates a configurable maintainer
struct MockConfigurableIndexKind: IndexKind {
    static let identifier = "mock-configurable"
    static let subspaceStructure = SubspaceStructure.hierarchical

    let dimensions: Int

    init(dimensions: Int = 0) {
        self.dimensions = dimensions
    }

    static func validateTypes(_ types: [Any.Type]) throws {
        // Accept any types for testing
    }
}

extension MockConfigurableIndexKind: IndexKindMaintainable {
    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        let maintainer = MockConfigurableIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            defaultDimensions: dimensions
        )

        // Apply matching configuration
        if let config = configurations.first(where: { $0.indexName == index.name }) as? MockVectorIndexConfig {
            maintainer.applyConfig(dimensions: config.dimensions, testParameter: config.testParameter)
        }

        return maintainer
    }
}

/// Mock IndexKind that creates a multi-configuration maintainer
struct MockMultiConfigIndexKind: IndexKind {
    static let identifier = "mock-multi-config"
    static let subspaceStructure = SubspaceStructure.flat

    static func validateTypes(_ types: [Any.Type]) throws {
        // Accept any types for testing
    }

    init() {}
}

extension MockMultiConfigIndexKind: IndexKindMaintainable {
    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        let maintainer = MockMultiConfigIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression
        )

        // Apply all matching configurations
        let matchingConfigs = configurations
            .filter { $0.indexName == index.name }
            .compactMap { $0 as? MockMultiLanguageIndexConfig }

        for config in matchingConfigs {
            maintainer.addLanguage(config.language)
        }

        return maintainer
    }
}

// MARK: - Mock IndexMaintainer

/// Mock IndexMaintainer with configuration tracking
final class MockConfigurableIndexMaintainer<Item: Persistable>: IndexMaintainer, @unchecked Sendable {
    let index: Index
    let subspace: Subspace
    let idExpression: KeyExpression

    // Track applied configuration
    var configurationApplied: Bool = false
    var appliedDimensions: Int
    var appliedTestParameter: String = ""

    init(index: Index, subspace: Subspace, idExpression: KeyExpression, defaultDimensions: Int = 0) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.appliedDimensions = defaultDimensions
    }

    func applyConfig(dimensions: Int, testParameter: String) {
        self.configurationApplied = true
        self.appliedDimensions = dimensions
        self.appliedTestParameter = testParameter
    }

    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // No-op for testing
    }

    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // No-op for testing
    }
}

/// Mock IndexMaintainer with multi-configuration tracking
final class MockMultiConfigIndexMaintainer<Item: Persistable>: IndexMaintainer, @unchecked Sendable {
    let index: Index
    let subspace: Subspace
    let idExpression: KeyExpression

    // Track applied configurations
    var appliedLanguages: Set<String> = []

    init(index: Index, subspace: Subspace, idExpression: KeyExpression) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
    }

    func addLanguage(_ language: String) {
        appliedLanguages.insert(language)
    }

    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // No-op for testing
    }

    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        // No-op for testing
    }
}
