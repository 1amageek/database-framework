#if !os(WASI)
#if FOUNDATION_DB
// IndexConfigurationTests.swift
// FDBIndexingTests - Tests for IndexConfiguration propagation and application

import Testing
import TestHeartbeat
import Foundation
import StorageKit
@testable import Core
@testable import DatabaseEngine

/// Tests for IndexConfiguration functionality
@Suite("IndexConfiguration Tests", .heartbeat)
struct IndexConfigurationTests {

    // MARK: - Provider Configuration Tests

    @Test("Provider applies the matching configuration")
    func indexConfigurationApplicableApply() async throws {
        let configuration = DimensionIndexConfiguration(
            fieldName: "embedding",
            modelTypeName: "IndexConfigurationEntity",
            dimensions: 384,
            distanceMetric: "cosine"
        )

        let index = Index(
            name: "IndexConfigurationEntity_embedding",
            kind: DimensionConfiguredIndexKind(dimensions: 384),
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: "IndexConfigurationEntity_embedding"
        )

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")
        let registry = try IndexMaintainerProviderRegistry(
            providers: [DimensionConfiguredIndexMaintainerProvider()]
        )
        let maintainer: any IndexMaintainer<IndexConfigurationEntity> = try registry.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: [configuration]
        )

        if let recordingMaintainer = maintainer as? ConfigurationRecordingIndexMaintainer<IndexConfigurationEntity> {
            #expect(recordingMaintainer.appliedDimensions == 384)
            #expect(recordingMaintainer.appliedDistanceMetric == "cosine")
            #expect(recordingMaintainer.configurationApplied == true)
        } else {
            Issue.record("Expected ConfigurationRecordingIndexMaintainer but got \(type(of: maintainer))")
        }
    }

    @Test("Provider applies all matching configurations")
    func multiIndexConfigurationApplicableApply() async throws {
        let configs: [any IndexConfiguration] = [
            LanguageIndexConfiguration(fieldName: "content", modelTypeName: "IndexConfigurationEntity", language: "en"),
            LanguageIndexConfiguration(fieldName: "content", modelTypeName: "IndexConfigurationEntity", language: "ja"),
            LanguageIndexConfiguration(fieldName: "content", modelTypeName: "IndexConfigurationEntity", language: "zh")
        ]

        let index = Index(
            name: "IndexConfigurationEntity_content",
            kind: LanguageConfiguredIndexKind(),
            rootExpression: FieldKeyExpression(fieldName: "content"),
            subspaceKey: "IndexConfigurationEntity_content"
        )

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")
        let registry = try IndexMaintainerProviderRegistry(
            providers: [LanguageConfiguredIndexMaintainerProvider()]
        )
        let maintainer: any IndexMaintainer<IndexConfigurationEntity> = try registry.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configs
        )

        if let recordingMaintainer = maintainer as? LanguageRecordingIndexMaintainer<IndexConfigurationEntity> {
            #expect(recordingMaintainer.appliedLanguages.count == 3)
            #expect(recordingMaintainer.appliedLanguages.contains("en"))
            #expect(recordingMaintainer.appliedLanguages.contains("ja"))
            #expect(recordingMaintainer.appliedLanguages.contains("zh"))
        } else {
            Issue.record("Expected LanguageRecordingIndexMaintainer but got \(type(of: maintainer))")
        }
    }

    @Test("Configuration not applied when index name doesn't match")
    func configurationNotAppliedForMismatchedIndex() async throws {
        let configuration = DimensionIndexConfiguration(
            fieldName: "embedding",
            modelTypeName: "IndexConfigurationEntity",
            dimensions: 768,
            distanceMetric: "euclidean"
        )
        let index = Index(
            name: "IndexConfigurationEntity_otherField",
            kind: DimensionConfiguredIndexKind(dimensions: 128),
            rootExpression: FieldKeyExpression(fieldName: "otherField"),
            subspaceKey: "IndexConfigurationEntity_otherField"
        )

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")
        let registry = try IndexMaintainerProviderRegistry(
            providers: [DimensionConfiguredIndexMaintainerProvider()]
        )
        let maintainer: any IndexMaintainer<IndexConfigurationEntity> = try registry.makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: [configuration]
        )

        if let recordingMaintainer = maintainer as? ConfigurationRecordingIndexMaintainer<IndexConfigurationEntity> {
            #expect(recordingMaintainer.configurationApplied == false)
            #expect(recordingMaintainer.appliedDimensions == 128)
        } else {
            Issue.record("Expected ConfigurationRecordingIndexMaintainer but got \(type(of: maintainer))")
        }
    }

    // MARK: - Configuration Name Matching Tests

    @Test("IndexConfiguration indexName is computed correctly")
    func indexConfigurationIndexName() {
        let configuration = DimensionIndexConfiguration(
            fieldName: "embedding",
            modelTypeName: "IndexConfigurationEntity",
            dimensions: 384,
            distanceMetric: "cosine"
        )

        // indexName should be "{modelTypeName}_{fieldName}"
        #expect(configuration.indexName == "IndexConfigurationEntity_embedding")
    }

    @Test("IndexConfiguration kindIdentifier matches expected kind")
    func indexConfigurationKindIdentifier() {
        #expect(DimensionIndexConfiguration.kindIdentifier == "dimension-configured")
        #expect(LanguageIndexConfiguration.kindIdentifier == "language-configured")
    }
}

// MARK: - Configuration Entity

@Persistable
struct IndexConfigurationEntity {
    var content: String = ""
    var embedding: [Float] = []
    var otherField: String = ""
}

// MARK: - Index Configuration Scenarios

/// Dimension-bearing configuration for single-configuration application.
struct DimensionIndexConfiguration: IndexConfiguration, Sendable {
    static var kindIdentifier: String { "dimension-configured" }

    let fieldName: String
    let _modelTypeName: String
    var modelTypeName: String { _modelTypeName }

    var indexName: String { "\(_modelTypeName)_\(fieldName)" }

    let dimensions: Int
    let distanceMetric: String

    init(
        fieldName: String,
        modelTypeName: String,
        dimensions: Int,
        distanceMetric: String
    ) {
        self.fieldName = fieldName
        self._modelTypeName = modelTypeName
        self.dimensions = dimensions
        self.distanceMetric = distanceMetric
    }
}

/// Language-bearing configuration used to verify multi-configuration application.
struct LanguageIndexConfiguration: IndexConfiguration, Sendable {
    static var kindIdentifier: String { "language-configured" }

    let fieldName: String
    let _modelTypeName: String
    var modelTypeName: String { _modelTypeName }

    var indexName: String { "\(_modelTypeName)_\(fieldName)" }

    let language: String

    init(fieldName: String, modelTypeName: String, language: String) {
        self.fieldName = fieldName
        self._modelTypeName = modelTypeName
        self.language = language
    }
}

// MARK: - Configured Index Kinds

/// Index kind carrying an expected vector dimension.
struct DimensionConfiguredIndexKind: IndexKind {
    static let identifier = "dimension-configured"
    static let subspaceStructure = SubspaceStructure.hierarchical

    let dimensions: Int
    var fieldNames: [String] = []
    var indexName: String { Self.identifier }
    var metadata: [String: IndexMetadataValue] {
        ["dimensions": .int(dimensions)]
    }

    init(dimensions: Int = 0) {
        self.dimensions = dimensions
    }

    static func validateTypes(_ types: [Any.Type]) throws {
        // Accept any types for testing
    }
}

struct DimensionConfiguredIndexMaintainerProvider: IndexMaintainerProvider {
    let kindIdentifier = DimensionConfiguredIndexKind.identifier

    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let dimensions = try index.kind.requireInt("dimensions")
        let configuration = configurations.first(where: {
            $0.indexName == index.name
        }) as? DimensionIndexConfiguration
        return ConfigurationRecordingIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurationApplied: configuration != nil,
            appliedDimensions: configuration?.dimensions ?? dimensions,
            appliedDistanceMetric: configuration?.distanceMetric ?? ""
        )
    }
}

/// Index kind accepting one configuration per language.
struct LanguageConfiguredIndexKind: IndexKind {
    static let identifier = "language-configured"
    static let subspaceStructure = SubspaceStructure.flat

    var fieldNames: [String] = []
    var indexName: String { Self.identifier }

    static func validateTypes(_ types: [Any.Type]) throws {
        // Accept any types for testing
    }

    init() {}
}

struct LanguageConfiguredIndexMaintainerProvider: IndexMaintainerProvider {
    let kindIdentifier = LanguageConfiguredIndexKind.identifier

    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let languages = Set(
            configurations
                .filter { $0.indexName == index.name }
                .compactMap { ($0 as? LanguageIndexConfiguration)?.language }
        )
        return LanguageRecordingIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            appliedLanguages: languages
        )
    }
}

// MARK: - Recording Index Maintainers

/// Entities the dimension configuration selected by the provider.
struct ConfigurationRecordingIndexMaintainer<Item: Persistable>: IndexMaintainer {
    let index: Index
    let subspace: Subspace
    let idExpression: KeyExpression

    let configurationApplied: Bool
    let appliedDimensions: Int
    let appliedDistanceMetric: String

    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Configuration observation does not mutate index data.
    }

    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        // Configuration observation does not scan index data.
    }
}

/// Entities every language configuration selected by the provider.
struct LanguageRecordingIndexMaintainer<Item: Persistable>: IndexMaintainer {
    let index: Index
    let subspace: Subspace
    let idExpression: KeyExpression

    let appliedLanguages: Set<String>

    func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Language configuration observation does not mutate index data.
    }

    func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        // Language configuration observation does not scan index data.
    }
}
#endif

#endif
