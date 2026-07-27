#if !os(WASI)
#if FOUNDATION_DB
// IndexConfigurationTests.swift
// Tests for runtime index configuration propagation and application.

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import DatabaseTypes
@testable import DatabaseKit
@testable import DatabaseEngine

/// Tests for runtime index configuration behavior.
@Suite("Index runtime configuration tests", .heartbeat)
struct IndexConfigurationTests {

    // MARK: - Provider Configuration Tests

    @Test("Provider applies the matching configuration")
    func indexConfigurationApplicableApply() async throws {
        let configuration = DimensionIndexConfiguration(
            fieldName: "embedding",
            entityName: "IndexConfigurationEntity",
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
        let configs: [any IndexRuntimeConfiguration] = [
            LanguageIndexConfiguration(fieldName: "content", entityName: "IndexConfigurationEntity", language: "en"),
            LanguageIndexConfiguration(fieldName: "content", entityName: "IndexConfigurationEntity", language: "ja"),
            LanguageIndexConfiguration(fieldName: "content", entityName: "IndexConfigurationEntity", language: "zh")
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
            entityName: "IndexConfigurationEntity",
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

    @Test("Runtime configuration indexName is computed correctly")
    func runtimeConfigurationIndexName() {
        let configuration = DimensionIndexConfiguration(
            fieldName: "embedding",
            entityName: "IndexConfigurationEntity",
            dimensions: 384,
            distanceMetric: "cosine"
        )

        // indexName is derived from the canonical entity and field names.
        #expect(configuration.indexName == "IndexConfigurationEntity_embedding")
    }

    @Test("Runtime configuration kindIdentifier matches expected kind")
    func runtimeConfigurationKindIdentifier() {
        #expect(DimensionIndexConfiguration.kindIdentifier == "dimension-configured")
        #expect(LanguageIndexConfiguration.kindIdentifier == "language-configured")
    }
}

// MARK: - Configuration Entity

@Persistable
struct IndexConfigurationEntity {
    var id: String = ""
    var content: String = ""
    var embedding: Vector = Vector(int8: [])
    var otherField: String = ""
}

// MARK: - Index Configuration Scenarios

/// Dimension-bearing configuration for single-configuration application.
struct DimensionIndexConfiguration: IndexRuntimeConfiguration, Sendable {
    static var kindIdentifier: String { "dimension-configured" }

    let fieldName: String
    let entityName: String

    let dimensions: Int
    let distanceMetric: String

    init(
        fieldName: String,
        entityName: String,
        dimensions: Int,
        distanceMetric: String
    ) {
        self.fieldName = fieldName
        self.entityName = entityName
        self.dimensions = dimensions
        self.distanceMetric = distanceMetric
    }
}

/// Language-bearing configuration used to verify multi-configuration application.
struct LanguageIndexConfiguration: IndexRuntimeConfiguration, Sendable {
    static var kindIdentifier: String { "language-configured" }

    let fieldName: String
    let entityName: String

    let language: String

    init(fieldName: String, entityName: String, language: String) {
        self.fieldName = fieldName
        self.entityName = entityName
        self.language = language
    }
}

// MARK: - Configured Index Kinds

/// Index kind carrying an expected vector dimension.
struct DimensionConfiguredIndexKind: IndexKind {
    typealias Model = IndexConfigurationEntity

    static let identifier = "dimension-configured"
    static let subspaceStructure = SubspaceStructure.hierarchical

    let dimensions: Int
    let indexFields: [IndexField<IndexConfigurationEntity>]
    var indexName: String { Self.identifier }
    var metadata: [String: FieldValue] {
        ["dimensions": .int64(Int64(dimensions))]
    }

    init(dimensions: Int = 0) {
        self.dimensions = dimensions
        self.indexFields = [
            IndexConfigurationEntity.fields.embedding.ascending
        ]
    }

    static func validateFields(
        _ fields: [FieldSchema]
    ) throws(IndexValidationError) {}
}

struct DimensionConfiguredIndexMaintainerProvider: IndexMaintainerProvider {
    let kindIdentifier = DimensionConfiguredIndexKind.identifier

    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
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
    typealias Model = IndexConfigurationEntity

    static let identifier = "language-configured"
    static let subspaceStructure = SubspaceStructure.flat

    let indexFields: [IndexField<IndexConfigurationEntity>]
    var indexName: String { Self.identifier }

    static func validateFields(
        _ fields: [FieldSchema]
    ) throws(IndexValidationError) {}

    init() {
        self.indexFields = [
            IndexConfigurationEntity.fields.content.ascending
        ]
    }
}

struct LanguageConfiguredIndexMaintainerProvider: IndexMaintainerProvider {
    let kindIdentifier = LanguageConfiguredIndexKind.identifier

    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
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
