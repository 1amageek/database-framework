#if !os(WASI)
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
            indexName: "IndexConfigurationEntity_embedding",
            dimensions: 384,
            distanceMetric: "cosine"
        )

        let descriptor = try #require(
            IndexConfigurationEntity.indexDescriptors.first {
                $0.name == "IndexConfigurationEntity_embedding"
            }
        )
        let index = ResolvedIndex(
            descriptor: descriptor,
            rootExpression: FieldKeyExpression(fieldName: "embedding")
        )

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")
        let maintainer: any IndexMaintainer<IndexConfigurationEntity> = try DimensionConfiguredIndexMaintainerProvider().makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: [configuration],
            wallClock: IndexConfigurationWallClock()
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
            LanguageIndexConfiguration(indexName: "IndexConfigurationEntity_content", language: "en"),
            LanguageIndexConfiguration(indexName: "IndexConfigurationEntity_content", language: "ja"),
            LanguageIndexConfiguration(indexName: "IndexConfigurationEntity_content", language: "zh"),
        ]

        let descriptor = try #require(
            IndexConfigurationEntity.indexDescriptors.first {
                $0.name == "IndexConfigurationEntity_content"
            }
        )
        let index = ResolvedIndex(
            descriptor: descriptor,
            rootExpression: FieldKeyExpression(fieldName: "content")
        )

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")
        let maintainer: any IndexMaintainer<IndexConfigurationEntity> = try LanguageConfiguredIndexMaintainerProvider().makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configs,
            wallClock: IndexConfigurationWallClock()
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
            indexName: "IndexConfigurationEntity_embedding",
            dimensions: 768,
            distanceMetric: "euclidean"
        )
        let descriptor = try #require(
            IndexConfigurationEntity.indexDescriptors.first {
                $0.name == "IndexConfigurationEntity_otherField"
            }
        )
        let index = ResolvedIndex(
            descriptor: descriptor,
            rootExpression: FieldKeyExpression(fieldName: "otherField")
        )

        let subspace = Subspace(prefix: Tuple("test", UUID().uuidString).pack())
        let idExpression = FieldKeyExpression(fieldName: "id")
        let maintainer: any IndexMaintainer<IndexConfigurationEntity> = try DimensionConfiguredIndexMaintainerProvider().makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: [configuration],
            wallClock: IndexConfigurationWallClock()
        )

        if let recordingMaintainer = maintainer as? ConfigurationRecordingIndexMaintainer<IndexConfigurationEntity> {
            #expect(recordingMaintainer.configurationApplied == false)
            #expect(recordingMaintainer.appliedDimensions == 128)
        } else {
            Issue.record("Expected ConfigurationRecordingIndexMaintainer but got \(type(of: maintainer))")
        }
    }

    // MARK: - Configuration Name Matching Tests

    @Test("Runtime configuration retains its explicit index name")
    func runtimeConfigurationIndexName() {
        let configuration = DimensionIndexConfiguration(
            indexName: "custom_runtime_index",
            dimensions: 384,
            distanceMetric: "cosine"
        )

        #expect(configuration.indexName == "custom_runtime_index")
    }

    @Test("Runtime configuration index type matches the declaration")
    func runtimeConfigurationKindIdentifier() {
        #expect(
            DimensionIndexConfiguration.indexType
                == .custom("dimension-configured")
        )
        #expect(
            LanguageIndexConfiguration.indexType
                == .custom("language-configured")
        )
    }
}

private struct IndexConfigurationWallClock: WallClock {
    let now = Timestamp(secondsSinceUnixEpoch: 0)
}

// MARK: - Configuration Entity

@Persistable
struct IndexConfigurationEntity {
    #Index(
        .custom(
            name: "IndexConfigurationEntity_embedding",
            definition: CustomIndexDefinition(
                identifier: "dimension-configured",
                keys: [.ascending(\IndexConfigurationEntity.embedding)],
                parameters: ["dimensions": .int64(384)]
            )
        ))
    #Index(
        .custom(
            name: "IndexConfigurationEntity_content",
            definition: CustomIndexDefinition(
                identifier: "language-configured",
                keys: [.ascending(\IndexConfigurationEntity.content)]
            )
        ))
    #Index(
        .custom(
            name: "IndexConfigurationEntity_otherField",
            definition: CustomIndexDefinition(
                identifier: "dimension-configured",
                keys: [.ascending(\IndexConfigurationEntity.otherField)],
                parameters: ["dimensions": .int64(128)]
            )
        ))

    var id: String = ""
    var content: String = ""
    var embedding: Vector = Vector(int8: [])
    var otherField: String = ""
}

// MARK: - Index Configuration Scenarios

/// Dimension-bearing configuration for single-configuration application.
struct DimensionIndexConfiguration: IndexRuntimeConfiguration, Sendable {
    static let indexType: IndexType = .custom("dimension-configured")

    let indexName: String

    let dimensions: Int
    let distanceMetric: String

    init(
        indexName: String,
        dimensions: Int,
        distanceMetric: String
    ) {
        self.indexName = indexName
        self.dimensions = dimensions
        self.distanceMetric = distanceMetric
    }
}

/// Language-bearing configuration used to verify multi-configuration application.
struct LanguageIndexConfiguration: IndexRuntimeConfiguration, Sendable {
    static let indexType: IndexType = .custom("language-configured")

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

struct DimensionConfiguredIndexMaintainerProvider: IndexMaintainerProvider {
    let indexType: IndexType = .custom("dimension-configured")

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        let matching = configurations.filter { $0.indexName == index.name }
        guard matching.count <= 1 else {
            throw IndexRuntimeConfigurationError.duplicateConfiguration(
                indexName: index.name
            )
        }
        guard case .custom(let definition) = index.definition,
            case .int64(let rawDimensions)? =
                definition.parameters["dimensions"],
            let declaredDimensions = Int(exactly: rawDimensions)
        else {
            throw IndexMaintainerProviderError.invalidDefinition(
                indexType: index.type,
                reason: "Custom index requires an integer 'dimensions' parameter"
            )
        }
        let configuration = matching.first as? DimensionIndexConfiguration
        return try IndexPhysicalLayout(
            name: "test.dimension-configured",
            revision: 1,
            parameters: FieldObject([
                (
                    "dimensions",
                    .int64(Int64(configuration?.dimensions ?? declaredDimensions))
                ),
                (
                    "distanceMetric",
                    .string(configuration?.distanceMetric ?? "")
                ),
            ])
        )
    }

    func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        _ = wallClock
        guard case .custom(let definition) = index.definition,
            case .int64(let rawDimensions)? = definition.parameters["dimensions"],
            let dimensions = Int(exactly: rawDimensions)
        else {
            throw IndexMaintainerProviderError.invalidDefinition(
                indexType: index.type,
                reason: "Custom index requires an integer 'dimensions' parameter"
            )
        }
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

struct LanguageConfiguredIndexMaintainerProvider: IndexMaintainerProvider {
    let indexType: IndexType = .custom("language-configured")

    func physicalLayout(
        for index: ResolvedIndex,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> IndexPhysicalLayout {
        let languages =
            configurations
            .filter { $0.indexName == index.name }
            .compactMap { ($0 as? LanguageIndexConfiguration)?.language }
            .sorted()
        return try IndexPhysicalLayout(
            name: "test.language-configured",
            revision: 1,
            parameters: FieldObject([
                ("languages", .array(languages.map(FieldValue.string)))
            ])
        )
    }

    func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        _ = wallClock
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
struct ConfigurationRecordingIndexMaintainer<Item: PersistedEntityValue>: IndexMaintainer {
    let index: ResolvedIndex
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
struct LanguageRecordingIndexMaintainer<Item: PersistedEntityValue>: IndexMaintainer {
    let index: ResolvedIndex
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
