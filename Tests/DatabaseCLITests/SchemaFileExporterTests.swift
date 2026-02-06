// SchemaFileExporterTests.swift
// Tests for Schema.Entity to YAML export

import Testing
import Foundation
@testable import DatabaseCLICore
@testable import DatabaseEngine
import Core

@Suite("Schema File Exporter Tests")
struct SchemaFileExporterTests {

    // MARK: - Helper

    /// Create an AnyIndexDescriptor for testing
    private func makeIndex(
        name: String,
        kindIdentifier: String,
        fieldNames: [String],
        unique: Bool = false,
        kindMetadata: [String: IndexMetadataValue] = [:]
    ) -> AnyIndexDescriptor {
        AnyIndexDescriptor(
            name: name,
            kind: AnyIndexKind(
                identifier: kindIdentifier,
                subspaceStructure: kindIdentifier == "scalar" ? .flat : .hierarchical,
                fieldNames: fieldNames,
                metadata: kindMetadata
            ),
            commonMetadata: [
                "unique": .bool(unique),
                "sparse": .bool(false)
            ]
        )
    }

    @Test("Export simple schema")
    func testExportSimpleSchema() throws {
        let catalog = Schema.Entity(
            name: "User",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "name", fieldNumber: 2, type: .string),
                FieldSchema(name: "age", fieldNumber: 3, type: .int64)
            ],
            directoryComponents: [
                .staticPath("app"),
                .staticPath("users")
            ],
            indexes: []
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("User:"))
        #expect(yaml.contains("\"#Directory\":"))
        #expect(yaml.contains("- app"))
        #expect(yaml.contains("- users"))
        #expect(yaml.contains("id: string"))
        #expect(yaml.contains("name: string"))
        #expect(yaml.contains("age: int64"))
    }

    @Test("Export schema with scalar index")
    func testExportScalarIndex() throws {
        let catalog = Schema.Entity(
            name: "User",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "email", fieldNumber: 2, type: .string)
            ],
            directoryComponents: [.staticPath("app"), .staticPath("users")],
            indexes: [
                makeIndex(
                    name: "email_scalar_idx",
                    kindIdentifier: "scalar",
                    fieldNames: ["email"],
                    unique: true
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("email: string#scalar(unique:true)"))
    }

    @Test("Export schema with vector index")
    func testExportVectorIndex() throws {
        let catalog = Schema.Entity(
            name: "Product",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "embedding", fieldNumber: 2, type: .double, isArray: true)
            ],
            directoryComponents: [.staticPath("catalog")],
            indexes: [
                makeIndex(
                    name: "embedding_vector_idx",
                    kindIdentifier: "vector",
                    fieldNames: ["embedding"],
                    kindMetadata: [
                        "dimensions": .int(384),
                        "metric": .string("cosine"),
                        "algorithm": .string("hnsw")
                    ]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("embedding: array<double>#vector(dimensions:384, metric:cosine, algorithm:hnsw)"))
    }

    @Test("Export schema with graph index")
    func testExportGraphIndex() throws {
        let catalog = Schema.Entity(
            name: "Follow",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "follower", fieldNumber: 2, type: .string),
                FieldSchema(name: "following", fieldNumber: 3, type: .string)
            ],
            directoryComponents: [.staticPath("social"), .staticPath("follows")],
            indexes: [
                makeIndex(
                    name: "social_graph",
                    kindIdentifier: "graph",
                    fieldNames: ["follower", "follows", "following"],
                    kindMetadata: [
                        "fromField": .string("follower"),
                        "edgeField": .string("follows"),
                        "toField": .string("following"),
                        "strategy": .string("tripleStore")
                    ]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("\"#Index\":"))
        #expect(yaml.contains("- kind: graph"))
        #expect(yaml.contains("from: follower"))
        #expect(yaml.contains("edge: follows"))
        #expect(yaml.contains("to: following"))
        #expect(yaml.contains("strategy: tripleStore"))
    }

    @Test("Export schema with dynamic directory")
    func testExportDynamicDirectory() throws {
        let catalog = Schema.Entity(
            name: "Order",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "tenantId", fieldNumber: 2, type: .string)
            ],
            directoryComponents: [
                .staticPath("orders"),
                .dynamicField(fieldName: "tenantId")
            ],
            indexes: []
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("\"#Directory\":"))
        #expect(yaml.contains("- orders"))
        #expect(yaml.contains("- field: tenantId"))
    }

    @Test("Export schema with composite index")
    func testExportCompositeIndex() throws {
        let catalog = Schema.Entity(
            name: "User",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "name", fieldNumber: 2, type: .string),
                FieldSchema(name: "age", fieldNumber: 3, type: .int64)
            ],
            directoryComponents: [.staticPath("app")],
            indexes: [
                makeIndex(
                    name: "name_age_idx",
                    kindIdentifier: "scalar",
                    fieldNames: ["name", "age"]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("\"#Index\":"))
        #expect(yaml.contains("- kind: scalar"))
        #expect(yaml.contains("name: name_age_idx"))
        #expect(yaml.contains("fields: [name, age]"))
    }

    @Test("Export schema with optional and array types")
    func testExportComplexTypes() throws {
        let catalog = Schema.Entity(
            name: "User",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "nickname", fieldNumber: 2, type: .string, isOptional: true),
                FieldSchema(name: "tags", fieldNumber: 3, type: .string, isArray: true),
                FieldSchema(name: "optionalTags", fieldNumber: 4, type: .string, isOptional: true, isArray: true)
            ],
            directoryComponents: [.staticPath("app")],
            indexes: []
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("nickname: optional<string>"))
        #expect(yaml.contains("tags: array<string>"))
        #expect(yaml.contains("optionalTags: optional<array<string>>"))
    }

    @Test("Export and re-import round-trip")
    func testRoundTrip() throws {
        let original = Schema.Entity(
            name: "Product",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "name", fieldNumber: 2, type: .string),
                FieldSchema(name: "price", fieldNumber: 3, type: .double),
                FieldSchema(name: "tags", fieldNumber: 3, type: .string, isArray: true)
            ],
            directoryComponents: [
                .staticPath("catalog"),
                .staticPath("products")
            ],
            indexes: [
                makeIndex(
                    name: "price_scalar_idx",
                    kindIdentifier: "scalar",
                    fieldNames: ["price"]
                )
            ]
        )

        // Export to YAML
        let yaml = try SchemaFileExporter.toYAML(original)
        print("=== YAML ===")
        print(yaml)
        print("=== END ===")

        // Re-import
        let reimported = try SchemaFileParser.parseYAML(yaml)

        // Verify
        #expect(reimported.name == original.name)
        #expect(reimported.fields.count == original.fields.count)
        #expect(reimported.directoryComponents.count == original.directoryComponents.count)
        #expect(reimported.indexes.count == original.indexes.count)

        // Check field names match
        for i in 0..<original.fields.count {
            #expect(reimported.fields[i].name == original.fields[i].name)
        }
    }

    @Test("Export fulltext index")
    func testExportFullTextIndex() throws {
        let catalog = Schema.Entity(
            name: "Article",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "content", fieldNumber: 2, type: .string)
            ],
            directoryComponents: [.staticPath("content")],
            indexes: [
                makeIndex(
                    name: "content_fulltext_idx",
                    kindIdentifier: "fulltext",
                    fieldNames: ["content"],
                    kindMetadata: [
                        "language": .string("english"),
                        "tokenizer": .string("standard")
                    ]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("content: string#fulltext(language:english, tokenizer:standard)"))
    }

    @Test("Export leaderboard index")
    func testExportLeaderboardIndex() throws {
        let catalog = Schema.Entity(
            name: "Player",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "score", fieldNumber: 2, type: .int64)
            ],
            directoryComponents: [.staticPath("game")],
            indexes: [
                makeIndex(
                    name: "score_leaderboard_idx",
                    kindIdentifier: "leaderboard",
                    fieldNames: ["score"],
                    kindMetadata: ["leaderboardName": .string("global_ranking")]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("score: int64#leaderboard(name:global_ranking)"))
    }
}
