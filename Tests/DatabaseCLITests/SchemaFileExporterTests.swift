// SchemaFileExporterTests.swift
// Tests for TypeCatalog to YAML export

import Testing
import Foundation
@testable import DatabaseCLICore
@testable import DatabaseEngine
import Core

@Suite("Schema File Exporter Tests")
struct SchemaFileExporterTests {

    @Test("Export simple schema")
    func testExportSimpleSchema() throws {
        let catalog = TypeCatalog(
            typeName: "User",
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
        #expect(yaml.contains("#Directory:"))
        #expect(yaml.contains("- app"))
        #expect(yaml.contains("- users"))
        #expect(yaml.contains("id: string"))
        #expect(yaml.contains("name: string"))
        #expect(yaml.contains("age: int64"))
    }

    @Test("Export schema with scalar index")
    func testExportScalarIndex() throws {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "email", fieldNumber: 2, type: .string)
            ],
            directoryComponents: [.staticPath("app"), .staticPath("users")],
            indexes: [
                IndexCatalog(
                    name: "email_scalar_idx",
                    kindIdentifier: "scalar",
                    fieldNames: ["email"],
                    unique: true,
                    metadata: [:]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("email: string#scalar(unique:true)"))
    }

    @Test("Export schema with vector index")
    func testExportVectorIndex() throws {
        let catalog = TypeCatalog(
            typeName: "Product",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "embedding", fieldNumber: 2, type: .double, isArray: true)
            ],
            directoryComponents: [.staticPath("catalog")],
            indexes: [
                IndexCatalog(
                    name: "embedding_vector_idx",
                    kindIdentifier: "vector",
                    fieldNames: ["embedding"],
                    unique: false,
                    metadata: [
                        "dimensions": "384",
                        "metric": "cosine",
                        "algorithm": "hnsw"
                    ]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("embedding: array<double>#vector(dimensions:384, metric:cosine, algorithm:hnsw)"))
    }

    @Test("Export schema with graph index")
    func testExportGraphIndex() throws {
        let catalog = TypeCatalog(
            typeName: "Follow",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "follower", fieldNumber: 2, type: .string),
                FieldSchema(name: "following", fieldNumber: 3, type: .string)
            ],
            directoryComponents: [.staticPath("social"), .staticPath("follows")],
            indexes: [
                IndexCatalog(
                    name: "social_graph",
                    kindIdentifier: "graph",
                    fieldNames: ["follower", "follows", "following"],
                    unique: false,
                    metadata: [
                        "fromField": "follower",
                        "edgeField": "follows",
                        "toField": "following",
                        "strategy": "tripleStore"
                    ]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("#Index:"))
        #expect(yaml.contains("- kind: graph"))
        #expect(yaml.contains("from: follower"))
        #expect(yaml.contains("edge: follows"))
        #expect(yaml.contains("to: following"))
        #expect(yaml.contains("strategy: tripleStore"))
    }

    @Test("Export schema with dynamic directory")
    func testExportDynamicDirectory() throws {
        let catalog = TypeCatalog(
            typeName: "Order",
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

        #expect(yaml.contains("#Directory:"))
        #expect(yaml.contains("- orders"))
        #expect(yaml.contains("- field: tenantId"))
    }

    @Test("Export schema with composite index")
    func testExportCompositeIndex() throws {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "name", fieldNumber: 2, type: .string),
                FieldSchema(name: "age", fieldNumber: 3, type: .int64)
            ],
            directoryComponents: [.staticPath("app")],
            indexes: [
                IndexCatalog(
                    name: "name_age_idx",
                    kindIdentifier: "scalar",
                    fieldNames: ["name", "age"],
                    unique: false,
                    metadata: [:]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("#Index:"))
        #expect(yaml.contains("- kind: scalar"))
        #expect(yaml.contains("name: name_age_idx"))
        #expect(yaml.contains("fields: [name, age]"))
    }

    @Test("Export schema with optional and array types")
    func testExportComplexTypes() throws {
        let catalog = TypeCatalog(
            typeName: "User",
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
        let original = TypeCatalog(
            typeName: "Product",
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
                IndexCatalog(
                    name: "price_scalar_idx",
                    kindIdentifier: "scalar",
                    fieldNames: ["price"],
                    unique: false,
                    metadata: [:]
                )
            ]
        )

        // Export to YAML
        let yaml = try SchemaFileExporter.toYAML(original)

        // Re-import
        let reimported = try SchemaFileParser.parseYAML(yaml)

        // Verify
        #expect(reimported.typeName == original.typeName)
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
        let catalog = TypeCatalog(
            typeName: "Article",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "content", fieldNumber: 2, type: .string)
            ],
            directoryComponents: [.staticPath("content")],
            indexes: [
                IndexCatalog(
                    name: "content_fulltext_idx",
                    kindIdentifier: "fulltext",
                    fieldNames: ["content"],
                    unique: false,
                    metadata: [
                        "language": "english",
                        "tokenizer": "standard"
                    ]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("content: string#fulltext(language:english, tokenizer:standard)"))
    }

    @Test("Export leaderboard index")
    func testExportLeaderboardIndex() throws {
        let catalog = TypeCatalog(
            typeName: "Player",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "score", fieldNumber: 2, type: .int64)
            ],
            directoryComponents: [.staticPath("game")],
            indexes: [
                IndexCatalog(
                    name: "score_leaderboard_idx",
                    kindIdentifier: "leaderboard",
                    fieldNames: ["score"],
                    unique: false,
                    metadata: ["leaderboardName": "global_ranking"]
                )
            ]
        )

        let yaml = try SchemaFileExporter.toYAML(catalog)

        #expect(yaml.contains("score: int64#leaderboard(name:global_ranking)"))
    }
}
