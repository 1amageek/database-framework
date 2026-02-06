// SchemaDefinitionIntegrationTests.swift
// Integration tests for schema definition workflow

import Testing
import Foundation
@testable import DatabaseCLICore
@testable import DatabaseEngine
import Core
import FoundationDB
import TestSupport

@Suite("Schema Definition Integration Tests", .serialized)
struct SchemaDefinitionIntegrationTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    @Test("Apply and retrieve schema")
    func testApplyAndRetrieve() async throws {
        let database = try FDBClient.openDatabase()

        // Clean up
        let directoryLayer = DirectoryLayer(database: database)
        try? await directoryLayer.remove(path: ["_schema"])

        // Create YAML
        let yaml = """
        TestUser:
          "#Directory": [test, schema_def, users]

          id: string
          name: string
          email: string#scalar(unique:true)
          age: int#scalar
        """

        // Parse
        let catalog = try SchemaFileParser.parseYAML(yaml)

        // Persist
        let registry = SchemaRegistry(database: database)
        try await registry.persist(catalog)

        // Retrieve
        let retrieved = try await registry.load(typeName: "TestUser")

        #expect(retrieved != nil)
        #expect(retrieved?.name == "TestUser")
        #expect(retrieved?.fields.count == 4)
        #expect(retrieved?.indexes.count == 2)

        // Clean up
        try await registry.delete(typeName: "TestUser")
    }

    @Test("Export schema to YAML")
    func testExportSchema() async throws {
        let database = try FDBClient.openDatabase()

        // Create catalog
        let catalog = Schema.Entity(
            name: "TestProduct",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "name", fieldNumber: 2, type: .string),
                FieldSchema(name: "price", fieldNumber: 3, type: .double)
            ],
            directoryComponents: [
                .staticPath("test"),
                .staticPath("products")
            ],
            indexes: [
                AnyIndexDescriptor(
                    name: "price_idx",
                    kind: AnyIndexKind(
                        identifier: "scalar",
                        subspaceStructure: .flat,
                        fieldNames: ["price"],
                        metadata: [:]
                    ),
                    commonMetadata: [
                        "unique": .bool(false),
                        "sparse": .bool(false)
                    ]
                )
            ]
        )

        // Persist
        let registry = SchemaRegistry(database: database)
        try await registry.persist(catalog)

        // Export
        let retrieved = try await registry.load(typeName: "TestProduct")
        #expect(retrieved != nil)

        let yaml = try SchemaFileExporter.toYAML(retrieved!)

        #expect(yaml.contains("TestProduct:"))
        #expect(yaml.contains("id: string"))
        #expect(yaml.contains("price: double#scalar"))

        // Clean up
        try await registry.delete(typeName: "TestProduct")
    }

    @Test("Round-trip: YAML -> Catalog -> FDB -> Catalog -> YAML")
    func testFullRoundTrip() async throws {
        let database = try FDBClient.openDatabase()
        let registry = SchemaRegistry(database: database)

        let originalYAML = """
        TestArticle:
          "#Directory": [test, articles]

          id: string
          title: string#fulltext(language:english)
          content: string#fulltext(language:english)
          publishDate: date#scalar
        """

        // Parse original YAML
        let catalog1 = try SchemaFileParser.parseYAML(originalYAML)

        // Persist to FDB
        try await registry.persist(catalog1)

        // Retrieve from FDB
        let catalog2 = try await registry.load(typeName: "TestArticle")
        #expect(catalog2 != nil)

        // Export to YAML
        let exportedYAML = try SchemaFileExporter.toYAML(catalog2!)

        // Parse exported YAML
        let catalog3 = try SchemaFileParser.parseYAML(exportedYAML)

        // Verify consistency
        #expect(catalog3.name == catalog1.name)
        #expect(catalog3.fields.count == catalog1.fields.count)
        #expect(catalog3.indexes.count == catalog1.indexes.count)

        // Clean up
        try await registry.delete(typeName: "TestArticle")
    }

    @Test("Apply schema with graph index")
    func testApplyGraphSchema() async throws {
        let database = try FDBClient.openDatabase()
        let registry = SchemaRegistry(database: database)

        let yaml = """
        TestFollow:
          "#Directory": [test, social, follows]

          id: string
          follower: string
          following: string
          timestamp: date

          "#Index":
            - kind: graph
              name: social_graph
              from: follower
              edge: follows
              to: following
              strategy: adjacency
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)
        try await registry.persist(catalog)

        let retrieved = try await registry.load(typeName: "TestFollow")
        #expect(retrieved != nil)

        let graphIndex = retrieved?.indexes.first { $0.kindIdentifier == "graph" }
        #expect(graphIndex != nil)
        #expect(graphIndex?.kind.metadata["fromField"]?.stringValue == "follower")
        #expect(graphIndex?.kind.metadata["strategy"]?.stringValue == "adjacency")

        // Clean up
        try await registry.delete(typeName: "TestFollow")
    }

    @Test("Apply schema with dynamic directory")
    func testApplyDynamicDirectory() async throws {
        let database = try FDBClient.openDatabase()
        let registry = SchemaRegistry(database: database)

        let yaml = """
        TestOrder:
          "#Directory":
            - test
            - orders
            - field: tenantId

          id: string
          tenantId: string
          amount: double
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)
        try await registry.persist(catalog)

        let retrieved = try await registry.load(typeName: "TestOrder")
        #expect(retrieved != nil)
        #expect(retrieved?.hasDynamicDirectory == true)
        #expect(retrieved?.dynamicFieldNames == ["tenantId"])

        // Clean up
        try await registry.delete(typeName: "TestOrder")
    }

    @Test("Apply multiple schemas")
    func testApplyMultipleSchemas() async throws {
        let database = try FDBClient.openDatabase()
        let registry = SchemaRegistry(database: database)

        // Use unique type names to avoid conflicts with old format data
        let testSuffix = UUID().uuidString.prefix(8)
        let schemas = [
            """
            TestMultiUser1_\(testSuffix):
              "#Directory": [test, multi, users]
              id: string
              name: string
            """,
            """
            TestMultiUser2_\(testSuffix):
              "#Directory": [test, multi, users2]
              id: string
              email: string#scalar(unique:true)
            """,
            """
            TestMultiUser3_\(testSuffix):
              "#Directory": [test, multi, users3]
              id: string
              age: int#scalar
            """
        ]

        for yaml in schemas {
            let catalog = try SchemaFileParser.parseYAML(yaml)
            try await registry.persist(catalog)
        }

        // Verify all exist by loading each one individually
        let loaded1 = try await registry.load(typeName: "TestMultiUser1_\(testSuffix)")
        let loaded2 = try await registry.load(typeName: "TestMultiUser2_\(testSuffix)")
        let loaded3 = try await registry.load(typeName: "TestMultiUser3_\(testSuffix)")
        #expect(loaded1 != nil)
        #expect(loaded2 != nil)
        #expect(loaded3 != nil)

        // Clean up
        try await registry.delete(typeName: "TestMultiUser1_\(testSuffix)")
        try await registry.delete(typeName: "TestMultiUser2_\(testSuffix)")
        try await registry.delete(typeName: "TestMultiUser3_\(testSuffix)")
    }

    @Test("Validate schema without persisting")
    func testValidateOnly() throws {
        let yaml = """
        ValidUser:
          "#Directory": [test, validation]

          id: string
          name: string
          email: string#scalar(unique:true)
        """

        // This should not throw
        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.name == "ValidUser")
        #expect(catalog.fields.count == 3)
    }

    @Test("Detect invalid schema during validation")
    func testInvalidSchemaValidation() throws {
        let yaml = """
        InvalidUser:
          "#Directory": [test, invalid]

          id: invalidtype
        """

        #expect(throws: SchemaFileError.self) {
            try SchemaFileParser.parseYAML(yaml)
        }
    }

    @Test("Delete non-existent schema")
    func testDeleteNonExistent() async throws {
        let database = try FDBClient.openDatabase()
        let registry = SchemaRegistry(database: database)

        // Should not throw, just no-op
        try await registry.delete(typeName: "NonExistentType123")
    }

    @Test("Overwrite existing schema")
    func testOverwriteSchema() async throws {
        let database = try FDBClient.openDatabase()
        let registry = SchemaRegistry(database: database)

        let yaml1 = """
        TestOverwrite:
          "#Directory": [test, overwrite]
          id: string
          name: string
        """

        let yaml2 = """
        TestOverwrite:
          "#Directory": [test, overwrite]
          id: string
          name: string
          email: string#scalar(unique:true)
        """

        // First version
        let catalog1 = try SchemaFileParser.parseYAML(yaml1)
        try await registry.persist(catalog1)

        let retrieved1 = try await registry.load(typeName: "TestOverwrite")
        #expect(retrieved1?.fields.count == 2)

        // Overwrite with second version
        let catalog2 = try SchemaFileParser.parseYAML(yaml2)
        try await registry.persist(catalog2)

        let retrieved2 = try await registry.load(typeName: "TestOverwrite")
        #expect(retrieved2?.fields.count == 3)

        // Clean up
        try await registry.delete(typeName: "TestOverwrite")
    }
}
