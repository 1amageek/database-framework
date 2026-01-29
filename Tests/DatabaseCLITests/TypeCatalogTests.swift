import Foundation
import Testing
@testable import DatabaseEngine

@Suite("TypeCatalog directory resolution")
struct TypeCatalogTests {

    // MARK: - resolvedDirectoryPath

    @Test func staticOnly() throws {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [],
            directoryComponents: [.staticPath("app"), .staticPath("users")],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath()
        #expect(path == ["app", "users"])
    }

    @Test func withDynamicField() throws {
        let catalog = TypeCatalog(
            typeName: "Order",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "tenantId"),
                .staticPath("orders"),
            ],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath(partitionValues: ["tenantId": "t_123"])
        #expect(path == ["tenants", "t_123", "orders"])
    }

    @Test func multipleDynamicFields() throws {
        let catalog = TypeCatalog(
            typeName: "Message",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "accountId"),
                .staticPath("channels"),
                .dynamicField(fieldName: "channelId"),
                .staticPath("messages"),
            ],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath(partitionValues: [
            "accountId": "acc_1",
            "channelId": "ch_2",
        ])
        #expect(path == ["tenants", "acc_1", "channels", "ch_2", "messages"])
    }

    @Test func missingPartitionValueThrows() {
        let catalog = TypeCatalog(
            typeName: "Order",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "tenantId"),
                .staticPath("orders"),
            ],
            indexes: []
        )
        #expect(throws: (any Error).self) {
            _ = try catalog.resolvedDirectoryPath()
        }
    }

    @Test func emptyComponents() throws {
        let catalog = TypeCatalog(
            typeName: "Simple",
            fields: [],
            directoryComponents: [],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath()
        #expect(path.isEmpty)
    }

    // MARK: - hasDynamicDirectory

    @Test func staticOnlyIsNotDynamic() {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [],
            directoryComponents: [.staticPath("app"), .staticPath("users")],
            indexes: []
        )
        #expect(!catalog.hasDynamicDirectory)
    }

    @Test func withDynamicFieldIsDynamic() {
        let catalog = TypeCatalog(
            typeName: "Order",
            fields: [],
            directoryComponents: [.staticPath("tenants"), .dynamicField(fieldName: "tenantId")],
            indexes: []
        )
        #expect(catalog.hasDynamicDirectory)
    }

    // MARK: - dynamicFieldNames

    @Test func dynamicFieldNamesExtraction() {
        let catalog = TypeCatalog(
            typeName: "Message",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "accountId"),
                .staticPath("channels"),
                .dynamicField(fieldName: "channelId"),
            ],
            indexes: []
        )
        #expect(catalog.dynamicFieldNames == ["accountId", "channelId"])
    }

    @Test func noDynamicFieldNames() {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [],
            directoryComponents: [.staticPath("app")],
            indexes: []
        )
        #expect(catalog.dynamicFieldNames.isEmpty)
    }
}

// MARK: - IndexCatalog Codable

@Suite("IndexCatalog Codable")
struct IndexCatalogCodableTests {

    @Test func roundTripsWithMetadata() throws {
        let original = IndexCatalog(
            name: "RDFTriple_graph_subject_predicate_object",
            kindIdentifier: "graph",
            fieldNames: ["subject", "predicate", "object"],
            unique: false,
            sparse: false,
            metadata: ["strategy": "tripleStore", "fromField": "subject", "edgeField": "predicate", "toField": "object"]
        )

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(IndexCatalog.self, from: data)

        #expect(decoded == original)
        #expect(decoded.metadata["strategy"] == "tripleStore")
        #expect(decoded.metadata["fromField"] == "subject")
    }

    @Test func decodesLegacyJSONWithoutMetadata() throws {
        // Simulate catalog JSON stored before `metadata` field was added
        let legacyJSON = """
        {
            "name": "User_email",
            "kindIdentifier": "scalar",
            "fieldNames": ["email"],
            "unique": true,
            "sparse": false
        }
        """
        let data = Data(legacyJSON.utf8)
        let decoded = try JSONDecoder().decode(IndexCatalog.self, from: data)

        #expect(decoded.name == "User_email")
        #expect(decoded.kindIdentifier == "scalar")
        #expect(decoded.fieldNames == ["email"])
        #expect(decoded.unique == true)
        #expect(decoded.sparse == false)
        #expect(decoded.metadata.isEmpty)
    }

    @Test func decodesEmptyMetadata() throws {
        let json = """
        {
            "name": "Test_idx",
            "kindIdentifier": "scalar",
            "fieldNames": ["field"],
            "unique": false,
            "sparse": false,
            "metadata": {}
        }
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(IndexCatalog.self, from: data)

        #expect(decoded.metadata.isEmpty)
    }

    @Test func typeCatalogRoundTripsWithIndexMetadata() throws {
        let catalog = TypeCatalog(
            typeName: "RDFTriple",
            fields: [],
            directoryComponents: [.staticPath("app"), .staticPath("triples")],
            indexes: [
                IndexCatalog(
                    name: "RDFTriple_graph_subject_predicate_object",
                    kindIdentifier: "graph",
                    fieldNames: ["subject", "predicate", "object"],
                    metadata: ["strategy": "hexastore", "fromField": "subject", "edgeField": "predicate", "toField": "object"]
                )
            ]
        )

        let data = try JSONEncoder().encode(catalog)
        let decoded = try JSONDecoder().decode(TypeCatalog.self, from: data)

        #expect(decoded == catalog)
        #expect(decoded.indexes[0].metadata["strategy"] == "hexastore")
    }
}
