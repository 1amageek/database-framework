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

// MARK: - AnyIndexDescriptor Codable

@Suite("AnyIndexDescriptor Codable")
struct AnyIndexDescriptorCodableTests {

    /// Create an AnyIndexDescriptor for testing
    private func makeIndex(
        name: String,
        kindIdentifier: String,
        fieldNames: [String],
        unique: Bool = false,
        sparse: Bool = false,
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
                "sparse": .bool(sparse)
            ]
        )
    }

    @Test func roundTripsWithMetadata() throws {
        let original = makeIndex(
            name: "RDFTriple_graph_subject_predicate_object",
            kindIdentifier: "graph",
            fieldNames: ["subject", "predicate", "object"],
            kindMetadata: [
                "strategy": .string("tripleStore"),
                "fromField": .string("subject"),
                "edgeField": .string("predicate"),
                "toField": .string("object")
            ]
        )

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(AnyIndexDescriptor.self, from: data)

        #expect(decoded == original)
        #expect(decoded.kind.metadata["strategy"]?.stringValue == "tripleStore")
        #expect(decoded.kind.metadata["fromField"]?.stringValue == "subject")
    }

    @Test func decodesEmptyMetadata() throws {
        let original = makeIndex(
            name: "Test_idx",
            kindIdentifier: "scalar",
            fieldNames: ["field"]
        )

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(AnyIndexDescriptor.self, from: data)

        #expect(decoded.kind.metadata.isEmpty)
    }

    @Test func typeCatalogRoundTripsWithIndexMetadata() throws {
        let catalog = TypeCatalog(
            typeName: "RDFTriple",
            fields: [],
            directoryComponents: [.staticPath("app"), .staticPath("triples")],
            indexes: [
                makeIndex(
                    name: "RDFTriple_graph_subject_predicate_object",
                    kindIdentifier: "graph",
                    fieldNames: ["subject", "predicate", "object"],
                    kindMetadata: [
                        "strategy": .string("hexastore"),
                        "fromField": .string("subject"),
                        "edgeField": .string("predicate"),
                        "toField": .string("object")
                    ]
                )
            ]
        )

        let data = try JSONEncoder().encode(catalog)
        let decoded = try JSONDecoder().decode(TypeCatalog.self, from: data)

        #expect(decoded == catalog)
        #expect(decoded.indexes[0].kind.metadata["strategy"]?.stringValue == "hexastore")
    }

    @Test func convenienceAccessors() {
        let index = makeIndex(
            name: "test_index",
            kindIdentifier: "scalar",
            fieldNames: ["email"],
            unique: true,
            sparse: true
        )

        #expect(index.unique == true)
        #expect(index.sparse == true)
        #expect(index.kindIdentifier == "scalar")
        #expect(index.fieldNames == ["email"])
    }

    @Test func storedFieldNamesAccessor() {
        let index = AnyIndexDescriptor(
            name: "test_index",
            kind: AnyIndexKind(
                identifier: "vector",
                subspaceStructure: .hierarchical,
                fieldNames: ["embedding"],
                metadata: [:]
            ),
            commonMetadata: [
                "unique": .bool(false),
                "sparse": .bool(false),
                "storedFieldNames": .stringArray(["title", "content"])
            ]
        )

        #expect(index.storedFieldNames == ["title", "content"])
    }
}
