#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing

@Suite("Schema.Entity directory resolution", .heartbeat)
struct SchemaEntityTests {
    @Test
    func staticOnly() throws {
        let entity = try makeEntity(
            name: "User",
            directoryComponents: [
                .staticPath("app"),
                .staticPath("users"),
            ]
        )

        #expect(try entity.resolvedDirectoryPath() == ["app", "users"])
        #expect(!entity.hasDynamicDirectory)
        #expect(entity.dynamicFieldNames.isEmpty)
    }

    @Test
    func resolvesDynamicFieldsInDeclarationOrder() throws {
        let entity = try makeEntity(
            name: "Message",
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "accountId"),
                .staticPath("channels"),
                .dynamicField(fieldName: "channelId"),
                .staticPath("messages"),
            ]
        )

        let path = try entity.resolvedDirectoryPath(
            partitionValues: [
                "accountId": "acc_1",
                "channelId": "ch_2",
            ]
        )

        #expect(
            path == [
                "tenants",
                "acc_1",
                "channels",
                "ch_2",
                "messages",
            ]
        )
        #expect(entity.hasDynamicDirectory)
        #expect(entity.dynamicFieldNames == ["accountId", "channelId"])
    }

    @Test
    func missingPartitionValueFails() throws {
        let entity = try makeEntity(
            name: "Order",
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "tenantId"),
                .staticPath("orders"),
            ]
        )

        do {
            _ = try entity.resolvedDirectoryPath()
            Issue.record("Expected a missing partition field error")
        } catch DirectoryPathError.missingFields(let fields) {
            #expect(fields == ["tenantId"])
        } catch {
            Issue.record("Unexpected directory resolution error: \(error)")
        }
    }

    @Test
    func emptyDirectoryResolvesToEmptyPath() throws {
        let entity = try makeEntity(
            name: "Simple",
            directoryComponents: []
        )

        #expect(try entity.resolvedDirectoryPath().isEmpty)
    }

    private func makeEntity(
        name: String,
        directoryComponents: [DirectoryPathComponent]
    ) throws -> Schema.Entity {
        try Schema.Entity(
            name: name,
            identifierType: .string,
            fields: [],
            directoryComponents: directoryComponents
        )
    }
}

@Suite("Index descriptor metadata", .heartbeat)
struct IndexDescriptorMetadataTests {
    @Test
    func preservesCanonicalKindAndOptions() {
        let descriptor = makeIndex(
            name: "User_email",
            kindIdentifier: "scalar",
            fieldNames: ["email"],
            unique: true,
            sparse: true
        )

        #expect(descriptor.entityName == "User")
        #expect(descriptor.name == "User_email")
        #expect(descriptor.kindIdentifier == "scalar")
        #expect(descriptor.fieldNames == ["email"])
        #expect(descriptor.unique)
        #expect(descriptor.sparse)
    }

    @Test
    func preservesKindSpecificFieldValues() {
        let descriptor = makeIndex(
            name: "Statement_graph",
            kindIdentifier: "graph",
            fieldNames: ["subject", "predicate", "object"],
            kindMetadata: [
                "strategy": .string("hexastore"),
                "fromField": .string("subject"),
                "edgeField": .string("predicate"),
                "toField": .string("object"),
            ]
        )

        #expect(
            descriptor.kind.metadata["strategy"]?.stringValue == "hexastore"
        )
        #expect(
            descriptor.kind.metadata["fromField"]?.stringValue == "subject"
        )
    }

    @Test
    func preservesStoredFieldsSeparatelyFromKindMetadata() {
        let descriptor = IndexDescriptorMetadata(
            entityName: "Document",
            name: "Document_embedding",
            kind: IndexKindMetadata(
                identifier: "vector",
                subspaceStructure: .hierarchical,
                fields: [
                    IndexFieldMetadata(
                        identity: FieldIdentity(
                            name: "embedding",
                            number: 1
                        )
                    )
                ],
                metadata: [
                    "dimensions": .int64(384),
                    "metric": .string("cosine"),
                ]
            ),
            storedFieldNames: ["title", "content"]
        )

        #expect(descriptor.storedFieldNames == ["title", "content"])
        #expect(descriptor.kind.metadata["dimensions"] == .int64(384))
    }

    private func makeIndex(
        name: String,
        kindIdentifier: String,
        fieldNames: [String],
        unique: Bool = false,
        sparse: Bool = false,
        kindMetadata: [String: FieldValue] = [:]
    ) -> IndexDescriptorMetadata {
        IndexDescriptorMetadata(
            entityName: "User",
            name: name,
            kind: IndexKindMetadata(
                identifier: kindIdentifier,
                subspaceStructure: kindIdentifier == "scalar"
                    ? .flat
                    : .hierarchical,
                fields: fieldNames.enumerated().map { offset, name in
                    IndexFieldMetadata(
                        identity: FieldIdentity(
                            name: name,
                            number: offset + 1
                        )
                    )
                },
                metadata: kindMetadata
            ),
            commonOptions: CommonIndexOptions(
                unique: unique,
                sparse: sparse
            )
        )
    }
}
#endif
