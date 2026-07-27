#if !os(WASI)
#if FOUNDATION_DB
import Foundation
import Testing
import TestHeartbeat
import DatabaseTypes
@testable import DatabaseKit
@testable import DatabaseEngine

@Suite("Index descriptor metadata", .heartbeat)
struct IndexDescriptorMetadataTests {
    @Persistable
    struct Product {
        #Directory<Product>("test", "index_descriptor_metadata")

        var id: String = UUID().uuidString
        var name: String = ""
        var category: String = ""
        var price: Double = 0

        #Index(
            .scalar,
            fields: [\Product.category],
            name: "Product_category"
        )
        #Index(
            .scalar,
            fields: [\Product.name],
            storedFields: [\Product.price],
            unique: true,
            name: "Product_name"
        )
    }

    private enum TestError: Error {
        case missingDescriptor(String)
    }

    private func descriptor(
        named name: String
    ) throws -> IndexDescriptor {
        guard let descriptor = try Product.indexDescriptors.first(
            where: { $0.name == name }
        ) else {
            throw TestError.missingDescriptor(name)
        }
        return descriptor
    }

    @Test("Kind metadata preserves canonical field identity")
    func kindMetadataPreservesFields() throws {
        let metadata = try descriptor(named: "Product_category").kind

        #expect(metadata.identifier == "scalar")
        #expect(metadata.fieldNames == ["category"])
        #expect(metadata.fields.first?.identity == Product.fields.category.identity)
        #expect(metadata.subspaceStructure == .flat)
        #expect(metadata.metadata.isEmpty)
    }

    @Test("Descriptor metadata preserves common options")
    func descriptorMetadataPreservesCommonOptions() throws {
        let descriptor = try descriptor(named: "Product_name")
        let metadata = IndexDescriptorMetadata(descriptor)

        #expect(metadata.entityName == Product.persistableType)
        #expect(metadata.name == descriptor.name)
        #expect(metadata.kindIdentifier == "scalar")
        #expect(metadata.fieldNames == ["name"])
        #expect(metadata.unique)
        #expect(!metadata.sparse)
        #expect(metadata.storedFieldNames == ["price"])
    }

    @Test("Descriptor metadata remains hashable and sendable")
    func descriptorMetadataIsHashableAndSendable() async throws {
        let metadata = IndexDescriptorMetadata(
            try descriptor(named: "Product_category")
        )
        let equalMetadata = metadata

        #expect(metadata == equalMetadata)
        #expect(Set([metadata, equalMetadata]).count == 1)

        let transferredName = await Task { metadata.name }.value
        #expect(transferredName == "Product_category")
    }

    @Test("Kind-specific metadata uses FieldValue primitives")
    func kindSpecificMetadataUsesFieldValue() {
        let metadata = IndexKindMetadata(
            identifier: "vector",
            subspaceStructure: .hierarchical,
            fields: [Product.fields.price.ascending.metadata],
            metadata: [
                "dimensions": .int64(384),
                "metric": .string("cosine"),
                "normalized": .bool(true),
                "weights": .array([.float32(0.25), .float32(0.75)]),
            ]
        )

        #expect(metadata.metadata["dimensions"] == .int64(384))
        #expect(metadata.metadata["metric"] == .string("cosine"))
        #expect(metadata.metadata["normalized"] == .bool(true))
        #expect(
            metadata.metadata["weights"]
                == .array([.float32(0.25), .float32(0.75)])
        )
    }
}
#endif
#endif
