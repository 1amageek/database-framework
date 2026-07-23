#if !os(WASI)
#if FOUNDATION_DB
/// IndexDescriptorMetadataTests.swift
/// Tests for IndexKindMetadata and IndexDescriptorMetadata type erasure

import Testing
import TestHeartbeat
import Foundation
@testable import Core
@testable import DatabaseEngine

@Suite("IndexDescriptorMetadata", .heartbeat)
struct IndexDescriptorMetadataTests {

    // MARK: - Test Model

    @Persistable
    struct IndexDescriptorProduct {
        #Directory<IndexDescriptorProduct>("test", "index_descriptor_metadata")

        var id: String = UUID().uuidString
        var name: String = ""
        var category: String = ""
        var price: Double = 0.0
        var embedding: [Float] = []

        #Index(ScalarIndexKind<IndexDescriptorProduct>(fields: [\IndexDescriptorProduct.category]), name: "TestProduct_category")
        #Index(ScalarIndexKind<IndexDescriptorProduct>(fields: [\IndexDescriptorProduct.name]), storedFields: [\IndexDescriptorProduct.price], unique: true, name: "TestProduct_name")
    }

    // MARK: - IndexKindMetadata Tests

    @Test func indexKindMetadataPropertiesExtracted() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let kindMetadata = categoryDesc.kind

        #expect(kindMetadata.identifier == "scalar")
        #expect(kindMetadata.fieldNames == ["category"])
        #expect(kindMetadata.subspaceStructure == .flat)
    }

    @Test func indexKindMetadataMetadataEmptyForScalar() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let kindMetadata = categoryDesc.kind

        // ScalarIndexKind has no kind-specific metadata (only fieldNames which is filtered)
        #expect(kindMetadata.metadata.isEmpty)
    }

    @Test func indexKindMetadataIsHashable() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let kindMetadata1 = categoryDesc.kind
        let kindMetadata2 = categoryDesc.kind

        #expect(kindMetadata1 == kindMetadata2)
        #expect(kindMetadata1.hashValue == kindMetadata2.hashValue)
    }

    // MARK: - IndexDescriptorMetadata Tests

    @Test func indexDescriptorMetadataPropertiesExtracted() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = IndexDescriptorMetadata(categoryDesc)

        #expect(anyDesc.name == categoryDesc.name)
        #expect(anyDesc.kindIdentifier == "scalar")
        #expect(anyDesc.fieldNames == ["category"])
        #expect(anyDesc.subspaceStructure == .flat)
    }

    @Test func indexDescriptorMetadataKindSeparated() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = IndexDescriptorMetadata(categoryDesc)

        // kind is a separate IndexKindMetadata
        #expect(anyDesc.kind.identifier == "scalar")
        #expect(anyDesc.kind.fieldNames == ["category"])
        #expect(anyDesc.kind.subspaceStructure == .flat)
    }

    // MARK: - CommonMetadata Tests

    @Test func uniqueInCommonMetadata() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let nameDesc = descriptors.first { $0.name.contains("name") }!

        let anyDesc = IndexDescriptorMetadata(nameDesc)

        let unique = anyDesc.commonMetadata["unique"]?.boolValue
        #expect(unique == true)
    }

    @Test func sparseInCommonMetadata() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = IndexDescriptorMetadata(categoryDesc)

        let sparse = anyDesc.commonMetadata["sparse"]?.boolValue
        #expect(sparse == false)
    }

    @Test func storedFieldNamesInCommonMetadata() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let nameDesc = descriptors.first { $0.name.contains("name") }!

        let anyDesc = IndexDescriptorMetadata(nameDesc)

        let storedFields = anyDesc.commonMetadata["storedFieldNames"]?.stringArrayValue
        #expect(storedFields == ["price"])
    }

    @Test func nonUniqueIndexHasUniqueFalse() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = IndexDescriptorMetadata(categoryDesc)

        let unique = anyDesc.commonMetadata["unique"]?.boolValue
        #expect(unique == false)
    }

    // MARK: - Sendable & Hashable Tests

    @Test func isSendable() async {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let anyDesc = IndexDescriptorMetadata(descriptors.first!)

        let task = Task {
            return anyDesc.name
        }
        let name = await task.value
        #expect(!name.isEmpty)
    }

    @Test func isHashable() {
        let descriptors = IndexDescriptorProduct.indexDescriptors
        let anyDesc1 = IndexDescriptorMetadata(descriptors.first!)
        let anyDesc2 = IndexDescriptorMetadata(descriptors.first!)

        #expect(anyDesc1 == anyDesc2)
        #expect(anyDesc1.hashValue == anyDesc2.hashValue)

        var set: Set<IndexDescriptorMetadata> = []
        set.insert(anyDesc1)
        set.insert(anyDesc2)
        #expect(set.count == 1)
    }

    // MARK: - IndexMetadataValue Tests

    @Test func metadataValueString() {
        let value = IndexMetadataValue.string("test")
        #expect(value.stringValue == "test")
        #expect(value.intValue == nil)
    }

    @Test func metadataValueInt() {
        let value = IndexMetadataValue.int(42)
        #expect(value.intValue == 42)
        #expect(value.stringValue == nil)
    }

    @Test func metadataValueDouble() {
        let value = IndexMetadataValue.double(3.14)
        #expect(value.doubleValue == 3.14)
        #expect(value.intValue == nil)
    }

    @Test func metadataValueBool() {
        let value = IndexMetadataValue.bool(true)
        #expect(value.boolValue == true)
        #expect(value.stringValue == nil)
    }

    @Test func metadataValueStringArray() {
        let value = IndexMetadataValue.stringArray(["a", "b", "c"])
        #expect(value.stringArrayValue == ["a", "b", "c"])
        #expect(value.intArrayValue == nil)
    }

    @Test func metadataValueIntArray() {
        let value = IndexMetadataValue.intArray([1, 2, 3])
        #expect(value.intArrayValue == [1, 2, 3])
        #expect(value.stringArrayValue == nil)
    }
}
#endif

#endif
