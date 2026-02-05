/// AnyIndexDescriptorTests.swift
/// Tests for AnyIndexKind and AnyIndexDescriptor type erasure

import Testing
import Foundation
@testable import Core
@testable import DatabaseEngine

@Suite("AnyIndexDescriptor")
struct AnyIndexDescriptorTests {

    // MARK: - Test Model

    @Persistable
    struct TestProduct {
        #Directory<TestProduct>("test", "any_index_desc")

        var id: String = UUID().uuidString
        var name: String = ""
        var category: String = ""
        var price: Double = 0.0
        var embedding: [Float] = []

        #Index(ScalarIndexKind<TestProduct>(fields: [\TestProduct.category]), name: "TestProduct_category")
        #Index(ScalarIndexKind<TestProduct>(fields: [\TestProduct.name]), storedFields: [\TestProduct.price], unique: true, name: "TestProduct_name")
    }

    // MARK: - AnyIndexKind Tests

    @Test func anyIndexKindPropertiesExtracted() {
        let descriptors = TestProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyKind = AnyIndexKind(categoryDesc.kind)

        #expect(anyKind.identifier == "scalar")
        #expect(anyKind.fieldNames == ["category"])
        #expect(anyKind.subspaceStructure == .flat)
    }

    @Test func anyIndexKindMetadataEmptyForScalar() {
        let descriptors = TestProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyKind = AnyIndexKind(categoryDesc.kind)

        // ScalarIndexKind has no kind-specific metadata (only fieldNames which is filtered)
        #expect(anyKind.metadata.isEmpty)
    }

    @Test func anyIndexKindIsHashable() {
        let descriptors = TestProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyKind1 = AnyIndexKind(categoryDesc.kind)
        let anyKind2 = AnyIndexKind(categoryDesc.kind)

        #expect(anyKind1 == anyKind2)
        #expect(anyKind1.hashValue == anyKind2.hashValue)
    }

    // MARK: - AnyIndexDescriptor Tests

    @Test func anyIndexDescriptorPropertiesExtracted() {
        let descriptors = TestProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = AnyIndexDescriptor(categoryDesc)

        #expect(anyDesc.name == categoryDesc.name)
        #expect(anyDesc.kindIdentifier == "scalar")
        #expect(anyDesc.fieldNames == ["category"])
        #expect(anyDesc.subspaceStructure == .flat)
    }

    @Test func anyIndexDescriptorKindSeparated() {
        let descriptors = TestProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = AnyIndexDescriptor(categoryDesc)

        // kind is a separate AnyIndexKind
        #expect(anyDesc.kind.identifier == "scalar")
        #expect(anyDesc.kind.fieldNames == ["category"])
        #expect(anyDesc.kind.subspaceStructure == .flat)
    }

    // MARK: - CommonMetadata Tests

    @Test func uniqueInCommonMetadata() {
        let descriptors = TestProduct.indexDescriptors
        let nameDesc = descriptors.first { $0.name.contains("name") }!

        let anyDesc = AnyIndexDescriptor(nameDesc)

        let unique = anyDesc.commonMetadata["unique"]?.boolValue
        #expect(unique == true)
    }

    @Test func sparseInCommonMetadata() {
        let descriptors = TestProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = AnyIndexDescriptor(categoryDesc)

        let sparse = anyDesc.commonMetadata["sparse"]?.boolValue
        #expect(sparse == false)
    }

    @Test func storedFieldNamesInCommonMetadata() {
        let descriptors = TestProduct.indexDescriptors
        let nameDesc = descriptors.first { $0.name.contains("name") }!

        let anyDesc = AnyIndexDescriptor(nameDesc)

        let storedFields = anyDesc.commonMetadata["storedFieldNames"]?.stringArrayValue
        #expect(storedFields == ["price"])
    }

    @Test func nonUniqueIndexHasUniqueFalse() {
        let descriptors = TestProduct.indexDescriptors
        let categoryDesc = descriptors.first { $0.name.contains("category") }!

        let anyDesc = AnyIndexDescriptor(categoryDesc)

        let unique = anyDesc.commonMetadata["unique"]?.boolValue
        #expect(unique == false)
    }

    // MARK: - Sendable & Hashable Tests

    @Test func isSendable() async {
        let descriptors = TestProduct.indexDescriptors
        let anyDesc = AnyIndexDescriptor(descriptors.first!)

        let task = Task {
            return anyDesc.name
        }
        let name = await task.value
        #expect(!name.isEmpty)
    }

    @Test func isHashable() {
        let descriptors = TestProduct.indexDescriptors
        let anyDesc1 = AnyIndexDescriptor(descriptors.first!)
        let anyDesc2 = AnyIndexDescriptor(descriptors.first!)

        #expect(anyDesc1 == anyDesc2)
        #expect(anyDesc1.hashValue == anyDesc2.hashValue)

        var set: Set<AnyIndexDescriptor> = []
        set.insert(anyDesc1)
        set.insert(anyDesc2)
        #expect(set.count == 1)
    }

    // MARK: - IndexMetadataValue Tests

    @Test func metadataValueString() {
        let value = IndexMetadataValue(from: "test")
        #expect(value?.stringValue == "test")
        #expect(value?.intValue == nil)
    }

    @Test func metadataValueInt() {
        let value = IndexMetadataValue(from: 42)
        #expect(value?.intValue == 42)
        #expect(value?.stringValue == nil)
    }

    @Test func metadataValueDouble() {
        let value = IndexMetadataValue(from: 3.14)
        #expect(value?.doubleValue == 3.14)
        #expect(value?.intValue == nil)
    }

    @Test func metadataValueBool() {
        let value = IndexMetadataValue(from: true)
        #expect(value?.boolValue == true)
        #expect(value?.stringValue == nil)
    }

    @Test func metadataValueStringArray() {
        let value = IndexMetadataValue(from: ["a", "b", "c"])
        #expect(value?.stringArrayValue == ["a", "b", "c"])
        #expect(value?.intArrayValue == nil)
    }

    @Test func metadataValueIntArray() {
        let value = IndexMetadataValue(from: [1, 2, 3])
        #expect(value?.intArrayValue == [1, 2, 3])
        #expect(value?.stringArrayValue == nil)
    }

    @Test func metadataValueUnsupportedTypeReturnsNil() {
        let value = IndexMetadataValue(from: Date())
        #expect(value == nil)
    }
}
