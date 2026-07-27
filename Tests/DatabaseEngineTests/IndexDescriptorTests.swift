#if !os(WASI)
#if FOUNDATION_DB
// IndexDescriptorTests.swift
// FDBIndexing Tests - IndexDescriptor tests

import Testing
import TestHeartbeat
import Foundation
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine

// Test model for KeyPath-based IndexDescriptor tests
@Persistable
struct IndexDescriptorUser {
    var id: String = ""
    var email: String
    var name: String
    var city: String
    var nickname: String?
    var salary: Double = 0.0
    var department: String = ""
    var region: String = ""
    var price: Double = 0.0
    var category: String = ""
}

@Suite("IndexDescriptor Tests", .heartbeat)
struct IndexDescriptorTests {

    // MARK: - Initialization Tests

    @Test("IndexDescriptor initializes with all parameters")
    func testInitialization() throws {
        let options = CommonIndexOptions(unique: true, sparse: false, metadata: ["key": "value"])

        let descriptor = try IndexDescriptor(
            name: "User_email",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.email.ascending],
            commonOptions: options
        )

        #expect(descriptor.name == "User_email")
        #expect(descriptor.fieldNames == ["email"])
        #expect(descriptor.kindIdentifier == "scalar")
        #expect(descriptor.commonOptions.unique == true)
        #expect(descriptor.commonOptions.sparse == false)
        #expect(descriptor.commonOptions.metadata == ["key": "value"])
    }

    @Test("IndexDescriptor initializes with default options")
    func testInitializationWithDefaults() throws {
        let descriptor = try IndexDescriptor(
            name: "User_email",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.email.ascending]
        )

        #expect(descriptor.name == "User_email")
        #expect(descriptor.fieldNames == ["email"])
        #expect(descriptor.commonOptions.unique == false)
        #expect(descriptor.commonOptions.sparse == false)
        #expect(descriptor.commonOptions.metadata.isEmpty)
    }

    // MARK: - Convenience Properties Tests

    @Test("IndexDescriptor isUnique property")
    func testIsUniqueProperty() throws {
        let uniqueDescriptor = try IndexDescriptor(
            name: "User_email",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.email.ascending],
            commonOptions: .init(unique: true)
        )
        #expect(uniqueDescriptor.isUnique == true)

        let nonUniqueDescriptor = try IndexDescriptor(
            name: "User_city",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.city.ascending],
            commonOptions: .init(unique: false)
        )
        #expect(nonUniqueDescriptor.isUnique == false)
    }

    @Test("IndexDescriptor isSparse property")
    func testIsSparseProperty() throws {
        let sparseDescriptor = try IndexDescriptor(
            name: "User_nickname",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.nickname.ascending],
            commonOptions: .init(sparse: true)
        )
        #expect(sparseDescriptor.isSparse == true)

        let nonSparseDescriptor = try IndexDescriptor(
            name: "User_email",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.email.ascending],
            commonOptions: .init(sparse: false)
        )
        #expect(nonSparseDescriptor.isSparse == false)
    }

    @Test("IndexDescriptor kindIdentifier property")
    func testKindIdentifierProperty() throws {
        let scalarDescriptor = try IndexDescriptor(
            name: "User_email",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.email.ascending]
        )
        #expect(scalarDescriptor.kindIdentifier == "scalar")

        let countDescriptor = try IndexDescriptor(
            name: "User_count_by_city",
            definition: .count,
            fields: [IndexDescriptorUser.fields.city.ascending]
        )
        #expect(countDescriptor.kindIdentifier == "count")
    }

    // MARK: - Composite Index Tests

    @Test("IndexDescriptor stores canonical composite field names")
    func testCompositeKeyPaths() throws {
        let descriptor = try IndexDescriptor(
            name: "Product_category_price",
            definition: .scalar,
            fields: [
                IndexDescriptorUser.fields.category.ascending,
                IndexDescriptorUser.fields.price.ascending,
            ]
        )

        #expect(descriptor.fieldNames == ["category", "price"])
    }

    // MARK: - Different Index Kinds Tests

    @Test("IndexDescriptor with count semantics")
    func countDefinition() throws {
        let descriptor = try IndexDescriptor(
            name: "User_count_by_city",
            definition: .count,
            fields: [IndexDescriptorUser.fields.city.ascending]
        )

        #expect(descriptor.kindIdentifier == "count")
    }

    @Test("IndexDescriptor with sum semantics")
    func sumDefinition() throws {
        let descriptor = try IndexDescriptor(
            name: "Employee_salary_by_dept",
            definition: .sum,
            fields: [
                IndexDescriptorUser.fields.department.ascending,
                IndexDescriptorUser.fields.salary.ascending,
            ]
        )

        #expect(descriptor.kindIdentifier == "sum")
        #expect(descriptor.fieldNames == ["department", "salary"])
    }

    @Test("IndexDescriptor with minimum semantics")
    func minimumDefinition() throws {
        let descriptor = try IndexDescriptor(
            name: "Product_min_price_by_region",
            definition: .minimum,
            fields: [
                IndexDescriptorUser.fields.region.ascending,
                IndexDescriptorUser.fields.price.ascending,
            ]
        )

        #expect(descriptor.kindIdentifier == "min")
    }

    @Test("IndexDescriptor with maximum semantics")
    func maximumDefinition() throws {
        let descriptor = try IndexDescriptor(
            name: "Product_max_price_by_region",
            definition: .maximum,
            fields: [
                IndexDescriptorUser.fields.region.ascending,
                IndexDescriptorUser.fields.price.ascending,
            ]
        )

        #expect(descriptor.kindIdentifier == "max")
    }

    @Test("IndexDescriptor with version semantics")
    func versionDefinition() throws {
        let descriptor = try IndexDescriptor(
            name: "Document_version_index",
            definition: .version(),
            fields: [IndexDescriptorUser.fields.email.ascending]
        )

        #expect(descriptor.kindIdentifier == "version")
    }

    // MARK: - Codable Tests

    // IndexDescriptor is compile-time schema metadata. Its canonical kind payload is
    // represented by IndexKindMetadata at runtime and on the wire.

    // MARK: - Description Tests

    // Descriptor identity is its stable schema name.

    @Test("IndexDescriptor description includes key information")
    func testDescription() throws {
        let descriptor = try IndexDescriptor(
            name: "User_email",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.email.ascending],
            commonOptions: .init(unique: true, sparse: false, metadata: ["key": "value"])
        )

        let description = descriptor.description

        #expect(description.contains("User_email"))
        #expect(description.contains("scalar"))
        #expect(description.contains("unique: true"))
        #expect(description.contains("metadata"))
    }

    // MARK: - Canonical Field Tests

    @Test("IndexDescriptor resolves KeyPath to a canonical field name")
    func testFieldNameConversion() throws {
        let descriptor = try IndexDescriptor(
            name: "User_email",
            definition: .scalar,
            fields: [IndexDescriptorUser.fields.email.ascending]
        )

        #expect(descriptor.fieldNames == ["email"])
        #expect(descriptor.kind.fieldNames == ["email"])
    }
}
#endif

#endif
