// IndexDescriptorTests.swift
// FDBIndexing Tests - IndexDescriptor tests

import Testing
import Foundation
import Core
@testable import DatabaseEngine

// Test model for KeyPath-based IndexDescriptor tests
@Persistable
struct TestUser {
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

@Suite("IndexDescriptor Tests")
struct IndexDescriptorTests {

    // MARK: - Initialization Tests

    @Test("IndexDescriptor initializes with all parameters")
    func testInitialization() throws {
        let kind = ScalarIndexKind()
        let options = CommonIndexOptions(unique: true, sparse: false, metadata: ["key": "value"])

        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\TestUser.email],
            kind: kind,
            commonOptions: options
        )

        #expect(descriptor.name == "User_email")
        #expect(descriptor.keyPaths.count == 1)
        #expect(type(of: descriptor.kind).identifier == "scalar")
        #expect(descriptor.commonOptions.unique == true)
        #expect(descriptor.commonOptions.sparse == false)
        #expect(descriptor.commonOptions.metadata == ["key": "value"])
    }

    @Test("IndexDescriptor initializes with default options")
    func testInitializationWithDefaults() throws {
        let kind = ScalarIndexKind()

        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\TestUser.email],
            kind: kind
        )

        #expect(descriptor.name == "User_email")
        #expect(descriptor.keyPaths.count == 1)
        #expect(descriptor.commonOptions.unique == false)
        #expect(descriptor.commonOptions.sparse == false)
        #expect(descriptor.commonOptions.metadata.isEmpty)
    }

    // MARK: - Convenience Properties Tests

    @Test("IndexDescriptor isUnique property")
    func testIsUniqueProperty() throws {
        let kind = ScalarIndexKind()

        let uniqueDescriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\TestUser.email],
            kind: kind,
            commonOptions: .init(unique: true)
        )
        #expect(uniqueDescriptor.isUnique == true)

        let nonUniqueDescriptor = IndexDescriptor(
            name: "User_city",
            keyPaths: [\TestUser.city],
            kind: kind,
            commonOptions: .init(unique: false)
        )
        #expect(nonUniqueDescriptor.isUnique == false)
    }

    @Test("IndexDescriptor isSparse property")
    func testIsSparseProperty() throws {
        let kind = ScalarIndexKind()

        let sparseDescriptor = IndexDescriptor(
            name: "User_nickname",
            keyPaths: [\TestUser.nickname],
            kind: kind,
            commonOptions: .init(sparse: true)
        )
        #expect(sparseDescriptor.isSparse == true)

        let nonSparseDescriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\TestUser.email],
            kind: kind,
            commonOptions: .init(sparse: false)
        )
        #expect(nonSparseDescriptor.isSparse == false)
    }

    @Test("IndexDescriptor kindIdentifier property")
    func testKindIdentifierProperty() throws {
        let scalarKind = ScalarIndexKind()
        let countKind = CountIndexKind()

        let scalarDescriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\TestUser.email],
            kind: scalarKind
        )
        #expect(scalarDescriptor.kindIdentifier == "scalar")

        let countDescriptor = IndexDescriptor(
            name: "User_count_by_city",
            keyPaths: [\TestUser.city],
            kind: countKind
        )
        #expect(countDescriptor.kindIdentifier == "count")
    }

    // MARK: - Composite Index Tests

    @Test("IndexDescriptor with composite key paths")
    func testCompositeKeyPaths() throws {
        let kind = ScalarIndexKind()

        let descriptor = IndexDescriptor(
            name: "Product_category_price",
            keyPaths: [\TestUser.category, \TestUser.price],
            kind: kind
        )

        #expect(descriptor.keyPaths.count == 2)
    }

    // MARK: - Different Index Kinds Tests

    @Test("IndexDescriptor with CountIndexKind")
    func testCountIndexKind() throws {
        let kind = CountIndexKind()

        let descriptor = IndexDescriptor(
            name: "User_count_by_city",
            keyPaths: [\TestUser.city],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "count")
    }

    @Test("IndexDescriptor with SumIndexKind")
    func testSumIndexKind() throws {
        let kind = SumIndexKind()

        let descriptor = IndexDescriptor(
            name: "Employee_salary_by_dept",
            keyPaths: [\TestUser.department, \TestUser.salary],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "sum")
        #expect(descriptor.keyPaths.count == 2)
    }

    @Test("IndexDescriptor with MinIndexKind")
    func testMinIndexKind() throws {
        let kind = MinIndexKind()

        let descriptor = IndexDescriptor(
            name: "Product_min_price_by_region",
            keyPaths: [\TestUser.region, \TestUser.price],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "min")
    }

    @Test("IndexDescriptor with MaxIndexKind")
    func testMaxIndexKind() throws {
        let kind = MaxIndexKind()

        let descriptor = IndexDescriptor(
            name: "Product_max_price_by_region",
            keyPaths: [\TestUser.region, \TestUser.price],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "max")
    }

    @Test("IndexDescriptor with VersionIndexKind")
    func testVersionIndexKind() throws {
        let kind = VersionIndexKind()

        // Note: VersionIndexKind typically uses a version field, using email as placeholder
        let descriptor = IndexDescriptor(
            name: "Document_version_index",
            keyPaths: [\TestUser.email],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "version")
    }

    // MARK: - Codable Tests

    // Note: Codable tests removed because IndexDescriptor contains 'any IndexKind'
    // which cannot be made Codable without type erasure. We removed the type-erased
    // wrapper (AnyIndexKind) in favor of simpler design using 'any IndexKind' directly.

    // MARK: - Description Tests

    // Note: Hashable/Equatable tests removed because IndexDescriptor contains
    // 'any IndexKind' which is not directly comparable. Equality would require
    // type erasure which we removed in favor of simpler design.

    @Test("IndexDescriptor description includes key information")
    func testDescription() throws {
        let kind = ScalarIndexKind()
        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\TestUser.email],
            kind: kind,
            commonOptions: .init(unique: true, sparse: false, metadata: ["key": "value"])
        )

        let description = descriptor.description

        #expect(description.contains("User_email"))
        #expect(description.contains("scalar"))
        #expect(description.contains("unique: true"))
        #expect(description.contains("metadata"))
    }

    // MARK: - KeyPath Conversion Tests

    @Test("IndexDescriptor fieldName conversion")
    func testFieldNameConversion() throws {
        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\TestUser.email],
            kind: ScalarIndexKind()
        )

        // Test that we can convert keyPath back to string
        if let keyPath = descriptor.keyPaths.first as? PartialKeyPath<TestUser> {
            let fieldName = TestUser.fieldName(for: keyPath)
            #expect(fieldName == "email")
        } else {
            Issue.record("Failed to cast keyPath to PartialKeyPath<TestUser>")
        }
    }
}
