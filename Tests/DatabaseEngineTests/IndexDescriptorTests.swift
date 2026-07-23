#if !os(WASI)
#if FOUNDATION_DB
// IndexDescriptorTests.swift
// FDBIndexing Tests - IndexDescriptor tests

import Testing
import TestHeartbeat
import Foundation
import Core
import DatabaseValue
@testable import DatabaseEngine

// Test model for KeyPath-based IndexDescriptor tests
@Persistable
struct IndexDescriptorUser {
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
        let kind = ScalarIndexKind<IndexDescriptorUser>(fields: [\.email])
        let options = CommonIndexOptions(unique: true, sparse: false, metadata: ["key": "value"])

        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\IndexDescriptorUser.email],
            kind: kind,
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
        let kind = ScalarIndexKind<IndexDescriptorUser>(fields: [\.email])

        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\IndexDescriptorUser.email],
            kind: kind
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
        let kind = ScalarIndexKind<IndexDescriptorUser>(fields: [\.email])

        let uniqueDescriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\IndexDescriptorUser.email],
            kind: kind,
            commonOptions: .init(unique: true)
        )
        #expect(uniqueDescriptor.isUnique == true)

        let nonUniqueDescriptor = IndexDescriptor(
            name: "User_city",
            keyPaths: [\IndexDescriptorUser.city],
            kind: ScalarIndexKind<IndexDescriptorUser>(fields: [\.city]),
            commonOptions: .init(unique: false)
        )
        #expect(nonUniqueDescriptor.isUnique == false)
    }

    @Test("IndexDescriptor isSparse property")
    func testIsSparseProperty() throws {
        let sparseDescriptor = IndexDescriptor(
            name: "User_nickname",
            keyPaths: [\IndexDescriptorUser.nickname],
            kind: ScalarIndexKind<IndexDescriptorUser>(fields: [\.nickname]),
            commonOptions: .init(sparse: true)
        )
        #expect(sparseDescriptor.isSparse == true)

        let nonSparseDescriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\IndexDescriptorUser.email],
            kind: ScalarIndexKind<IndexDescriptorUser>(fields: [\.email]),
            commonOptions: .init(sparse: false)
        )
        #expect(nonSparseDescriptor.isSparse == false)
    }

    @Test("IndexDescriptor kindIdentifier property")
    func testKindIdentifierProperty() throws {
        let scalarKind = ScalarIndexKind<IndexDescriptorUser>(fields: [\.email])
        let countKind = CountIndexKind<IndexDescriptorUser>(groupBy: [\.city])

        let scalarDescriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\IndexDescriptorUser.email],
            kind: scalarKind
        )
        #expect(scalarDescriptor.kindIdentifier == "scalar")

        let countDescriptor = IndexDescriptor(
            name: "User_count_by_city",
            keyPaths: [\IndexDescriptorUser.city],
            kind: countKind
        )
        #expect(countDescriptor.kindIdentifier == "count")
    }

    // MARK: - Composite Index Tests

    @Test("IndexDescriptor stores canonical composite field names")
    func testCompositeKeyPaths() throws {
        let kind = ScalarIndexKind<IndexDescriptorUser>(fields: [\.category, \.price])

        let descriptor = IndexDescriptor(
            name: "Product_category_price",
            keyPaths: [\IndexDescriptorUser.category, \IndexDescriptorUser.price],
            kind: kind
        )

        #expect(descriptor.fieldNames == ["category", "price"])
    }

    // MARK: - Different Index Kinds Tests

    @Test("IndexDescriptor with CountIndexKind")
    func testCountIndexKind() throws {
        let kind = CountIndexKind<IndexDescriptorUser>(groupBy: [\.city])

        let descriptor = IndexDescriptor(
            name: "User_count_by_city",
            keyPaths: [\IndexDescriptorUser.city],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "count")
    }

    @Test("IndexDescriptor with SumIndexKind")
    func testSumIndexKind() throws {
        let kind = SumIndexKind(groupBy: [\IndexDescriptorUser.department], value: \IndexDescriptorUser.salary)

        let descriptor = IndexDescriptor(
            name: "Employee_salary_by_dept",
            keyPaths: [\IndexDescriptorUser.department, \IndexDescriptorUser.salary],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "sum")
        #expect(descriptor.fieldNames == ["department", "salary"])
    }

    @Test("IndexDescriptor with MinIndexKind")
    func testMinIndexKind() throws {
        let kind = MinIndexKind(groupBy: [\IndexDescriptorUser.region], value: \IndexDescriptorUser.price)

        let descriptor = IndexDescriptor(
            name: "Product_min_price_by_region",
            keyPaths: [\IndexDescriptorUser.region, \IndexDescriptorUser.price],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "min")
    }

    @Test("IndexDescriptor with MaxIndexKind")
    func testMaxIndexKind() throws {
        let kind = MaxIndexKind(groupBy: [\IndexDescriptorUser.region], value: \IndexDescriptorUser.price)

        let descriptor = IndexDescriptor(
            name: "Product_max_price_by_region",
            keyPaths: [\IndexDescriptorUser.region, \IndexDescriptorUser.price],
            kind: kind
        )

        #expect(descriptor.kindIdentifier == "max")
    }

    @Test("IndexDescriptor with VersionIndexKind")
    func testVersionIndexKind() throws {
        let kind = VersionIndexKind<IndexDescriptorUser>(field: \.email)

        // Note: VersionIndexKind typically uses a version field, using email as placeholder
        let descriptor = IndexDescriptor(
            name: "Document_version_index",
            keyPaths: [\IndexDescriptorUser.email],
            kind: kind
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
        let kind = ScalarIndexKind<IndexDescriptorUser>(fields: [\.email])
        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\IndexDescriptorUser.email],
            kind: kind,
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
        let descriptor = IndexDescriptor(
            name: "User_email",
            keyPaths: [\IndexDescriptorUser.email],
            kind: ScalarIndexKind<IndexDescriptorUser>(fields: [\.email])
        )

        #expect(descriptor.fieldNames == ["email"])
        #expect(descriptor.kind.fieldNames == ["email"])
    }
}
#endif

#endif
