import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

@Suite("MIN/MAX Edge Case Tests")
struct MinMaxEdgeCaseTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Test Model

    struct Product: Persistable {
        typealias ID = String

        var id: String
        var region: String
        var category: String
        var price: Int64

        init(id: String, region: String, category: String, price: Int64) {
            self.id = id
            self.region = region
            self.category = category
            self.price = price
        }

        static var persistableType: String { "Product" }
        static var allFields: [String] { ["id", "region", "category", "price"] }
        static var indexDescriptors: [IndexDescriptor] { [] }

        static func fieldNumber(for fieldName: String) -> Int? { nil }
        static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

        subscript(dynamicMember member: String) -> (any Sendable)? {
            switch member {
            case "id": return id
            case "region": return region
            case "category": return category
            case "price": return price
            default: return nil
            }
        }

        static func fieldName<Value>(for keyPath: KeyPath<Product, Value>) -> String {
            switch keyPath {
            case \Product.id: return "id"
            case \Product.region: return "region"
            case \Product.category: return "category"
            case \Product.price: return "price"
            default: return "\(keyPath)"
            }
        }

        static func fieldName(for keyPath: PartialKeyPath<Product>) -> String {
            switch keyPath {
            case \Product.id: return "id"
            case \Product.region: return "region"
            case \Product.category: return "category"
            case \Product.price: return "price"
            default: return "\(keyPath)"
            }
        }

        static func fieldName(for keyPath: AnyKeyPath) -> String {
            if let partial = keyPath as? PartialKeyPath<Product> {
                return fieldName(for: partial)
            }
            return "\(keyPath)"
        }
    }

    // MARK: - Composite Grouping Tests

    @Test("MIN with composite grouping keys")
    func testMinCompositeGrouping() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_composite_grouping", testId).pack())

        // Index with composite grouping: region + category
        let index = Index(
            name: "product_min_by_region_category",
            kind: MinIndexKind<Product, Int64>(groupBy: [\.region, \.category], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_min_by_region_category",
            itemTypes: Set(["Product"])
        )

        let maintainer = MinIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let products = [
            Product(id: "p1", region: "US", category: "Electronics", price: 999),
            Product(id: "p2", region: "US", category: "Electronics", price: 499),
            Product(id: "p3", region: "US", category: "Books", price: 29),
            Product(id: "p4", region: "EU", category: "Electronics", price: 1299),
            Product(id: "p5", region: "EU", category: "Books", price: 19),
        ]

        try await database.withTransaction { transaction in
            for product in products {
                try await maintainer.updateIndex(
                    oldItem: nil as Product?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Test getMin with composite grouping
        let usElectronicsMin = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["US", "Electronics"], transaction: transaction)
        }
        #expect(usElectronicsMin == 499, "US-Electronics min should be 499")

        let usBooksMin = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["US", "Books"], transaction: transaction)
        }
        #expect(usBooksMin == 29, "US-Books min should be 29")

        let euElectronicsMin = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["EU", "Electronics"], transaction: transaction)
        }
        #expect(euElectronicsMin == 1299, "EU-Electronics min should be 1299")

        let euBooksMin = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["EU", "Books"], transaction: transaction)
        }
        #expect(euBooksMin == 19, "EU-Books min should be 19")
    }

    @Test("MAX with composite grouping keys")
    func testMaxCompositeGrouping() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "max_composite_grouping", testId).pack())

        // Index with composite grouping: region + category
        let index = Index(
            name: "product_max_by_region_category",
            kind: MaxIndexKind<Product, Int64>(groupBy: [\.region, \.category], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_max_by_region_category",
            itemTypes: Set(["Product"])
        )

        let maintainer = MaxIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let products = [
            Product(id: "p1", region: "US", category: "Electronics", price: 999),
            Product(id: "p2", region: "US", category: "Electronics", price: 499),
            Product(id: "p3", region: "US", category: "Books", price: 29),
            Product(id: "p4", region: "EU", category: "Electronics", price: 1299),
            Product(id: "p5", region: "EU", category: "Books", price: 19),
        ]

        try await database.withTransaction { transaction in
            for product in products {
                try await maintainer.updateIndex(
                    oldItem: nil as Product?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Test getMax with composite grouping
        let usElectronicsMax = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["US", "Electronics"], transaction: transaction)
        }
        #expect(usElectronicsMax == 999, "US-Electronics max should be 999")

        let usBooksMax = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["US", "Books"], transaction: transaction)
        }
        #expect(usBooksMax == 29, "US-Books max should be 29")

        let euElectronicsMax = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["EU", "Electronics"], transaction: transaction)
        }
        #expect(euElectronicsMax == 1299, "EU-Electronics max should be 1299")

        let euBooksMax = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["EU", "Books"], transaction: transaction)
        }
        #expect(euBooksMax == 19, "EU-Books max should be 19")
    }

    @Test("getAllMins with composite grouping")
    func testGetAllMinsCompositeGrouping() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_batch_composite", testId).pack())

        let index = Index(
            name: "product_min_by_region_category",
            kind: MinIndexKind<Product, Int64>(groupBy: [\.region, \.category], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_min_by_region_category",
            itemTypes: Set(["Product"])
        )

        let maintainer = MinIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let products = [
            Product(id: "p1", region: "US", category: "Electronics", price: 999),
            Product(id: "p2", region: "US", category: "Electronics", price: 499),
            Product(id: "p3", region: "US", category: "Books", price: 29),
            Product(id: "p4", region: "EU", category: "Electronics", price: 1299),
            Product(id: "p5", region: "EU", category: "Books", price: 19),
            Product(id: "p6", region: "APAC", category: "Electronics", price: 899),
        ]

        try await database.withTransaction { transaction in
            for product in products {
                try await maintainer.updateIndex(
                    oldItem: nil as Product?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Test getAllMins with composite grouping
        let mins = try await database.withTransaction { transaction in
            try await maintainer.getAllMins(transaction: transaction)
        }

        #expect(mins.count == 5, "Should have 5 groups (3 regions × 2 categories - 1)")

        // Build dictionary for verification
        var minsByGroup: [String: Int64] = [:]
        for result in mins {
            guard result.grouping.count == 2 else {
                #expect(Bool(false), "Grouping should have 2 elements")
                continue
            }
            let region = result.grouping[0] as! String
            let category = result.grouping[1] as! String
            let key = "\(region)-\(category)"
            minsByGroup[key] = result.min
        }

        // Verify all expected groups
        #expect(minsByGroup["US-Electronics"] == 499)
        #expect(minsByGroup["US-Books"] == 29)
        #expect(minsByGroup["EU-Electronics"] == 1299)
        #expect(minsByGroup["EU-Books"] == 19)
        #expect(minsByGroup["APAC-Electronics"] == 899)
    }

    @Test("getAllMaxs with composite grouping")
    func testGetAllMaxsCompositeGrouping() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "max_batch_composite", testId).pack())

        let index = Index(
            name: "product_max_by_region_category",
            kind: MaxIndexKind<Product, Int64>(groupBy: [\.region, \.category], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_max_by_region_category",
            itemTypes: Set(["Product"])
        )

        let maintainer = MaxIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let products = [
            Product(id: "p1", region: "US", category: "Electronics", price: 999),
            Product(id: "p2", region: "US", category: "Electronics", price: 499),
            Product(id: "p3", region: "US", category: "Books", price: 29),
            Product(id: "p4", region: "EU", category: "Electronics", price: 1299),
            Product(id: "p5", region: "EU", category: "Books", price: 19),
            Product(id: "p6", region: "APAC", category: "Electronics", price: 899),
        ]

        try await database.withTransaction { transaction in
            for product in products {
                try await maintainer.updateIndex(
                    oldItem: nil as Product?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Test getAllMaxs with composite grouping
        let maxs = try await database.withTransaction { transaction in
            try await maintainer.getAllMaxs(transaction: transaction)
        }

        #expect(maxs.count == 5, "Should have 5 groups")

        // Build dictionary for verification
        var maxsByGroup: [String: Int64] = [:]
        for result in maxs {
            guard result.grouping.count == 2 else {
                #expect(Bool(false), "Grouping should have 2 elements")
                continue
            }
            let region = result.grouping[0] as! String
            let category = result.grouping[1] as! String
            let key = "\(region)-\(category)"
            maxsByGroup[key] = result.max
        }

        // Verify all expected groups
        #expect(maxsByGroup["US-Electronics"] == 999)
        #expect(maxsByGroup["US-Books"] == 29)
        #expect(maxsByGroup["EU-Electronics"] == 1299)
        #expect(maxsByGroup["EU-Books"] == 19)
        #expect(maxsByGroup["APAC-Electronics"] == 899)
    }

    // MARK: - Empty Group Tests (Sparse Index Behavior)

    @Test("MIN with empty group after deleting all items")
    func testMinEmptyGroupAfterDelete() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_empty_group", testId).pack())

        let index = Index(
            name: "product_min_by_region",
            kind: MinIndexKind<Product, Int64>(groupBy: [\.region], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_min_by_region",
            itemTypes: Set(["Product"])
        )

        let maintainer = MinIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let product = Product(id: "p1", region: "US", category: "Electronics", price: 999)

        // Insert
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as Product?,
                newItem: product,
                transaction: transaction
            )
        }

        // Verify it exists
        let minBefore = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["US"], transaction: transaction)
        }
        #expect(minBefore == 999)

        // Delete the only item in the group
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: product,
                newItem: nil as Product?,
                transaction: transaction
            )
        }

        // Should throw error (group is empty, Layer 2 should be cleared)
        await #expect(throws: IndexError.self) {
            try await database.withTransaction { transaction in
                _ = try await maintainer.getMin(groupingValues: ["US"], transaction: transaction)
            }
        }
    }

    @Test("MAX with empty group after deleting all items")
    func testMaxEmptyGroupAfterDelete() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "max_empty_group", testId).pack())

        let index = Index(
            name: "product_max_by_region",
            kind: MaxIndexKind<Product, Int64>(groupBy: [\.region], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_max_by_region",
            itemTypes: Set(["Product"])
        )

        let maintainer = MaxIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let product = Product(id: "p1", region: "US", category: "Electronics", price: 999)

        // Insert
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: nil as Product?,
                newItem: product,
                transaction: transaction
            )
        }

        // Verify it exists
        let maxBefore = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["US"], transaction: transaction)
        }
        #expect(maxBefore == 999)

        // Delete the only item in the group
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: product,
                newItem: nil as Product?,
                transaction: transaction
            )
        }

        // Should throw error (group is empty, Layer 2 should be cleared)
        await #expect(throws: IndexError.self) {
            try await database.withTransaction { transaction in
                _ = try await maintainer.getMax(groupingValues: ["US"], transaction: transaction)
            }
        }
    }

    @Test("getAllMins excludes empty groups")
    func testGetAllMinsExcludesEmptyGroups() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_batch_empty", testId).pack())

        let index = Index(
            name: "product_min_by_region",
            kind: MinIndexKind<Product, Int64>(groupBy: [\.region], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_min_by_region",
            itemTypes: Set(["Product"])
        )

        let maintainer = MinIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let products = [
            Product(id: "p1", region: "US", category: "Electronics", price: 999),
            Product(id: "p2", region: "US", category: "Books", price: 49),
            Product(id: "p3", region: "EU", category: "Electronics", price: 1299),
            Product(id: "p4", region: "APAC", category: "Electronics", price: 899),
        ]

        try await database.withTransaction { transaction in
            for product in products {
                try await maintainer.updateIndex(
                    oldItem: nil as Product?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Delete all EU items
        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: products[2],
                newItem: nil as Product?,
                transaction: transaction
            )
        }

        let mins = try await database.withTransaction { transaction in
            try await maintainer.getAllMins(transaction: transaction)
        }

        // Should only have 2 groups (EU was deleted)
        #expect(mins.count == 2, "Should have 2 groups (US, APAC)")

        var minsByRegion: [String: Int64] = [:]
        for result in mins {
            let region = result.grouping[0] as! String
            minsByRegion[region] = result.min
        }

        #expect(minsByRegion["US"] == 49)
        #expect(minsByRegion["APAC"] == 899)
        #expect(minsByRegion["EU"] == nil, "EU should not be in results (deleted)")
    }

    // MARK: - Group Movement Tests (Critical for 2-Layer Architecture)

    @Test("MIN: Update that moves item to different group updates both groups")
    func testMinGroupMovement() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_group_move", testId).pack())

        let index = Index(
            name: "product_min_by_region",
            kind: MinIndexKind<Product, Int64>(groupBy: [\.region], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_min_by_region",
            itemTypes: Set(["Product"])
        )

        let maintainer = MinIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Initial state: US has 2 items (min: 49), EU has 1 item (min: 1299)
        let products = [
            Product(id: "p1", region: "US", category: "Electronics", price: 999),
            Product(id: "p2", region: "US", category: "Books", price: 49),
            Product(id: "p3", region: "EU", category: "Electronics", price: 1299),
        ]

        try await database.withTransaction { transaction in
            for product in products {
                try await maintainer.updateIndex(
                    oldItem: nil as Product?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Verify initial state
        let usMinBefore = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["US"], transaction: transaction)
        }
        #expect(usMinBefore == 49, "US min should be 49")

        let euMinBefore = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["EU"], transaction: transaction)
        }
        #expect(euMinBefore == 1299, "EU min should be 1299")

        // Move p2 from US to EU with new price
        let oldProduct = Product(id: "p2", region: "US", category: "Books", price: 49)
        let newProduct = Product(id: "p2", region: "EU", category: "Books", price: 39)

        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: oldProduct,
                newItem: newProduct,
                transaction: transaction
            )
        }

        // Verify US group updated (min should now be 999)
        let usMinAfter = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["US"], transaction: transaction)
        }
        #expect(usMinAfter == 999, "US min should be 999 after p2 moved out")

        // Verify EU group updated (min should now be 39)
        let euMinAfter = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["EU"], transaction: transaction)
        }
        #expect(euMinAfter == 39, "EU min should be 39 after p2 moved in")

        // Verify batch query reflects changes
        let mins = try await database.withTransaction { transaction in
            try await maintainer.getAllMins(transaction: transaction)
        }
        #expect(mins.count == 2, "Should have 2 groups")

        var minsByRegion: [String: Int64] = [:]
        for result in mins {
            let region = result.grouping[0] as! String
            minsByRegion[region] = result.min
        }

        #expect(minsByRegion["US"] == 999)
        #expect(minsByRegion["EU"] == 39)
    }

    @Test("MAX: Update that moves item to different group updates both groups")
    func testMaxGroupMovement() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "max_group_move", testId).pack())

        let index = Index(
            name: "product_max_by_region",
            kind: MaxIndexKind<Product, Int64>(groupBy: [\.region], value: \.price),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "price")
            ]),
            subspaceKey: "product_max_by_region",
            itemTypes: Set(["Product"])
        )

        let maintainer = MaxIndexMaintainer<Product, Int64>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        // Initial state: US has 2 items (max: 999), EU has 1 item (max: 1299)
        let products = [
            Product(id: "p1", region: "US", category: "Electronics", price: 999),
            Product(id: "p2", region: "US", category: "Books", price: 49),
            Product(id: "p3", region: "EU", category: "Electronics", price: 1299),
        ]

        try await database.withTransaction { transaction in
            for product in products {
                try await maintainer.updateIndex(
                    oldItem: nil as Product?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Verify initial state
        let usMaxBefore = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["US"], transaction: transaction)
        }
        #expect(usMaxBefore == 999, "US max should be 999")

        let euMaxBefore = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["EU"], transaction: transaction)
        }
        #expect(euMaxBefore == 1299, "EU max should be 1299")

        // Move p1 from US to EU with new higher price
        let oldProduct = Product(id: "p1", region: "US", category: "Electronics", price: 999)
        let newProduct = Product(id: "p1", region: "EU", category: "Electronics", price: 1599)

        try await database.withTransaction { transaction in
            try await maintainer.updateIndex(
                oldItem: oldProduct,
                newItem: newProduct,
                transaction: transaction
            )
        }

        // Verify US group updated (max should now be 49)
        let usMaxAfter = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["US"], transaction: transaction)
        }
        #expect(usMaxAfter == 49, "US max should be 49 after p1 moved out")

        // Verify EU group updated (max should now be 1599)
        let euMaxAfter = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["EU"], transaction: transaction)
        }
        #expect(euMaxAfter == 1599, "EU max should be 1599 after p1 moved in")

        // Verify batch query reflects changes
        let maxs = try await database.withTransaction { transaction in
            try await maintainer.getAllMaxs(transaction: transaction)
        }
        #expect(maxs.count == 2, "Should have 2 groups")

        var maxsByRegion: [String: Int64] = [:]
        for result in maxs {
            let region = result.grouping[0] as! String
            maxsByRegion[region] = result.max
        }

        #expect(maxsByRegion["US"] == 49)
        #expect(maxsByRegion["EU"] == 1599)
    }
}
