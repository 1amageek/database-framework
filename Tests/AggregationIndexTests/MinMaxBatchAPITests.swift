import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

@Suite("MIN/MAX Batch API Tests")
struct MinMaxBatchAPITests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Test Models

    struct Order: Persistable {
        typealias ID = String

        var id: String
        var region: String
        var category: String
        var amount: Double

        init(id: String = UUID().uuidString, region: String, category: String, amount: Double) {
            self.id = id
            self.region = region
            self.category = category
            self.amount = amount
        }

        static var persistableType: String { "Order" }
        static var allFields: [String] { ["id", "region", "category", "amount"] }
        static var indexDescriptors: [IndexDescriptor] { [] }

        static func fieldNumber(for fieldName: String) -> Int? { nil }
        static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

        subscript(dynamicMember member: String) -> (any Sendable)? {
            switch member {
            case "id": return id
            case "region": return region
            case "category": return category
            case "amount": return amount
            default: return nil
            }
        }

        static func fieldName<Value>(for keyPath: KeyPath<Order, Value>) -> String {
            switch keyPath {
            case \Order.id: return "id"
            case \Order.region: return "region"
            case \Order.category: return "category"
            case \Order.amount: return "amount"
            default: return "\(keyPath)"
            }
        }

        static func fieldName(for keyPath: PartialKeyPath<Order>) -> String {
            switch keyPath {
            case \Order.id: return "id"
            case \Order.region: return "region"
            case \Order.category: return "category"
            case \Order.amount: return "amount"
            default: return "\(keyPath)"
            }
        }

        static func fieldName(for keyPath: AnyKeyPath) -> String {
            if let partial = keyPath as? PartialKeyPath<Order> {
                return fieldName(for: partial)
            }
            return "\(keyPath)"
        }
    }

    // MARK: - getAllMins Tests

    @Test("getAllMins returns all groups")
    func testGetAllMinsReturnsAllGroups() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_batch", testId).pack())


        let index = Index(
            name: "order_min_by_region",
            kind: MinIndexKind<Order, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "order_min_by_region",
            itemTypes: Set(["Order"])
        )

        let maintainer = MinIndexMaintainer<Order, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let orders = [
            Order(id: "o1", region: "US", category: "Electronics", amount: 999.0),
            Order(id: "o2", region: "US", category: "Books", amount: 49.0),
            Order(id: "o3", region: "EU", category: "Electronics", amount: 1299.0),
            Order(id: "o4", region: "EU", category: "Books", amount: 39.0),
            Order(id: "o5", region: "APAC", category: "Electronics", amount: 899.0)
        ]

        try await database.withTransaction { transaction in
            for order in orders {
                try await maintainer.updateIndex(
                    oldItem: nil as Order?,
                    newItem: order,
                    transaction: transaction
                )
            }
        }

        // Test getAllMins
        let mins = try await database.withTransaction { transaction in
            try await maintainer.getAllMins(transaction: transaction)
        }

        #expect(mins.count == 3, "Should have 3 groups (US, EU, APAC)")

        // Verify MIN values for each group
        var minsByRegion: [String: Double] = [:]
        for result in mins {
            let region = result.grouping[0] as! String
            minsByRegion[region] = result.min
        }

        #expect(minsByRegion["US"] == 49.0, "US min should be 49.0")
        #expect(minsByRegion["EU"] == 39.0, "EU min should be 39.0")
        #expect(minsByRegion["APAC"] == 899.0, "APAC min should be 899.0")
    }

    @Test("getAllMaxs returns all groups")
    func testGetAllMaxsReturnsAllGroups() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "max_batch", testId).pack())


        let index = Index(
            name: "order_max_by_region",
            kind: MaxIndexKind<Order, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "order_max_by_region",
            itemTypes: Set(["Order"])
        )

        let maintainer = MaxIndexMaintainer<Order, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let orders = [
            Order(id: "o1", region: "US", category: "Electronics", amount: 999.0),
            Order(id: "o2", region: "US", category: "Books", amount: 49.0),
            Order(id: "o3", region: "EU", category: "Electronics", amount: 1299.0),
            Order(id: "o4", region: "EU", category: "Books", amount: 39.0),
            Order(id: "o5", region: "APAC", category: "Electronics", amount: 899.0)
        ]

        try await database.withTransaction { transaction in
            for order in orders {
                try await maintainer.updateIndex(
                    oldItem: nil as Order?,
                    newItem: order,
                    transaction: transaction
                )
            }
        }

        // Test getAllMaxs
        let maxs = try await database.withTransaction { transaction in
            try await maintainer.getAllMaxs(transaction: transaction)
        }

        #expect(maxs.count == 3, "Should have 3 groups (US, EU, APAC)")

        // Verify MAX values for each group
        var maxsByRegion: [String: Double] = [:]
        for result in maxs {
            let region = result.grouping[0] as! String
            maxsByRegion[region] = result.max
        }

        #expect(maxsByRegion["US"] == 999.0, "US max should be 999.0")
        #expect(maxsByRegion["EU"] == 1299.0, "EU max should be 1299.0")
        #expect(maxsByRegion["APAC"] == 899.0, "APAC max should be 899.0")
    }

    @Test("getAllMins performance with large dataset")
    func testGetAllMinsPerformance() async throws {
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_perf", testId).pack())


        let index = Index(
            name: "order_min_by_region",
            kind: MinIndexKind<Order, Double>(groupBy: [\.region], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "order_min_by_region",
            itemTypes: Set(["Order"])
        )

        let maintainer = MinIndexMaintainer<Order, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let regions = ["US", "EU", "APAC", "SA", "AF"]
        var orders: [Order] = []

        // Create 1000 orders across 5 regions (200 per region)
        for i in 0..<1000 {
            let region = regions[i % regions.count]
            let amount = Double.random(in: 10.0...10000.0)
            orders.append(Order(id: "o\(i)", region: region, category: "Test", amount: amount))
        }

        try await database.withTransaction { transaction in
            for order in orders {
                try await maintainer.updateIndex(
                    oldItem: nil as Order?,
                    newItem: order,
                    transaction: transaction
                )
            }
        }

        // Measure getAllMins performance
        let start = DispatchTime.now()
        let mins = try await database.withTransaction { transaction in
            try await maintainer.getAllMins(transaction: transaction)
        }
        let end = DispatchTime.now()

        let elapsed = Double(end.uptimeNanoseconds - start.uptimeNanoseconds) / 1_000_000_000

        #expect(mins.count == 5, "Should have 5 groups")
        print("getAllMins (1000 items, 5 groups): \(String(format: "%.2f", elapsed * 1000))ms")

        // Should be fast (O(groups) not O(items))
        #expect(elapsed < 0.1, "getAllMins should complete in < 100ms")
    }
}
