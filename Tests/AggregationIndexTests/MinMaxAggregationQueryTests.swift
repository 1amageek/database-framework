import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

@Suite("MIN/MAX AggregationQuery Integration Tests", .serialized)
struct MinMaxAggregationQueryTests {

    init() async throws {
        try await FDBTestSetup.shared.initialize()
    }

    // MARK: - Test Models

    @Persistable
    struct OrderMinTest {
        #Directory<OrderMinTest>("test", "order_min")

        var id: String = UUID().uuidString
        var region: String = ""
        var category: String = ""
        var amount: Double = 0.0
        var quantity: Int64 = 0

        #Index(MinIndexKind<OrderMinTest, Double>(
            groupBy: [\.region],
            value: \.amount
        ))
    }

    @Persistable
    struct OrderMaxTest {
        #Directory<OrderMaxTest>("test", "order_max")

        var id: String = UUID().uuidString
        var region: String = ""
        var category: String = ""
        var amount: Double = 0.0
        var quantity: Int64 = 0

        #Index(MaxIndexKind<OrderMaxTest, Double>(
            groupBy: [\.region],
            value: \.amount
        ))
    }

    @Persistable
    struct OrderMixedTest {
        #Directory<OrderMixedTest>("test", "order_mixed")

        var id: String = UUID().uuidString
        var region: String = ""
        var category: String = ""
        var amount: Double = 0.0
        var quantity: Int64 = 0

        #Index(CountIndexKind<OrderMixedTest>(groupBy: [\.region]))
        #Index(MinIndexKind<OrderMixedTest, Double>(groupBy: [\.region], value: \.amount))
        #Index(MaxIndexKind<OrderMixedTest, Double>(groupBy: [\.region], value: \.amount))
    }

    @Persistable
    struct OrderInt64Test {
        #Directory<OrderInt64Test>("test", "order_int64")

        var id: String = UUID().uuidString
        var region: String = ""
        var category: String = ""
        var amount: Double = 0.0
        var quantity: Int64 = 0

        #Index(MinIndexKind<OrderInt64Test, Int64>(
            groupBy: [\.region],
            value: \.quantity
        ))
    }

    // MARK: - End-to-End Integration Tests

    @Test("MIN aggregation end-to-end with context.aggregate().execute()")
    func testMinAggregationEndToEnd() async throws {
        // Create schema from Persistable type
        let schema = Schema([OrderMinTest.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert test data
        let orders = [
            OrderMinTest(region: "US", category: "Electronics", amount: 999.99, quantity: 2),
            OrderMinTest(region: "US", category: "Books", amount: 49.99, quantity: 5),
            OrderMinTest(region: "EU", category: "Electronics", amount: 1299.00, quantity: 1),
            OrderMinTest(region: "EU", category: "Books", amount: 39.99, quantity: 3),
            OrderMinTest(region: "APAC", category: "Electronics", amount: 899.00, quantity: 2),
        ]

        for order in orders {
            context.insert(order)
        }
        try await context.save()

        // Execute MIN aggregation query
        let results = try await context.aggregate(OrderMinTest.self)
            .groupBy(\OrderMinTest.region)
            .min(\OrderMinTest.amount, as: "minAmount")
            .execute()

        #expect(results.count == 3, "Should have 3 regions")

        // Build dictionary for verification
        var minByRegion: [String: Double] = [:]
        for result in results {
            guard case .string(let region) = result.groupKey["region"],
                  let minFieldValue = result.aggregates["minAmount"],
                  case .double(let minAmount) = minFieldValue else {
                #expect(Bool(false), "Missing expected fields in result")
                continue
            }
            minByRegion[region] = minAmount
        }

        #expect(minByRegion["US"] == 49.99, "US min should be 49.99")
        #expect(minByRegion["EU"] == 39.99, "EU min should be 39.99")
        #expect(minByRegion["APAC"] == 899.00, "APAC min should be 899.00")
    }

    @Test("MAX aggregation end-to-end with context.aggregate().execute()")
    func testMaxAggregationEndToEnd() async throws {
        // Create schema from Persistable type
        let schema = Schema([OrderMaxTest.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert test data
        let orders = [
            OrderMaxTest(region: "US", category: "Electronics", amount: 999.99, quantity: 2),
            OrderMaxTest(region: "US", category: "Books", amount: 49.99, quantity: 5),
            OrderMaxTest(region: "EU", category: "Electronics", amount: 1299.00, quantity: 1),
            OrderMaxTest(region: "EU", category: "Books", amount: 39.99, quantity: 3),
            OrderMaxTest(region: "APAC", category: "Electronics", amount: 899.00, quantity: 2),
        ]

        for order in orders {
            context.insert(order)
        }
        try await context.save()

        // Execute MAX aggregation query
        let results = try await context.aggregate(OrderMaxTest.self)
            .groupBy(\OrderMaxTest.region)
            .max(\OrderMaxTest.amount, as: "maxAmount")
            .execute()

        #expect(results.count == 3, "Should have 3 regions")

        // Build dictionary for verification
        var maxByRegion: [String: Double] = [:]
        for result in results {
            guard case .string(let region) = result.groupKey["region"],
                  let maxFieldValue = result.aggregates["maxAmount"],
                  case .double(let maxAmount) = maxFieldValue else {
                #expect(Bool(false), "Missing expected fields in result")
                continue
            }
            maxByRegion[region] = maxAmount
        }

        #expect(maxByRegion["US"] == 999.99, "US max should be 999.99")
        #expect(maxByRegion["EU"] == 1299.00, "EU max should be 1299.00")
        #expect(maxByRegion["APAC"] == 899.00, "APAC max should be 899.00")
    }

    @Test("Mixed MIN/MAX/COUNT aggregation end-to-end")
    func testMixedMinMaxCountAggregation() async throws {
        // Create schema from Persistable type
        let schema = Schema([OrderMixedTest.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert test data
        let orders = [
            OrderMixedTest(region: "US", category: "Electronics", amount: 999.99, quantity: 2),
            OrderMixedTest(region: "US", category: "Books", amount: 49.99, quantity: 5),
            OrderMixedTest(region: "EU", category: "Electronics", amount: 1299.00, quantity: 1),
            OrderMixedTest(region: "EU", category: "Books", amount: 39.99, quantity: 3),
            OrderMixedTest(region: "APAC", category: "Electronics", amount: 899.00, quantity: 2),
        ]

        for order in orders {
            context.insert(order)
        }
        try await context.save()

        // Execute mixed aggregation query
        let results = try await context.aggregate(OrderMixedTest.self)
            .groupBy(\OrderMixedTest.region)
            .count(as: "orderCount")
            .min(\OrderMixedTest.amount, as: "minAmount")
            .max(\OrderMixedTest.amount, as: "maxAmount")
            .execute()

        #expect(results.count == 3, "Should have 3 regions")

        // Build dictionary for verification
        var statsByRegion: [String: (count: Int64, min: Double, max: Double)] = [:]
        for result in results {
            guard case .string(let region) = result.groupKey["region"],
                  let countFieldValue = result.aggregates["orderCount"],
                  case .int64(let count) = countFieldValue,
                  let minFieldValue = result.aggregates["minAmount"],
                  case .double(let minAmount) = minFieldValue,
                  let maxFieldValue = result.aggregates["maxAmount"],
                  case .double(let maxAmount) = maxFieldValue else {
                #expect(Bool(false), "Missing expected fields in result")
                continue
            }
            statsByRegion[region] = (count, minAmount, maxAmount)
        }

        // Verify US
        guard let usStats = statsByRegion["US"] else {
            #expect(Bool(false), "US stats not found")
            return
        }
        #expect(usStats.count == 2, "US should have 2 orders")
        #expect(usStats.min == 49.99, "US min should be 49.99")
        #expect(usStats.max == 999.99, "US max should be 999.99")

        // Verify EU
        guard let euStats = statsByRegion["EU"] else {
            #expect(Bool(false), "EU stats not found")
            return
        }
        #expect(euStats.count == 2, "EU should have 2 orders")
        #expect(euStats.min == 39.99, "EU min should be 39.99")
        #expect(euStats.max == 1299.00, "EU max should be 1299.00")

        // Verify APAC
        guard let apacStats = statsByRegion["APAC"] else {
            #expect(Bool(false), "APAC stats not found")
            return
        }
        #expect(apacStats.count == 1, "APAC should have 1 order")
        #expect(apacStats.min == 899.00, "APAC min should be 899.00")
        #expect(apacStats.max == 899.00, "APAC max should be 899.00 (same as min)")
    }

    @Test("MIN aggregation with Int64 type")
    func testMinAggregationWithInt64() async throws {
        // Create schema from Persistable type
        let schema = Schema([OrderInt64Test.self])
        let container = try await FDBContainer(for: schema, security: .disabled)
        let context = container.newContext()

        // Insert test data
        let orders = [
            OrderInt64Test(region: "US", category: "Electronics", amount: 999.99, quantity: 10),
            OrderInt64Test(region: "US", category: "Books", amount: 49.99, quantity: 2),
            OrderInt64Test(region: "EU", category: "Electronics", amount: 1299.00, quantity: 5),
        ]

        for order in orders {
            context.insert(order)
        }
        try await context.save()

        // Execute MIN aggregation query on Int64 field
        let results = try await context.aggregate(OrderInt64Test.self)
            .groupBy(\OrderInt64Test.region)
            .min(\OrderInt64Test.quantity, as: "minQuantity")
            .execute()

        #expect(results.count == 2, "Should have 2 regions")

        // Build dictionary for verification
        var minByRegion: [String: Int64] = [:]
        for result in results {
            guard case .string(let region) = result.groupKey["region"],
                  let minFieldValue = result.aggregates["minQuantity"],
                  case .int64(let minQuantity) = minFieldValue else {
                #expect(Bool(false), "Missing expected fields in result")
                continue
            }
            minByRegion[region] = minQuantity
        }

        #expect(minByRegion["US"] == 2, "US min quantity should be 2")
        #expect(minByRegion["EU"] == 5, "EU min quantity should be 5")
    }
}
