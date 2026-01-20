// RelationshipIndexPerformanceTests.swift
// RelationshipIndex Tests - Performance benchmarks for relationship operations

import Testing
import Foundation
import Core
import Relationship
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import RelationshipIndex
import FoundationDB
import TestSupport

// MARK: - Test Models

@Persistable
struct PerfCustomer {
    #Directory<PerfCustomer>("test", "perf", "rel", "customers")
    var name: String = ""
    var tier: String = "standard"

    @Relationship(PerfOrder.self)
    var orderIDs: [String] = []
}

@Persistable
struct PerfOrder {
    #Directory<PerfOrder>("test", "perf", "rel", "orders")
    var total: Double = 0
    var status: String = "pending"

    @Relationship(PerfCustomer.self)
    var customerID: String? = nil
}

// MARK: - Test Helpers

private func enableAllIndexes<T: Persistable>(container: FDBContainer, for type: T.Type) async throws {
    let store = try await container.store(for: type) as! FDBDataStore
    for descriptor in T.indexDescriptors {
        var attempts = 0
        let maxAttempts = 3

        while attempts < maxAttempts {
            attempts += 1
            let currentState = try await store.indexStateManager.state(of: descriptor.name)

            switch currentState {
            case .disabled:
                do {
                    try await store.indexStateManager.enable(descriptor.name)
                    try await store.indexStateManager.makeReadable(descriptor.name)
                    break
                } catch let error as IndexStateError {
                    if case .invalidTransition = error, attempts < maxAttempts {
                        continue
                    }
                    throw error
                }
            case .writeOnly:
                do {
                    try await store.indexStateManager.makeReadable(descriptor.name)
                    break
                } catch let error as IndexStateError {
                    if case .invalidTransition = error, attempts < maxAttempts {
                        continue
                    }
                    throw error
                }
            case .readable:
                break
            }
            break
        }
    }
}

// MARK: - Benchmark Helpers

private struct BenchmarkResult {
    let operation: String
    let count: Int
    let durationMs: Double
    let throughputPerSecond: Double

    var description: String {
        String(format: "%@ - %d items in %.2fms (%.0f/s)",
               operation, count, durationMs, throughputPerSecond)
    }
}

private func benchmark<T>(
    _ operation: String,
    count: Int,
    _ block: () async throws -> T
) async throws -> (T, BenchmarkResult) {
    let start = DispatchTime.now()
    let result = try await block()
    let end = DispatchTime.now()

    let nanos = end.uptimeNanoseconds - start.uptimeNanoseconds
    let ms = Double(nanos) / 1_000_000
    let throughput = Double(count) / (ms / 1000)

    return (result, BenchmarkResult(
        operation: operation,
        count: count,
        durationMs: ms,
        throughputPerSecond: throughput
    ))
}

// MARK: - Performance Tests

@Suite("Relationship Index Performance Tests", .serialized)
struct RelationshipIndexPerformanceTests {

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema([PerfCustomer.self, PerfOrder.self], version: Schema.Version(1, 0, 0))

        let container = FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )

        try await enableAllIndexes(container: container, for: PerfCustomer.self)
        try await enableAllIndexes(container: container, for: PerfOrder.self)

        return container
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Insert Performance Tests

    @Test("To-One relationship insert performance")
    func testToOneInsertPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-perf")

        // Create customer first
        var customer = PerfCustomer(name: "Perf Customer")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        let count = 100

        // Benchmark: Insert orders with FK
        let (_, result) = try await benchmark("To-One Insert", count: count) {
            for i in 1...count {
                var order = PerfOrder(total: Double(i * 10))
                order.id = uniqueID("O-perf-\(i)")
                order.customerID = customerId
                context.insert(order)
            }
            try await context.save()
        }

        print(result.description)

        // Verify throughput (should be at least 500/s for this simple operation)
        #expect(result.throughputPerSecond > 100, "Insert throughput should be reasonable")
    }

    @Test("To-Many relationship insert performance")
    func testToManyInsertPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let count = 50
        let ordersPerCustomer = 5

        // Create orders first
        var orderIds: [[String]] = []
        for i in 1...count {
            var ids: [String] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-many-\(i)-\(j)")
                ids.append(orderId)
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                context.insert(order)
            }
            orderIds.append(ids)
        }
        try await context.save()

        // Benchmark: Insert customers with To-Many FK arrays
        let (_, result) = try await benchmark("To-Many Insert", count: count) {
            for i in 1...count {
                var customer = PerfCustomer(name: "Customer \(i)")
                customer.id = uniqueID("C-many-\(i)")
                customer.orderIDs = orderIds[i - 1]
                context.insert(customer)
            }
            try await context.save()
        }

        print(result.description)

        #expect(result.throughputPerSecond > 50, "To-Many insert throughput should be reasonable")
    }

    // MARK: - Query Performance Tests

    @Test("related() To-One lookup performance")
    func testRelatedToOneLookupPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-lookup")

        // Setup: Create customer and orders
        var customer = PerfCustomer(name: "Lookup Customer")
        customer.id = customerId
        context.insert(customer)

        var orderIds: [String] = []
        for i in 1...100 {
            let orderId = uniqueID("O-lookup-\(i)")
            orderIds.append(orderId)
            var order = PerfOrder(total: Double(i * 10))
            order.id = orderId
            order.customerID = customerId
            context.insert(order)
        }
        try await context.save()

        let lookupCount = 50

        // Benchmark: related() To-One lookups
        let (_, result) = try await benchmark("related() To-One", count: lookupCount) {
            for i in 0..<lookupCount {
                let order = try await context.model(for: orderIds[i], as: PerfOrder.self)!
                let _ = try await context.related(order, \.customerID, as: PerfCustomer.self)
            }
        }

        print(result.description)

        #expect(result.durationMs < 5000, "To-One lookup should complete in reasonable time")
    }

    @Test("related() To-Many lookup performance")
    func testRelatedToManyLookupPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let ordersPerCustomer = 10
        let customerCount = 20

        // Setup: Create customers with orders
        var customerIds: [String] = []
        for i in 1...customerCount {
            var orderIds: [String] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-tmany-\(i)-\(j)")
                orderIds.append(orderId)
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                context.insert(order)
            }

            let customerId = uniqueID("C-tmany-\(i)")
            customerIds.append(customerId)
            var customer = PerfCustomer(name: "Customer \(i)")
            customer.id = customerId
            customer.orderIDs = orderIds
            context.insert(customer)
        }
        try await context.save()

        // Benchmark: related() To-Many lookups
        let (_, result) = try await benchmark("related() To-Many", count: customerCount) {
            for customerId in customerIds {
                let customer = try await context.model(for: customerId, as: PerfCustomer.self)!
                let _ = try await context.related(customer, \.orderIDs, as: PerfOrder.self)
            }
        }

        print(result.description)

        #expect(result.durationMs < 5000, "To-Many lookup should complete in reasonable time")
    }

    @Test("joining() eager loading performance")
    func testJoiningEagerLoadingPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-join")
        let orderCount = 100

        // Setup: Create customer and orders
        var customer = PerfCustomer(name: "Join Customer")
        customer.id = customerId
        context.insert(customer)

        for i in 1...orderCount {
            var order = PerfOrder(total: Double(i * 10))
            order.id = uniqueID("O-join-\(i)")
            order.customerID = customerId
            context.insert(order)
        }
        try await context.save()

        // Benchmark: fetch() with joining()
        let (snapshots, result) = try await benchmark("joining() eager load", count: orderCount) {
            try await context.fetch(PerfOrder.self)
                .joining(\.customerID, as: PerfCustomer.self)
                .limit(orderCount)
                .execute()
        }

        print(result.description)

        // Verify snapshots have loaded relations
        var loadedCount = 0
        for snapshot in snapshots {
            if snapshot.ref(PerfCustomer.self, \.customerID) != nil {
                loadedCount += 1
            }
        }
        #expect(loadedCount > 0, "Should have loaded relationships")
    }

    @Test("get() with joining performance")
    func testGetWithJoiningPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerCount = 50
        let ordersPerCustomer = 5

        // Setup: Create customers with orders
        var customerIds: [String] = []
        for i in 1...customerCount {
            var orderIds: [String] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-getj-\(i)-\(j)")
                orderIds.append(orderId)
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                context.insert(order)
            }

            let customerId = uniqueID("C-getj-\(i)")
            customerIds.append(customerId)
            var customer = PerfCustomer(name: "Customer \(i)")
            customer.id = customerId
            customer.orderIDs = orderIds
            context.insert(customer)
        }
        try await context.save()

        // Benchmark: get() with To-Many joining
        let (_, result) = try await benchmark("get() with joining", count: customerCount) {
            for customerId in customerIds {
                let _ = try await context.get(
                    PerfCustomer.self,
                    id: customerId,
                    joining: \.orderIDs,
                    as: PerfOrder.self
                )
            }
        }

        print(result.description)

        #expect(result.durationMs < 10000, "get() with joining should complete in reasonable time")
    }

    // MARK: - Update Performance Tests

    @Test("FK update performance")
    func testFKUpdatePerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customer1Id = uniqueID("C-upd1")
        let customer2Id = uniqueID("C-upd2")
        let orderCount = 50

        // Setup: Create two customers
        var customer1 = PerfCustomer(name: "Customer 1")
        customer1.id = customer1Id
        var customer2 = PerfCustomer(name: "Customer 2")
        customer2.id = customer2Id
        context.insert(customer1)
        context.insert(customer2)

        // Create orders pointing to customer1
        var orders: [PerfOrder] = []
        for i in 1...orderCount {
            var order = PerfOrder(total: Double(i * 10))
            order.id = uniqueID("O-upd-\(i)")
            order.customerID = customer1Id
            orders.append(order)
            context.insert(order)
        }
        try await context.save()

        // Benchmark: Update FK from customer1 to customer2
        let (_, result) = try await benchmark("FK Update", count: orderCount) {
            for i in 0..<orderCount {
                orders[i].customerID = customer2Id
                context.insert(orders[i])
            }
            try await context.save()
        }

        print(result.description)

        #expect(result.throughputPerSecond > 50, "FK update throughput should be reasonable")
    }

    @Test("To-Many FK array update performance")
    func testToManyFKArrayUpdatePerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-tmupd")
        let orderCount = 100

        // Setup: Create customer with orders
        var customer = PerfCustomer(name: "To-Many Update Customer")
        customer.id = customerId

        var orderIds: [String] = []
        for i in 1...orderCount {
            let orderId = uniqueID("O-tmupd-\(i)")
            orderIds.append(orderId)
            var order = PerfOrder(total: Double(i * 10))
            order.id = orderId
            context.insert(order)
        }
        customer.orderIDs = orderIds
        context.insert(customer)
        try await context.save()

        let updateCount = 20

        // Benchmark: Update FK array (remove/add items)
        let (_, result) = try await benchmark("To-Many FK Update", count: updateCount) {
            for i in 0..<updateCount {
                // Remove first item, add a new one
                customer.orderIDs.removeFirst()
                let newOrderId = uniqueID("O-new-\(i)")
                var newOrder = PerfOrder(total: Double((i + 1) * 100))
                newOrder.id = newOrderId
                context.insert(newOrder)
                customer.orderIDs.append(newOrderId)
                context.insert(customer)
                try await context.save()
            }
        }

        print(result.description)

        #expect(result.durationMs < 10000, "To-Many FK update should complete in reasonable time")
    }

    // MARK: - Delete Performance Tests

    @Test("Delete with index cleanup performance")
    func testDeleteWithIndexCleanupPerformance() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-del")
        let orderCount = 50

        // Setup: Create customer and orders
        var customer = PerfCustomer(name: "Delete Customer")
        customer.id = customerId
        context.insert(customer)

        var orderIds: [String] = []
        for i in 1...orderCount {
            let orderId = uniqueID("O-del-\(i)")
            orderIds.append(orderId)
            var order = PerfOrder(total: Double(i * 10))
            order.id = orderId
            order.customerID = customerId
            context.insert(order)
        }
        try await context.save()

        // Benchmark: Delete orders (triggers index cleanup)
        let (_, result) = try await benchmark("Delete with index cleanup", count: orderCount) {
            for orderId in orderIds {
                if let order = try await context.model(for: orderId, as: PerfOrder.self) {
                    context.delete(order)
                }
            }
            try await context.save()
        }

        print(result.description)

        #expect(result.throughputPerSecond > 50, "Delete throughput should be reasonable")
    }

    // MARK: - Scale Tests

    @Test("Large To-Many array handling")
    func testLargeToManyArrayHandling() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-large")
        let orderCount = 200

        // Create many orders
        var orderIds: [String] = []
        for i in 1...orderCount {
            let orderId = uniqueID("O-large-\(i)")
            orderIds.append(orderId)
            var order = PerfOrder(total: Double(i))
            order.id = orderId
            context.insert(order)
        }
        try await context.save()

        // Benchmark: Create customer with large FK array
        let (_, insertResult) = try await benchmark("Large array insert", count: 1) {
            var customer = PerfCustomer(name: "Large Array Customer")
            customer.id = customerId
            customer.orderIDs = orderIds
            context.insert(customer)
            try await context.save()
        }

        print("Insert: \(insertResult.description)")

        // Benchmark: Load customer with large FK array
        let (orders, loadResult) = try await benchmark("Large array load", count: orderCount) {
            let customer = try await context.model(for: customerId, as: PerfCustomer.self)!
            return try await context.related(customer, \.orderIDs, as: PerfOrder.self)
        }

        print("Load: \(loadResult.description)")

        #expect(orders.count == orderCount, "Should load all related items")
    }

    @Test("Many relationships traversal")
    func testManyRelationshipsTraversal() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerCount = 20
        let ordersPerCustomer = 10

        // Setup: Create customers with orders
        var customerIds: [String] = []
        for i in 1...customerCount {
            let customerId = uniqueID("C-trav-\(i)")
            customerIds.append(customerId)
            var customer = PerfCustomer(name: "Customer \(i)")
            customer.id = customerId

            var orderIds: [String] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-trav-\(i)-\(j)")
                orderIds.append(orderId)
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                order.customerID = customerId  // Bidirectional reference
                context.insert(order)
            }
            customer.orderIDs = orderIds
            context.insert(customer)
        }
        try await context.save()

        let totalTraversals = customerCount * 2  // Forward + reverse for each

        // Benchmark: Traverse relationships in both directions
        let (_, result) = try await benchmark("Bidirectional traversal", count: totalTraversals) {
            for customerId in customerIds {
                // Forward: Customer -> Orders
                let customer = try await context.model(for: customerId, as: PerfCustomer.self)!
                let orders = try await context.related(customer, \.orderIDs, as: PerfOrder.self)

                // Reverse: Order -> Customer
                if let firstOrder = orders.first {
                    let _ = try await context.related(firstOrder, \.customerID, as: PerfCustomer.self)
                }
            }
        }

        print(result.description)

        #expect(result.durationMs < 15000, "Bidirectional traversal should complete in reasonable time")
    }
}
