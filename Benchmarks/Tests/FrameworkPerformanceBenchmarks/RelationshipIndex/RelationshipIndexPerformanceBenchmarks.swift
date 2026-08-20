#if FOUNDATION_DB
// RelationshipIndexPerformanceBenchmarks.swift
// RelationshipIndex Tests - Performance benchmarks for relationship operations

import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine
@testable import RelationshipIndex
import StorageKit
import FDBStorage
import DatabaseRuntime

// MARK: - Test Models

@Persistable
struct PerfCustomer {
    #Directory<PerfCustomer>("test", "perf", "rel", "customers")
    var id: String = UUID().uuidString
    var name: String = ""
    var tier: String = "standard"

    @Relationship
    var orders: [PersistableReference<PerfOrder>] = []
}

@Persistable
struct PerfOrder {
    #Directory<PerfOrder>("test", "perf", "rel", "orders")
    var id: String = UUID().uuidString
    var total: Double = 0
    var status: String = "pending"

    @Relationship
    var customer: PersistableReference<PerfCustomer>? = nil
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

@Suite(
    "Relationship Index Performance Tests",
    .foundationDBBenchmark,
    .serialized,
    .heartbeat
)
struct RelationshipIndexPerformanceBenchmarks {

    private func setupContainer() async throws -> DBContainer {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let database = try await FoundationDBBenchmarkEnvironment.shared.makeEngine()

        let schema = try Schema(
            entities: [
                try PerfCustomer.schemaEntity,
                try PerfOrder.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )

        let container = try await DBContainer.open(
            for: schema,
            configuration: .benchmarking(storageEngine: database),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(PerfCustomer.self), try DatabaseFrameworkRuntime.entity(PerfOrder.self),
                ]),
            security: .benchmarkingDisabled
            )

        return container
    }

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Insert Performance Tests

    @Test("To-One relationship insert performance")
    func testToOneInsertPerformance() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let customerId = uniqueID("C-perf")

        // Create customer first
        var customer = PerfCustomer(name: "Perf Customer")
        customer.id = customerId
        try context.insert(customer)
        try await context.save()
        let customerReference = try context.reference(to: customer)

        let count = 100

        // Benchmark: Insert orders with typed references
        let (_, result) = try await benchmark("To-One Insert", count: count) {
            for i in 1...count {
                var order = PerfOrder(total: Double(i * 10))
                order.id = uniqueID("O-perf-\(i)")
                order.customer = customerReference
                try context.insert(order)
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
        let context = container.benchmarkContext()

        let count = 50
        let ordersPerCustomer = 5

        // Create orders first
        var orderReferences: [[PersistableReference<PerfOrder>]] = []
        for i in 1...count {
            var references: [PersistableReference<PerfOrder>] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-many-\(i)-\(j)")
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                references.append(try context.reference(to: order))
                try context.insert(order)
            }
            orderReferences.append(references)
        }
        try await context.save()

        // Benchmark: Insert customers with to-many reference arrays
        let (_, result) = try await benchmark("To-Many Insert", count: count) {
            for i in 1...count {
                var customer = PerfCustomer(name: "Customer \(i)")
                customer.id = uniqueID("C-many-\(i)")
                customer.orders = orderReferences[i - 1]
                try context.insert(customer)
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
        let context = container.benchmarkContext()

        let customerId = uniqueID("C-lookup")

        // Setup: Create customer and orders
        var customer = PerfCustomer(name: "Lookup Customer")
        customer.id = customerId
        try context.insert(customer)
        let customerReference = try context.reference(to: customer)

        var orderIds: [String] = []
        for i in 1...100 {
            let orderId = uniqueID("O-lookup-\(i)")
            orderIds.append(orderId)
            var order = PerfOrder(total: Double(i * 10))
            order.id = orderId
            order.customer = customerReference
            try context.insert(order)
        }
        try await context.save()

        let lookupCount = 50

        // Benchmark: related() To-One lookups
        let (_, result) = try await benchmark("related() To-One", count: lookupCount) {
            for i in 0..<lookupCount {
                let order = try await context.model(for: orderIds[i], as: PerfOrder.self)!
                let _ = try await context.related(
                    order,
                    PerfOrder.fields.customer
                )
            }
        }

        print(result.description)

        #expect(result.durationMs < 5000, "To-One lookup should complete in reasonable time")
    }

    @Test("related() To-Many lookup performance")
    func testRelatedToManyLookupPerformance() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let ordersPerCustomer = 10
        let customerCount = 20

        // Setup: Create customers with orders
        var customerIds: [String] = []
        for i in 1...customerCount {
            var orderReferences: [PersistableReference<PerfOrder>] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-tmany-\(i)-\(j)")
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                orderReferences.append(try context.reference(to: order))
                try context.insert(order)
            }

            let customerId = uniqueID("C-tmany-\(i)")
            customerIds.append(customerId)
            var customer = PerfCustomer(name: "Customer \(i)")
            customer.id = customerId
            customer.orders = orderReferences
            try context.insert(customer)
        }
        try await context.save()

        // Benchmark: related() To-Many lookups
        let (_, result) = try await benchmark("related() To-Many", count: customerCount) {
            for customerId in customerIds {
                let customer = try await context.model(for: customerId, as: PerfCustomer.self)!
                let _ = try await context.related(
                    customer,
                    PerfCustomer.fields.orders
                )
            }
        }

        print(result.description)

        #expect(result.durationMs < 5000, "To-Many lookup should complete in reasonable time")
    }

    @Test("joining() eager loading performance")
    func testJoiningEagerLoadingPerformance() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let customerId = uniqueID("C-join")
        let orderCount = 100

        // Setup: Create customer and orders
        var customer = PerfCustomer(name: "Join Customer")
        customer.id = customerId
        try context.insert(customer)
        let customerReference = try context.reference(to: customer)

        for i in 1...orderCount {
            var order = PerfOrder(total: Double(i * 10))
            order.id = uniqueID("O-join-\(i)")
            order.customer = customerReference
            try context.insert(order)
        }
        try await context.save()

        // Benchmark: fetch() with joining()
        let (snapshots, result) = try await benchmark("joining() eager load", count: orderCount) {
            try await context.fetch(PerfOrder.self)
                .joining(PerfOrder.fields.customer)
                .limit(orderCount)
                .execute()
        }

        print(result.description)

        // Verify snapshots have loaded relations
        var loadedCount = 0
        for snapshot in snapshots {
            if try snapshot.ref(PerfOrder.fields.customer) != nil {
                loadedCount += 1
            }
        }
        #expect(loadedCount > 0, "Should have loaded relationships")
    }

    @Test("get() with joining performance")
    func testGetWithJoiningPerformance() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let customerCount = 50
        let ordersPerCustomer = 5

        // Setup: Create customers with orders
        var customerReferences: [PersistableReference<PerfCustomer>] = []
        for i in 1...customerCount {
            var orderReferences: [PersistableReference<PerfOrder>] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-getj-\(i)-\(j)")
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                orderReferences.append(try context.reference(to: order))
                try context.insert(order)
            }

            let customerId = uniqueID("C-getj-\(i)")
            var customer = PerfCustomer(name: "Customer \(i)")
            customer.id = customerId
            customer.orders = orderReferences
            customerReferences.append(try context.reference(to: customer))
            try context.insert(customer)
        }
        try await context.save()

        // Benchmark: get() with To-Many joining
        let (_, result) = try await benchmark("get() with joining", count: customerCount) {
            for customerReference in customerReferences {
                let _ = try await context.get(
                    customerReference,
                    joining: PerfCustomer.fields.orders
                )
            }
        }

        print(result.description)

        #expect(result.durationMs < 10000, "get() with joining should complete in reasonable time")
    }

    // MARK: - Update Performance Tests

    @Test("Typed reference update performance")
    func testReferenceUpdatePerformance() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let customer1Id = uniqueID("C-upd1")
        let customer2Id = uniqueID("C-upd2")
        let orderCount = 50

        // Setup: Create two customers
        var customer1 = PerfCustomer(name: "Customer 1")
        customer1.id = customer1Id
        var customer2 = PerfCustomer(name: "Customer 2")
        customer2.id = customer2Id
        try context.insert(customer1)
        try context.insert(customer2)
        let customer1Reference = try context.reference(to: customer1)
        let customer2Reference = try context.reference(to: customer2)

        // Create orders pointing to customer1
        var orders: [PerfOrder] = []
        for i in 1...orderCount {
            var order = PerfOrder(total: Double(i * 10))
            order.id = uniqueID("O-upd-\(i)")
            order.customer = customer1Reference
            orders.append(order)
            try context.insert(order)
        }
        try await context.save()

        // Benchmark: Update references from customer1 to customer2
        let (_, result) = try await benchmark("Reference Update", count: orderCount) {
            for i in 0..<orderCount {
                orders[i].customer = customer2Reference
                try context.upsert(orders[i])
            }
            try await context.save()
        }

        print(result.description)

        #expect(result.throughputPerSecond > 50, "Reference update throughput should be reasonable")
    }

    @Test("To-Many reference array update performance")
    func testToManyReferenceArrayUpdatePerformance() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let customerId = uniqueID("C-tmupd")
        let orderCount = 100

        // Setup: Create customer with orders
        var customer = PerfCustomer(name: "To-Many Update Customer")
        customer.id = customerId

        var orderReferences: [PersistableReference<PerfOrder>] = []
        for i in 1...orderCount {
            let orderId = uniqueID("O-tmupd-\(i)")
            var order = PerfOrder(total: Double(i * 10))
            order.id = orderId
            orderReferences.append(try context.reference(to: order))
            try context.insert(order)
        }
        customer.orders = orderReferences
        try context.insert(customer)
        try await context.save()

        let updateCount = 20

        // Benchmark: Update reference array (remove/add items)
        let (_, result) = try await benchmark("To-Many Reference Update", count: updateCount) {
            for i in 0..<updateCount {
                // Remove first item, add a new one
                customer.orders.removeFirst()
                let newOrderId = uniqueID("O-new-\(i)")
                var newOrder = PerfOrder(total: Double((i + 1) * 100))
                newOrder.id = newOrderId
                try context.insert(newOrder)
                customer.orders.append(try context.reference(to: newOrder))
                try context.upsert(customer)
                try await context.save()
            }
        }

        print(result.description)

        #expect(result.durationMs < 10000, "To-Many reference update should complete in reasonable time")
    }

    // MARK: - Delete Performance Tests

    @Test("Delete with relationship catalog cleanup performance")
    func testDeleteWithRelationshipCatalogCleanupPerformance() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let customerId = uniqueID("C-del")
        let orderCount = 50

        // Setup: Create customer and orders
        var customer = PerfCustomer(name: "Delete Customer")
        customer.id = customerId
        try context.insert(customer)
        let customerReference = try context.reference(to: customer)

        var orderIds: [String] = []
        for i in 1...orderCount {
            let orderId = uniqueID("O-del-\(i)")
            orderIds.append(orderId)
            var order = PerfOrder(total: Double(i * 10))
            order.id = orderId
            order.customer = customerReference
            try context.insert(order)
        }
        try await context.save()

        // Benchmark: Delete owners while clearing canonical catalog entries
        let (_, result) = try await benchmark("Delete with catalog cleanup", count: orderCount) {
            for orderId in orderIds {
                if let order = try await context.model(for: orderId, as: PerfOrder.self) {
                    try context.delete(order)
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
        let context = container.benchmarkContext()

        let customerId = uniqueID("C-large")
        let orderCount = 200

        // Create many orders
        var orderReferences: [PersistableReference<PerfOrder>] = []
        for i in 1...orderCount {
            let orderId = uniqueID("O-large-\(i)")
            var order = PerfOrder(total: Double(i))
            order.id = orderId
            orderReferences.append(try context.reference(to: order))
            try context.insert(order)
        }
        try await context.save()

        // Benchmark: Create customer with a large reference array
        let (_, insertResult) = try await benchmark("Large array insert", count: 1) {
            var customer = PerfCustomer(name: "Large Array Customer")
            customer.id = customerId
            customer.orders = orderReferences
            try context.insert(customer)
            try await context.save()
        }

        print("Insert: \(insertResult.description)")

        // Benchmark: Load customer with a large reference array
        let (orders, loadResult) = try await benchmark("Large array load", count: orderCount) {
            let customer = try await context.model(for: customerId, as: PerfCustomer.self)!
            return try await context.related(
                customer,
                PerfCustomer.fields.orders
            )
        }

        print("Load: \(loadResult.description)")

        #expect(orders.count == orderCount, "Should load all related items")
    }

    @Test("Many relationships traversal")
    func testManyRelationshipsTraversal() async throws {
        let container = try await setupContainer()
        let context = container.benchmarkContext()

        let customerCount = 20
        let ordersPerCustomer = 10

        // Setup: Create customers with orders
        var customerReferences: [PersistableReference<PerfCustomer>] = []
        for i in 1...customerCount {
            let customerId = uniqueID("C-trav-\(i)")
            var customer = PerfCustomer(name: "Customer \(i)")
            customer.id = customerId
            let customerReference = try context.reference(to: customer)
            customerReferences.append(customerReference)

            var orderReferences: [PersistableReference<PerfOrder>] = []
            for j in 1...ordersPerCustomer {
                let orderId = uniqueID("O-trav-\(i)-\(j)")
                var order = PerfOrder(total: Double(j * 10))
                order.id = orderId
                order.customer = customerReference
                orderReferences.append(try context.reference(to: order))
                try context.insert(order)
            }
            customer.orders = orderReferences
            try context.insert(customer)
        }
        try await context.save()

        let totalTraversals = customerCount * 2  // Forward + reverse for each

        // Benchmark: Traverse direct references and the canonical inverse catalog
        let (_, result) = try await benchmark("Bidirectional traversal", count: totalTraversals) {
            for customerReference in customerReferences {
                // Forward: Customer -> Orders
                let customer = try await context.model(for: customerReference)!
                let orders = try await context.related(
                    customer,
                    PerfCustomer.fields.orders
                )
                #expect(orders.count == ordersPerCustomer)

                // Reverse: Customer <- Orders through the catalog
                let inverse = try await context.inverseRelationshipResolver().referencedBy(
                    customerReference,
                    from: PerfOrder.self,
                    via: PerfOrder.fields.customer,
                    limit: ordersPerCustomer
                )
                #expect(inverse.entities.count == ordersPerCustomer)
            }
        }

        print(result.description)

        #expect(result.durationMs < 15000, "Bidirectional traversal should complete in reasonable time")
    }
}
#endif
