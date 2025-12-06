// RelationshipIndexTests.swift
// DatabaseEngine Tests - @Relationship macro and index tests

import Testing
import Foundation
import Core
import Relationship
@testable import DatabaseEngine
@testable import ScalarIndex
@testable import RelationshipIndex
import FoundationDB
import TestSupport

// MARK: - Test Models with @Relationship

/// Customer model with To-Many relationship
@Persistable
struct RTestCustomer {
    #Directory<RTestCustomer>("test", "relationship", "customers")
    var name: String
    var tier: String = "standard"

    // To-Many FK field: orderIDs -> Order
    @Relationship(RTestOrder.self)
    var orderIDs: [String] = []
}

/// Order model with to-one relationship to Customer
/// New design: FK field is explicit, @Relationship marks it with related type
@Persistable
struct RTestOrder {
    #Directory<RTestOrder>("test", "relationship", "orders")
    var total: Double
    var status: String = "pending"

    // To-one FK field: customerID -> Customer
    @Relationship(RTestCustomer.self)
    var customerID: String? = nil
}

// MARK: - Test Helpers

/// Enable all indexes for a Persistable type to readable state
/// Skips if already enabled to avoid state transition errors
private func enableAllIndexes<T: Persistable>(container: FDBContainer, for type: T.Type) async throws {
    let store = try await container.store(for: type) as! FDBDataStore
    for descriptor in T.indexDescriptors {
        let currentState = try await store.indexStateManager.state(of: descriptor.name)
        if currentState == .disabled {
            try await store.indexStateManager.enable(descriptor.name)
            try await store.indexStateManager.makeReadable(descriptor.name)
        } else if currentState == .writeOnly {
            try await store.indexStateManager.makeReadable(descriptor.name)
        }
        // If already readable, skip
    }
}

// MARK: - Macro Generation Tests

@Suite("Relationship Macro Generation Tests")
struct RelationshipMacroGenerationTests {

    @Test("FK field is accessible as regular field")
    func testFKFieldAccess() throws {
        // RTestOrder should have customerID field in allFields
        #expect(RTestOrder.allFields.contains("customerID"))
    }

    @Test("FK field is accessible directly")
    func testFKFieldDirect() throws {
        var order = RTestOrder(total: 99.99)

        // Set FK value directly
        order.customerID = "C001"

        // Access directly
        #expect(order.customerID == "C001")
    }

    @Test("FK field is accessible via dynamicMember")
    func testFKFieldDynamicMember() throws {
        var order = RTestOrder(total: 99.99)

        // Set FK value directly
        order.customerID = "C001"

        // Access via dynamicMember subscript
        let fkValue = order[dynamicMember: "customerID"]
        #expect(fkValue as? String == "C001")
    }

    @Test("ScalarIndex is generated for relationship FK")
    func testRelationshipIndexGeneration() throws {
        // RTestOrder should have index named "RTestOrder_customer"
        // (derived from customerID -> customer)
        let indexNames = RTestOrder.indexDescriptors.map { $0.name }
        #expect(indexNames.contains("RTestOrder_customer"))

        // Find the index descriptor
        let relationshipIndex = RTestOrder.indexDescriptors.first { $0.name == "RTestOrder_customer" }
        #expect(relationshipIndex != nil)

        // Verify it's a ScalarIndex
        if let index = relationshipIndex {
            #expect(index.kindIdentifier == "scalar")
        }
    }

    @Test("RelationshipDescriptor is generated")
    func testRelationshipDescriptorGeneration() throws {
        // RTestOrder should have relationship descriptor for customerID
        let orderDescriptors = RTestOrder.relationshipDescriptors
        #expect(orderDescriptors.count == 1)

        let customerRel = orderDescriptors.first
        #expect(customerRel?.propertyName == "customerID")  // FK field name
        #expect(customerRel?.relatedTypeName == "RTestCustomer")
        #expect(customerRel?.isToMany == false)
        #expect(customerRel?.relationshipPropertyName == "customer")  // derived name
    }

    @Test("fieldName(for:) returns FK field name")
    func testFieldNameForFK() throws {
        let fieldName = RTestOrder.fieldName(for: \RTestOrder.customerID)
        #expect(fieldName == "customerID")
    }

    // MARK: - To-Many Tests

    @Test("To-Many FK field is accessible as regular field")
    func testToManyFKFieldAccess() throws {
        // RTestCustomer should have orderIDs field in allFields
        #expect(RTestCustomer.allFields.contains("orderIDs"))
    }

    @Test("To-Many FK field is accessible directly")
    func testToManyFKFieldDirect() throws {
        var customer = RTestCustomer(name: "Alice")

        // Set FK array value directly
        customer.orderIDs = ["O001", "O002", "O003"]

        // Access directly
        #expect(customer.orderIDs.count == 3)
        #expect(customer.orderIDs.contains("O001"))
        #expect(customer.orderIDs.contains("O002"))
        #expect(customer.orderIDs.contains("O003"))
    }

    @Test("ScalarIndex is generated for To-Many relationship FK")
    func testToManyRelationshipIndexGeneration() throws {
        // RTestCustomer should have index named "RTestCustomer_orders"
        // (derived from orderIDs -> orders)
        let indexNames = RTestCustomer.indexDescriptors.map { $0.name }
        #expect(indexNames.contains("RTestCustomer_orders"))

        // Find the index descriptor
        let relationshipIndex = RTestCustomer.indexDescriptors.first { $0.name == "RTestCustomer_orders" }
        #expect(relationshipIndex != nil)

        // Verify it's a ScalarIndex
        if let index = relationshipIndex {
            #expect(index.kindIdentifier == "scalar")
        }
    }

    @Test("To-Many RelationshipDescriptor is generated")
    func testToManyRelationshipDescriptorGeneration() throws {
        // RTestCustomer should have relationship descriptor for orderIDs
        let customerDescriptors = RTestCustomer.relationshipDescriptors
        #expect(customerDescriptors.count == 1)

        let ordersRel = customerDescriptors.first
        #expect(ordersRel?.propertyName == "orderIDs")  // FK field name
        #expect(ordersRel?.relatedTypeName == "RTestOrder")
        #expect(ordersRel?.isToMany == true)
        #expect(ordersRel?.relationshipPropertyName == "orders")  // derived name
    }

    @Test("fieldName(for:) returns To-Many FK field name")
    func testFieldNameForToManyFK() throws {
        let fieldName = RTestCustomer.fieldName(for: \RTestCustomer.orderIDs)
        #expect(fieldName == "orderIDs")
    }
}

// MARK: - Index Update Tests (requires FDB)

@Suite("Relationship Index Update Tests", .serialized)
struct RelationshipIndexUpdateTests {

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema([RTestCustomer.self, RTestOrder.self], version: Schema.Version(1, 0, 0))

        let container = FDBContainer(
            database: database,
            schema: schema
        )

        // Enable all indexes for testing
        try await enableAllIndexes(container: container, for: RTestOrder.self)
        try await enableAllIndexes(container: container, for: RTestCustomer.self)

        return container
    }

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("Relationship index is updated on save")
    func testRelationshipIndexUpdate() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-idx")
        let orderId = uniqueID("O-idx")

        // Create customer
        var customer = RTestCustomer(name: "Alice")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // Create order with relationship
        var order = RTestOrder(total: 99.99)
        order.id = orderId
        order.customerID = customerId
        context.insert(order)
        try await context.save()

        // Verify index entry exists
        let indexExists = try await verifyRelationshipIndexEntry(
            container: container,
            orderType: RTestOrder.self,
            indexName: "RTestOrder_customer",
            customerID: customerId,
            orderID: orderId
        )
        #expect(indexExists == true, "Relationship index entry should exist")
    }

    @Test("Relationship index is cleared on FK change")
    func testRelationshipIndexClearOnFKChange() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customer1Id = uniqueID("C-idx")
        let customer2Id = uniqueID("C-idx")
        let orderId = uniqueID("O-idx")

        // Create two customers
        var customer1 = RTestCustomer(name: "Alice")
        customer1.id = customer1Id
        var customer2 = RTestCustomer(name: "Bob")
        customer2.id = customer2Id
        context.insert(customer1)
        context.insert(customer2)
        try await context.save()

        // Create order with relationship to customer1
        var order = RTestOrder(total: 99.99)
        order.id = orderId
        order.customerID = customer1Id
        context.insert(order)
        try await context.save()

        // Change FK to customer2
        order.customerID = customer2Id
        context.insert(order)  // Re-insert to mark as modified
        try await context.save()

        // Verify old index entry is cleared
        let oldIndexExists = try await verifyRelationshipIndexEntry(
            container: container,
            orderType: RTestOrder.self,
            indexName: "RTestOrder_customer",
            customerID: customer1Id,
            orderID: orderId
        )
        #expect(oldIndexExists == false, "Old relationship index entry should be cleared")

        // Verify new index entry exists
        let newIndexExists = try await verifyRelationshipIndexEntry(
            container: container,
            orderType: RTestOrder.self,
            indexName: "RTestOrder_customer",
            customerID: customer2Id,
            orderID: orderId
        )
        #expect(newIndexExists == true, "New relationship index entry should exist")
    }

    @Test("Relationship index is cleared on delete")
    func testRelationshipIndexClearOnDelete() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-idx")
        let orderId = uniqueID("O-idx")

        // Create customer
        var customer = RTestCustomer(name: "Alice")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // Create order with relationship
        var order = RTestOrder(total: 99.99)
        order.id = orderId
        order.customerID = customerId
        context.insert(order)
        try await context.save()

        // Verify index exists before delete
        let beforeDelete = try await verifyRelationshipIndexEntry(
            container: container,
            orderType: RTestOrder.self,
            indexName: "RTestOrder_customer",
            customerID: customerId,
            orderID: orderId
        )
        #expect(beforeDelete == true, "Index should exist before delete")

        // Delete order
        context.delete(order)
        try await context.save()

        // Verify index entry is cleared
        let afterDelete = try await verifyRelationshipIndexEntry(
            container: container,
            orderType: RTestOrder.self,
            indexName: "RTestOrder_customer",
            customerID: customerId,
            orderID: orderId
        )
        #expect(afterDelete == false, "Relationship index entry should be cleared after delete")
    }

    // MARK: - Helper Functions

    /// Verify if a relationship index entry exists
    private func verifyRelationshipIndexEntry<T: Persistable>(
        container: FDBContainer,
        orderType: T.Type,
        indexName: String,
        customerID: String,
        orderID: String
    ) async throws -> Bool {
        var exists = false

        try await container.database.withTransaction { tx in
            let subspace = try await container.resolveDirectory(for: orderType)
            let indexSubspace = subspace.subspace(SubspaceKey.indexes)
            let relationshipIndexSubspace = indexSubspace.subspace(indexName)

            // Index key structure: [indexSubspace].pack(Tuple([customerID, orderID]))
            // FDBContext.buildIndexKey packs all values and id into a single Tuple
            let key = relationshipIndexSubspace.pack(Tuple([customerID, orderID]))

            if let _ = try await tx.getValue(for: key, snapshot: false) {
                exists = true
            }
        }

        return exists
    }
}

// MARK: - related() Query Tests

@Suite("Relationship Query Tests", .serialized)
struct RelationshipQueryTests {

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema([RTestCustomer.self, RTestOrder.self], version: Schema.Version(1, 0, 0))

        let container = FDBContainer(
            database: database,
            schema: schema
        )

        // Enable all indexes for testing
        try await enableAllIndexes(container: container, for: RTestOrder.self)
        try await enableAllIndexes(container: container, for: RTestCustomer.self)

        return container
    }

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("related() loads to-one related item")
    func testRelatedToOne() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-rel")
        let orderId = uniqueID("O-rel")

        // Create customer
        var customer = RTestCustomer(name: "Alice")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // Create order with relationship
        var order = RTestOrder(total: 99.99)
        order.id = orderId
        order.customerID = customerId
        context.insert(order)
        try await context.save()

        // Load order and get related customer using new API
        let loadedOrder = try await context.model(for: orderId, as: RTestOrder.self)
        #expect(loadedOrder != nil)

        // New API: related(item, \.fkField, as: RelatedType.self)
        let relatedCustomer = try await context.related(loadedOrder!, \.customerID, as: RTestCustomer.self)
        #expect(relatedCustomer != nil)
        #expect(relatedCustomer?.id == customerId)
        #expect(relatedCustomer?.name == "Alice")
    }

    @Test("related() returns nil for missing FK")
    func testRelatedToOneNilFK() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let orderId = uniqueID("O-rel-nil")

        // Create order WITHOUT customer FK
        var order = RTestOrder(total: 99.99)
        order.id = orderId
        // order.customerID is nil
        context.insert(order)
        try await context.save()

        // Load order and try to get related customer
        guard let loadedOrder = try await context.model(for: orderId, as: RTestOrder.self) else {
            Issue.record("Order should exist after insert")
            return
        }

        // New API: related(item, \.fkField, as: RelatedType.self)
        let relatedCustomer = try await context.related(loadedOrder, \.customerID, as: RTestCustomer.self)
        #expect(relatedCustomer == nil)
    }
}

// MARK: - Snapshot and get() Tests

@Suite("Snapshot Tests", .serialized)
struct SnapshotTests {

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema([RTestCustomer.self, RTestOrder.self], version: Schema.Version(1, 0, 0))

        let container = FDBContainer(
            database: database,
            schema: schema
        )

        // Enable all indexes for testing
        try await enableAllIndexes(container: container, for: RTestOrder.self)
        try await enableAllIndexes(container: container, for: RTestCustomer.self)

        return container
    }

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("get() returns Snapshot with item")
    func testGetReturnsSnapshot() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-snap")

        // Create customer
        var customer = RTestCustomer(name: "Alice")
        customer.id = customerId
        customer.tier = "gold"
        context.insert(customer)
        try await context.save()

        // Get by ID using new API
        let snapshot = try await context.get(RTestCustomer.self, id: customerId)
        #expect(snapshot != nil)

        // Access properties via dynamicMember
        #expect(snapshot?.id == customerId)
        #expect(snapshot?.name == "Alice")
        #expect(snapshot?.tier == "gold")
    }

    @Test("get() returns nil for missing item")
    func testGetReturnsNilForMissing() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        // Get non-existent item with unique ID
        let snapshot = try await context.get(RTestCustomer.self, id: uniqueID("nonexistent"))
        #expect(snapshot == nil)
    }

    @Test("get() with joining loads related item")
    func testGetWithJoining() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-snap")
        let orderId = uniqueID("O-snap")

        // Create customer
        var customer = RTestCustomer(name: "Bob")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // Create order with relationship
        var order = RTestOrder(total: 150.00)
        order.id = orderId
        order.customerID = customerId
        context.insert(order)
        try await context.save()

        // Get order with customer joined
        let snapshot = try await context.get(
            RTestOrder.self,
            id: orderId,
            joining: \.customerID,
            as: RTestCustomer.self
        )
        #expect(snapshot != nil)

        // Access order properties
        #expect(snapshot?.total == 150.00)

        // Access related customer via ref()
        let relatedCustomer = snapshot?.ref(RTestCustomer.self, \.customerID)
        #expect(relatedCustomer != nil)
        #expect(relatedCustomer?.id == customerId)
        #expect(relatedCustomer?.name == "Bob")
    }

    @Test("get() with joining returns nil relation for nil FK")
    func testGetWithJoiningNilFK() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let orderId = uniqueID("O-snap")

        // Create order WITHOUT customer FK
        var order = RTestOrder(total: 75.00)
        order.id = orderId
        // customerID is nil
        context.insert(order)
        try await context.save()

        // Get order with joining
        let snapshot = try await context.get(
            RTestOrder.self,
            id: orderId,
            joining: \.customerID,
            as: RTestCustomer.self
        )
        #expect(snapshot != nil)

        // Order properties are accessible
        #expect(snapshot?.total == 75.00)

        // Related customer should be nil
        let relatedCustomer = snapshot?.ref(RTestCustomer.self, \.customerID)
        #expect(relatedCustomer == nil)
    }

    @Test("Snapshot item property returns the underlying item")
    func testSnapshotItemProperty() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-snap")

        // Create customer
        var customer = RTestCustomer(name: "Charlie")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // Get snapshot
        let snapshot = try await context.get(RTestCustomer.self, id: customerId)
        #expect(snapshot != nil)

        // Access underlying item
        let item = snapshot?.item
        #expect(item?.id == customerId)
        #expect(item?.name == "Charlie")
    }

    @Test("execute() returns Snapshot array")
    func testExecuteReturnsSnapshots() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let id1 = uniqueID("C-fetch")
        let id2 = uniqueID("C-fetch")
        let id3 = uniqueID("C-fetch")

        // Create multiple customers
        var customer1 = RTestCustomer(name: "Alice")
        customer1.id = id1
        var customer2 = RTestCustomer(name: "Bob")
        customer2.id = id2
        var customer3 = RTestCustomer(name: "Charlie")
        customer3.id = id3

        context.insert(customer1)
        context.insert(customer2)
        context.insert(customer3)
        try await context.save()

        // Fetch all customers - check that our 3 are included
        let snapshots = try await context.fetch(RTestCustomer.self).execute()
        #expect(snapshots.count >= 3)

        // Verify our customers are in the results
        let ids = Set(snapshots.map { $0.id })
        #expect(ids.contains(id1))
        #expect(ids.contains(id2))
        #expect(ids.contains(id3))
    }

    // MARK: - To-Many Snapshot Tests

    @Test("get() with To-Many joining loads related items")
    func testGetWithToManyJoining() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-many")
        let orderId1 = uniqueID("O-many")
        let orderId2 = uniqueID("O-many")
        let orderId3 = uniqueID("O-many")

        // Create orders first
        var order1 = RTestOrder(total: 100.00)
        order1.id = orderId1
        var order2 = RTestOrder(total: 200.00)
        order2.id = orderId2
        var order3 = RTestOrder(total: 300.00)
        order3.id = orderId3
        context.insert(order1)
        context.insert(order2)
        context.insert(order3)
        try await context.save()

        // Create customer with order IDs
        var customer = RTestCustomer(name: "Alice")
        customer.id = customerId
        customer.orderIDs = [orderId1, orderId2, orderId3]
        context.insert(customer)
        try await context.save()

        // Get customer with orders joined
        let snapshot = try await context.get(
            RTestCustomer.self,
            id: customerId,
            joining: \.orderIDs,
            as: RTestOrder.self
        )
        #expect(snapshot != nil)

        // Access customer properties
        #expect(snapshot?.name == "Alice")
        #expect(snapshot?.orderIDs.count == 3)

        // Access related orders via refs()
        let orders = snapshot?.refs(RTestOrder.self, \.orderIDs) ?? []
        #expect(orders.count == 3)

        // Verify order data
        let totals = Set(orders.map { $0.total })
        #expect(totals.contains(100.00))
        #expect(totals.contains(200.00))
        #expect(totals.contains(300.00))
    }

    @Test("get() with empty FK array returns empty refs()")
    func testGetWithEmptyFKArray() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-many")

        // Create customer with empty orderIDs
        var customer = RTestCustomer(name: "Bob")
        customer.id = customerId
        // orderIDs is empty by default
        context.insert(customer)
        try await context.save()

        // Get customer with orders joined
        let snapshot = try await context.get(
            RTestCustomer.self,
            id: customerId,
            joining: \.orderIDs,
            as: RTestOrder.self
        )
        #expect(snapshot != nil)
        #expect(snapshot?.name == "Bob")

        // refs() should return empty array
        let orders = snapshot?.refs(RTestOrder.self, \.orderIDs) ?? []
        #expect(orders.isEmpty)
    }

    @Test("refs() returns empty array when not loaded")
    func testRefsReturnsEmptyWhenNotLoaded() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-many")
        let orderId = uniqueID("O-many")

        // Create orders
        var order1 = RTestOrder(total: 50.00)
        order1.id = orderId
        context.insert(order1)
        try await context.save()

        // Create customer with order ID
        var customer = RTestCustomer(name: "Charlie")
        customer.id = customerId
        customer.orderIDs = [orderId]
        context.insert(customer)
        try await context.save()

        // Get customer WITHOUT joining
        let snapshot = try await context.get(RTestCustomer.self, id: customerId)
        #expect(snapshot != nil)

        // refs() should return empty array (not loaded)
        let orders = snapshot?.refs(RTestOrder.self, \.orderIDs) ?? []
        #expect(orders.isEmpty)
    }

    @Test("refs() handles non-existent FK IDs gracefully")
    func testRefsHandlesNonExistentIDs() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-many")
        let orderId = uniqueID("O-many")
        let nonExistentId1 = uniqueID("O-nonexistent")
        let nonExistentId2 = uniqueID("O-nonexistent")

        // Create only one order
        var order1 = RTestOrder(total: 75.00)
        order1.id = orderId
        context.insert(order1)
        try await context.save()

        // Create customer with some non-existent order IDs
        var customer = RTestCustomer(name: "Diana")
        customer.id = customerId
        customer.orderIDs = [orderId, nonExistentId1, nonExistentId2]
        context.insert(customer)
        try await context.save()

        // Get customer with orders joined
        let snapshot = try await context.get(
            RTestCustomer.self,
            id: customerId,
            joining: \.orderIDs,
            as: RTestOrder.self
        )
        #expect(snapshot != nil)

        // refs() should only return existing orders
        let orders = snapshot?.refs(RTestOrder.self, \.orderIDs) ?? []
        #expect(orders.count == 1)
        #expect(orders.first?.id == orderId)
    }

    @Test("related() loads To-Many related items")
    func testRelatedToMany() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-many")
        let orderId1 = uniqueID("O-many")
        let orderId2 = uniqueID("O-many")

        // Create orders
        var order1 = RTestOrder(total: 10.00)
        order1.id = orderId1
        var order2 = RTestOrder(total: 20.00)
        order2.id = orderId2
        context.insert(order1)
        context.insert(order2)
        try await context.save()

        // Create customer with order IDs
        var customer = RTestCustomer(name: "Eve")
        customer.id = customerId
        customer.orderIDs = [orderId1, orderId2]
        context.insert(customer)
        try await context.save()

        // Load customer and get related orders using related() API
        let loadedCustomer = try await context.model(for: customerId, as: RTestCustomer.self)
        #expect(loadedCustomer != nil)

        let relatedOrders = try await context.related(loadedCustomer!, \.orderIDs, as: RTestOrder.self)
        #expect(relatedOrders.count == 2)

        let orderIDs = Set(relatedOrders.map { $0.id })
        #expect(orderIDs.contains(orderId1))
        #expect(orderIDs.contains(orderId2))
    }

    @Test("related() returns empty array for empty FK array")
    func testRelatedToManyEmptyArray() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-many")

        // Create customer with no orders
        var customer = RTestCustomer(name: "Frank")
        customer.id = customerId
        context.insert(customer)
        try await context.save()

        // Load customer
        let loadedCustomer = try await context.model(for: customerId, as: RTestCustomer.self)
        #expect(loadedCustomer != nil)

        // related() should return empty array
        let relatedOrders = try await context.related(loadedCustomer!, \.orderIDs, as: RTestOrder.self)
        #expect(relatedOrders.isEmpty)
    }
}

// MARK: - To-Many Index Update Tests

@Suite("To-Many Relationship Index Update Tests", .serialized)
struct ToManyRelationshipIndexUpdateTests {

    // MARK: - Helper Methods

    private func setupContainer() async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()

        let schema = Schema([RTestCustomer.self, RTestOrder.self], version: Schema.Version(1, 0, 0))

        let container = FDBContainer(
            database: database,
            schema: schema
        )

        // Enable all indexes for testing
        try await enableAllIndexes(container: container, for: RTestOrder.self)
        try await enableAllIndexes(container: container, for: RTestCustomer.self)

        return container
    }

    /// Generate unique test ID to avoid conflicts with parallel tests
    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    @Test("To-Many relationship index entries are created on save")
    func testToManyIndexCreation() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-tm-idx")
        let orderId1 = uniqueID("O-tm-idx")
        let orderId2 = uniqueID("O-tm-idx")

        // Create orders
        var order1 = RTestOrder(total: 100.00)
        order1.id = orderId1
        var order2 = RTestOrder(total: 200.00)
        order2.id = orderId2
        context.insert(order1)
        context.insert(order2)
        try await context.save()

        // Create customer with order IDs
        var customer = RTestCustomer(name: "Alice")
        customer.id = customerId
        customer.orderIDs = [orderId1, orderId2]
        context.insert(customer)
        try await context.save()

        // Verify index entries exist for each order ID
        let index1Exists = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId1,
            customerID: customerId
        )
        #expect(index1Exists == true, "Index entry for order 1 should exist")

        let index2Exists = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId2,
            customerID: customerId
        )
        #expect(index2Exists == true, "Index entry for order 2 should exist")
    }

    @Test("To-Many index entries are updated when FK array changes")
    func testToManyIndexUpdateOnChange() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-tm-idx")
        let orderId1 = uniqueID("O-tm-idx")
        let orderId2 = uniqueID("O-tm-idx")
        let orderId3 = uniqueID("O-tm-idx")

        // Create orders
        var order1 = RTestOrder(total: 100.00)
        order1.id = orderId1
        var order2 = RTestOrder(total: 200.00)
        order2.id = orderId2
        var order3 = RTestOrder(total: 300.00)
        order3.id = orderId3
        context.insert(order1)
        context.insert(order2)
        context.insert(order3)
        try await context.save()

        // Create customer with initial order IDs
        var customer = RTestCustomer(name: "Bob")
        customer.id = customerId
        customer.orderIDs = [orderId1, orderId2]
        context.insert(customer)
        try await context.save()

        // Verify initial index entries
        var idx1 = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId1,
            customerID: customerId
        )
        #expect(idx1 == true)

        // Update customer: remove order 1, add order 3
        customer.orderIDs = [orderId2, orderId3]
        context.insert(customer)
        try await context.save()

        // Verify old index entry is removed
        idx1 = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId1,
            customerID: customerId
        )
        #expect(idx1 == false, "Old index entry should be removed")

        // Verify new index entry exists
        let idx3 = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId3,
            customerID: customerId
        )
        #expect(idx3 == true, "New index entry should exist")

        // Verify unchanged entry still exists
        let idx2 = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId2,
            customerID: customerId
        )
        #expect(idx2 == true, "Unchanged index entry should still exist")
    }

    @Test("To-Many index entries are cleared on delete")
    func testToManyIndexClearOnDelete() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let customerId = uniqueID("C-tm-idx")
        let orderId = uniqueID("O-tm-idx")

        // Create orders
        var order1 = RTestOrder(total: 100.00)
        order1.id = orderId
        context.insert(order1)
        try await context.save()

        // Create customer with order ID
        var customer = RTestCustomer(name: "Charlie")
        customer.id = customerId
        customer.orderIDs = [orderId]
        context.insert(customer)
        try await context.save()

        // Verify index exists
        var indexExists = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId,
            customerID: customerId
        )
        #expect(indexExists == true)

        // Delete customer
        context.delete(customer)
        try await context.save()

        // Verify index is cleared
        indexExists = try await verifyToManyIndexEntry(
            container: container,
            indexName: "RTestCustomer_orders",
            orderID: orderId,
            customerID: customerId
        )
        #expect(indexExists == false, "Index entry should be cleared after delete")
    }

    // MARK: - Helper Functions

    private func verifyToManyIndexEntry(
        container: FDBContainer,
        indexName: String,
        orderID: String,
        customerID: String
    ) async throws -> Bool {
        var exists = false

        try await container.database.withTransaction { tx in
            let subspace = try await container.resolveDirectory(for: RTestCustomer.self)
            let indexSubspace = subspace.subspace(SubspaceKey.indexes)
            let relationshipIndexSubspace = indexSubspace.subspace(indexName)

            // For To-Many indexes, the key structure is: [indexSubspace].pack(Tuple([fkValue, itemID]))
            // where fkValue is each element from the array
            let key = relationshipIndexSubspace.pack(Tuple([orderID, customerID]))

            if let _ = try await tx.getValue(for: key, snapshot: false) {
                exists = true
            }
        }

        return exists
    }
}
