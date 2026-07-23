#if !os(WASI)
#if FOUNDATION_DB
// CircularReferenceTests.swift
// Test if @Reference macro allows circular type references

import Testing
import TestHeartbeat
import Foundation
import Core
import DatabaseValue

// MARK: - Test 1: Basic circular reference with @Reference macro

/// Test struct A referencing B
struct CircularReferenceNodeA {
    var id: String = "A001"

    @Reference(CircularReferenceNodeB.self)
    var bId: String?
}

/// Test struct B referencing A
struct CircularReferenceNodeB {
    var id: String = "B001"

    @Reference(CircularReferenceNodeA.self)
    var aId: String?
}

// MARK: - Test 2: With @Persistable

@Persistable
struct RefCustomer {
    var name: String

    // Reference to RefOrder - does this cause circular dependency?
    @Reference(RefOrder.self)
    var orderIDs: [String] = []
}

@Persistable
struct RefOrder {
    var total: Double

    // Reference back to RefCustomer
    @Reference(RefCustomer.self)
    var customerID: String? = nil
}

// MARK: - Tests

@Suite("Circular Reference Tests", .heartbeat)
struct CircularReferenceTests {

    @Test("Basic structs can reference each other via @Reference")
    func testBasicCircularReference() {
        var a = CircularReferenceNodeA()
        a.bId = "B001"

        var b = CircularReferenceNodeB()
        b.aId = "A001"

        #expect(a.bId == "B001")
        #expect(b.aId == "A001")
    }

    @Test("@Persistable structs can reference each other via @Reference")
    func testPersistableCircularReference() {
        var customer = RefCustomer(name: "Alice")
        customer.orderIDs = ["O001", "O002"]

        var order = RefOrder(total: 99.99)
        order.customerID = customer.id

        #expect(customer.orderIDs.count == 2)
        #expect(order.customerID == customer.id)
    }

    @Test("Types are accessible in static context")
    func testStaticTypeAccess() {
        // Can we access the referenced type at runtime?
        let customerType = RefCustomer.self
        let orderType = RefOrder.self

        #expect(String(describing: customerType) == "RefCustomer")
        #expect(String(describing: orderType) == "RefOrder")
    }
}
#endif

#endif
