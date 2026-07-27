#if !os(WASI)
#if FOUNDATION_DB
import Testing
import TestHeartbeat
import DatabaseKit
import DatabaseTypes

@Persistable
struct RefCustomer {
    var id: String = ""
    var name: String

    @Relationship(deleteRule: .nullify)
    var orders: [PersistableReference<RefOrder>] = []
}

@Persistable
struct RefOrder {
    var id: String = ""
    var total: Double

    @Relationship(deleteRule: .nullify)
    var customer: PersistableReference<RefCustomer>? = nil
}

@Suite("Circular Reference Tests", .heartbeat)
struct CircularReferenceTests {
    @Test("Persistable references preserve circular entity identities")
    func persistableCircularReferences() throws {
        var customer = RefCustomer(name: "Alice")
        customer.id = "C001"
        var order = RefOrder(total: 99.99)
        order.id = "O001"

        let orderReference = try PersistableReference<RefOrder>(
            identity: EntityReference(
                entity: RefOrder.persistableType,
                id: .string(order.id)
            )
        )
        let customerReference = try PersistableReference<RefCustomer>(
            identity: EntityReference(
                entity: RefCustomer.persistableType,
                id: .string(customer.id)
            )
        )
        customer.orders = [orderReference]
        order.customer = customerReference

        #expect(customer.orders.first?.identity.id == .string(order.id))
        #expect(order.customer?.identity.id == .string(customer.id))
    }

    @Test("Circular relationship metadata names both entity targets")
    func circularRelationshipMetadata() throws {
        let customerRelationship = try #require(
            RefCustomer.relationshipDescriptors.first
        )
        let orderRelationship = try #require(
            RefOrder.relationshipDescriptors.first
        )

        #expect(customerRelationship.relatedTypeName == RefOrder.persistableType)
        #expect(orderRelationship.relatedTypeName == RefCustomer.persistableType)
    }
}
#endif

#endif
