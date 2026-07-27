#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

@Suite("MIN/MAX Composite Primary Key Tests", .serialized, .heartbeat)
struct MinMaxCompositePrimaryKeyTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Test Model with Composite Primary Key

    @Persistable
    struct MultiTenantOrder {
        // Composite identity encoded as "tenantId:orderId".
        var id: String = ""
        var tenantId: String
        var orderId: String
        var region: String
        var amount: Double
    }

    private static func makeOrder(
        tenantId: String,
        orderId: String,
        region: String,
        amount: Double
    ) -> MultiTenantOrder {
        var order = MultiTenantOrder(
            tenantId: tenantId,
            orderId: orderId,
            region: region,
            amount: amount
        )
        order.id = "\(tenantId):\(orderId)"
        return order
    }

    // MARK: - Tests

    @Test("MIN with composite primary key")
    func testMinWithCompositePrimaryKey() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "min_composite_pk", testId).pack())

        let index = Index(
            name: "order_min_by_region",
            kind: numericAggregationIndexMetadata(
                .minimum,
                groupingFields: [
                    FieldIdentity(name: "region", number: 4)
                ],
                valueField: FieldIdentity(name: "amount", number: 5),
                valueType: .float64
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "order_min_by_region",
            itemTypes: Set(["MultiTenantOrder"])
        )

        let maintainer = MinIndexMaintainer<MultiTenantOrder, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "tenantId"),
                FieldKeyExpression(fieldName: "orderId")
            ])
        )

        let orders = [
            Self.makeOrder(tenantId: "tenant1", orderId: "o1", region: "US", amount: 999.0),
            Self.makeOrder(tenantId: "tenant1", orderId: "o2", region: "US", amount: 49.0),
            Self.makeOrder(tenantId: "tenant2", orderId: "o1", region: "EU", amount: 1299.0),
            Self.makeOrder(tenantId: "tenant2", orderId: "o2", region: "EU", amount: 39.0),
        ]

        try await database.withTransaction { transaction in
            for order in orders {
                try await maintainer.updateIndex(
                    oldItem: nil as MultiTenantOrder?,
                    newItem: order,
                    transaction: transaction
                )
            }
        }

        // Test getMin
        let usMin = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["US"], transaction: transaction)
        }
        #expect(usMin == 49.0, "US min should be 49.0")

        let euMin = try await database.withTransaction { transaction in
            try await maintainer.getMin(groupingValues: ["EU"], transaction: transaction)
        }
        #expect(euMin == 39.0, "EU min should be 39.0")

        // Test getAllMins with composite primary key
        let mins = try await database.withTransaction { transaction in
            try await maintainer.getAllMins(transaction: transaction)
        }

        #expect(mins.count == 2, "Should have 2 groups")

        var minsByRegion: [String: (value: Double, itemId: Tuple)] = [:]
        for result in mins {
            let region = result.grouping[0] as! String
            minsByRegion[region] = (result.min, result.itemId)
        }

        // Verify MIN values
        #expect(minsByRegion["US"]?.value == 49.0)
        #expect(minsByRegion["EU"]?.value == 39.0)

        // Verify composite primary keys
        let usItemId = minsByRegion["US"]!.itemId
        #expect(usItemId.count == 2, "Primary key should have 2 elements")
        #expect(usItemId[0] as? String == "tenant1")
        #expect(usItemId[1] as? String == "o2")

        let euItemId = minsByRegion["EU"]!.itemId
        #expect(euItemId.count == 2, "Primary key should have 2 elements")
        #expect(euItemId[0] as? String == "tenant2")
        #expect(euItemId[1] as? String == "o2")
    }

    @Test("MAX with composite primary key")
    func testMaxWithCompositePrimaryKey() async throws {
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString
        let indexSubspace = Subspace(prefix: Tuple("test", "max_composite_pk", testId).pack())

        let index = Index(
            name: "order_max_by_region",
            kind: numericAggregationIndexMetadata(
                .maximum,
                groupingFields: [
                    FieldIdentity(name: "region", number: 4)
                ],
                valueField: FieldIdentity(name: "amount", number: 5),
                valueType: .float64
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "order_max_by_region",
            itemTypes: Set(["MultiTenantOrder"])
        )

        let maintainer = MaxIndexMaintainer<MultiTenantOrder, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "tenantId"),
                FieldKeyExpression(fieldName: "orderId")
            ])
        )

        let orders = [
            Self.makeOrder(tenantId: "tenant1", orderId: "o1", region: "US", amount: 999.0),
            Self.makeOrder(tenantId: "tenant1", orderId: "o2", region: "US", amount: 49.0),
            Self.makeOrder(tenantId: "tenant2", orderId: "o1", region: "EU", amount: 1299.0),
            Self.makeOrder(tenantId: "tenant2", orderId: "o2", region: "EU", amount: 39.0),
        ]

        try await database.withTransaction { transaction in
            for order in orders {
                try await maintainer.updateIndex(
                    oldItem: nil as MultiTenantOrder?,
                    newItem: order,
                    transaction: transaction
                )
            }
        }

        // Test getMax
        let usMax = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["US"], transaction: transaction)
        }
        #expect(usMax == 999.0, "US max should be 999.0")

        let euMax = try await database.withTransaction { transaction in
            try await maintainer.getMax(groupingValues: ["EU"], transaction: transaction)
        }
        #expect(euMax == 1299.0, "EU max should be 1299.0")

        // Test getAllMaxs with composite primary key
        let maxs = try await database.withTransaction { transaction in
            try await maintainer.getAllMaxs(transaction: transaction)
        }

        #expect(maxs.count == 2, "Should have 2 groups")

        var maxsByRegion: [String: (value: Double, itemId: Tuple)] = [:]
        for result in maxs {
            let region = result.grouping[0] as! String
            maxsByRegion[region] = (result.max, result.itemId)
        }

        // Verify MAX values
        #expect(maxsByRegion["US"]?.value == 999.0)
        #expect(maxsByRegion["EU"]?.value == 1299.0)

        // Verify composite primary keys
        let usItemId = maxsByRegion["US"]!.itemId
        #expect(usItemId.count == 2, "Primary key should have 2 elements")
        #expect(usItemId[0] as? String == "tenant1")
        #expect(usItemId[1] as? String == "o1")

        let euItemId = maxsByRegion["EU"]!.itemId
        #expect(euItemId.count == 2, "Primary key should have 2 elements")
        #expect(euItemId[0] as? String == "tenant2")
        #expect(euItemId[1] as? String == "o1")
    }
}
#endif
