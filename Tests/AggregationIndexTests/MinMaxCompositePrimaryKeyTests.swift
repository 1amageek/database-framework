#if FOUNDATION_DB
import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

@Suite("MIN/MAX Composite Primary Key Tests", .foundationDBScenario, .serialized, .heartbeat)
struct MinMaxCompositePrimaryKeyTests {

    init() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
    }

    // MARK: - Test Model with Composite Primary Key

    struct TenantOrderIdentifier:
        PersistableIdentifier,
        FieldValueEncodable,
        FieldValueDecodable,
        Hashable
    {
        let tenantId: String
        let orderId: String

        static let persistableIdentifierType: PersistableIdentifierType =
            .composite([.string, .string])
        static let fieldSchemaType: FieldSchemaType = .object

        var persistableIdentifierValue: ReferenceIdentifier {
            .composite([.string(tenantId), .string(orderId)])
        }

        func encodeFieldValue() throws(PersistableEncodingError) -> FieldValue {
            do {
                return .object(
                    try FieldObject([
                        (key: "tenantId", value: .string(tenantId)),
                        (key: "orderId", value: .string(orderId)),
                    ])
                )
            } catch {
                throw .invalidScalar(
                    type: "TenantOrderIdentifier",
                    reason: "Composite identifier fields must have unique names"
                )
            }
        }

        static func decodeFieldValue(
            _ value: FieldValue,
            field: String
        ) throws(PersistableDecodingError) -> TenantOrderIdentifier {
            guard case .object(let object) = value,
                  case .string(let tenantId) = object["tenantId"],
                  case .string(let orderId) = object["orderId"] else {
                throw .invalidValue(
                    field: field,
                    expected: "a tenant-order identifier object"
                )
            }
            return TenantOrderIdentifier(
                tenantId: tenantId,
                orderId: orderId
            )
        }
    }

    @Persistable
    struct MultiTenantOrder {
        var id: TenantOrderIdentifier
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
        MultiTenantOrder(
            id: TenantOrderIdentifier(
                tenantId: tenantId,
                orderId: orderId
            ),
            tenantId: tenantId,
            orderId: orderId,
            region: region,
            amount: amount
        )
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
            idExpression: FieldKeyExpression(fieldName: "id")
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
            let region = result.grouping[0].stringValue!
            minsByRegion[region] = (result.min, result.itemId)
        }

        // Verify MIN values
        #expect(minsByRegion["US"]?.value == 49.0)
        #expect(minsByRegion["EU"]?.value == 39.0)

        // Verify composite primary keys
        let usItemId = try Self.decodeIdentifier(minsByRegion["US"]!.itemId)
        #expect(usItemId.tenantId == "tenant1")
        #expect(usItemId.orderId == "o2")

        let euItemId = try Self.decodeIdentifier(minsByRegion["EU"]!.itemId)
        #expect(euItemId.tenantId == "tenant2")
        #expect(euItemId.orderId == "o2")
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
            idExpression: FieldKeyExpression(fieldName: "id")
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
            let region = result.grouping[0].stringValue!
            maxsByRegion[region] = (result.max, result.itemId)
        }

        // Verify MAX values
        #expect(maxsByRegion["US"]?.value == 999.0)
        #expect(maxsByRegion["EU"]?.value == 1299.0)

        // Verify composite primary keys
        let usItemId = try Self.decodeIdentifier(maxsByRegion["US"]!.itemId)
        #expect(usItemId.tenantId == "tenant1")
        #expect(usItemId.orderId == "o1")

        let euItemId = try Self.decodeIdentifier(maxsByRegion["EU"]!.itemId)
        #expect(euItemId.tenantId == "tenant2")
        #expect(euItemId.orderId == "o1")
    }

    private static func decodeIdentifier(
        _ tuple: Tuple
    ) throws -> TenantOrderIdentifier {
        let value = try PersistableIdentifierKeyCodec.value(
            from: tuple,
            expectedType: TenantOrderIdentifier.persistableIdentifierType
        )
        guard case .composite(let components) = value,
              components.count == 2,
              case .string(let tenantId) = components[0],
              case .string(let orderId) = components[1] else {
            throw PersistableDecodingError.invalidValue(
                field: "id",
                expected: "a tenant-order composite identifier"
            )
        }
        return TenantOrderIdentifier(
            tenantId: tenantId,
            orderId: orderId
        )
    }
}
#endif
