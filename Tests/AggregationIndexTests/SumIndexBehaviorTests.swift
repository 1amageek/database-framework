#if FOUNDATION_DB
// SumIndexBehaviorTests.swift
// Integration tests for SumIndex behavior with FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

@Persistable
struct RegionalSale {
    var id: String = UUID().uuidString
    var category: String
    var region: String
    var amount: Double
}

// MARK: - Sum Index Context

private struct SumIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: SumIndexMaintainer<RegionalSale, Double>

    init(indexName: String = "RegionalSale_category_amount") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "sum", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: category + amount (grouping + sum value)
        let index = Index(
            name: indexName,
            kind: numericAggregationIndexMetadata(
                .sum,
                groupingFields: [
                    FieldIdentity(name: "category", number: 2)
                ],
                valueField: FieldIdentity(name: "amount", number: 4),
                valueType: .float64
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["RegionalSale"])
        )

        self.maintainer = SumIndexMaintainer<RegionalSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func getSum(for category: String) async throws -> Double {
        guard let sum = try await getOptionalSum(for: category) else {
            throw AggregationIndexError.noData("No SUM value for category")
        }
        return sum
    }

    func getOptionalSum(for category: String) async throws -> Double? {
        try await database.withTransaction { transaction in
            try await maintainer.getSumAsDouble(
                groupingValues: [.string(category)],
                transaction: transaction
            )
        }
    }

    func getAllSums() async throws -> [(grouping: [FieldValue], sum: Double)] {
        try await database.withTransaction { transaction in
            try await maintainer.getAllSumsAsDouble(transaction: transaction)
        }
    }
}

// MARK: - Behavior Tests

@Suite("SumIndex Behavior Tests", .tags(.fdb), .foundationDBScenario, .serialized, .heartbeat)
struct SumIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds value to sum")
    func testInsertAddsValue() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sale = RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as RegionalSale?,
                newItem: sale,
                transaction: transaction
            )
        }

        let sum = try await ctx.getSum(for: "Electronics")
        #expect(abs(sum - 1000.0) < 0.01, "Sum should be 1000.0 after insert")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts to same group accumulate")
    func testMultipleInsertsAccumulate() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sales = [
            RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            RegionalSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 1500.0),
            RegionalSale(id: "sale3", category: "Electronics", region: "Kyoto", amount: 500.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RegionalSale?,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        let sum = try await ctx.getSum(for: "Electronics")
        #expect(abs(sum - 3000.0) < 0.01, "Sum should be 3000.0 (1000+1500+500)")

        try await ctx.cleanup()
    }

    @Test("Inserts to different groups are independent")
    func testDifferentGroupsIndependent() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sales = [
            RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            RegionalSale(id: "sale2", category: "Clothing", region: "Tokyo", amount: 500.0),
            RegionalSale(id: "sale3", category: "Electronics", region: "Osaka", amount: 1500.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RegionalSale?,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        let electronicsSum = try await ctx.getSum(for: "Electronics")
        let clothingSum = try await ctx.getSum(for: "Clothing")

        #expect(abs(electronicsSum - 2500.0) < 0.01, "Electronics sum should be 2500.0")
        #expect(abs(clothingSum - 500.0) < 0.01, "Clothing sum should be 500.0")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete subtracts value from sum")
    func testDeleteSubtractsValue() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sale = RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as RegionalSale?,
                newItem: sale,
                transaction: transaction
            )
        }

        let sumBefore = try await ctx.getSum(for: "Electronics")
        #expect(abs(sumBefore - 1000.0) < 0.01)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: sale,
                newItem: nil as RegionalSale?,
                transaction: transaction
            )
        }

        let sumAfter = try await ctx.getOptionalSum(for: "Electronics")
        #expect(sumAfter == nil, "SUM over an empty group must be nil")

        try await ctx.cleanup()
    }

    @Test("Delete partial from group")
    func testDeletePartialFromGroup() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sale1 = RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)
        let sale2 = RegionalSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 1500.0)

        // Insert both
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil as RegionalSale?, newItem: sale1, transaction: transaction)
            try await ctx.maintainer.updateIndex(oldItem: nil as RegionalSale?, newItem: sale2, transaction: transaction)
        }

        let sumBefore = try await ctx.getSum(for: "Electronics")
        #expect(abs(sumBefore - 2500.0) < 0.01)

        // Delete sale1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: sale1,
                newItem: nil as RegionalSale?,
                transaction: transaction
            )
        }

        let sumAfter = try await ctx.getSum(for: "Electronics")
        #expect(abs(sumAfter - 1500.0) < 0.01, "Sum should be 1500.0 after partial delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update same group adjusts sum")
    func testUpdateSameGroupAdjustsSum() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sale = RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as RegionalSale?,
                newItem: sale,
                transaction: transaction
            )
        }

        // Update amount (same category)
        let updatedSale = RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1500.0)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: sale,
                newItem: updatedSale,
                transaction: transaction
            )
        }

        let sum = try await ctx.getSum(for: "Electronics")
        #expect(abs(sum - 1500.0) < 0.01, "Sum should be updated to 1500.0")

        try await ctx.cleanup()
    }

    @Test("Update different group moves sum")
    func testUpdateDifferentGroupMovesSum() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sale = RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as RegionalSale?,
                newItem: sale,
                transaction: transaction
            )
        }

        let electronicsBefore = try await ctx.getSum(for: "Electronics")
        let clothingBefore = try await ctx.getOptionalSum(for: "Clothing")
        #expect(abs(electronicsBefore - 1000.0) < 0.01)
        #expect(clothingBefore == nil)

        // Update category from Electronics to Clothing
        let updatedSale = RegionalSale(id: "sale1", category: "Clothing", region: "Tokyo", amount: 1000.0)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: sale,
                newItem: updatedSale,
                transaction: transaction
            )
        }

        let electronicsAfter = try await ctx.getOptionalSum(for: "Electronics")
        let clothingAfter = try await ctx.getSum(for: "Clothing")
        #expect(electronicsAfter == nil, "Electronics sum should be absent")
        #expect(abs(clothingAfter - 1000.0) < 0.01, "Clothing sum should be 1000.0")

        try await ctx.cleanup()
    }

    // MARK: - Decimal Precision Tests

    @Test("Decimal values are handled correctly")
    func testDecimalPrecision() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sales = [
            RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 99.99),
            RegionalSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 149.50),
            RegionalSale(id: "sale3", category: "Electronics", region: "Kyoto", amount: 0.01)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RegionalSale?,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        let sum = try await ctx.getSum(for: "Electronics")
        // Expected: 99.99 + 149.50 + 0.01 = 249.50
        #expect(abs(sum - 249.50) < 0.01, "Sum should be 249.50")

        try await ctx.cleanup()
    }

    @Test("Negative values are supported")
    func testNegativeValues() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sales = [
            RegionalSale(id: "sale1", category: "Returns", region: "Tokyo", amount: -500.0),
            RegionalSale(id: "sale2", category: "Returns", region: "Osaka", amount: -300.0),
            RegionalSale(id: "sale3", category: "Returns", region: "Kyoto", amount: 100.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RegionalSale?,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        let sum = try await ctx.getSum(for: "Returns")
        // Expected: -500 + -300 + 100 = -700
        #expect(abs(sum - (-700.0)) < 0.01, "Sum should be -700.0")

        try await ctx.cleanup()
    }

    // MARK: - Query Tests

    @Test("GetAllSums returns all groups")
    func testGetAllSumsReturnsAllGroups() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sales = [
            RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            RegionalSale(id: "sale2", category: "Clothing", region: "Osaka", amount: 500.0),
            RegionalSale(id: "sale3", category: "Food", region: "Kyoto", amount: 200.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as RegionalSale?,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        let allSums = try await ctx.getAllSums()
        #expect(allSums.count == 3, "Should have 3 groups")

        let total = allSums.reduce(0.0) { $0 + $1.sum }
        #expect(abs(total - 1700.0) < 0.01, "Total sum should be 1700.0")

        try await ctx.cleanup()
    }

    @Test("GetSum for non-existent group returns nil")
    func testGetSumNonExistentReturnsNil() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sum = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.getSum(
                groupingValues: ["NonExistentCategory"],
                transaction: transaction
            )
        }
        #expect(sum == nil, "A non-existent SUM group has no value")

        try await ctx.cleanup()
    }

    // MARK: - Composite Grouping Tests

    @Test("Composite grouping with region and category")
    func testCompositeGrouping() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "sum", "composite", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("RegionalSale_region_category_amount")

        // Expression: region + category + amount
        let index = Index(
            name: "RegionalSale_region_category_amount",
            kind: numericAggregationIndexMetadata(
                .sum,
                groupingFields: [
                    FieldIdentity(name: "region", number: 3),
                    FieldIdentity(name: "category", number: 2),
                ],
                valueField: FieldIdentity(name: "amount", number: 4),
                valueType: .float64
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "RegionalSale_region_category_amount",
            itemTypes: Set(["RegionalSale"])
        )

        let maintainer = SumIndexMaintainer<RegionalSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let sales = [
            RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            RegionalSale(id: "sale2", category: "Electronics", region: "Tokyo", amount: 500.0),
            RegionalSale(id: "sale3", category: "Clothing", region: "Tokyo", amount: 300.0),
            RegionalSale(id: "sale4", category: "Electronics", region: "Osaka", amount: 800.0)
        ]

        try await database.withTransaction { transaction in
            for sale in sales {
                try await maintainer.updateIndex(
                    oldItem: nil as RegionalSale?,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        // Query Tokyo+Electronics
        let tokyoElectronics = try await database.withTransaction { transaction in
            try await maintainer.getSumAsDouble(
                groupingValues: ["Tokyo", "Electronics"],
                transaction: transaction
            )
        }

        // Query Tokyo+Clothing
        let tokyoClothing = try await database.withTransaction { transaction in
            try await maintainer.getSumAsDouble(
                groupingValues: ["Tokyo", "Clothing"],
                transaction: transaction
            )
        }

        // Query Osaka+Electronics
        let osakaElectronics = try await database.withTransaction { transaction in
            try await maintainer.getSumAsDouble(
                groupingValues: ["Osaka", "Electronics"],
                transaction: transaction
            )
        }

        #expect(tokyoElectronics == 1500.0, "Tokyo+Electronics should be 1500.0")
        #expect(tokyoClothing == 300.0, "Tokyo+Clothing should be 300.0")
        #expect(osakaElectronics == 800.0, "Osaka+Electronics should be 800.0")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to sum")
    func testScanItemAddsToSum() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SumIndexContext()

        let sales = [
            RegionalSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            RegionalSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 500.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.scanItem(
                    sale,
                    id: Tuple(sale.id),
                    transaction: transaction
                )
            }
        }

        let sum = try await ctx.getSum(for: "Electronics")
        #expect(abs(sum - 1500.0) < 0.01, "Sum should be 1500.0 after scanItem")

        try await ctx.cleanup()
    }
}
#endif
