// SumIndexBehaviorTests.swift
// Integration tests for SumIndex behavior with FDB

import Testing
import Foundation
import FoundationDB
import Core
import TestSupport
@testable import DatabaseEngine
@testable import AggregationIndex

// MARK: - Test Model

struct SumTestSale: Persistable {
    typealias ID = String

    var id: String
    var category: String
    var region: String
    var amount: Double

    init(id: String = UUID().uuidString, category: String, region: String, amount: Double) {
        self.id = id
        self.category = category
        self.region = region
        self.amount = amount
    }

    static var persistableType: String { "SumTestSale" }
    static var allFields: [String] { ["id", "category", "region", "amount"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "category": return category
        case "region": return region
        case "amount": return amount
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<SumTestSale, Value>) -> String {
        switch keyPath {
        case \SumTestSale.id: return "id"
        case \SumTestSale.category: return "category"
        case \SumTestSale.region: return "region"
        case \SumTestSale.amount: return "amount"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<SumTestSale>) -> String {
        switch keyPath {
        case \SumTestSale.id: return "id"
        case \SumTestSale.category: return "category"
        case \SumTestSale.region: return "region"
        case \SumTestSale.amount: return "amount"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<SumTestSale> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct TestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: SumIndexMaintainer<SumTestSale, Double>

    init(indexName: String = "SumTestSale_category_amount") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "sum", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        // Expression: category + amount (grouping + sum value)
        let index = Index(
            name: indexName,
            kind: SumIndexKind<SumTestSale, Double>(groupBy: [\.category], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["SumTestSale"])
        )

        self.maintainer = SumIndexMaintainer<SumTestSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func getSum(for category: String) async throws -> Double {
        try await database.withTransaction { transaction in
            try await maintainer.getSum(
                groupingValues: [category],
                transaction: transaction
            )
        }
    }

    func getAllSums() async throws -> [(grouping: [any TupleElement], sum: Double)] {
        try await database.withTransaction { transaction in
            try await maintainer.getAllSums(transaction: transaction)
        }
    }
}

// MARK: - Behavior Tests

@Suite("SumIndex Behavior Tests", .tags(.fdb), .serialized)
struct SumIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds value to sum")
    func testInsertAddsValue() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sale = SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as SumTestSale?,
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sales = [
            SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            SumTestSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 1500.0),
            SumTestSale(id: "sale3", category: "Electronics", region: "Kyoto", amount: 500.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SumTestSale?,
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sales = [
            SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            SumTestSale(id: "sale2", category: "Clothing", region: "Tokyo", amount: 500.0),
            SumTestSale(id: "sale3", category: "Electronics", region: "Osaka", amount: 1500.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SumTestSale?,
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sale = SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as SumTestSale?,
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
                newItem: nil as SumTestSale?,
                transaction: transaction
            )
        }

        let sumAfter = try await ctx.getSum(for: "Electronics")
        #expect(abs(sumAfter) < 0.01, "Sum should be 0.0 after delete")

        try await ctx.cleanup()
    }

    @Test("Delete partial from group")
    func testDeletePartialFromGroup() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sale1 = SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)
        let sale2 = SumTestSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 1500.0)

        // Insert both
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil as SumTestSale?, newItem: sale1, transaction: transaction)
            try await ctx.maintainer.updateIndex(oldItem: nil as SumTestSale?, newItem: sale2, transaction: transaction)
        }

        let sumBefore = try await ctx.getSum(for: "Electronics")
        #expect(abs(sumBefore - 2500.0) < 0.01)

        // Delete sale1
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: sale1,
                newItem: nil as SumTestSale?,
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sale = SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as SumTestSale?,
                newItem: sale,
                transaction: transaction
            )
        }

        // Update amount (same category)
        let updatedSale = SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1500.0)
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sale = SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as SumTestSale?,
                newItem: sale,
                transaction: transaction
            )
        }

        let electronicsBefore = try await ctx.getSum(for: "Electronics")
        let clothingBefore = try await ctx.getSum(for: "Clothing")
        #expect(abs(electronicsBefore - 1000.0) < 0.01)
        #expect(abs(clothingBefore) < 0.01)

        // Update category from Electronics to Clothing
        let updatedSale = SumTestSale(id: "sale1", category: "Clothing", region: "Tokyo", amount: 1000.0)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: sale,
                newItem: updatedSale,
                transaction: transaction
            )
        }

        let electronicsAfter = try await ctx.getSum(for: "Electronics")
        let clothingAfter = try await ctx.getSum(for: "Clothing")
        #expect(abs(electronicsAfter) < 0.01, "Electronics sum should be 0.0")
        #expect(abs(clothingAfter - 1000.0) < 0.01, "Clothing sum should be 1000.0")

        try await ctx.cleanup()
    }

    // MARK: - Decimal Precision Tests

    @Test("Decimal values are handled correctly")
    func testDecimalPrecision() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sales = [
            SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 99.99),
            SumTestSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 149.50),
            SumTestSale(id: "sale3", category: "Electronics", region: "Kyoto", amount: 0.01)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SumTestSale?,
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sales = [
            SumTestSale(id: "sale1", category: "Returns", region: "Tokyo", amount: -500.0),
            SumTestSale(id: "sale2", category: "Returns", region: "Osaka", amount: -300.0),
            SumTestSale(id: "sale3", category: "Returns", region: "Kyoto", amount: 100.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SumTestSale?,
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sales = [
            SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            SumTestSale(id: "sale2", category: "Clothing", region: "Osaka", amount: 500.0),
            SumTestSale(id: "sale3", category: "Food", region: "Kyoto", amount: 200.0)
        ]

        try await ctx.database.withTransaction { transaction in
            for sale in sales {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as SumTestSale?,
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

    @Test("GetSum for non-existent group returns zero")
    func testGetSumNonExistentReturnsZero() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sum = try await ctx.getSum(for: "NonExistentCategory")
        #expect(abs(sum) < 0.01, "Sum for non-existent group should be 0.0")

        try await ctx.cleanup()
    }

    // MARK: - Composite Grouping Tests

    @Test("Composite grouping with region and category")
    func testCompositeGrouping() async throws {
        try await FDBTestSetup.shared.initialize()
        let database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "sum", "composite", String(testId)).pack())
        let indexSubspace = subspace.subspace("I").subspace("SumTestSale_region_category_amount")

        // Expression: region + category + amount
        let index = Index(
            name: "SumTestSale_region_category_amount",
            kind: SumIndexKind<SumTestSale, Double>(groupBy: [\.region, \.category], value: \.amount),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "region"),
                FieldKeyExpression(fieldName: "category"),
                FieldKeyExpression(fieldName: "amount")
            ]),
            subspaceKey: "SumTestSale_region_category_amount",
            itemTypes: Set(["SumTestSale"])
        )

        let maintainer = SumIndexMaintainer<SumTestSale, Double>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let sales = [
            SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            SumTestSale(id: "sale2", category: "Electronics", region: "Tokyo", amount: 500.0),
            SumTestSale(id: "sale3", category: "Clothing", region: "Tokyo", amount: 300.0),
            SumTestSale(id: "sale4", category: "Electronics", region: "Osaka", amount: 800.0)
        ]

        try await database.withTransaction { transaction in
            for sale in sales {
                try await maintainer.updateIndex(
                    oldItem: nil as SumTestSale?,
                    newItem: sale,
                    transaction: transaction
                )
            }
        }

        // Query Tokyo+Electronics
        let tokyoElectronics = try await database.withTransaction { transaction in
            try await maintainer.getSum(
                groupingValues: ["Tokyo", "Electronics"],
                transaction: transaction
            )
        }

        // Query Tokyo+Clothing
        let tokyoClothing = try await database.withTransaction { transaction in
            try await maintainer.getSum(
                groupingValues: ["Tokyo", "Clothing"],
                transaction: transaction
            )
        }

        // Query Osaka+Electronics
        let osakaElectronics = try await database.withTransaction { transaction in
            try await maintainer.getSum(
                groupingValues: ["Osaka", "Electronics"],
                transaction: transaction
            )
        }

        #expect(abs(tokyoElectronics - 1500.0) < 0.01, "Tokyo+Electronics should be 1500.0")
        #expect(abs(tokyoClothing - 300.0) < 0.01, "Tokyo+Clothing should be 300.0")
        #expect(abs(osakaElectronics - 800.0) < 0.01, "Osaka+Electronics should be 800.0")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - Scan Tests

    @Test("ScanItem adds to sum")
    func testScanItemAddsToSum() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try TestContext()

        let sales = [
            SumTestSale(id: "sale1", category: "Electronics", region: "Tokyo", amount: 1000.0),
            SumTestSale(id: "sale2", category: "Electronics", region: "Osaka", amount: 500.0)
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
