// BitmapIndexBehaviorTests.swift
// Comprehensive tests for BitmapIndex behavior with FDB

import Testing
import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
@testable import BitmapIndex
@testable import DatabaseEngine

@Persistable
private struct BitmapSecuredItem: SecurityPolicy {
    #Directory<BitmapSecuredItem>("bitmap-secured")
    #Index(
        .bitmap(
            name: "BitmapSecuredItem_status",
            field: \BitmapSecuredItem.status
        )
    )

    var id: String
    var ownerID: String
    var status: String

    static func permitsRead(
        of resource: borrowing BitmapSecuredItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.roles.contains("search") == true
    }

    static func permitsCreate(
        _ newResource: borrowing BitmapSecuredItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        newResource.ownerID == context.principal?.identifier
    }

    static func permitsUpdate(
        from resource: borrowing BitmapSecuredItem,
        to newResource: borrowing BitmapSecuredItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
            && newResource.ownerID == resource.ownerID
    }

    static func permitsDelete(
        _ resource: borrowing BitmapSecuredItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }
}

#if FOUNDATION_DB
import Foundation
import FDBStorage

// MARK: - Test Model

@Persistable
struct BitmapIndexedProduct {
    var id: String
    var category: String
    var brand: String
    var inStock: Bool

    init(id: String, category: String, brand: String) {
        self.init(id: id, category: category, brand: brand, inStock: true)
    }
}

// MARK: - Bitmap Index Context

private struct BitmapIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: BitmapIndexMaintainer<BitmapIndexedProduct>

    init(indexName: String = "BitmapIndexedProduct_category") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "bitmap", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)

        let index = try ResolvedIndex(
            for: BitmapIndexedProduct.self,
            name: indexName,
            definition: bitmapIndexDefinition(
                fieldName: "category",
                fieldNumber: 2
            ),
            rootExpression: FieldKeyExpression(fieldName: "category"),
            itemTypes: Set(["BitmapIndexedProduct"])
        )

        self.maintainer = BitmapIndexMaintainer<BitmapIndexedProduct>(
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

    func getBitmap(for value: String) async throws -> RoaringBitmap {
        try await database.withTransaction { transaction in
            try await maintainer.getBitmap(
                for: [.string(value)],
                transaction: transaction
            )
        }
    }

    func getCount(for value: String) async throws -> Int {
        try await database.withTransaction { transaction in
            try await maintainer.getCount(
                for: [.string(value)],
                transaction: transaction
            )
        }
    }

    func andQuery(values: [[FieldValue]]) async throws -> RoaringBitmap {
        try await database.withTransaction { transaction in
            try await maintainer.andQuery(values: values, transaction: transaction)
        }
    }

    func orQuery(values: [[FieldValue]]) async throws -> RoaringBitmap {
        try await database.withTransaction { transaction in
            try await maintainer.orQuery(values: values, transaction: transaction)
        }
    }

    func getPrimaryKeys(from bitmap: RoaringBitmap) async throws -> [Tuple] {
        try await database.withTransaction { transaction in
            try await maintainer.getPrimaryKeys(from: bitmap, transaction: transaction)
        }
    }

    func getAllDistinctValues() async throws -> [String] {
        try await database.withTransaction { transaction in
            let values = try await maintainer.getAllDistinctValues(transaction: transaction)
            return values.compactMap { value in
                guard let first = value.first,
                      case .string(let string) = first else {
                    return nil
                }
                return string
            }
        }
    }
}

#endif

@Suite("Bitmap retained resource lifecycle")
struct BitmapRetainedResourceLifecycleTests {
    @Test("Rejected admission releases ownership before meter reuse")
    func rejectedAdmissionReleasesOwnershipBeforeMeterReuse() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("bitmap-retained-resource").pack())
        let reader = BitmapIndexReader(subspace: subspace)
        let bitmap = RoaringBitmap([0, 1, 2].map(UInt32.init))
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                bitmap.serializedBytes(),
                for: subspace.subspace("data").pack(Tuple("active"))
            )
            for identifier in 0..<3 {
                try transaction.setValue(
                    Tuple("item-\(identifier)").pack(),
                    for: subspace.subspace("ids").pack(Tuple(identifier))
                )
            }
        }

        func read(using execution: ReadExecutionContext) async throws -> Int {
            try await engine.withTransaction { transaction in
                let retainedBitmap = try await reader.retainedBitmap(
                    for: ["active"],
                    transaction: transaction,
                    workMeter: execution.workMeter
                )
                let primaryKeys = try await reader.retainedPrimaryKeys(
                    for: retainedBitmap,
                    transaction: transaction,
                    workMeter: execution.workMeter
                )
                #expect(execution.workMeter.retainedIntermediateBytes > 0)
                return primaryKeys.count
            }
        }

        let measurement = ReadExecutionContext(
            monotonicClock: TestProcessMonotonicClock()
        )
        #expect(try await read(using: measurement) == 3)
        #expect(measurement.workMeter.retainedIntermediateRows == 0)
        #expect(measurement.workMeter.retainedIntermediateBytes == 0)
        let maximum = measurement.workMeter.peakIntermediateBytes
        #expect(maximum > 0)

        let constrained = ReadExecutionContext(
            options: ReadExecutionOptions(
                budget: ExecutionBudget(maximumIntermediateBytes: maximum)
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let blocker = try constrained.workMeter.reserveIntermediate(
            bytes: 1,
            at: .indexScan
        )
        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await read(using: constrained)
        }
        #expect(constrained.workMeter.retainedIntermediateBytes == 1)
        blocker.release()
        #expect(constrained.workMeter.retainedIntermediateBytes == 0)

        #expect(try await read(using: constrained) == 3)
        #expect(constrained.workMeter.retainedIntermediateRows == 0)
        #expect(constrained.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Direct bitmap aggregates preserve LIST authorization")
    func directBitmapAggregatesPreserveListAuthorization() async throws {
        let maintainerProvider = BitmapIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(BitmapSecuredItem.self)
        try BitmapReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(maintainerProvider)
        let container = try await DBContainer.open(
            for: try Schema(entities: [try BitmapSecuredItem.schemaEntity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "bitmap-direct-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: maintainerProvider)
                ],
                entityRuntimes: [entityRuntime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(BitmapSecuredItem.self)
                ]
            ),
            security: .enabled()
        )
        defer { await container.shutdown() }

        let owner = Principal(identifier: "owner", roles: ["search"])
        let reader = Principal(identifier: "reader", roles: ["search"])
        let blocked = Principal(identifier: "blocked")
        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal(owner.identifier),
            access: [.read, .write]
        )
        try await container.grantTestBaseAccess(
            to: .principal(reader.identifier),
            access: .read
        )
        try await container.grantTestBaseAccess(
            to: .principal(blocked.identifier),
            access: .read
        )
        #endif

        let ownerContext = container.testBaseContext(
            authorization: .authenticated(owner)
        )
        try ownerContext.insert(
            BitmapSecuredItem(
                id: "secured-item",
                ownerID: owner.identifier,
                status: "active"
            )
        )
        try await ownerContext.save()

        let blockedContext = container.testBaseContext(
            authorization: .authenticated(blocked)
        )
        for status in ["active", "absent"] {
            let query = blockedContext.bitmap(BitmapSecuredItem.self)
                .field(BitmapSecuredItem.fields.status)
                .equals(status)
            do {
                _ = try await query.count()
                Issue.record("Bitmap count must enforce LIST authorization")
            } catch let error as SecurityError {
                #expect(error.operation == .list)
            }
            do {
                _ = try await query.getBitmap()
                Issue.record("Bitmap retrieval must enforce LIST authorization")
            } catch let error as SecurityError {
                #expect(error.operation == .list)
            }
        }

        let readerContext = container.testBaseContext(
            authorization: .authenticated(reader)
        )
        let authorizedQuery = readerContext.bitmap(BitmapSecuredItem.self)
            .field(BitmapSecuredItem.fields.status)
            .equals("active")
        let count = try await authorizedQuery.count()
        #expect(count == 1)
        let bitmap = try await authorizedQuery.getBitmap()
        #expect(bitmap.cardinality == 1)
    }
}

// MARK: - RoaringBitmap Unit Tests

@Suite("RoaringBitmap Unit Tests", .heartbeat)
struct RoaringBitmapUnitTests {

    @Test("Add and contains single value")
    func testAddAndContains() {
        var bitmap = RoaringBitmap()
        bitmap.add(42)

        #expect(bitmap.contains(42), "Should contain 42")
        #expect(!bitmap.contains(43), "Should not contain 43")
        #expect(bitmap.cardinality == 1)
    }

    @Test("Add multiple values")
    func testAddMultipleValues() {
        var bitmap = RoaringBitmap()
        bitmap.add(1)
        bitmap.add(100)
        bitmap.add(1000)
        bitmap.add(10000)

        #expect(bitmap.cardinality == 4)
        #expect(bitmap.contains(1))
        #expect(bitmap.contains(100))
        #expect(bitmap.contains(1000))
        #expect(bitmap.contains(10000))
    }

    @Test("Remove value")
    func testRemove() {
        var bitmap = RoaringBitmap()
        bitmap.add(1)
        bitmap.add(2)
        bitmap.add(3)

        bitmap.remove(2)

        #expect(bitmap.cardinality == 2)
        #expect(bitmap.contains(1))
        #expect(!bitmap.contains(2))
        #expect(bitmap.contains(3))
    }

    @Test("Remove non-existent value is no-op")
    func testRemoveNonExistent() {
        var bitmap = RoaringBitmap()
        bitmap.add(1)

        bitmap.remove(999)

        #expect(bitmap.cardinality == 1)
        #expect(bitmap.contains(1))
    }

    @Test("AND operation")
    func testAndOperation() {
        var a = RoaringBitmap()
        a.add(1)
        a.add(2)
        a.add(3)

        var b = RoaringBitmap()
        b.add(2)
        b.add(3)
        b.add(4)

        let result = a && b

        #expect(result.cardinality == 2)
        #expect(!result.contains(1))
        #expect(result.contains(2))
        #expect(result.contains(3))
        #expect(!result.contains(4))
    }

    @Test("OR operation")
    func testOrOperation() {
        var a = RoaringBitmap()
        a.add(1)
        a.add(2)

        var b = RoaringBitmap()
        b.add(3)
        b.add(4)

        let result = a || b

        #expect(result.cardinality == 4)
        #expect(result.contains(1))
        #expect(result.contains(2))
        #expect(result.contains(3))
        #expect(result.contains(4))
    }

    @Test("Difference operation (ANDNOT)")
    func testDifferenceOperation() {
        var a = RoaringBitmap()
        a.add(1)
        a.add(2)
        a.add(3)

        var b = RoaringBitmap()
        b.add(2)
        b.add(3)
        b.add(4)

        let result = a - b

        #expect(result.cardinality == 1)
        #expect(result.contains(1))
        #expect(!result.contains(2))
        #expect(!result.contains(3))
    }

    @Test("Persisted representation round-trip")
    func persistedRepresentationRoundTrip() throws {
        var original = RoaringBitmap()
        for i in stride(from: 0, to: 1000, by: 7) {
            original.add(UInt32(i))
        }

        let bytes = try original.serializedBytes()
        let decoded = try RoaringBitmap(serializedBytes: bytes)

        #expect(decoded == original)
        #expect(decoded.cardinality == original.cardinality)
        #expect(Array(bytes.prefix(8)) == [0x52, 0x42, 0x4D, 1, 1, 0, 0, 0])
    }

    @Test("Persisted representation rejects truncated input")
    func persistedRepresentationRejectsTruncatedInput() throws {
        var original = RoaringBitmap()
        original.add(1)
        let bytes = try original.serializedBytes()

        #expect(throws: RoaringBitmapFormatError.self) {
            _ = try RoaringBitmap(serializedBytes: bytes[0..<(bytes.count - 1)])
        }
    }

    @Test("Persisted representation rejects trailing input")
    func persistedRepresentationRejectsTrailingInput() throws {
        var original = RoaringBitmap()
        original.add(1)
        let bytes = try original.serializedBytes().appending(0)

        #expect(throws: RoaringBitmapFormatError.trailingBytes(1)) {
            _ = try RoaringBitmap(serializedBytes: bytes)
        }
    }

    @Test("Persisted representation rejects an unknown format version")
    func persistedRepresentationRejectsUnknownFormatVersion() throws {
        var original = RoaringBitmap()
        original.add(1)
        let serialized = try original.serializedBytes()
        let bytes = ByteString.copying(count: serialized.count) { destination in
            serialized.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
            destination[3] = 2
        }

        #expect(throws: RoaringBitmapFormatError.unsupportedFormatVersion(2)) {
            _ = try RoaringBitmap(serializedBytes: bytes)
        }
    }

    @Test("Empty bitmap operations")
    func testEmptyBitmapOperations() {
        let empty = RoaringBitmap()

        #expect(empty.cardinality == 0)
        #expect(!empty.contains(0))
        #expect(!empty.contains(UInt32.max))

        var nonEmpty = RoaringBitmap()
        nonEmpty.add(1)

        // AND with empty
        let andResult = nonEmpty && empty
        #expect(andResult.cardinality == 0)

        // OR with empty
        let orResult = nonEmpty || empty
        #expect(orResult.cardinality == 1)
    }

    @Test("Values across multiple containers")
    func testMultipleContainers() {
        var bitmap = RoaringBitmap()

        // Container boundaries are at 65536 (2^16)
        bitmap.add(0)           // Container 0
        bitmap.add(65535)       // Container 0 (last)
        bitmap.add(65536)       // Container 1 (first)
        bitmap.add(131072)      // Container 2

        #expect(bitmap.cardinality == 4)
        #expect(bitmap.contains(0))
        #expect(bitmap.contains(65535))
        #expect(bitmap.contains(65536))
        #expect(bitmap.contains(131072))
    }

    @Test("Duplicate add is idempotent")
    func testDuplicateAdd() {
        var bitmap = RoaringBitmap()
        bitmap.add(42)
        bitmap.add(42)
        bitmap.add(42)

        #expect(bitmap.cardinality == 1)
    }

    @Test("Sequence initialization sorts and deduplicates every container")
    func sequenceInitializationSortsAndDeduplicates() {
        let values: [UInt32] = [
            131_072, 65_537, 2, 65_536, 1, 2, 131_072, 0,
        ]

        let bitmap = RoaringBitmap(values)

        #expect(bitmap.toArray() == [0, 1, 2, 65_536, 65_537, 131_072])
    }

    @Test("Equality compares logical values across container forms")
    func equalityComparesLogicalValuesAcrossContainerForms() throws {
        let bitmapBacked = RoaringBitmap((0..<5_000).map(UInt32.init))
        let runBacked = RoaringBitmap.range(0..<5_000)

        #expect(bitmapBacked == runBacked)
        #expect(runBacked == bitmapBacked)

        let roundTrip = try RoaringBitmap(
            serializedBytes: runBacked.serializedBytes()
        )
        #expect(roundTrip == bitmapBacked)

        let reference = RoaringBitmap((2_500..<7_500).map(UInt32.init))
        let union = bitmapBacked || reference
        let expectedUnion = RoaringBitmap.range(0..<7_500)
        #expect(union == expectedUnion)
    }

    @Test("Set operations match the reference algebra across containers")
    func setOperationsMatchReferenceAlgebra() {
        let leftValues = (0..<12_000).compactMap { index -> UInt32? in
            index.isMultiple(of: 2) || index.isMultiple(of: 11)
                ? UInt32(index * 17)
                : nil
        }
        let rightValues = (0..<13_000).compactMap { index -> UInt32? in
            index.isMultiple(of: 3) || index.isMultiple(of: 7)
                ? UInt32(index * 19)
                : nil
        }
        let leftReference = Set(leftValues)
        let rightReference = Set(rightValues)
        let left = RoaringBitmap(leftValues.reversed())
        let right = RoaringBitmap(rightValues + Array(rightValues.prefix(64)))

        #expect(
            (left && right).toArray()
                == leftReference.intersection(rightReference).sorted()
        )
        #expect(
            (left || right).toArray()
                == leftReference.union(rightReference).sorted()
        )
        #expect(
            (left - right).toArray()
                == leftReference.subtracting(rightReference).sorted()
        )
        #expect(
            (left ^ right).toArray()
                == leftReference.symmetricDifference(rightReference).sorted()
        )
    }

    @Test("Run containers preserve membership while splitting and merging")
    func runContainersSplitAndMerge() {
        var bitmap = RoaringBitmap.range(0..<65_536)

        bitmap.remove(0)
        bitmap.remove(32_768)
        bitmap.remove(65_535)

        #expect(bitmap.cardinality == 65_533)
        #expect(!bitmap.contains(0))
        #expect(!bitmap.contains(32_768))
        #expect(!bitmap.contains(65_535))
        #expect(bitmap.contains(1))
        #expect(bitmap.contains(32_767))
        #expect(bitmap.contains(32_769))
        #expect(bitmap.contains(65_534))

        bitmap.add(32_768)
        bitmap.add(0)
        bitmap.add(65_535)

        #expect(bitmap == RoaringBitmap.range(0..<65_536))
    }

    @Test("Complement respects an exclusive universe boundary")
    func complementRespectsUniverseBoundary() {
        let bitmap = RoaringBitmap([0, 2, 5, 9, 10] as [UInt32])

        #expect(bitmap.complement(universeSize: 10).toArray() == [1, 3, 4, 6, 7, 8])
    }
}

#if FOUNDATION_DB

// MARK: - BitmapIndexMaintainer Behavior Tests

@Suite("BitmapIndex Maintainer Behavior Tests", .tags(.fdb), .serialized, .heartbeat)
struct BitmapIndexMaintainerBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert adds to bitmap")
    func testInsertAddsToBitmap() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let product = BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony")

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as BitmapIndexedProduct?,
                newItem: product,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 1, "Should have 1 entry in electronics bitmap")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts with same category")
    func testMultipleInsertsWithSameCategory() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "electronics", brand: "Samsung"),
            BitmapIndexedProduct(id: "p3", category: "electronics", brand: "LG"),
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 3, "Should have 3 entries in electronics bitmap")

        try await ctx.cleanup()
    }

    @Test("Multiple inserts with different categories")
    func testMultipleInsertsWithDifferentCategories() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "clothing", brand: "Nike"),
            BitmapIndexedProduct(id: "p3", category: "books", brand: "Penguin"),
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let distinctValues = try await ctx.getAllDistinctValues()
        #expect(distinctValues.count == 3, "Should have 3 distinct categories")

        let electronicsCount = try await ctx.getCount(for: "electronics")
        let clothingCount = try await ctx.getCount(for: "clothing")
        let booksCount = try await ctx.getCount(for: "books")

        #expect(electronicsCount == 1)
        #expect(clothingCount == 1)
        #expect(booksCount == 1)

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes from bitmap")
    func testDeleteRemovesFromBitmap() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let product = BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as BitmapIndexedProduct?,
                newItem: product,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.getCount(for: "electronics")
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: product,
                newItem: nil as BitmapIndexedProduct?,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.getCount(for: "electronics")
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    @Test("Delete one of many maintains others")
    func testDeleteOneOfMany() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "electronics", brand: "Samsung"),
            BitmapIndexedProduct(id: "p3", category: "electronics", brand: "LG"),
        ]

        // Insert all
        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Delete one
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: products[1],  // Samsung
                newItem: nil as BitmapIndexedProduct?,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 2, "Should have 2 entries after deleting one")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update category changes bitmap membership")
    func testUpdateCategory() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let oldProduct = BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as BitmapIndexedProduct?,
                newItem: oldProduct,
                transaction: transaction
            )
        }

        let electronicsCountBefore = try await ctx.getCount(for: "electronics")
        #expect(electronicsCountBefore == 1)

        // Update category
        let newProduct = BitmapIndexedProduct(id: "p1", category: "appliances", brand: "Sony")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: oldProduct,
                newItem: newProduct,
                transaction: transaction
            )
        }

        let electronicsCountAfter = try await ctx.getCount(for: "electronics")
        let appliancesCount = try await ctx.getCount(for: "appliances")

        #expect(electronicsCountAfter == 0, "Electronics should be empty")
        #expect(appliancesCount == 1, "Appliances should have 1 entry")

        try await ctx.cleanup()
    }

    @Test("Update non-indexed field keeps bitmap membership")
    func testUpdateNonIndexedField() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let oldProduct = BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony")

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil as BitmapIndexedProduct?,
                newItem: oldProduct,
                transaction: transaction
            )
        }

        // Update brand (non-indexed field)
        let newProduct = BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Panasonic")
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: oldProduct,
                newItem: newProduct,
                transaction: transaction
            )
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 1, "Should still have 1 entry")

        try await ctx.cleanup()
    }

    // MARK: - AND Query Tests

    @Test("AND query returns intersection")
    func testAndQueryReturnsIntersection() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        // Create separate maintainers for category and brand
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        let subspace = Subspace(prefix: Tuple("test", "bitmap", String(testId)).pack())

        let categoryIndexSubspace = subspace.subspace("I").subspace("category_idx")
        let brandIndexSubspace = subspace.subspace("I").subspace("brand_idx")

        let categoryMaintainer = BitmapIndexMaintainer<BitmapIndexedProduct>(
            index: try ResolvedIndex(
                for: BitmapIndexedProduct.self,
                name: "category_idx",
                definition: bitmapIndexDefinition(
                    fieldName: "category",
                    fieldNumber: 2
                ),
                rootExpression: FieldKeyExpression(fieldName: "category"),
                itemTypes: Set(["BitmapIndexedProduct"])
            ),
            subspace: categoryIndexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let brandMaintainer = BitmapIndexMaintainer<BitmapIndexedProduct>(
            index: try ResolvedIndex(
                for: BitmapIndexedProduct.self,
                name: "brand_idx",
                definition: bitmapIndexDefinition(
                    fieldName: "brand",
                    fieldNumber: 3
                ),
                rootExpression: FieldKeyExpression(fieldName: "brand"),
                itemTypes: Set(["BitmapIndexedProduct"])
            ),
            subspace: brandIndexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id")
        )

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "electronics", brand: "Samsung"),
            BitmapIndexedProduct(id: "p3", category: "clothing", brand: "Sony"),
            BitmapIndexedProduct(id: "p4", category: "clothing", brand: "Nike"),
        ]

        // Insert into both indexes
        try await database.withTransaction { transaction in
            for product in products {
                try await categoryMaintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
                try await brandMaintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        // Get bitmaps and perform AND
        let electronicsBitmap = try await database.withTransaction { transaction in
            try await categoryMaintainer.getBitmap(
                for: [.string("electronics")],
                transaction: transaction
            )
        }
        let sonyBitmap = try await database.withTransaction { transaction in
            try await brandMaintainer.getBitmap(
                for: [.string("Sony")],
                transaction: transaction
            )
        }

        let intersection = electronicsBitmap && sonyBitmap
        #expect(intersection.cardinality == 1, "Only 1 product is both electronics AND Sony")

        // Cleanup
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    // MARK: - OR Query Tests

    @Test("OR query returns union")
    func testOrQueryReturnsUnion() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "clothing", brand: "Nike"),
            BitmapIndexedProduct(id: "p3", category: "books", brand: "Penguin"),
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let result = try await ctx.orQuery(values: [["electronics"], ["clothing"]])
        #expect(result.cardinality == 2, "Should have 2 products (electronics OR clothing)")

        try await ctx.cleanup()
    }

    // MARK: - GetAllDistinctValues Tests

    @Test("getAllDistinctValues returns all categories")
    func testGetAllDistinctValues() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "clothing", brand: "Nike"),
            BitmapIndexedProduct(id: "p3", category: "books", brand: "Penguin"),
            BitmapIndexedProduct(id: "p4", category: "electronics", brand: "Samsung"),
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let distinctValues = try await ctx.getAllDistinctValues()
        #expect(distinctValues.count == 3, "Should have 3 distinct categories")
        #expect(distinctValues.contains("electronics"))
        #expect(distinctValues.contains("clothing"))
        #expect(distinctValues.contains("books"))

        try await ctx.cleanup()
    }

    // MARK: - Primary Key Retrieval Tests

    @Test("getPrimaryKeys returns correct IDs")
    func testGetPrimaryKeysReturnsCorrectIds() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "product-001", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "product-002", category: "electronics", brand: "Samsung"),
            BitmapIndexedProduct(id: "product-003", category: "clothing", brand: "Nike"),
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let bitmap = try await ctx.getBitmap(for: "electronics")
        let primaryKeys = try await ctx.getPrimaryKeys(from: bitmap)

        #expect(primaryKeys.count == 2, "Should have 2 primary keys for electronics")

        let idStrings = primaryKeys.compactMap { $0[0] as? String }
        #expect(idStrings.contains("product-001"))
        #expect(idStrings.contains("product-002"))
        #expect(!idStrings.contains("product-003"))

        try await ctx.cleanup()
    }

    // MARK: - ScanItem Tests

    @Test("scanItem adds to bitmap")
    func testScanItemAddsToBitmap() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "electronics", brand: "Samsung"),
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.scanItem(
                    product,
                    id: Tuple(product.id),
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 2, "Should have 2 entries after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - computeIndexKeys Tests

    @Test("computeIndexKeys returns expected keys")
    func testComputeIndexKeys() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let product = BitmapIndexedProduct(id: "p1", category: "electronics", brand: "Sony")
        let keys = try await ctx.maintainer.computeIndexKeys(for: product, id: Tuple("p1"))

        // Should have keys for data subspace entry
        #expect(!keys.isEmpty, "Should have at least one index key")

        try await ctx.cleanup()
    }
}

// MARK: - Edge Cases Tests

@Suite("BitmapIndex Edge Cases", .tags(.fdb), .serialized, .heartbeat)
struct BitmapIndexEdgeCasesTests {

    @Test("Empty bitmap query returns empty")
    func testEmptyBitmapQuery() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let bitmap = try await ctx.getBitmap(for: "nonexistent")
        #expect(bitmap.cardinality == 0, "Non-existent category should return empty bitmap")

        try await ctx.cleanup()
    }

    @Test("Sequential ID management across transactions")
    func testSequentialIdManagement() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        // Insert products in separate transactions
        for i in 1...5 {
            let product = BitmapIndexedProduct(id: "p\(i)", category: "electronics", brand: "Brand\(i)")
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.getCount(for: "electronics")
        #expect(count == 5, "Should have 5 entries with sequential IDs")

        let bitmap = try await ctx.getBitmap(for: "electronics")
        let primaryKeys = try await ctx.getPrimaryKeys(from: bitmap)
        #expect(primaryKeys.count == 5, "Should retrieve all 5 primary keys")

        try await ctx.cleanup()
    }

    @Test("Special characters in field values")
    func testSpecialCharactersInValues() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await BitmapIndexContext()

        let products = [
            BitmapIndexedProduct(id: "p1", category: "electronics & gadgets", brand: "Sony"),
            BitmapIndexedProduct(id: "p2", category: "home/kitchen", brand: "KitchenAid"),
            BitmapIndexedProduct(id: "p3", category: "toys (kids)", brand: "LEGO"),
        ]

        try await ctx.database.withTransaction { transaction in
            for product in products {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil as BitmapIndexedProduct?,
                    newItem: product,
                    transaction: transaction
                )
            }
        }

        let distinctValues = try await ctx.getAllDistinctValues()
        #expect(distinctValues.count == 3)

        let count1 = try await ctx.getCount(for: "electronics & gadgets")
        let count2 = try await ctx.getCount(for: "home/kitchen")
        let count3 = try await ctx.getCount(for: "toys (kids)")

        #expect(count1 == 1)
        #expect(count2 == 1)
        #expect(count3 == 1)

        try await ctx.cleanup()
    }
}
#endif
