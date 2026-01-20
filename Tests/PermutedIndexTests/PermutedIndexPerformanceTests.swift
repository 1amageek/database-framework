// PermutedIndexPerformanceTests.swift
// Performance benchmarks for PermutedIndex operations

import Testing
import Foundation
import FoundationDB
import Core
import Permuted
import TestSupport
@testable import DatabaseEngine
@testable import PermutedIndex

// MARK: - Benchmark Context

private struct BenchmarkContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: PermutedIndexMaintainer<BenchmarkLocation>
    let permutation: Permutation

    init(permutation: Permutation? = nil) throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("bench", "permuted", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace("compound")

        // Default permutation: [1, 0, 2] - (city, country, name)
        let perm = permutation ?? (try! Permutation(indices: [1, 0, 2]))
        self.permutation = perm

        let kind = PermutedIndexKind<BenchmarkLocation>(
            fields: [\BenchmarkLocation.country, \BenchmarkLocation.city, \BenchmarkLocation.name],
            permutation: perm
        )

        let index = Index(
            name: "compound",
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "country"),
                FieldKeyExpression(fieldName: "city"),
                FieldKeyExpression(fieldName: "name")
            ]),
            subspaceKey: "compound",
            itemTypes: Set(["BenchmarkLocation"])
        )

        self.maintainer = PermutedIndexMaintainer<BenchmarkLocation>(
            index: index,
            permutation: perm,
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
}

// MARK: - Benchmark Model

private struct BenchmarkLocation: Persistable {
    typealias ID = String

    var id: String
    var country: String
    var city: String
    var name: String

    init(id: String = UUID().uuidString, country: String, city: String, name: String) {
        self.id = id
        self.country = country
        self.city = city
        self.name = name
    }

    static var persistableType: String { "BenchmarkLocation" }
    static var allFields: [String] { ["id", "country", "city", "name"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "country": return country
        case "city": return city
        case "name": return name
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<BenchmarkLocation, Value>) -> String {
        switch keyPath {
        case \BenchmarkLocation.id: return "id"
        case \BenchmarkLocation.country: return "country"
        case \BenchmarkLocation.city: return "city"
        case \BenchmarkLocation.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BenchmarkLocation>) -> String {
        switch keyPath {
        case \BenchmarkLocation.id: return "id"
        case \BenchmarkLocation.country: return "country"
        case \BenchmarkLocation.city: return "city"
        case \BenchmarkLocation.name: return "name"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<BenchmarkLocation> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Data

private let countries = ["Japan", "USA", "UK", "Germany", "France", "Italy", "Spain", "China", "Korea", "Brazil"]
private let cities = ["Tokyo", "New York", "London", "Berlin", "Paris", "Rome", "Madrid", "Shanghai", "Seoul", "Sao Paulo"]
private let names = ["Station A", "Station B", "Station C", "Station D", "Station E", "Station F", "Station G", "Station H"]

private func generateLocations(count: Int) -> [BenchmarkLocation] {
    (0..<count).map { i in
        BenchmarkLocation(
            country: countries[i % countries.count],
            city: cities[i % cities.count],
            name: names[i % names.count]
        )
    }
}

// MARK: - Benchmark Helper

private struct BenchmarkResult {
    let operation: String
    let count: Int
    let durationMs: Double
    let throughput: Double

    var description: String {
        String(format: "%@ (%d items): %.2f ms (%.0f ops/s)",
               operation, count, durationMs, throughput)
    }
}

private func measure<T>(_ operation: () async throws -> T) async throws -> (result: T, durationMs: Double) {
    let start = DispatchTime.now()
    let result = try await operation()
    let end = DispatchTime.now()
    let nanos = end.uptimeNanoseconds - start.uptimeNanoseconds
    return (result, Double(nanos) / 1_000_000)
}

// MARK: - Performance Tests

@Suite("PermutedIndex Performance Tests", .tags(.fdb, .performance), .serialized)
struct PermutedIndexPerformanceTests {

    // MARK: - Bulk Insert Tests

    @Test("Bulk insert performance - 100 locations")
    func testBulkInsert100() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let locations = generateLocations(count: 100)

        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for location in locations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(locations.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Bulk insert",
            count: locations.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        #expect(durationMs < 5000, "Bulk insert of 100 locations should complete within 5s")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - 1000 locations")
    func testBulkInsert1000() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let locations = generateLocations(count: 1000)

        let batchSize = 100
        let (_, durationMs) = try await measure {
            for batch in stride(from: 0, to: locations.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, locations.count)
                let batchLocations = Array(locations[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for location in batchLocations {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: location,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        let throughput = Double(locations.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Bulk insert",
            count: locations.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        #expect(durationMs < 30000, "Bulk insert of 1000 locations should complete within 30s")

        try await ctx.cleanup()
    }

    // MARK: - Prefix Query Tests

    @Test("Prefix query performance - single field")
    func testPrefixQuerySingleField() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 500 locations
        let locations = generateLocations(count: 500)
        let batchSize = 100
        for batch in stride(from: 0, to: locations.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, locations.count)
            let batchLocations = Array(locations[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for location in batchLocations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }
        }

        // Query by city prefix (first field in permuted order [1, 0, 2])
        let testCities = ["Tokyo", "New York", "London"]

        for city in testCities {
            let (results, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.scanByPrefix(
                        prefixValues: [city],
                        transaction: transaction
                    )
                }
            }

            print(String(format: "Prefix query (city=%@): %.2f ms, %d results",
                        city, durationMs, results.count))

            #expect(durationMs < 5000, "Prefix query should complete within 5s")
        }

        try await ctx.cleanup()
    }

    @Test("Prefix query performance - two fields")
    func testPrefixQueryTwoFields() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 500 locations
        let locations = generateLocations(count: 500)
        let batchSize = 100
        for batch in stride(from: 0, to: locations.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, locations.count)
            let batchLocations = Array(locations[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for location in batchLocations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }
        }

        // Query by city + country prefix (permuted order)
        let testPrefixes: [(city: String, country: String)] = [
            ("Tokyo", "Japan"),
            ("New York", "USA"),
            ("London", "UK")
        ]

        for (city, country) in testPrefixes {
            let (results, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.scanByPrefix(
                        prefixValues: [city, country],
                        transaction: transaction
                    )
                }
            }

            print(String(format: "Prefix query (city=%@, country=%@): %.2f ms, %d results",
                        city, country, durationMs, results.count))

            #expect(durationMs < 5000, "Prefix query should complete within 5s")
        }

        try await ctx.cleanup()
    }

    // MARK: - Exact Match Tests

    @Test("Exact match performance")
    func testExactMatchPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 500 locations
        let locations = generateLocations(count: 500)
        let batchSize = 100
        for batch in stride(from: 0, to: locations.count, by: batchSize) {
            let batchEnd = min(batch + batchSize, locations.count)
            let batchLocations = Array(locations[batch..<batchEnd])

            try await ctx.database.withTransaction { transaction in
                for location in batchLocations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }
        }

        // Exact match in permuted order: (city, country, name)
        let testMatches: [(city: String, country: String, name: String)] = [
            ("Tokyo", "Japan", "Station A"),
            ("New York", "USA", "Station B"),
            ("London", "UK", "Station C")
        ]

        for (city, country, name) in testMatches {
            let (results, durationMs) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.scanByExactMatch(
                        values: [city, country, name],
                        transaction: transaction
                    )
                }
            }

            print(String(format: "Exact match (city=%@, country=%@, name=%@): %.2f ms, %d results",
                        city, country, name, durationMs, results.count))

            #expect(durationMs < 5000, "Exact match should complete within 5s")
        }

        try await ctx.cleanup()
    }

    // MARK: - Scan All Tests

    @Test("Scan all performance")
    func testScanAllPerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let counts = [100, 500]

        for count in counts {
            // Clear and re-insert
            try await ctx.cleanup()
            let freshCtx = try BenchmarkContext()

            let locations = generateLocations(count: count)
            let batchSize = 100
            for batch in stride(from: 0, to: locations.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, locations.count)
                let batchLocations = Array(locations[batch..<batchEnd])

                try await freshCtx.database.withTransaction { transaction in
                    for location in batchLocations {
                        try await freshCtx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: location,
                            transaction: transaction
                        )
                    }
                }
            }

            let (results, durationMs) = try await measure {
                try await freshCtx.database.withTransaction { transaction in
                    try await freshCtx.maintainer.scanAll(transaction: transaction)
                }
            }

            print(String(format: "Scan all (%d locations): %.2f ms, %d results",
                        count, durationMs, results.count))

            #expect(results.count == count, "Should scan all \(count) entries")
            #expect(durationMs < 10000, "Scan all should complete within 10s")

            try await freshCtx.cleanup()
        }
    }

    // MARK: - Update Tests

    @Test("Update performance")
    func testUpdatePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 100 locations
        let locations = generateLocations(count: 100)

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Update city for all locations
        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for location in locations {
                    let updated = BenchmarkLocation(
                        id: location.id,
                        country: location.country,
                        city: "Updated\(location.city)",  // Changed city
                        name: location.name
                    )
                    try await ctx.maintainer.updateIndex(
                        oldItem: location,
                        newItem: updated,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(locations.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Update",
            count: locations.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        // Verify old entries are removed
        let oldResults = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.scanByPrefix(prefixValues: ["Tokyo"], transaction: transaction)
        }
        #expect(oldResults.isEmpty, "Old entries should be removed")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Insert 100 locations
        let locations = generateLocations(count: 100)

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Delete all
        let (_, durationMs) = try await measure {
            try await ctx.database.withTransaction { transaction in
                for location in locations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: location,
                        newItem: nil,
                        transaction: transaction
                    )
                }
            }
        }

        let throughput = Double(locations.count) / (durationMs / 1000)
        print(BenchmarkResult(
            operation: "Delete",
            count: locations.count,
            durationMs: durationMs,
            throughput: throughput
        ).description)

        // Verify all entries are removed
        let remaining = try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.scanAll(transaction: transaction)
        }
        #expect(remaining.isEmpty, "All entries should be deleted")

        try await ctx.cleanup()
    }

    // MARK: - Permutation Comparison Tests

    @Test("Different permutations comparison")
    func testDifferentPermutations() async throws {
        try await FDBTestSetup.shared.initialize()

        let permutations = [
            try! Permutation(indices: [0, 1, 2]),  // Identity
            try! Permutation(indices: [1, 0, 2]),  // Swap first two
            try! Permutation(indices: [2, 1, 0]),  // Reverse
        ]

        for perm in permutations {
            let ctx = try BenchmarkContext(permutation: perm)
            let locations = generateLocations(count: 100)

            let (_, insertDuration) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    for location in locations {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: location,
                            transaction: transaction
                        )
                    }
                }
            }

            // Query by first field in permuted order
            let firstFieldValue: String
            switch perm.indices.first {
            case 0: firstFieldValue = "Japan"  // country
            case 1: firstFieldValue = "Tokyo"  // city
            case 2: firstFieldValue = "Station A"  // name
            default: firstFieldValue = "Japan"
            }

            let (results, queryDuration) = try await measure {
                try await ctx.database.withTransaction { transaction in
                    try await ctx.maintainer.scanByPrefix(
                        prefixValues: [firstFieldValue],
                        transaction: transaction
                    )
                }
            }

            print(String(format: "Permutation %@: insert %.2f ms, query %.2f ms (%d results)",
                        perm.description, insertDuration, queryDuration, results.count))

            try await ctx.cleanup()
        }
    }

    // MARK: - Inverse Permutation Tests

    @Test("Inverse permutation performance")
    func testInversePermutation() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        // Test inverse conversion
        let permutedValues: [any TupleElement] = ["Tokyo", "Japan", "Station A"]

        let (originalValues, durationMs) = try await measure {
            try ctx.maintainer.toOriginalOrder(permutedValues)
        }

        print(String(format: "Inverse permutation: %.4f ms", durationMs))

        // Verify conversion
        #expect(originalValues.count == 3)
        #expect((originalValues[0] as? String) == "Japan", "First should be country")
        #expect((originalValues[1] as? String) == "Tokyo", "Second should be city")
        #expect((originalValues[2] as? String) == "Station A", "Third should be name")

        try await ctx.cleanup()
    }

    // MARK: - Scale Tests

    @Test("Scale test - 2000 locations")
    func testScale2000() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext()

        let locationCount = 2000
        let locations = generateLocations(count: locationCount)

        // Insert
        let batchSize = 100
        let (_, insertDuration) = try await measure {
            for batch in stride(from: 0, to: locations.count, by: batchSize) {
                let batchEnd = min(batch + batchSize, locations.count)
                let batchLocations = Array(locations[batch..<batchEnd])

                try await ctx.database.withTransaction { transaction in
                    for location in batchLocations {
                        try await ctx.maintainer.updateIndex(
                            oldItem: nil,
                            newItem: location,
                            transaction: transaction
                        )
                    }
                }
            }
        }

        print(String(format: "Insert %d locations: %.2f ms (%.0f ops/s)",
                    locationCount, insertDuration, Double(locationCount) / (insertDuration / 1000)))

        // Prefix query
        let (prefixResults, prefixDuration) = try await measure {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.scanByPrefix(prefixValues: ["Tokyo"], transaction: transaction)
            }
        }

        print(String(format: "Prefix query (%d locations): %.2f ms, %d results",
                    locationCount, prefixDuration, prefixResults.count))

        // Scan all
        let (scanResults, scanDuration) = try await measure {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.scanAll(transaction: transaction)
            }
        }

        print(String(format: "Scan all (%d locations): %.2f ms",
                    locationCount, scanDuration))

        #expect(scanResults.count == locationCount, "Should scan all locations")

        try await ctx.cleanup()
    }
}
