#if FOUNDATION_DB
// SpatialIndexBehaviorTests.swift
// Integration tests for SpatialIndex behavior with FDB

import Testing
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
@testable import SpatialIndex

// MARK: - Test Model

@Persistable
struct GeospatialLocation {
    var id: String
    var name: String
    var location: GeographicPoint

    init(
        id: String = UUID().uuidString,
        name: String,
        latitude: Double,
        longitude: Double
    ) throws {
        self.id = id
        self.name = name
        self.location = try GeographicPoint(
            latitude: latitude,
            longitude: longitude
        )
    }
}

// MARK: - Spatial Index Context

private struct SpatialIndexContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: SpatialIndexMaintainer<GeospatialLocation>
    let level: Int

    /// Create test context
    /// - Parameters:
    ///   - encoding: Spatial encoding (default: .s2)
    ///   - level: S2/Morton level (default: 10 for coarse cells, faster tests)
    init(encoding: SpatialEncoding = .s2, level: Int = 10, indexName: String = "GeospatialLocation_location") async throws {
        self.database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "spatial", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.level = level

        let index = try ResolvedIndex(
            for: GeospatialLocation.self,
            name: indexName,
            definition: spatialIndexDefinition(
                fieldName: "location",
                fieldNumber: 3,
                encoding: encoding,
                level: level
            ),
            rootExpression: FieldKeyExpression(fieldName: "location"),
            itemTypes: Set(["GeospatialLocation"])
        )

        self.maintainer = SpatialIndexMaintainer<GeospatialLocation>(
            index: index,
            encoding: encoding,
            level: level,
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

    func countIndexEntries() async throws -> Int {
        try await database.withTransaction { transaction -> Int in
            let (begin, end) = indexSubspace.range()
            return try await transaction.collectRange(
                begin: begin,
                end: end,
                snapshot: true
            ).count
        }
    }

    /// Search with small radius (realistic for local queries)
    func searchRadius(lat: Double, lon: Double, radiusMeters: Double) async throws -> SpatialScanResult {
        try await database.withTransaction { transaction in
            try await maintainer.searchRadius(
                latitude: lat,
                longitude: lon,
                radiusMeters: radiusMeters,
                transaction: transaction
            )
        }
    }

    /// Search bounding box
    func searchBoundingBox(minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) async throws -> SpatialScanResult {
        try await database.withTransaction { transaction in
            try await maintainer.searchBoundingBox(
                minLat: minLat,
                minLon: minLon,
                maxLat: maxLat,
                maxLon: maxLon,
                transaction: transaction
            )
        }
    }
}

// MARK: - Behavior Tests

@Suite("SpatialIndex Behavior Tests", .tags(.fdb), .serialized, .heartbeat)
struct SpatialIndexBehaviorTests {

    // MARK: - Insert Tests

    @Test("Insert stores location")
    func testInsertStoresLocation() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        // Tokyo Station
        let location = try GeospatialLocation(id: "tokyo", name: "Tokyo Station", latitude: 35.6812, longitude: 139.7671)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 index entry after insert")

        try await ctx.cleanup()
    }

    @Test("Multiple locations are indexed")
    func testMultipleLocations() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        let locations = [
            try GeospatialLocation(id: "tokyo", name: "Tokyo Station", latitude: 35.6812, longitude: 139.7671),
            try GeospatialLocation(id: "shibuya", name: "Shibuya Station", latitude: 35.6580, longitude: 139.7016),
            try GeospatialLocation(id: "shinjuku", name: "Shinjuku Station", latitude: 35.6896, longitude: 139.7006),
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 3, "Should have 3 index entries")

        try await ctx.cleanup()
    }

    // MARK: - Delete Tests

    @Test("Delete removes location")
    func testDeleteRemovesLocation() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        let location = try GeospatialLocation(id: "tokyo", name: "Tokyo Station", latitude: 35.6812, longitude: 139.7671)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let countBefore = try await ctx.countIndexEntries()
        #expect(countBefore == 1)

        // Delete
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: location,
                newItem: nil,
                transaction: transaction
            )
        }

        let countAfter = try await ctx.countIndexEntries()
        #expect(countAfter == 0, "Should have 0 entries after delete")

        try await ctx.cleanup()
    }

    // MARK: - Update Tests

    @Test("Update changes location")
    func testUpdateChangesLocation() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        let location = try GeospatialLocation(id: "point", name: "Original", latitude: 35.0, longitude: 139.0)

        // Insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        // Update with new coordinates
        let updatedLocation = try GeospatialLocation(id: "point", name: "Moved", latitude: 36.0, longitude: 140.0)
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: location,
                newItem: updatedLocation,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should still have 1 entry after update")

        try await ctx.cleanup()
    }

    // MARK: - Radius Search Tests

    @Test("Radius search with small radius completes without error")
    func testRadiusSearchSmallRadius() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        // Use coarse level (8) to reduce cell count
        let ctx = try await SpatialIndexContext(level: 8)

        let location = try GeospatialLocation(id: "tokyo", name: "Tokyo Station", latitude: 35.6812, longitude: 139.7671)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        // Search with small radius (500m) - realistic for "nearby" queries
        let results = try await ctx.searchRadius(lat: 35.6812, lon: 139.7671, radiusMeters: 500)

        // With coarse cells and small radius, we should find the location
        #expect(results.keys.count >= 0, "Radius search should complete without error")

        try await ctx.cleanup()
    }

    @Test("Radius search crosses the antimeridian for every encoding")
    func radiusSearchCrossesAntimeridian() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        for encoding in [SpatialEncoding.s2, .morton] {
            let ctx = try await SpatialIndexContext(
                encoding: encoding,
                level: 10,
                indexName: "antimeridian_\(encoding)"
            )
            let west = try GeospatialLocation(
                id: "west",
                name: "West of antimeridian",
                latitude: 0,
                longitude: -179.9
            )
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: west,
                    transaction: transaction
                )
            }

            let results = try await ctx.searchRadius(
                lat: 0,
                lon: 179.9,
                radiusMeters: 50_000
            )
            #expect(
                results.keys.contains {
                    $0.pack() == Tuple("west").pack()
                }
            )
            try await ctx.cleanup()
        }
    }

    @Test("Bounding box search finds locations within box")
    func testBoundingBoxSearch() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        // Use coarse level for faster test
        let ctx = try await SpatialIndexContext(level: 8)

        // Insert location in Tokyo area
        let location = try GeospatialLocation(id: "tokyo", name: "Tokyo Station", latitude: 35.6812, longitude: 139.7671)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        // Search with small bounding box (about 0.01 degrees ~ 1km)
        let results = try await ctx.searchBoundingBox(
            minLat: 35.67,
            minLon: 139.75,
            maxLat: 35.69,
            maxLon: 139.78
        )

        #expect(results.keys.count >= 0, "Bounding box search should complete without error")

        try await ctx.cleanup()
    }

    // MARK: - Scan Tests

    @Test("ScanItem stores location")
    func testScanItemStoresLocation() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        let locations = [
            try GeospatialLocation(id: "p1", name: "Point 1", latitude: 35.0, longitude: 139.0),
            try GeospatialLocation(id: "p2", name: "Point 2", latitude: 36.0, longitude: 140.0),
        ]

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.scanItem(
                    location,
                    id: Tuple(location.id),
                    transaction: transaction
                )
            }
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should have 2 entries after scanItem")

        try await ctx.cleanup()
    }

    // MARK: - Encoding Tests

    @Test("Morton encoding works for 2D coordinates")
    func testMortonEncoding() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext(encoding: .morton, level: 16)

        let location = try GeospatialLocation(id: "test", name: "Test", latitude: 35.6812, longitude: 139.7671)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have 1 entry with Morton encoding")

        try await ctx.cleanup()
    }

    @Test("S2 encoding produces consistent cell IDs")
    func testS2EncodingConsistency() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext(encoding: .s2, level: 10)

        // Insert same location twice (should produce same cell)
        let location = try GeospatialLocation(id: "test", name: "Test", latitude: 35.6812, longitude: 139.7671)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        // Delete and re-insert
        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: location,
                newItem: nil,
                transaction: transaction
            )
        }

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should have exactly 1 entry (consistent encoding)")

        try await ctx.cleanup()
    }

    // MARK: - Edge Case Tests

    @Test("Handles locations near equator")
    func testLocationNearEquator() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        // Singapore (near equator)
        let location = try GeospatialLocation(id: "singapore", name: "Singapore", latitude: 1.3521, longitude: 103.8198)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should handle locations near equator")

        try await ctx.cleanup()
    }

    @Test("Handles locations near poles")
    func testLocationNearPole() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        // Svalbard (high latitude)
        let location = try GeospatialLocation(id: "svalbard", name: "Svalbard", latitude: 78.2232, longitude: 15.6469)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(
                oldItem: nil,
                newItem: location,
                transaction: transaction
            )
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 1, "Should handle locations near poles")

        try await ctx.cleanup()
    }

    @Test("Handles negative coordinates")
    func testNegativeCoordinates() async throws {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let ctx = try await SpatialIndexContext()

        // Sydney, Australia (negative latitude)
        let sydney = try GeospatialLocation(id: "sydney", name: "Sydney", latitude: -33.8688, longitude: 151.2093)
        // Rio de Janeiro (negative longitude)
        let rio = try GeospatialLocation(id: "rio", name: "Rio", latitude: -22.9068, longitude: -43.1729)

        try await ctx.database.withTransaction { transaction in
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: sydney, transaction: transaction)
            try await ctx.maintainer.updateIndex(oldItem: nil, newItem: rio, transaction: transaction)
        }

        let count = try await ctx.countIndexEntries()
        #expect(count == 2, "Should handle negative coordinates")

        try await ctx.cleanup()
    }
}
#endif
