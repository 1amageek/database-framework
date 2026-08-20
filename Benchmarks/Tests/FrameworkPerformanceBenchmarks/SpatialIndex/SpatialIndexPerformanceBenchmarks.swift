#if FOUNDATION_DB
// SpatialIndexPerformanceBenchmarks.swift
// Performance benchmarks for SpatialIndex

import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
import StorageKit
import FDBStorage
@testable import DatabaseEngine
@testable import SpatialIndex

// MARK: - Test Model

@Persistable
struct BenchmarkLocation {
    var id: String
    var name: String
    var location: GeographicPoint

    var latitude: Double { location.latitude }
    var longitude: Double { location.longitude }

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

// MARK: - Spatial Benchmark Context

private struct BenchmarkContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: SpatialIndexMaintainer<BenchmarkLocation>
    let level: Int

    init(encoding: SpatialEncoding = .s2, level: Int = 12, indexName: String = "BenchmarkLocation_location") async throws {
        self.database = try await FoundationDBBenchmarkEnvironment.shared.makeEngine()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("benchmark", "spatial", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.level = level

        let index = try ResolvedIndex(
            for: BenchmarkLocation.self,
            name: indexName,
            definition: spatialIndexDefinition(
                fieldName: "location",
                fieldNumber: 3,
                encoding: encoding,
                level: level
            ),
            rootExpression: FieldKeyExpression(fieldName: "location"),
            itemTypes: Set(["BenchmarkLocation"])
        )

        self.maintainer = SpatialIndexMaintainer<BenchmarkLocation>(
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

// MARK: - Location Generation

/// Generate random location within a bounding box
private func randomLocation(
    id: String,
    minLat: Double = 35.5,
    maxLat: Double = 35.8,
    minLon: Double = 139.5,
    maxLon: Double = 139.9
) throws -> BenchmarkLocation {
    try BenchmarkLocation(
        id: id,
        name: "Location \(id)",
        latitude: Double.random(in: minLat...maxLat),
        longitude: Double.random(in: minLon...maxLon)
    )
}

/// Generate locations clustered around a center point
private func clusteredLocation(
    id: String,
    centerLat: Double,
    centerLon: Double,
    radiusKm: Double
) throws -> BenchmarkLocation {
    // Approximate: 1 degree latitude ≈ 111km
    let latOffset = Double.random(in: -radiusKm/111...radiusKm/111)
    // Approximate: 1 degree longitude ≈ 111km * cos(lat)
    let lonOffset = Double.random(in: -radiusKm/111...radiusKm/111) / cos(centerLat * .pi / 180)

    return try BenchmarkLocation(
        id: id,
        name: "Location \(id)",
        latitude: centerLat + latOffset,
        longitude: centerLon + lonOffset
    )
}

// MARK: - Performance Tests

@Suite("SpatialIndex Performance Tests", .serialized, .heartbeat)
struct SpatialIndexPerformanceBenchmarks {

    // MARK: - Setup

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Bulk Insert Performance

    @Test("Bulk insert performance - 100 locations")
    func testBulkInsert100Locations() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(level: 12)

        let locationCount = 100
        let locations = try (0..<locationCount).map { i in
            try randomLocation(id: "\(uniqueID("loc"))-\(i)")
        }

        let startTime = DispatchTime.now()

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let totalMs = Double(totalNs) / 1_000_000

        print("SpatialIndex Bulk Insert (100 locations):")
        print("  - Total time: \(String(format: "%.2f", totalMs))ms")
        print("  - Throughput: \(String(format: "%.0f", Double(locationCount) / (Double(totalNs) / 1_000_000_000)))/s")

        #expect(totalMs < 10000, "Bulk insert of \(locationCount) locations should complete in under 10s")

        try await ctx.cleanup()
    }

    @Test("Bulk insert performance - varying count")
    func testBulkInsertVaryingCount() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()

        for count in [50, 100, 200] {
            let ctx = try await BenchmarkContext(level: 12)

            let locations = try (0..<count).map { i in
                try randomLocation(id: "\(uniqueID("loc"))-\(i)")
            }

            let startTime = DispatchTime.now()

            try await ctx.database.withTransaction { transaction in
                for location in locations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let totalMs = Double(totalNs) / 1_000_000

            print("SpatialIndex Insert (\(count) locations): \(String(format: "%.2f", totalMs))ms")

            try await ctx.cleanup()
        }
    }

    // MARK: - Radius Search Performance

    @Test("Radius search performance - small radius")
    func testRadiusSearchSmallRadius() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        // Use coarse level to reduce cell count
        let ctx = try await BenchmarkContext(level: 8)

        // Setup: Insert locations around Tokyo
        let centerLat = 35.6812
        let centerLon = 139.7671
        let locationCount = 100

        let locations = try (0..<locationCount).map { i in
            try clusteredLocation(
                id: "\(uniqueID("loc"))-\(i)",
                centerLat: centerLat,
                centerLon: centerLon,
                radiusKm: 5.0
            )
        }

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Radius search
        let searchCount = 10
        let startTime = DispatchTime.now()

        for _ in 0..<searchCount {
            let results = try await ctx.searchRadius(
                lat: centerLat,
                lon: centerLon,
                radiusMeters: 1000  // 1km
            )
            _ = results.keys.count
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("SpatialIndex Radius Search (1km, level 8):")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        #expect(avgMs < 200, "Radius search should be under 200ms average")

        try await ctx.cleanup()
    }

    @Test("Radius search performance - varying radius")
    func testRadiusSearchVaryingRadius() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(level: 8)

        // Setup: Insert locations
        let centerLat = 35.6812
        let centerLon = 139.7671
        let locationCount = 100

        let locations = try (0..<locationCount).map { i in
            try clusteredLocation(
                id: "\(uniqueID("loc"))-\(i)",
                centerLat: centerLat,
                centerLon: centerLon,
                radiusKm: 10.0
            )
        }

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Test different radii
        for radiusMeters in [500.0, 1000.0, 2000.0, 5000.0] {
            let searchCount = 5
            let startTime = DispatchTime.now()

            for _ in 0..<searchCount {
                let results = try await ctx.searchRadius(
                    lat: centerLat,
                    lon: centerLon,
                    radiusMeters: radiusMeters
                )
                _ = results.keys.count
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

            print("SpatialIndex Radius Search (\(Int(radiusMeters))m): \(String(format: "%.2f", avgMs))ms")
        }

        try await ctx.cleanup()
    }

    // MARK: - Bounding Box Search Performance

    @Test("Bounding box search performance")
    func testBoundingBoxSearchPerformance() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(level: 8)

        // Setup: Insert locations in Tokyo area
        let locationCount = 100
        let locations = try (0..<locationCount).map { i in
            try randomLocation(
                id: "\(uniqueID("loc"))-\(i)",
                minLat: 35.6,
                maxLat: 35.8,
                minLon: 139.6,
                maxLon: 139.9
            )
        }

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Bounding box search
        let searchCount = 10
        let startTime = DispatchTime.now()

        for _ in 0..<searchCount {
            let results = try await ctx.searchBoundingBox(
                minLat: 35.65,
                minLon: 139.70,
                maxLat: 35.75,
                maxLon: 139.85
            )
            _ = results.keys.count
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

        print("SpatialIndex Bounding Box Search:")
        print("  - Total searches: \(searchCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        #expect(avgMs < 200, "Bounding box search should be under 200ms average")

        try await ctx.cleanup()
    }

    // MARK: - Level Comparison

    @Test("S2 level comparison")
    func testS2LevelComparison() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()

        let locationCount = 50
        let centerLat = 35.6812
        let centerLon = 139.7671

        for level in [6, 8, 10, 12] {
            let ctx = try await BenchmarkContext(level: level)

            let locations = try (0..<locationCount).map { i in
                try clusteredLocation(
                    id: "\(uniqueID("loc"))-\(i)",
                    centerLat: centerLat,
                    centerLon: centerLon,
                    radiusKm: 5.0
                )
            }

            try await ctx.database.withTransaction { transaction in
                for location in locations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }

            let searchCount = 5
            let startTime = DispatchTime.now()

            for _ in 0..<searchCount {
                let results = try await ctx.searchRadius(
                    lat: centerLat,
                    lon: centerLon,
                    radiusMeters: 2000
                )
                _ = results.keys.count
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

            print("SpatialIndex Level \(level): \(String(format: "%.2f", avgMs))ms avg")

            try await ctx.cleanup()
        }
    }

    // MARK: - Encoding Comparison

    @Test("S2 vs Morton encoding comparison")
    func testEncodingComparison() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()

        let locationCount = 50

        for encoding in [SpatialEncoding.s2, SpatialEncoding.morton] {
            let ctx = try await BenchmarkContext(encoding: encoding, level: 10)

            let locations = try (0..<locationCount).map { i in
                try randomLocation(id: "\(uniqueID("loc"))-\(i)")
            }

            let insertStartTime = DispatchTime.now()

            try await ctx.database.withTransaction { transaction in
                for location in locations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }

            let insertEndTime = DispatchTime.now()
            let insertMs = Double(insertEndTime.uptimeNanoseconds - insertStartTime.uptimeNanoseconds) / 1_000_000

            print("SpatialIndex \(encoding) Insert (\(locationCount)): \(String(format: "%.2f", insertMs))ms")

            try await ctx.cleanup()
        }
    }

    // MARK: - Update Performance

    @Test("Update performance")
    func testUpdatePerformance() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(level: 10)

        // Setup: Insert initial locations
        let locationCount = 50
        var locations = try (0..<locationCount).map { i in
            try randomLocation(id: "\(uniqueID("loc"))-\(i)")
        }

        let indexedLocations = locations
        try await ctx.database.withTransaction { transaction in
            for location in indexedLocations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Updates
        let updateCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<updateCount {
            let oldLocation = locations[i]
            let newLocation = try BenchmarkLocation(
                id: oldLocation.id,
                name: "Updated \(i)",
                latitude: oldLocation.latitude + 0.01,
                longitude: oldLocation.longitude + 0.01
            )

            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: oldLocation,
                    newItem: newLocation,
                    transaction: transaction
                )
            }

            locations[i] = newLocation
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(updateCount) / 1_000_000

        print("SpatialIndex Update Performance:")
        print("  - Total updates: \(updateCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        #expect(avgMs < 100, "Update should be under 100ms average")

        try await ctx.cleanup()
    }

    // MARK: - Delete Performance

    @Test("Delete performance")
    func testDeletePerformance() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()
        let ctx = try await BenchmarkContext(level: 10)

        // Setup: Insert locations
        let locationCount = 50
        let locations = try (0..<locationCount).map { i in
            try randomLocation(id: "\(uniqueID("loc"))-\(i)")
        }

        try await ctx.database.withTransaction { transaction in
            for location in locations {
                try await ctx.maintainer.updateIndex(
                    oldItem: nil,
                    newItem: location,
                    transaction: transaction
                )
            }
        }

        // Benchmark: Deletes
        let deleteCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<deleteCount {
            try await ctx.database.withTransaction { transaction in
                try await ctx.maintainer.updateIndex(
                    oldItem: locations[i],
                    newItem: nil,
                    transaction: transaction
                )
            }
        }

        let endTime = DispatchTime.now()
        let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let avgMs = Double(totalNs) / Double(deleteCount) / 1_000_000

        print("SpatialIndex Delete Performance:")
        print("  - Total deletes: \(deleteCount)")
        print("  - Average latency: \(String(format: "%.2f", avgMs))ms")

        #expect(avgMs < 50, "Delete should be under 50ms average")

        try await ctx.cleanup()
    }

    // MARK: - Scalability Test

    @Test("Search scalability - increasing location count")
    func testSearchScalability() async throws {
        try await FoundationDBBenchmarkEnvironment.shared.initialize()

        let centerLat = 35.6812
        let centerLon = 139.7671

        for count in [50, 100, 200] {
            let ctx = try await BenchmarkContext(level: 8)

            let locations = try (0..<count).map { i in
                try clusteredLocation(
                    id: "\(uniqueID("loc"))-\(i)",
                    centerLat: centerLat,
                    centerLon: centerLon,
                    radiusKm: 5.0
                )
            }

            try await ctx.database.withTransaction { transaction in
                for location in locations {
                    try await ctx.maintainer.updateIndex(
                        oldItem: nil,
                        newItem: location,
                        transaction: transaction
                    )
                }
            }

            let searchCount = 5
            let startTime = DispatchTime.now()

            for _ in 0..<searchCount {
                let results = try await ctx.searchRadius(
                    lat: centerLat,
                    lon: centerLon,
                    radiusMeters: 2000
                )
                _ = results.keys.count
            }

            let endTime = DispatchTime.now()
            let totalNs = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
            let avgMs = Double(totalNs) / Double(searchCount) / 1_000_000

            print("SpatialIndex Search (\(count) locations): \(String(format: "%.2f", avgMs))ms avg")

            try await ctx.cleanup()
        }
    }
}
#endif
