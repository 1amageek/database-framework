// SpatialIndexPerformanceTests.swift
// Performance benchmarks for SpatialIndex

import Testing
import Foundation
import Core
import FoundationDB
import Spatial
import TestSupport
@testable import DatabaseEngine
@testable import SpatialIndex

// MARK: - Test Model

struct BenchmarkLocation: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var latitude: Double
    var longitude: Double

    init(id: String = UUID().uuidString, name: String, latitude: Double, longitude: Double) {
        self.id = id
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
    }

    static var persistableType: String { "BenchmarkLocation" }
    static var allFields: [String] { ["id", "name", "latitude", "longitude"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "latitude": return latitude
        case "longitude": return longitude
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<BenchmarkLocation, Value>) -> String {
        switch keyPath {
        case \BenchmarkLocation.id: return "id"
        case \BenchmarkLocation.name: return "name"
        case \BenchmarkLocation.latitude: return "latitude"
        case \BenchmarkLocation.longitude: return "longitude"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<BenchmarkLocation>) -> String {
        switch keyPath {
        case \BenchmarkLocation.id: return "id"
        case \BenchmarkLocation.name: return "name"
        case \BenchmarkLocation.latitude: return "latitude"
        case \BenchmarkLocation.longitude: return "longitude"
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

// MARK: - Test Helper

private struct BenchmarkContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: SpatialIndexMaintainer<BenchmarkLocation>
    let level: Int

    init(encoding: SpatialEncoding = .s2, level: Int = 12, indexName: String = "BenchmarkLocation_location") throws {
        self.database = try FDBClient.openDatabase()
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("benchmark", "spatial", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.level = level

        let kind = SpatialIndexKind<BenchmarkLocation>(
            latitude: \.latitude,
            longitude: \.longitude,
            encoding: encoding,
            level: level
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "latitude"),
                FieldKeyExpression(fieldName: "longitude")
            ]),
            subspaceKey: indexName,
            itemTypes: Set(["BenchmarkLocation"])
        )

        self.maintainer = SpatialIndexMaintainer<BenchmarkLocation>(
            index: index,
            encoding: kind.encoding,
            level: kind.level,
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
) -> BenchmarkLocation {
    BenchmarkLocation(
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
) -> BenchmarkLocation {
    // Approximate: 1 degree latitude ≈ 111km
    let latOffset = Double.random(in: -radiusKm/111...radiusKm/111)
    // Approximate: 1 degree longitude ≈ 111km * cos(lat)
    let lonOffset = Double.random(in: -radiusKm/111...radiusKm/111) / cos(centerLat * .pi / 180)

    return BenchmarkLocation(
        id: id,
        name: "Location \(id)",
        latitude: centerLat + latOffset,
        longitude: centerLon + lonOffset
    )
}

// MARK: - Performance Tests

@Suite("SpatialIndex Performance Tests", .serialized)
struct SpatialIndexPerformanceTests {

    // MARK: - Setup

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    // MARK: - Bulk Insert Performance

    @Test("Bulk insert performance - 100 locations")
    func testBulkInsert100Locations() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(level: 12)

        let locationCount = 100
        let locations = (0..<locationCount).map { i in
            randomLocation(id: "\(uniqueID("loc"))-\(i)")
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
        try await FDBTestSetup.shared.initialize()

        for count in [50, 100, 200] {
            let ctx = try BenchmarkContext(level: 12)

            let locations = (0..<count).map { i in
                randomLocation(id: "\(uniqueID("loc"))-\(i)")
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
        try await FDBTestSetup.shared.initialize()
        // Use coarse level to reduce cell count
        let ctx = try BenchmarkContext(level: 8)

        // Setup: Insert locations around Tokyo
        let centerLat = 35.6812
        let centerLon = 139.7671
        let locationCount = 100

        let locations = (0..<locationCount).map { i in
            clusteredLocation(
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(level: 8)

        // Setup: Insert locations
        let centerLat = 35.6812
        let centerLon = 139.7671
        let locationCount = 100

        let locations = (0..<locationCount).map { i in
            clusteredLocation(
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(level: 8)

        // Setup: Insert locations in Tokyo area
        let locationCount = 100
        let locations = (0..<locationCount).map { i in
            randomLocation(
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
        try await FDBTestSetup.shared.initialize()

        let locationCount = 50
        let centerLat = 35.6812
        let centerLon = 139.7671

        for level in [6, 8, 10, 12] {
            let ctx = try BenchmarkContext(level: level)

            let locations = (0..<locationCount).map { i in
                clusteredLocation(
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
        try await FDBTestSetup.shared.initialize()

        let locationCount = 50

        for encoding in [SpatialEncoding.s2, SpatialEncoding.morton] {
            let ctx = try BenchmarkContext(encoding: encoding, level: 10)

            let locations = (0..<locationCount).map { i in
                randomLocation(id: "\(uniqueID("loc"))-\(i)")
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(level: 10)

        // Setup: Insert initial locations
        let locationCount = 50
        var locations = (0..<locationCount).map { i in
            randomLocation(id: "\(uniqueID("loc"))-\(i)")
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

        // Benchmark: Updates
        let updateCount = 30
        let startTime = DispatchTime.now()

        for i in 0..<updateCount {
            let oldLocation = locations[i]
            let newLocation = BenchmarkLocation(
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
        try await FDBTestSetup.shared.initialize()
        let ctx = try BenchmarkContext(level: 10)

        // Setup: Insert locations
        let locationCount = 50
        let locations = (0..<locationCount).map { i in
            randomLocation(id: "\(uniqueID("loc"))-\(i)")
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
        try await FDBTestSetup.shared.initialize()

        let centerLat = 35.6812
        let centerLon = 139.7671

        for count in [50, 100, 200] {
            let ctx = try BenchmarkContext(level: 8)

            let locations = (0..<count).map { i in
                clusteredLocation(
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
