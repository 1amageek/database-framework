import Testing
import Foundation
import Core
import DatabaseValue
import DatabaseEngine
import Geospatial
import StorageKit
@testable import SpatialIndex

private struct SpatialPlannerItem: Persistable {
    typealias ID = String

    var id: String
    var latitude: Double
    var longitude: Double

    static var persistableType: String { "SpatialPlannerItem" }
    static var allFields: [String] { ["id", "latitude", "longitude"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "latitude": return latitude
        case "longitude": return longitude
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<SpatialPlannerItem, Value>) -> String {
        switch keyPath {
        case \SpatialPlannerItem.id: return "id"
        case \SpatialPlannerItem.latitude: return "latitude"
        case \SpatialPlannerItem.longitude: return "longitude"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<SpatialPlannerItem>) -> String {
        switch keyPath {
        case \SpatialPlannerItem.id: return "id"
        case \SpatialPlannerItem.latitude: return "latitude"
        case \SpatialPlannerItem.longitude: return "longitude"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<SpatialPlannerItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

private struct SpatialPlannerIntIDItem: Persistable {
    typealias ID = Int64

    var id: Int64
    var location: GeoPoint

    static var persistableType: String { "SpatialPlannerIntIDItem" }
    static var allFields: [String] { ["id", "location"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "location": return location
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<SpatialPlannerIntIDItem, Value>) -> String {
        switch keyPath {
        case \SpatialPlannerIntIDItem.id: return "id"
        case \SpatialPlannerIntIDItem.location: return "location"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<SpatialPlannerIntIDItem>) -> String {
        switch keyPath {
        case \SpatialPlannerIntIDItem.id: return "id"
        case \SpatialPlannerIntIDItem.location: return "location"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<SpatialPlannerIntIDItem> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

@Suite("Spatial scan planning")
struct SpatialScanPlannerTests {
    @Test("S2 constraints produce covering cells")
    func s2ConstraintProducesCells() throws {
        let plan = try SpatialScanPlanner.plan(
            for: SpatialConstraint(
                type: .withinDistance(
                    center: (latitude: 35.6812, longitude: 139.7671),
                    radiusMeters: 1000
                )
            ),
            encoding: .s2,
            level: 12
        )

        guard case .cells(let cells) = plan else {
            Issue.record("Expected S2 cell scan plan")
            return
        }
        #expect(!cells.isEmpty)
    }

    @Test("Morton constraints produce code ranges")
    func mortonConstraintProducesCodeRange() throws {
        let plan = try SpatialScanPlanner.plan(
            for: SpatialConstraint(
                type: .withinBounds(
                    minLat: 35.0,
                    minLon: 139.0,
                    maxLat: 36.0,
                    maxLon: 140.0
                )
            ),
            encoding: .morton,
            level: 12
        )

        guard case .codeRange(let minCode, let maxCode) = plan else {
            Issue.record("Expected Morton code-range scan plan")
            return
        }
        #expect(minCode <= maxCode)
    }

    @Test("Morton write code matches scan planner coordinate contract")
    func mortonWriteCodeMatchesPlannerCoordinateContract() async throws {
        let kind = SpatialIndexKind<SpatialPlannerItem>(
            latitude: \.latitude,
            longitude: \.longitude,
            encoding: .morton,
            level: 12
        )
        let index = Index(
            name: "location",
            kind: kind,
            rootExpression: KeyExpressionFactory.from(
                keyPaths: ["latitude", "longitude"]
            )
        )
        let maintainer = SpatialIndexMaintainer<SpatialPlannerItem>(
            index: index,
            encoding: .morton,
            level: 12,
            subspace: Subspace("spatial"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
        let item = SpatialPlannerItem(id: "tokyo", latitude: 35.6812, longitude: 139.7671)
        let keys = try await maintainer.computeIndexKeys(for: item, id: Tuple("tokyo"))

        let keyTuple = try Subspace("spatial").unpack(keys[0])
        let storedElement = try #require(keyTuple[0])
        let storedCode = UInt64(bitPattern: try TypeConversion.int64(from: storedElement))
        let plannerCode = SpatialScanPlanner.mortonCode(
            latitude: item.latitude,
            longitude: item.longitude,
            level: 12
        )

        #expect(storedCode == plannerCode)
    }

    @Test("fetch limit derives bounded candidate scan budget")
    func fetchLimitDerivesCandidateScanBudget() {
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: nil) == nil)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 0) == nil)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 1) == 256)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 10) == 320)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 1_000) == 10_000)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 20_000) == 20_000)
    }

    @Test("range read limit tracks remaining candidate budget")
    func rangeReadLimitTracksRemainingCandidateBudget() {
        #expect(SpatialScanBudget.rangeReadLimit(totalLimit: nil, emittedCount: 10) == 0)
        #expect(SpatialScanBudget.rangeReadLimit(totalLimit: 100, emittedCount: 25) == 76)
        #expect(SpatialScanBudget.rangeReadLimit(totalLimit: 100, emittedCount: 100) == 1)
    }

    @Test("spatial primary-key extraction supports non-string IDs")
    func spatialPrimaryKeyExtractionSupportsNonStringIDs() throws {
        let item = SpatialPlannerIntIDItem(
            id: 42,
            location: GeoPoint(35.6812, 139.7671)
        )

        let primaryKey = try SpatialPrimaryKey.tuple(for: item)

        #expect(Data(primaryKey.pack()) == Data(Tuple(Int64(42)).pack()))
    }
}
