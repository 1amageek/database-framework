import DatabaseTypes
import Testing
@testable import SpatialIndex

@Suite("Canonical spatial values")
struct CanonicalSpatialValueTests {
    @Test("Bounding boxes reject invalid coordinates instead of clamping")
    func rejectsInvalidCoordinates() {
        #expect(throws: BoundingBoxError.self) {
            _ = try BoundingBox(
                minLatitude: -91,
                minLongitude: 139,
                maxLatitude: 36,
                maxLongitude: 140
            )
        }
    }

    @Test("Bounding boxes reject inverted ranges")
    func rejectsInvertedRanges() throws {
        let southwest = try GeographicPoint(latitude: 36, longitude: 139)
        let northeast = try GeographicPoint(latitude: 35, longitude: 140)

        #expect(throws: BoundingBoxError.self) {
            _ = try BoundingBox(
                southwest: southwest,
                northeast: northeast
            )
        }
    }

    @Test("Bounding box centers remain canonical geographic points")
    func derivesCanonicalCenter() throws {
        let bounds = try BoundingBox(
            minLatitude: 34,
            minLongitude: 138,
            maxLatitude: 36,
            maxLongitude: 140
        )

        let center = try bounds.center()
        let expected = try GeographicPoint(latitude: 35, longitude: 139)

        #expect(center == expected)
    }

    @Test("Bounding boxes reject negative radius")
    func rejectsNegativeRadius() throws {
        let center = try GeographicPoint(latitude: 35, longitude: 139)

        #expect(throws: BoundingBoxError.self) {
            _ = try BoundingBox.around(center: center, radiusKm: -1)
        }
    }
}
