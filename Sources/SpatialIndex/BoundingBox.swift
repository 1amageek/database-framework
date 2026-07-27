import DatabaseMath
import DatabaseTypes

/// A validated rectangular region used by spatial queries.
public struct BoundingBox: Sendable, Equatable {
    public let southwest: GeographicPoint
    public let northeast: GeographicPoint

    public init(
        southwest: GeographicPoint,
        northeast: GeographicPoint
    ) throws(BoundingBoxError) {
        guard southwest.latitude <= northeast.latitude else {
            throw .invertedLatitudeRange(
                minimum: southwest.latitude,
                maximum: northeast.latitude
            )
        }
        guard southwest.longitude <= northeast.longitude else {
            throw .invertedLongitudeRange(
                minimum: southwest.longitude,
                maximum: northeast.longitude
            )
        }
        self.southwest = southwest
        self.northeast = northeast
    }

    public init(
        minLatitude: Double,
        minLongitude: Double,
        maxLatitude: Double,
        maxLongitude: Double
    ) throws(BoundingBoxError) {
        let southwest: GeographicPoint
        do {
            southwest = try GeographicPoint(
                latitude: minLatitude,
                longitude: minLongitude
            )
        } catch {
            throw .invalidSouthwest(error)
        }

        let northeast: GeographicPoint
        do {
            northeast = try GeographicPoint(
                latitude: maxLatitude,
                longitude: maxLongitude
            )
        } catch {
            throw .invalidNortheast(error)
        }

        try self.init(southwest: southwest, northeast: northeast)
    }

    public static func around(
        center: GeographicPoint,
        radiusKm: Double
    ) throws(BoundingBoxError) -> BoundingBox {
        guard radiusKm.isFinite else {
            throw .nonFiniteRadius
        }
        guard radiusKm >= 0 else {
            throw .negativeRadius(radiusKm)
        }

        let latitudeDelta = radiusKm / 111.0
        let longitudeDelta = radiusKm / (
            111.0 * DatabaseMath.cosine(center.latitude * .pi / 180)
        )

        return try BoundingBox(
            minLatitude: center.latitude - latitudeDelta,
            minLongitude: center.longitude - longitudeDelta,
            maxLatitude: center.latitude + latitudeDelta,
            maxLongitude: center.longitude + longitudeDelta
        )
    }

    public func center() throws(BoundingBoxError) -> GeographicPoint {
        do {
            return try GeographicPoint(
                latitude: (southwest.latitude + northeast.latitude) / 2,
                longitude: (southwest.longitude + northeast.longitude) / 2
            )
        } catch {
            throw .invalidCenter(error)
        }
    }

    public func contains(_ point: GeographicPoint) -> Bool {
        point.latitude >= southwest.latitude &&
            point.latitude <= northeast.latitude &&
            point.longitude >= southwest.longitude &&
            point.longitude <= northeast.longitude
    }
}
