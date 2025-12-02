// QueryTypes.swift
// DatabaseEngine - Common types for queries

import Foundation
import Core

// MARK: - Geo Point

/// A geographic point with latitude and longitude
public struct GeoPoint: Sendable, Codable, Equatable {
    /// Latitude (-90 to 90)
    public let latitude: Double

    /// Longitude (-180 to 180)
    public let longitude: Double

    public init(latitude: Double, longitude: Double) {
        self.latitude = min(90, max(-90, latitude))
        self.longitude = min(180, max(-180, longitude))
    }

    /// Create from coordinates (lat, lon)
    public init(_ latitude: Double, _ longitude: Double) {
        self.init(latitude: latitude, longitude: longitude)
    }

    /// Calculate distance to another point using Haversine formula
    ///
    /// - Parameter other: The other point
    /// - Returns: Distance in kilometers
    public func distance(to other: GeoPoint) -> Double {
        let earthRadiusKm = 6371.0

        let lat1 = latitude * .pi / 180
        let lat2 = other.latitude * .pi / 180
        let deltaLat = (other.latitude - latitude) * .pi / 180
        let deltaLon = (other.longitude - longitude) * .pi / 180

        let a = sin(deltaLat / 2) * sin(deltaLat / 2) +
                cos(lat1) * cos(lat2) *
                sin(deltaLon / 2) * sin(deltaLon / 2)
        let c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return earthRadiusKm * c
    }

    /// Check if this point is within a radius of another point
    public func isWithin(radiusKm: Double, of other: GeoPoint) -> Bool {
        distance(to: other) <= radiusKm
    }
}

// MARK: - Bounding Box

/// A rectangular geographic region
public struct BoundingBox: Sendable, Codable, Equatable {
    /// Southwest corner (minimum lat/lon)
    public let southwest: GeoPoint

    /// Northeast corner (maximum lat/lon)
    public let northeast: GeoPoint

    public init(southwest: GeoPoint, northeast: GeoPoint) {
        self.southwest = southwest
        self.northeast = northeast
    }

    /// Create from min/max coordinates
    public init(
        minLatitude: Double,
        minLongitude: Double,
        maxLatitude: Double,
        maxLongitude: Double
    ) {
        self.southwest = GeoPoint(latitude: minLatitude, longitude: minLongitude)
        self.northeast = GeoPoint(latitude: maxLatitude, longitude: maxLongitude)
    }

    /// Create a bounding box centered at a point with a radius
    public static func around(
        center: GeoPoint,
        radiusKm: Double
    ) -> BoundingBox {
        // Approximate degrees per km
        let latDelta = radiusKm / 111.0  // 1 degree latitude ≈ 111 km
        let lonDelta = radiusKm / (111.0 * cos(center.latitude * .pi / 180))

        return BoundingBox(
            minLatitude: center.latitude - latDelta,
            minLongitude: center.longitude - lonDelta,
            maxLatitude: center.latitude + latDelta,
            maxLongitude: center.longitude + lonDelta
        )
    }

    /// Check if a point is within this bounding box
    public func contains(_ point: GeoPoint) -> Bool {
        point.latitude >= southwest.latitude &&
        point.latitude <= northeast.latitude &&
        point.longitude >= southwest.longitude &&
        point.longitude <= northeast.longitude
    }
}

// MARK: - Distance Unit

/// Units for distance measurements
public enum DistanceUnit: Sendable {
    case meters
    case kilometers
    case miles

    /// Convert to meters
    public func toMeters(_ value: Double) -> Double {
        switch self {
        case .meters:
            return value
        case .kilometers:
            return value * 1000
        case .miles:
            return value * 1609.344
        }
    }
}

// MARK: - Aggregate Result

/// Result of a GROUP BY aggregation query
public struct AggregateResult<T: Persistable>: Sendable {
    /// Group key values (field name -> value)
    public let groupKey: [String: AnySendable]

    /// Aggregation results (aggregation name -> value)
    public let aggregates: [String: AnySendable]

    /// Number of records in this group
    public let count: Int

    public init(
        groupKey: [String: AnySendable],
        aggregates: [String: AnySendable],
        count: Int
    ) {
        self.groupKey = groupKey
        self.aggregates = aggregates
        self.count = count
    }
}
