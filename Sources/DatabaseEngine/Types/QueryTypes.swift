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
///
/// **Type Preservation**:
/// - `groupKey`: Preserves original types via `FieldValue` (int64, double, string, etc.)
/// - `aggregates`: Returns typed results (int64 for count, double for sum/avg, original type for min/max)
///
/// **Empty Results**:
/// - `min`/`max` return `nil` in `aggregates` for empty groups (not zero)
/// - `count` returns `0` for empty groups
/// - `sum`/`avg` return `FieldValue.double(0.0)` for empty groups
public struct AggregateResult<T: Persistable>: Sendable {
    /// Group key values (field name → typed value)
    public let groupKey: [String: FieldValue]

    /// Aggregation results (aggregation name → typed value)
    /// - count: `FieldValue.int64`
    /// - sum/avg: `FieldValue.double`
    /// - min/max: `FieldValue?` (nil for empty groups)
    public let aggregates: [String: FieldValue?]

    /// Number of records in this group
    public let count: Int

    public init(
        groupKey: [String: FieldValue],
        aggregates: [String: FieldValue?],
        count: Int
    ) {
        self.groupKey = groupKey
        self.aggregates = aggregates
        self.count = count
    }

    // MARK: - Convenience Accessors

    /// Get aggregate value as Double (for sum, avg, or numeric min/max)
    ///
    /// - Parameter name: The aggregation name
    /// - Returns: Double value, or nil if not found or not numeric
    public func aggregateDouble(_ name: String) -> Double? {
        aggregates[name]??.asDouble
    }

    /// Get aggregate value as Int64 (for count)
    ///
    /// - Parameter name: The aggregation name
    /// - Returns: Int64 value, or nil if not found or not integer
    public func aggregateInt64(_ name: String) -> Int64? {
        aggregates[name]??.int64Value
    }

    /// Get aggregate value as String (for string min/max)
    ///
    /// - Parameter name: The aggregation name
    /// - Returns: String value, or nil if not found or not string
    public func aggregateString(_ name: String) -> String? {
        aggregates[name]??.stringValue
    }

    /// Get group key value as Int64
    ///
    /// - Parameter name: The field name
    /// - Returns: Int64 value, or nil if not found or not integer
    public func groupKeyInt64(_ name: String) -> Int64? {
        groupKey[name]?.int64Value
    }

    /// Get group key value as String
    ///
    /// - Parameter name: The field name
    /// - Returns: String value, or nil if not found or not string
    public func groupKeyString(_ name: String) -> String? {
        groupKey[name]?.stringValue
    }

    /// Get group key value as Double
    ///
    /// - Parameter name: The field name
    /// - Returns: Double value, or nil if not found or not double
    public func groupKeyDouble(_ name: String) -> Double? {
        groupKey[name]?.doubleValue
    }
}
