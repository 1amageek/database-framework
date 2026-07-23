// QueryTypes.swift
// DatabaseEngine - Common types for queries

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseMath

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

        let halfDeltaLatitudeSine = DatabaseMath.sine(deltaLat / 2)
        let halfDeltaLongitudeSine = DatabaseMath.sine(deltaLon / 2)
        let a = halfDeltaLatitudeSine * halfDeltaLatitudeSine +
                DatabaseMath.cosine(lat1) * DatabaseMath.cosine(lat2) *
                halfDeltaLongitudeSine * halfDeltaLongitudeSine
        let c = 2 * DatabaseMath.arcTangent(
            y: DatabaseMath.squareRoot(a),
            x: DatabaseMath.squareRoot(1 - a)
        )

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
        let lonDelta = radiusKm / (111.0 * DatabaseMath.cosine(center.latitude * .pi / 180))

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
/// - `aggregates`: Preserves exact integer kinds for count/sum and integral averages;
///   floating-point inputs remain floating-point; min/max preserve their input type.
///
/// **Empty Results**:
/// - `min`/`max` return `nil` in `aggregates` for empty groups (not zero)
/// - `count` returns `0` for empty groups
/// - `sum`/`avg` return `nil` when every input is null or the group is empty
///
/// A record count is not implicit metadata. Callers request `count` as an
/// aggregate and read its `FieldValue.int64` result like every other aggregate.
public struct AggregateResult<T: Persistable>: Sendable {
    /// Group key values (field name → typed value)
    public let groupKey: [String: FieldValue]

    /// Aggregation results (aggregation name → typed value)
    /// - count: `FieldValue.int64`
    /// - sum: exact `FieldValue.int64`/`uint64`, or `double` for floating inputs
    /// - avg: exact integer when integral, otherwise an exactly convertible `double`
    /// - min/max: `FieldValue?` (nil for empty groups)
    public let aggregates: [String: FieldValue?]

    public init(
        groupKey: [String: FieldValue],
        aggregates: [String: FieldValue?]
    ) {
        self.groupKey = groupKey
        self.aggregates = aggregates
    }

    // MARK: - Convenience Accessors

    /// Get aggregate value as Double (for sum, avg, or numeric min/max)
    ///
    /// - Parameter name: The aggregation name
    /// - Returns: Double value, or nil when the aggregate is absent or null.
    /// - Throws: A typed error for non-numeric, non-finite, or lossy values.
    public func aggregateDouble(
        _ name: String
    ) throws(AggregateResultError) -> Double? {
        guard let stored = aggregates[name], let value = stored else {
            return nil
        }
        switch value {
        case .int64(let integer):
            guard let result = Double(exactly: integer) else {
                throw .notExactlyRepresentableAsDouble(
                    name: name,
                    value: value
                )
            }
            return result
        case .uint64(let integer):
            guard let result = Double(exactly: integer) else {
                throw .notExactlyRepresentableAsDouble(
                    name: name,
                    value: value
                )
            }
            return result
        case .double(let floatingPoint):
            guard floatingPoint.isFinite else {
                throw .nonFiniteDouble(name: name)
            }
            return floatingPoint
        case .null:
            return nil
        case .string, .bool, .data, .rdfTerm, .array:
            throw .nonNumericValue(name: name, value: value)
        }
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

public enum AggregateResultError: Error, Sendable, Equatable {
    case nonNumericValue(name: String, value: FieldValue)
    case notExactlyRepresentableAsDouble(name: String, value: FieldValue)
    case nonFiniteDouble(name: String)
}
