// SpatialQuery.swift
// Query DSL - Spatial queries (geo-location, bounding boxes, etc.)

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
    internal func toMeters(_ value: Double) -> Double {
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

// MARK: - Spatial Query

/// A spatial query for geographic filtering
public struct SpatialQuery<T: Persistable>: @unchecked Sendable {
    /// Base query with filters
    public let baseQuery: Query<T>

    /// Location field (AnyKeyPath is immutable and thread-safe)
    public let locationField: AnyKeyPath

    /// Location field name
    public let locationFieldName: String

    /// Spatial condition
    public let condition: SpatialCondition

    /// Spatial condition types
    public enum SpatialCondition: Sendable {
        /// Points within a bounding box
        case withinBounds(BoundingBox)

        /// Points within a radius of a center point
        case nearBy(center: GeoPoint, radiusKm: Double)

        /// Points that intersect with a bounding box
        case intersects(BoundingBox)
    }

    internal init(
        baseQuery: Query<T>,
        locationField: AnyKeyPath,
        locationFieldName: String,
        condition: SpatialCondition
    ) {
        self.baseQuery = baseQuery
        self.locationField = locationField
        self.locationFieldName = locationFieldName
        self.condition = condition
    }

    /// Add distance-based ordering (nearest first)
    public func orderByDistance(from center: GeoPoint) -> SpatialQuery<T> {
        // For now, just return self - ordering would need to be handled in execution
        self
    }

    /// Limit results
    public func limit(_ count: Int) -> SpatialQuery<T> {
        var newBase = baseQuery
        newBase.fetchLimit = count
        return SpatialQuery(
            baseQuery: newBase,
            locationField: locationField,
            locationFieldName: locationFieldName,
            condition: condition
        )
    }
}

// MARK: - Query Extension

extension Query {
    /// Find records within a bounding box
    ///
    /// **Usage**:
    /// ```swift
    /// let bounds = BoundingBox(
    ///     minLatitude: 35.6,
    ///     minLongitude: 139.6,
    ///     maxLatitude: 35.8,
    ///     maxLongitude: 139.8
    /// )
    /// let nearby = try await context.fetch(Restaurant.self)
    ///     .within(bounds: bounds, locationField: \.location)
    ///     .execute()
    /// ```
    public func within(
        bounds: BoundingBox,
        locationField keyPath: KeyPath<T, GeoPoint>
    ) -> SpatialQuery<T> {
        SpatialQuery(
            baseQuery: self,
            locationField: keyPath,
            locationFieldName: T.fieldName(for: keyPath),
            condition: .withinBounds(bounds)
        )
    }

    /// Find records within a bounding box (optional location field)
    public func within(
        bounds: BoundingBox,
        locationField keyPath: KeyPath<T, GeoPoint?>
    ) -> SpatialQuery<T> {
        SpatialQuery(
            baseQuery: self,
            locationField: keyPath,
            locationFieldName: T.fieldName(for: keyPath),
            condition: .withinBounds(bounds)
        )
    }

    /// Find records near a point within a radius
    ///
    /// **Usage**:
    /// ```swift
    /// let center = GeoPoint(latitude: 35.6762, longitude: 139.6503)
    /// let nearbyStores = try await context.fetch(Store.self)
    ///     .nearBy(center: center, radiusKm: 5.0, locationField: \.location)
    ///     .execute()
    /// ```
    public func nearBy(
        center: GeoPoint,
        radiusKm: Double,
        locationField keyPath: KeyPath<T, GeoPoint>
    ) -> SpatialQuery<T> {
        SpatialQuery(
            baseQuery: self,
            locationField: keyPath,
            locationFieldName: T.fieldName(for: keyPath),
            condition: .nearBy(center: center, radiusKm: radiusKm)
        )
    }

    /// Find records near a point within a radius (optional location field)
    public func nearBy(
        center: GeoPoint,
        radiusKm: Double,
        locationField keyPath: KeyPath<T, GeoPoint?>
    ) -> SpatialQuery<T> {
        SpatialQuery(
            baseQuery: self,
            locationField: keyPath,
            locationFieldName: T.fieldName(for: keyPath),
            condition: .nearBy(center: center, radiusKm: radiusKm)
        )
    }

    /// Find records near a point with distance in specified unit
    public func nearBy(
        center: GeoPoint,
        radius: Double,
        unit: DistanceUnit,
        locationField keyPath: KeyPath<T, GeoPoint>
    ) -> SpatialQuery<T> {
        let radiusKm = unit.toMeters(radius) / 1000.0
        return nearBy(center: center, radiusKm: radiusKm, locationField: keyPath)
    }
}

// MARK: - Distance Calculation

extension GeoPoint {
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
