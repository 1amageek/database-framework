// SpatialQuery.swift
// SpatialIndex - Query extension for spatial search

import Foundation
import DatabaseEngine
import Core

// MARK: - Spatial Query Builder

/// Builder for spatial search queries
///
/// **Usage**:
/// ```swift
/// import SpatialIndex
///
/// let stores = try await context.findNearby(Store.self)
///     .location(\.geoPoint)
///     .within(radiusKm: 5.0, of: currentLocation)
///     .limit(10)
///     .execute()
/// ```
public struct SpatialQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var spatialConstraint: SpatialConstraint?
    private var fetchLimit: Int?
    private var shouldOrderByDistance: Bool = false
    private var referencePoint: GeoPoint?

    internal init(queryContext: IndexQueryContext, fieldName: String) {
        self.queryContext = queryContext
        self.fieldName = fieldName
    }

    /// Search within a bounding box
    ///
    /// - Parameter bounds: The bounding box to search within
    /// - Returns: Updated query builder
    public func within(bounds: BoundingBox) -> Self {
        var copy = self
        copy.spatialConstraint = SpatialConstraint(
            type: .withinBounds(
                minLat: bounds.southwest.latitude,
                minLon: bounds.southwest.longitude,
                maxLat: bounds.northeast.latitude,
                maxLon: bounds.northeast.longitude
            )
        )
        return copy
    }

    /// Search within a radius of a center point
    ///
    /// - Parameters:
    ///   - radiusKm: Radius in kilometers
    ///   - center: Center point
    /// - Returns: Updated query builder
    public func within(radiusKm: Double, of center: GeoPoint) -> Self {
        var copy = self
        copy.spatialConstraint = SpatialConstraint(
            type: .withinDistance(
                center: (latitude: center.latitude, longitude: center.longitude),
                radiusMeters: radiusKm * 1000.0
            )
        )
        copy.referencePoint = center
        return copy
    }

    /// Search within a polygon
    ///
    /// - Parameter polygon: Array of points defining the polygon
    /// - Returns: Updated query builder
    public func within(polygon: [GeoPoint]) -> Self {
        var copy = self
        let points = polygon.map { (latitude: $0.latitude, longitude: $0.longitude) }
        copy.spatialConstraint = SpatialConstraint(type: .withinPolygon(points: points))
        return copy
    }

    /// Order results by distance from reference point (nearest first)
    ///
    /// - Returns: Updated query builder
    public func orderByDistance() -> Self {
        var copy = self
        copy.shouldOrderByDistance = true
        return copy
    }

    /// Limit the number of results
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.fetchLimit = count
        return copy
    }

    /// Execute the spatial search
    ///
    /// - Returns: Array of (item, distance) tuples if orderByDistance, otherwise just items with nil distance
    /// - Throws: Error if search fails or constraint not set
    public func execute() async throws -> [(item: T, distance: Double?)] {
        guard let constraint = spatialConstraint else {
            throw SpatialQueryError.noConstraint
        }

        let indexName = buildIndexName()

        let items = try await queryContext.executeSpatialSearch(
            type: T.self,
            indexName: indexName,
            constraint: constraint,
            limit: fetchLimit
        )

        // Calculate distances if we have a reference point and ordering is requested
        if shouldOrderByDistance, let ref = referencePoint {
            return items
                .compactMap { item -> (item: T, distance: Double?, location: GeoPoint)? in
                    // Try to get the GeoPoint from the item using the field name
                    guard let location = extractGeoPoint(from: item) else {
                        return (item: item, distance: nil, location: GeoPoint(0, 0))
                    }
                    let distance = ref.distance(to: location)
                    return (item: item, distance: distance, location: location)
                }
                .sorted { ($0.distance ?? Double.infinity) < ($1.distance ?? Double.infinity) }
                .map { (item: $0.item, distance: $0.distance) }
        }

        // Return items without distance calculation
        return items.map { (item: $0, distance: nil) }
    }

    /// Execute and return only items (without distance)
    ///
    /// - Returns: Array of matching items
    /// - Throws: Error if search fails
    public func executeItems() async throws -> [T] {
        let results = try await execute()
        return results.map { $0.item }
    }

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            guard descriptor.kindIdentifier == SpatialIndexKind<T>.identifier else {
                return false
            }
            guard let kind = descriptor.kind as? SpatialIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    /// Build the index name based on type and field
    ///
    /// Uses IndexDescriptor lookup for reliable index name resolution.
    private func buildIndexName() -> String {
        if let descriptor = findIndexDescriptor() {
            return descriptor.name
        }
        // Fallback to conventional format
        return "\(T.persistableType)_spatial_\(fieldName)"
    }

    /// Extract GeoPoint from item using Persistable dynamicMember subscript
    private func extractGeoPoint(from item: T) -> GeoPoint? {
        guard let value = item[dynamicMember: fieldName] else { return nil }
        return value as? GeoPoint
    }
}

// MARK: - Spatial Entry Point

/// Entry point for spatial queries
public struct SpatialEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the location field to search
    ///
    /// - Parameter keyPath: KeyPath to the GeoPoint field
    /// - Returns: Spatial query builder
    public func location(_ keyPath: KeyPath<T, GeoPoint>) -> SpatialQueryBuilder<T> {
        SpatialQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath)
        )
    }

    /// Specify the optional location field to search
    ///
    /// - Parameter keyPath: KeyPath to the optional GeoPoint field
    /// - Returns: Spatial query builder
    public func location(_ keyPath: KeyPath<T, GeoPoint?>) -> SpatialQueryBuilder<T> {
        SpatialQueryBuilder(
            queryContext: queryContext,
            fieldName: T.fieldName(for: keyPath)
        )
    }
}

// MARK: - FDBContext Extension

extension FDBContext {

    /// Start a spatial search query
    ///
    /// This method is available when you import `SpatialIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import SpatialIndex
    ///
    /// let stores = try await context.findNearby(Store.self)
    ///     .location(\.geoPoint)
    ///     .within(radiusKm: 5.0, of: currentLocation)
    ///     .orderByDistance()
    ///     .limit(10)
    ///     .execute()
    /// // Returns: [(item: Store, distance: Double?)]
    /// ```
    ///
    /// - Parameter type: The Persistable type to search
    /// - Returns: Entry point for configuring the search
    public func findNearby<T: Persistable>(_ type: T.Type) -> SpatialEntryPoint<T> {
        SpatialEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Spatial Query Error

/// Errors for spatial query operations
public enum SpatialQueryError: Error, CustomStringConvertible {
    /// No spatial constraint provided
    case noConstraint

    /// Index not found
    case indexNotFound(String)

    /// Invalid polygon (not enough points)
    case invalidPolygon(String)

    public var description: String {
        switch self {
        case .noConstraint:
            return "No spatial constraint provided for spatial search"
        case .indexNotFound(let name):
            return "Spatial index not found: \(name)"
        case .invalidPolygon(let reason):
            return "Invalid polygon: \(reason)"
        }
    }
}
