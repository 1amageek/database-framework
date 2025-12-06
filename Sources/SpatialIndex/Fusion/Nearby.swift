// Nearby.swift
// SpatialIndex - Spatial search query for Fusion
//
// This file is part of SpatialIndex module, not DatabaseEngine.
// DatabaseEngine does not know about SpatialIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Spatial

/// Spatial search query for Fusion
///
/// Searches geographic locations within radius or bounds.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Store.self) {
///     Nearby(\.location, context: context.indexQueryContext)
///         .within(radiusKm: 5, of: userLocation)
/// }
/// .execute()
/// ```
public struct Nearby<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var constraint: SpatialConstraint?
    private var referencePoint: (latitude: Double, longitude: Double)?

    // MARK: - Initialization (FusionContext)

    /// Create a Nearby query for a GeoPoint field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the GeoPoint field
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Store.self) {
    ///     Nearby(\.location).within(radiusKm: 5, of: userLocation)
    /// }
    /// ```
    public init(_ keyPath: KeyPath<T, GeoPoint>) {
        guard let context = FusionContext.current else {
            fatalError("Nearby must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Nearby query for an optional GeoPoint field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the optional GeoPoint field
    public init(_ keyPath: KeyPath<T, GeoPoint?>) {
        guard let context = FusionContext.current else {
            fatalError("Nearby must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Nearby query for a GeoPoint field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the GeoPoint field
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, GeoPoint>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Nearby query for an optional GeoPoint field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional GeoPoint field
    ///   - context: IndexQueryContext for database access
    public init(_ keyPath: KeyPath<T, GeoPoint?>, context: IndexQueryContext) {
        self.fieldName = T.fieldName(for: keyPath)
        self.queryContext = context
    }

    /// Create a Nearby query with a field name string
    ///
    /// - Parameters:
    ///   - fieldName: The field name to search
    ///   - context: IndexQueryContext for database access
    public init(fieldName: String, context: IndexQueryContext) {
        self.fieldName = fieldName
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Search within a radius of a center point
    ///
    /// - Parameters:
    ///   - radiusKm: Radius in kilometers
    ///   - center: Center point
    /// - Returns: Updated query
    public func within(radiusKm: Double, of center: GeoPoint) -> Self {
        var copy = self
        copy.constraint = SpatialConstraint(
            type: .withinDistance(
                center: (latitude: center.latitude, longitude: center.longitude),
                radiusMeters: radiusKm * 1000.0
            )
        )
        copy.referencePoint = (latitude: center.latitude, longitude: center.longitude)
        return copy
    }

    /// Search within a bounding box
    ///
    /// - Parameter bounds: The bounding box
    /// - Returns: Updated query
    public func within(bounds: BoundingBox) -> Self {
        var copy = self
        copy.constraint = SpatialConstraint(
            type: .withinBounds(
                minLat: bounds.southwest.latitude,
                minLon: bounds.southwest.longitude,
                maxLat: bounds.northeast.latitude,
                maxLon: bounds.northeast.longitude
            )
        )
        // Use center of bounding box as reference for distance scoring
        let centerLat = (bounds.southwest.latitude + bounds.northeast.latitude) / 2
        let centerLon = (bounds.southwest.longitude + bounds.northeast.longitude) / 2
        copy.referencePoint = (latitude: centerLat, longitude: centerLon)
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            // 1. Filter by kindIdentifier
            guard descriptor.kindIdentifier == SpatialIndexKind<T>.identifier else {
                return false
            }
            // 2. Match by fieldName
            guard let kind = descriptor.kind as? SpatialIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let constraint = constraint else {
            throw FusionQueryError.invalidConfiguration("No spatial constraint specified")
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldName,
                kind: "spatial"
            )
        }

        let indexName = descriptor.name

        // Execute spatial search
        var results = try await queryContext.executeSpatialSearch(
            type: T.self,
            indexName: indexName,
            constraint: constraint,
            limit: nil
        )

        // Filter to candidates if provided
        if let candidateIds = candidates {
            results = results.filter { candidateIds.contains("\($0.id)") }
        }

        // Calculate distance scores
        guard let ref = referencePoint else {
            return results.map { ScoredResult(item: $0, score: 1.0) }
        }

        let refPoint = GeoPoint(ref.latitude, ref.longitude)

        // Extract locations and calculate distances
        let itemsWithDistance: [(item: T, distance: Double)] = results.compactMap { item in
            guard let location = item[dynamicMember: fieldName] as? GeoPoint else {
                return nil
            }
            let distance = refPoint.distance(to: location)
            return (item: item, distance: distance)
        }

        // Normalize distance to score (closer = higher score)
        guard let maxDist = itemsWithDistance.map(\.distance).max(), maxDist > 0 else {
            return results.map { ScoredResult(item: $0, score: 1.0) }
        }

        return itemsWithDistance
            .map { ScoredResult(item: $0.item, score: 1.0 - $0.distance / maxDist) }
            .sorted { $0.score > $1.score }
    }
}
