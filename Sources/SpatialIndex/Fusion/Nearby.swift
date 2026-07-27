// Nearby.swift
// SpatialIndex - Spatial search query for Fusion
//
// This file is part of SpatialIndex module, not DatabaseEngine.
// DatabaseEngine does not know about SpatialIndexKind.

import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Spatial search query for Fusion
///
/// Searches geographic locations within radius or bounds.
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(Store.self) {
///     Nearby(\.location)
///         .within(radiusKm: 5, of: userLocation)
/// }
/// .execute()
/// ```
public struct Nearby<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let field: FieldIdentity
    private var constraint: SpatialConstraint?
    private var referencePoint: GeographicPoint?

    // MARK: - Initialization (FusionContext)

    /// Create a Nearby query for a GeographicPoint field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the GeographicPoint field
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Store.self) {
    ///     Nearby(\.location).within(radiusKm: 5, of: userLocation)
    /// }
    /// ```
    public init(_ field: Field<T, GeographicPoint>) {
        guard let context = FusionContext.current else {
            fatalError("Nearby must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Nearby query for an optional GeographicPoint field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the optional GeographicPoint field
    public init(_ field: Field<T, GeographicPoint?>) {
        guard let context = FusionContext.current else {
            fatalError("Nearby must be used within context.fuse { } block")
        }
        self.field = field.identity
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context)

    /// Create a Nearby query for a GeographicPoint field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the GeographicPoint field
    ///   - context: IndexQueryContext for database access
    public init(
        _ field: Field<T, GeographicPoint>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.queryContext = context
    }

    /// Create a Nearby query for an optional GeographicPoint field with explicit context
    ///
    /// - Parameters:
    ///   - keyPath: KeyPath to the optional GeographicPoint field
    ///   - context: IndexQueryContext for database access
    public init(
        _ field: Field<T, GeographicPoint?>,
        context: IndexQueryContext
    ) {
        self.field = field.identity
        self.queryContext = context
    }

    // MARK: - Configuration

    /// Search within a radius of a center point
    ///
    /// - Parameters:
    ///   - radiusKm: Radius in kilometers
    ///   - center: Center point
    /// - Returns: Updated query
    public func within(radiusKm: Double, of center: GeographicPoint) -> Self {
        var copy = self
        copy.constraint = SpatialConstraint(
            type: .withinDistance(
                center: center,
                radiusMeters: radiusKm * 1000.0
            )
        )
        copy.referencePoint = center
        return copy
    }

    /// Search within a bounding box
    ///
    /// - Parameter bounds: The bounding box
    /// - Returns: Updated query
    public func within(bounds: BoundingBox) throws(BoundingBoxError) -> Self {
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
        copy.referencePoint = try bounds.center()
        return copy
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        queryContext.schema.indexDescriptors(for: T.persistableType).first {
            $0.kind.identifier == "spatial"
                && $0.fieldNames == [field.name]
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        guard let constraint = constraint else {
            throw FusionQueryError.invalidConfiguration("No spatial constraint specified")
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: field.name,
                kind: "spatial"
            )
        }

        let (encoding, level) = try spatialConfiguration(
            descriptor.kind
        )

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute spatial search
        let primaryKeys: [Tuple] = try await queryContext.withTransaction { transaction in
            try await self.searchSpatial(
                constraint: constraint,
                level: level,
                encoding: encoding,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys
        var items = try await queryContext.fetchItems(ids: primaryKeys, type: T.self)
        var matchingItems: [T] = []
        matchingItems.reserveCapacity(items.count)
        for item in items where try matches(item, constraint: constraint) {
            matchingItems.append(item)
        }
        items = matchingItems

        // Filter to candidates if provided
        if let candidateIDs = candidates {
            items = items.filter { candidateIDs.contains($0.id) }
        }

        // Calculate distance scores
        guard let ref = referencePoint else {
            return items.map { ScoredResult(item: $0, score: 1.0) }
        }

        // Extract locations and calculate distances
        var itemsWithDistance: [(item: T, distance: Double)] = []
        itemsWithDistance.reserveCapacity(items.count)
        for item in items {
            guard let coordinate = try coordinate(from: item) else {
                continue
            }
            let distance = CellDistanceCalculator.haversineDistance(
                from: ref,
                to: coordinate
            )
            itemsWithDistance.append((item: item, distance: distance))
        }

        // Normalize distance to score (closer = higher score)
        guard let maxDist = itemsWithDistance.map(\.distance).max(), maxDist > 0 else {
            return items.map { ScoredResult(item: $0, score: 1.0) }
        }

        return itemsWithDistance
            .map { ScoredResult(item: $0.item, score: 1.0 - $0.distance / maxDist) }
            .sorted { $0.score > $1.score }
    }

    // MARK: - Spatial Index Reading

    /// Index structure:
    /// - Key: `[indexSubspace][spatialCode][primaryKey]`
    /// - Value: empty

    /// Search spatial index
    private func searchSpatial(
        constraint: SpatialConstraint,
        level: Int,
        encoding: SpatialEncoding,
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let plan = try SpatialScanPlanner.plan(for: constraint, encoding: encoding, level: level)
        let scanner = SpatialCellScanner(indexSubspace: indexSubspace, encoding: encoding, level: level)
        let (keys, _) = try await scanner.scan(plan: plan, limit: nil, transaction: transaction)
        return keys
    }

    private func matches(
        _ item: T,
        constraint: SpatialConstraint
    ) throws -> Bool {
        guard let coordinate = try coordinate(from: item) else {
            return false
        }
        switch constraint.type {
        case .withinDistance(let center, let radiusMeters):
            return CellDistanceCalculator.haversineDistance(
                from: center,
                to: coordinate
            ) <= radiusMeters

        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            return coordinate.latitude >= minLat &&
                coordinate.latitude <= maxLat &&
                coordinate.longitude >= minLon &&
                coordinate.longitude <= maxLon

        case .withinPolygon(let points):
            return isPointInPolygon(point: coordinate, polygon: points)
        }
    }

    private func isPointInPolygon(
        point: GeographicPoint,
        polygon: [GeographicPoint]
    ) -> Bool {
        guard polygon.count >= 3 else {
            return false
        }

        var inside = false
        var previous = polygon.count - 1

        for index in 0..<polygon.count {
            let currentPoint = polygon[index]
            let previousPoint = polygon[previous]

            if ((currentPoint.latitude > point.latitude) != (previousPoint.latitude > point.latitude)) &&
                (point.longitude < (previousPoint.longitude - currentPoint.longitude) *
                    (point.latitude - currentPoint.latitude) /
                    (previousPoint.latitude - currentPoint.latitude) +
                    currentPoint.longitude) {
                inside.toggle()
            }

            previous = index
        }

        return inside
    }

    private func coordinate(from item: T) throws -> GeographicPoint? {
        guard let value = try item.persistedFieldValue(for: field) else {
            throw SpatialIndexMaintenanceError.missingCoordinate(
                fieldName: field.name
            )
        }
        switch value {
        case .null:
            return nil
        case .geographicPoint(let point):
            return point
        case .geographicPosition(let position):
            return position.point
        default:
            throw SpatialIndexMaintenanceError.unsupportedCoordinateValue(
                fieldName: field.name
            )
        }
    }

    private func spatialConfiguration(
        _ metadata: IndexKindMetadata
    ) throws -> (encoding: SpatialEncoding, level: Int) {
        let definition = try IndexDefinition(metadata: metadata)
        guard case .spatial(let encoding, let level) = definition else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: field.name,
                kind: "spatial"
            )
        }
        return (encoding, level)
    }
}
