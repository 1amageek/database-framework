// SpatialQuery.swift
// SpatialIndex - Query extension for spatial search
//
// Design: Follows GraphIndex Query patterns with SpatialCellScanner integration.

import Foundation
import DatabaseEngine
import Core
import FoundationDB
import Spatial

// MARK: - Spatial Query Result

/// Result of a spatial query with metadata about completeness
///
/// **Design Reference**: Follows GraphIndex patterns with LimitReason support.
public struct SpatialQueryResult<T: Persistable>: Sendable {
    /// Items matching the query with optional distance information
    public let items: [(item: T, distance: Double?)]

    /// Reason why the query was incomplete, if applicable
    public let limitReason: LimitReason?

    /// Whether the query completed without hitting any limits
    public var isComplete: Bool {
        limitReason == nil
    }

    /// Number of items returned
    public var count: Int {
        items.count
    }

    public init(items: [(item: T, distance: Double?)], limitReason: LimitReason?) {
        self.items = items
        self.limitReason = limitReason
    }
}

// MARK: - K-Nearest Neighbors Result

/// Result of a K-nearest neighbors query
///
/// Unlike `SpatialQueryResult`, this always includes distance information
/// and items are sorted by distance (ascending).
///
/// **Usage**:
/// ```swift
/// let result = try await context.findNearby(Store.self)
///     .location(\.geoPoint)
///     .nearest(k: 10, from: userLocation)
///     .executeKNN()
///
/// for (store, distance) in result.items {
///     print("\(store.name): \(distance)m away")
/// }
/// ```
public struct SpatialKNNResult<T: Persistable>: Sendable {
    /// Items sorted by distance (ascending), always includes distance
    public let items: [(item: T, distance: Double)]

    /// Requested K value
    public let k: Int

    /// Final search radius used (in meters)
    public let searchRadiusMeters: Double

    /// Reason why less than K results were returned, if applicable
    public let limitReason: LimitReason?

    /// Whether K or more items were found
    public var isComplete: Bool {
        items.count >= k
    }

    /// Number of items returned
    public var count: Int {
        items.count
    }

    public init(items: [(item: T, distance: Double)], k: Int, searchRadiusMeters: Double, limitReason: LimitReason?) {
        self.items = items
        self.k = k
        self.searchRadiusMeters = searchRadiusMeters
        self.limitReason = limitReason
    }
}

// MARK: - Polygon Query Options

/// Options for polygon spatial queries
///
/// **Usage**:
/// ```swift
/// let result = try await context.findNearby(Store.self)
///     .location(\.geoPoint)
///     .within(polygon: points, options: PolygonQueryOptions(type: .convex))
///     .execute()
/// ```
public struct PolygonQueryOptions: Sendable {
    /// Type of polygon for optimization hints
    public enum PolygonType: Sendable {
        /// Simple polygon (default) - uses ray casting algorithm
        case simple
        /// Convex polygon - can use optimized cross-product algorithm
        case convex
        /// Complex polygon - uses winding number algorithm (handles self-intersecting)
        ///
        /// **Advantages over Ray Casting**:
        /// - Correctly handles self-intersecting polygons
        /// - Better numerical stability at edges
        ///
        /// **Reference**: Hormann & Agathos (2001)
        case complex
    }

    /// The polygon type (affects algorithm selection)
    public let type: PolygonType

    /// Whether to validate input coordinates
    public let validateInput: Bool

    /// Interior holes for polygon-with-holes queries
    ///
    /// When set, points must be inside the exterior polygon but NOT inside any hole.
    public let holes: [[GeoPoint]]

    /// Create polygon query options
    ///
    /// - Parameters:
    ///   - type: Polygon type for algorithm selection (default: .simple)
    ///   - validateInput: Whether to validate coordinates (default: true)
    ///   - holes: Interior holes to exclude (default: empty)
    public init(
        type: PolygonType = .simple,
        validateInput: Bool = true,
        holes: [[GeoPoint]] = []
    ) {
        self.type = type
        self.validateInput = validateInput
        self.holes = holes
    }
}

// MARK: - Spatial Query Builder

/// Builder for spatial search queries
///
/// **Usage**:
/// ```swift
/// import SpatialIndex
///
/// let result = try await context.findNearby(Store.self)
///     .location(\.geoPoint)
///     .within(radiusKm: 5.0, of: currentLocation)
///     .orderByDistance()
///     .limit(10)
///     .execute()
///
/// for (store, distance) in result.items {
///     print("\(store.name): \(distance ?? 0)m")
/// }
///
/// if !result.isComplete {
///     print("More results available: \(result.limitReason!)")
/// }
/// ```
public struct SpatialQueryBuilder<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var spatialConstraint: SpatialConstraint?
    private var fetchLimit: Int?
    private var shouldOrderByDistance: Bool = false
    private var referencePoint: GeoPoint?
    private var polygonOptions: PolygonQueryOptions = PolygonQueryOptions()

    // KNN parameters
    private var knnK: Int?
    private var knnInitialRadiusKm: Double = 1.0
    private var knnMaxRadiusKm: Double = 100.0
    private var knnExpansionFactor: Double = 2.0
    private var knnMaxIterations: Int = 10
    private var knnMaxKeysPerIteration: Int = 10000
    private var knnMaxTotalKeys: Int = 50000

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
    /// Points are verified using ray casting algorithm to ensure they are
    /// actually inside the polygon (not just inside the bounding box).
    ///
    /// **Validation**:
    /// - Requires at least 3 points
    /// - All coordinates must be in valid ranges (-90 to 90 for latitude, -180 to 180 for longitude)
    ///
    /// **Limitations**:
    /// - Polygons crossing the antimeridian (±180° longitude) are not fully supported
    /// - For such polygons, consider splitting into two separate queries
    ///
    /// - Parameter polygon: Array of points defining the polygon (minimum 3 points)
    /// - Parameter options: Polygon query options (default: simple polygon with validation)
    /// - Returns: Updated query builder
    /// - Note: Invalid polygons will cause `execute()` to throw `SpatialQueryError.invalidPolygon`
    public func within(polygon: [GeoPoint], options: PolygonQueryOptions = PolygonQueryOptions()) -> Self {
        var copy = self
        let points = polygon.map { (latitude: $0.latitude, longitude: $0.longitude) }
        copy.spatialConstraint = SpatialConstraint(type: .withinPolygon(points: points))
        copy.polygonOptions = options
        return copy
    }

    /// Order results by distance from reference point (nearest first)
    ///
    /// **Note**: This only has effect when a reference point is set via:
    /// - `within(radiusKm:of:)` - center point becomes reference
    /// - `nearest(k:from:)` - center point becomes reference
    ///
    /// For `within(bounds:)` or `within(polygon:)` queries without a reference point,
    /// this method has no effect and results are returned in index order.
    ///
    /// - Returns: Updated query builder
    public func orderByDistance() -> Self {
        var copy = self
        copy.shouldOrderByDistance = true
        return copy
    }

    /// Limit the number of results
    ///
    /// Limit is applied during index scanning for efficiency,
    /// not after fetching all items.
    ///
    /// **Note**: Values ≤ 0 are ignored (no limit applied).
    ///
    /// - Parameter count: Maximum number of results (must be > 0)
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        guard count > 0 else {
            // Invalid limit values are ignored (no limit applied)
            return self
        }
        var copy = self
        copy.fetchLimit = count
        return copy
    }

    /// Execute the spatial search
    ///
    /// - Returns: SpatialQueryResult with items and metadata
    /// - Throws: Error if search fails or constraint not set
    public func execute() async throws -> SpatialQueryResult<T> {
        guard let constraint = spatialConstraint else {
            throw SpatialQueryError.noConstraint
        }

        // Validate polygon if applicable
        if case .withinPolygon(let points) = constraint.type {
            if polygonOptions.validateInput {
                try validatePolygon(points)
            }
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw SpatialQueryError.indexNotFound(buildIndexName())
        }

        // Get index configuration from kind
        let level: Int
        let encoding: SpatialEncoding
        if let kind = descriptor.kind as? SpatialIndexKind<T> {
            level = kind.level
            encoding = kind.encoding
        } else {
            level = 15 // Default S2 level
            encoding = .s2
        }

        let indexName = descriptor.name

        // Get index subspace
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute spatial search with SpatialCellScanner
        let scanResult: SpatialScanResult = try await queryContext.withTransaction { transaction in
            try await self.searchSpatial(
                constraint: constraint,
                level: level,
                encoding: encoding,
                indexSubspace: indexSubspace,
                transaction: transaction
            )
        }

        // Fetch items by primary keys (limit already applied during scanning)
        var items = try await queryContext.fetchItems(ids: scanResult.keys, type: T.self)

        // Apply polygon filtering if needed
        if case .withinPolygon(let points) = constraint.type {
            items = items.filter { item in
                guard let location = extractGeoPoint(from: item) else { return false }
                return isPointInPolygon(point: location, polygon: points)
            }
        }

        // Apply radius filtering for precise results
        // Note: Covering cells may include points outside the exact radius
        if case .withinDistance(let center, let radiusMeters) = constraint.type {
            let centerPoint = GeoPoint(center.latitude, center.longitude)
            items = items.filter { item in
                // Items without location are excluded from radius-based queries
                guard let location = extractGeoPoint(from: item) else { return false }
                return distanceInMeters(from: centerPoint, to: location) <= radiusMeters
            }
        }

        // Calculate distances if we have a reference point
        // Distance is always calculated when referencePoint exists (e.g., radius query)
        // orderByDistance() only controls whether results are sorted by distance
        let resultsWithDistance: [(item: T, distance: Double?)]
        if let ref = referencePoint {
            let itemsWithDistances = items.map { item -> (item: T, distance: Double?) in
                guard let location = extractGeoPoint(from: item) else {
                    return (item: item, distance: nil)
                }
                return (item: item, distance: distanceInMeters(from: ref, to: location))
            }

            if shouldOrderByDistance {
                resultsWithDistance = itemsWithDistances.sorted {
                    ($0.distance ?? Double.infinity) < ($1.distance ?? Double.infinity)
                }
            } else {
                resultsWithDistance = itemsWithDistances
            }
        } else {
            resultsWithDistance = items.map { (item: $0, distance: nil) }
        }

        return SpatialQueryResult(items: resultsWithDistance, limitReason: scanResult.limitReason)
    }

    // MARK: - Spatial Index Reading

    /// Search spatial index using SpatialCellScanner
    ///
    /// **Design**: Uses centralized SpatialCellScanner for efficient scanning
    /// with early limit application and proper deduplication.
    private func searchSpatial(
        constraint: SpatialConstraint,
        level: Int,
        encoding: SpatialEncoding,
        indexSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> SpatialScanResult {
        // Get covering cells based on constraint type
        let coveringCells: [UInt64]
        switch constraint.type {
        case .withinDistance(let center, let radiusMeters):
            coveringCells = S2Geometry.getCoveringCells(
                latitude: center.latitude,
                longitude: center.longitude,
                radiusMeters: radiusMeters,
                level: level
            )
        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            coveringCells = S2Geometry.getCoveringCellsForBox(
                minLat: minLat,
                minLon: minLon,
                maxLat: maxLat,
                maxLon: maxLon,
                level: level
            )
        case .withinPolygon(let points):
            // Get bounding box of polygon for covering cells
            let lats = points.map { $0.latitude }
            let lons = points.map { $0.longitude }
            coveringCells = S2Geometry.getCoveringCellsForBox(
                minLat: lats.min() ?? 0,
                minLon: lons.min() ?? 0,
                maxLat: lats.max() ?? 0,
                maxLon: lons.max() ?? 0,
                level: level
            )
        }

        // Use SpatialCellScanner for efficient, deduplicated scanning with early limit
        let scanner = SpatialCellScanner(
            indexSubspace: indexSubspace,
            encoding: encoding,
            level: level
        )

        let (keys, limitReason) = try await scanner.scanCells(
            cellIds: coveringCells,
            limit: fetchLimit,
            transaction: transaction
        )

        return SpatialScanResult(keys: keys, limitReason: limitReason)
    }

    /// Execute and return only items (without distance)
    ///
    /// - Returns: Array of matching items
    /// - Throws: Error if search fails
    public func executeItems() async throws -> [T] {
        let results = try await execute()
        return results.items.map { $0.item }
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

    // MARK: - Distance Calculation

    /// Calculate distance between two points in meters
    ///
    /// **Unit Convention**:
    /// - `GeoPoint.distance(to:)` returns **kilometers** (Haversine formula)
    /// - Internal spatial operations use **meters** (S2Geometry convention)
    /// - This helper ensures consistent meter-based calculations
    ///
    /// - Parameters:
    ///   - from: Source point
    ///   - to: Destination point
    /// - Returns: Distance in meters
    private func distanceInMeters(from: GeoPoint, to: GeoPoint) -> Double {
        from.distance(to: to) * 1000.0
    }

    // MARK: - Point-in-Polygon

    /// Point-in-polygon test using ray casting algorithm
    ///
    /// **Algorithm**: Cast a ray from the point to infinity and count intersections
    /// with polygon edges. Odd count = inside, even count = outside.
    ///
    /// **Reference**: "Computational Geometry: Algorithms and Applications"
    /// (de Berg et al.) - Chapter 3
    ///
    /// **Time Complexity**: O(n) where n = number of polygon vertices
    ///
    /// - Parameters:
    ///   - point: Point to test
    ///   - polygon: Polygon vertices as (latitude, longitude) tuples
    /// - Returns: true if point is inside polygon
    private func isPointInPolygon(
        point: GeoPoint,
        polygon: [(latitude: Double, longitude: Double)]
    ) -> Bool {
        guard polygon.count >= 3 else { return false }

        // Convert tuple polygon to GeoPoint array for winding number
        let geoPolygon = polygon.map { GeoPoint($0.latitude, $0.longitude) }

        // Select algorithm based on polygon type
        switch polygonOptions.type {
        case .convex:
            // Use optimized cross-product algorithm for convex polygons
            return isPointInConvexPolygon(point: point, polygon: polygon)

        case .complex:
            // Use Winding Number for complex/self-intersecting polygons
            // Also handles holes if specified
            if polygonOptions.holes.isEmpty {
                return WindingNumber.isPointInPolygon(point: point, polygon: geoPolygon)
            } else {
                return WindingNumber.isPointInPolygonWithHoles(
                    point: point,
                    exterior: geoPolygon,
                    holes: polygonOptions.holes
                )
            }

        case .simple:
            // Default: Ray Casting algorithm
            // Check holes first if specified
            if !polygonOptions.holes.isEmpty {
                for hole in polygonOptions.holes {
                    if isPointInSimplePolygon(point: point, polygon: hole.map { ($0.latitude, $0.longitude) }) {
                        return false  // Inside a hole
                    }
                }
            }
            return isPointInSimplePolygon(point: point, polygon: polygon)
        }
    }

    /// Ray casting point-in-polygon for simple polygons
    private func isPointInSimplePolygon(
        point: GeoPoint,
        polygon: [(latitude: Double, longitude: Double)]
    ) -> Bool {
        var inside = false
        let n = polygon.count
        var j = n - 1

        for i in 0..<n {
            let yi = polygon[i].latitude
            let yj = polygon[j].latitude
            let xi = polygon[i].longitude
            let xj = polygon[j].longitude

            // Ray casting: check if horizontal ray from point crosses edge
            if ((yi > point.latitude) != (yj > point.latitude)) &&
               (point.longitude < (xj - xi) * (point.latitude - yi) / (yj - yi) + xi) {
                inside = !inside
            }
            j = i
        }

        return inside
    }

    /// Point-in-convex-polygon test using cross product
    ///
    /// **Algorithm**: For convex polygons, if the point is inside, it will be
    /// on the same side of all edges. We check this using the cross product.
    ///
    /// **Time Complexity**: O(n) where n = number of polygon vertices
    /// **Reference**: "Computational Geometry" (de Berg) - Chapter 1
    ///
    /// - Parameters:
    ///   - point: Point to test
    ///   - polygon: Convex polygon vertices (must be ordered consistently)
    /// - Returns: true if point is inside the convex polygon
    private func isPointInConvexPolygon(
        point: GeoPoint,
        polygon: [(latitude: Double, longitude: Double)]
    ) -> Bool {
        guard polygon.count >= 3 else { return false }

        var sign: Int? = nil
        let n = polygon.count

        for i in 0..<n {
            let p1 = polygon[i]
            let p2 = polygon[(i + 1) % n]

            // Cross product to determine which side of the edge the point is on
            let cross = (p2.longitude - p1.longitude) * (point.latitude - p1.latitude) -
                        (p2.latitude - p1.latitude) * (point.longitude - p1.longitude)

            let currentSign = cross > 0 ? 1 : (cross < 0 ? -1 : 0)

            if currentSign != 0 {
                if sign == nil {
                    sign = currentSign
                } else if sign != currentSign {
                    return false  // Point is outside (different side of an edge)
                }
            }
        }

        return true
    }

    // MARK: - Polygon Validation

    /// Validate polygon for spatial query
    ///
    /// **Checks**:
    /// 1. Minimum 3 points required
    /// 2. All coordinates must be in valid ranges
    ///
    /// - Parameter points: Polygon vertices
    /// - Throws: SpatialQueryError.invalidPolygon if validation fails
    private func validatePolygon(_ points: [(latitude: Double, longitude: Double)]) throws {
        guard points.count >= 3 else {
            throw SpatialQueryError.invalidPolygon("Polygon requires at least 3 points, got \(points.count)")
        }

        for (index, point) in points.enumerated() {
            guard (-90...90).contains(point.latitude) else {
                throw SpatialQueryError.invalidPolygon(
                    "Point \(index): Latitude \(point.latitude) must be between -90 and 90"
                )
            }
            guard (-180...180).contains(point.longitude) else {
                throw SpatialQueryError.invalidPolygon(
                    "Point \(index): Longitude \(point.longitude) must be between -180 and 180"
                )
            }
        }
    }

    // MARK: - K-Nearest Neighbors

    /// Configure K-nearest neighbors search
    ///
    /// This sets up a KNN query that uses adaptive radius expansion to find
    /// the K nearest items to a center point.
    ///
    /// **Algorithm**: Adaptive Radius Expansion
    /// 1. Start with initial radius
    /// 2. If fewer than K results, expand radius and retry
    /// 3. Continue until K results found or max radius reached
    /// 4. Sort all results by distance and return top K
    ///
    /// **Reference**: Lu et al., "Efficient Processing of k Nearest Neighbor
    /// Joins using MapReduce", PVLDB 2012
    ///
    /// - Parameters:
    ///   - k: Number of nearest neighbors to find
    ///   - center: Center point to measure distances from
    ///   - initialRadiusKm: Starting search radius (default: 1km)
    ///   - maxRadiusKm: Maximum search radius (default: 100km)
    ///   - expansionFactor: Radius multiplier for each iteration (default: 2.0)
    /// - Returns: Updated query builder configured for KNN
    public func nearest(
        k: Int,
        from center: GeoPoint,
        initialRadiusKm: Double = 1.0,
        maxRadiusKm: Double = 100.0,
        expansionFactor: Double = 2.0
    ) -> Self {
        var copy = self
        copy.knnK = k
        copy.referencePoint = center
        copy.knnInitialRadiusKm = initialRadiusKm
        copy.knnMaxRadiusKm = maxRadiusKm
        copy.knnExpansionFactor = expansionFactor
        return copy
    }

    /// Execute K-nearest neighbors search
    ///
    /// Finds the K nearest items to the center point specified in `nearest(k:from:)`.
    ///
    /// **Usage**:
    /// ```swift
    /// let result = try await context.findNearby(Store.self)
    ///     .location(\.geoPoint)
    ///     .nearest(k: 10, from: userLocation)
    ///     .executeKNN()
    ///
    /// for (store, distance) in result.items {
    ///     print("\(store.name): \(distance)m away")
    /// }
    ///
    /// if !result.isComplete {
    ///     print("Only found \(result.count) of \(result.k) requested items")
    /// }
    /// ```
    ///
    /// - Returns: SpatialKNNResult with K nearest items sorted by distance
    /// - Throws: SpatialQueryError if KNN not configured, parameters invalid, or index not found
    public func executeKNN() async throws -> SpatialKNNResult<T> {
        guard let k = knnK else {
            throw SpatialQueryError.noConstraint
        }
        guard let center = referencePoint else {
            throw SpatialQueryError.noConstraint
        }

        // Validate KNN parameters
        try validateKNNParameters(k: k)

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw SpatialQueryError.indexNotFound(buildIndexName())
        }

        // Get index configuration from kind
        let level: Int
        let encoding: SpatialEncoding
        if let kind = descriptor.kind as? SpatialIndexKind<T> {
            level = kind.level
            encoding = kind.encoding
        } else {
            level = 15
            encoding = .s2
        }

        let indexName = descriptor.name
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Adaptive radius expansion algorithm
        var currentRadiusMeters = knnInitialRadiusKm * 1000.0
        let maxRadiusMeters = knnMaxRadiusKm * 1000.0
        var allCandidates: [(item: T, distance: Double)] = []
        var seenIds: Set<AnyHashable> = []  // Use AnyHashable for stable ID comparison
        var iterations = 0
        var totalKeysScanned = 0
        var lastUsedRadiusMeters = currentRadiusMeters  // Track actual last used radius
        var limitReason: LimitReason? = nil

        while allCandidates.count < k && currentRadiusMeters <= maxRadiusMeters && iterations < knnMaxIterations {
            iterations += 1
            lastUsedRadiusMeters = currentRadiusMeters

            // Get covering cells for current radius
            let coveringCells = S2Geometry.getCoveringCells(
                latitude: center.latitude,
                longitude: center.longitude,
                radiusMeters: currentRadiusMeters,
                level: level
            )

            // Scan cells with per-iteration limit to prevent DoS
            let scanResult: SpatialScanResult = try await queryContext.withTransaction { transaction in
                let scanner = SpatialCellScanner(
                    indexSubspace: indexSubspace,
                    encoding: encoding,
                    level: level
                )
                let (keys, scanLimitReason) = try await scanner.scanCells(
                    cellIds: coveringCells,
                    limit: knnMaxKeysPerIteration,
                    transaction: transaction
                )
                return SpatialScanResult(keys: keys, limitReason: scanLimitReason)
            }

            totalKeysScanned += scanResult.keys.count

            // Check total keys budget
            if totalKeysScanned >= knnMaxTotalKeys {
                limitReason = .maxCellsReached(scanned: totalKeysScanned, limit: knnMaxTotalKeys)
                break
            }

            // Fetch items
            let items = try await queryContext.fetchItems(ids: scanResult.keys, type: T.self)

            // Calculate distances and deduplicate using AnyHashable for stable comparison
            for item in items {
                let itemId = AnyHashable(item.id)
                guard !seenIds.contains(itemId) else { continue }
                seenIds.insert(itemId)

                guard let location = extractGeoPoint(from: item) else { continue }

                let distanceMeters = distanceInMeters(from: center, to: location)

                // Only include if within current radius (covering cells may extend beyond)
                if distanceMeters <= currentRadiusMeters {
                    allCandidates.append((item: item, distance: distanceMeters))
                }
            }

            // If we have enough candidates, we can stop
            if allCandidates.count >= k {
                break
            }

            // Expand radius for next iteration
            currentRadiusMeters *= knnExpansionFactor
        }

        // Sort by distance and take top K
        let sorted = allCandidates.sorted { $0.distance < $1.distance }
        let topK = Array(sorted.prefix(k))

        // Determine limit reason if not already set
        if limitReason == nil && topK.count < k {
            if iterations >= knnMaxIterations {
                limitReason = .maxCellsReached(scanned: iterations, limit: knnMaxIterations)
            } else {
                // maxRadiusMeters exceeded or insufficient data in search area
                limitReason = .maxResultsReached(returned: topK.count, limit: k)
            }
        }

        return SpatialKNNResult(
            items: topK,
            k: k,
            searchRadiusMeters: lastUsedRadiusMeters,  // Use actual last used radius
            limitReason: limitReason
        )
    }

    /// Validate KNN parameters
    ///
    /// - Parameter k: Number of nearest neighbors to find
    /// - Throws: SpatialQueryError if parameters are invalid
    private func validateKNNParameters(k: Int) throws {
        // k must be positive
        guard k > 0 else {
            throw SpatialQueryError.invalidKNNParameters("k must be positive, got \(k)")
        }

        // Radius values must be positive and finite
        guard knnInitialRadiusKm > 0 && knnInitialRadiusKm.isFinite else {
            throw SpatialQueryError.invalidRadius("initialRadiusKm must be positive and finite, got \(knnInitialRadiusKm)")
        }
        guard knnMaxRadiusKm > 0 && knnMaxRadiusKm.isFinite else {
            throw SpatialQueryError.invalidRadius("maxRadiusKm must be positive and finite, got \(knnMaxRadiusKm)")
        }
        guard knnMaxRadiusKm >= knnInitialRadiusKm else {
            throw SpatialQueryError.invalidRadius("maxRadiusKm (\(knnMaxRadiusKm)) must be >= initialRadiusKm (\(knnInitialRadiusKm))")
        }

        // Expansion factor must be > 1.0 and finite
        guard knnExpansionFactor > 1.0 && knnExpansionFactor.isFinite else {
            throw SpatialQueryError.invalidKNNParameters("expansionFactor must be > 1.0 and finite, got \(knnExpansionFactor)")
        }
    }

    // MARK: - True K-Nearest Neighbors (Priority Queue + Cell Pruning)

    /// Execute True K-Nearest Neighbors search using Priority Queue + Cell Pruning
    ///
    /// This method uses a more efficient algorithm than `executeKNN()`:
    ///
    /// **Algorithm** (Samet, 2006):
    /// 1. Start with the cell containing the query point
    /// 2. Use a priority queue ordered by minimum distance to query
    /// 3. For each cell, if minDistance > k-th best distance, prune
    /// 4. Continue until k results found or no more cells to explore
    ///
    /// **Advantages over Adaptive Radius**:
    /// - Guaranteed to find k nearest (if they exist)
    /// - No arbitrary radius parameter needed
    /// - Efficient pruning reduces unnecessary cell scans
    /// - Works well with sparse data
    ///
    /// **Usage**:
    /// ```swift
    /// let result = try await context.findNearby(Store.self)
    ///     .location(\.geoPoint)
    ///     .nearest(k: 10, from: userLocation)
    ///     .executeTrueKNN()
    /// ```
    ///
    /// **Reference**: Samet, H. "Foundations of Multidimensional and Metric Data Structures", 2006
    ///
    /// - Returns: SpatialKNNResult with K nearest items sorted by distance
    /// - Throws: SpatialQueryError if KNN not configured or index not found
    public func executeTrueKNN() async throws -> SpatialKNNResult<T> {
        guard let k = knnK else {
            throw SpatialQueryError.noConstraint
        }
        guard let center = referencePoint else {
            throw SpatialQueryError.noConstraint
        }

        // Validate k
        guard k > 0 else {
            throw SpatialQueryError.invalidKNNParameters("k must be positive, got \(k)")
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw SpatialQueryError.indexNotFound(buildIndexName())
        }

        // Get index configuration from kind
        let level: Int
        let encoding: SpatialEncoding
        if let kind = descriptor.kind as? SpatialIndexKind<T> {
            level = kind.level
            encoding = kind.encoding
        } else {
            level = 15
            encoding = .s2
        }

        let indexName = descriptor.name
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Create the true KNN search instance
        let knnSearch = SpatialKNNSearch<T>(
            queryContext: queryContext,
            indexSubspace: indexSubspace,
            encoding: encoding,
            level: level,
            fieldName: fieldName,
            maxCellsToScan: knnMaxKeysPerIteration,
            maxPointsToScan: knnMaxTotalKeys
        )

        // Execute true KNN search
        let results: [(item: T, distance: Double)] = try await queryContext.withTransaction { transaction in
            try await knnSearch.findKNearest(
                k: k,
                from: center,
                transaction: transaction
            )
        }

        // Determine limit reason if not enough results
        let limitReason: LimitReason?
        if results.count < k {
            limitReason = .maxResultsReached(returned: results.count, limit: k)
        } else {
            limitReason = nil
        }

        // Get the search radius (maximum distance found)
        let searchRadius = results.last?.distance ?? 0

        return SpatialKNNResult(
            items: results,
            k: k,
            searchRadiusMeters: searchRadius,
            limitReason: limitReason
        )
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
    /// let result = try await context.findNearby(Store.self)
    ///     .location(\.geoPoint)
    ///     .within(radiusKm: 5.0, of: currentLocation)
    ///     .orderByDistance()
    ///     .limit(10)
    ///     .execute()
    ///
    /// // Access items with distances
    /// for (store, distance) in result.items {
    ///     print("\(store.name): \(distance ?? 0)m away")
    /// }
    ///
    /// // Check if all results were returned
    /// if !result.isComplete {
    ///     print("More results available")
    /// }
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

    /// Invalid KNN parameters (k, expansionFactor, etc.)
    case invalidKNNParameters(String)

    /// Invalid limit value
    case invalidLimit(String)

    /// Invalid radius value
    case invalidRadius(String)

    public var description: String {
        switch self {
        case .noConstraint:
            return "No spatial constraint provided for spatial search"
        case .indexNotFound(let name):
            return "Spatial index not found: \(name)"
        case .invalidPolygon(let reason):
            return "Invalid polygon: \(reason)"
        case .invalidKNNParameters(let reason):
            return "Invalid KNN parameters: \(reason)"
        case .invalidLimit(let reason):
            return "Invalid limit: \(reason)"
        case .invalidRadius(let reason):
            return "Invalid radius: \(reason)"
        }
    }
}
