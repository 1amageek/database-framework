// SpatialQuery.swift
// SpatialIndex - Query extension for spatial search
//
// Design: Follows GraphIndex Query patterns with SpatialCellScanner integration.

@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

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

    /// Whether the source exhausted without a resource limit.
    ///
    /// A complete result may contain fewer than `k` items when the index has
    /// fewer matching entities.
    public var isComplete: Bool {
        limitReason == nil
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
    public let holes: [[GeographicPoint]]

    /// Create polygon query options
    ///
    /// - Parameters:
    ///   - type: Polygon type for algorithm selection (default: .simple)
    ///   - validateInput: Whether to validate coordinates (default: true)
    ///   - holes: Interior holes to exclude (default: empty)
    public init(
        type: PolygonType = .simple,
        validateInput: Bool = true,
        holes: [[GeographicPoint]] = []
    ) {
        self.type = type
        self.validateInput = validateInput
        self.holes = holes
    }
}

// MARK: - Spatial KNN Resource Limits

public struct SpatialKNNResourceLimits: Sendable, Equatable {
    public let maximumIterations: Int
    public let maximumKeysPerIteration: Int
    public let maximumTotalKeys: Int

    public init(
        maximumIterations: Int = 10,
        maximumKeysPerIteration: Int = 10_000,
        maximumTotalKeys: Int = 50_000
    ) {
        self.maximumIterations = maximumIterations
        self.maximumKeysPerIteration = maximumKeysPerIteration
        self.maximumTotalKeys = maximumTotalKeys
    }

    public static let `default` = SpatialKNNResourceLimits()
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
    private let field: FieldIdentity
    private var spatialConstraint: SpatialConstraint?
    private var fetchLimit: Int?
    private var shouldOrderByDistance: Bool = false
    private var referencePoint: GeographicPoint?
    private var polygonOptions: PolygonQueryOptions = PolygonQueryOptions()
    private var configurationError: SpatialQueryError?

    // KNN parameters
    private var knnK: Int?
    private var knnInitialRadiusKm: Double = 1.0
    private var knnMaxRadiusKm: Double = 100.0
    private var knnExpansionFactor: Double = 2.0
    private var knnMaxIterations: Int = 10
    private var knnMaxKeysPerIteration: Int = 10000
    private var knnMaxTotalKeys: Int = 50000

    internal init(
        queryContext: IndexQueryContext,
        field: FieldIdentity
    ) {
        self.queryContext = queryContext
        self.field = field
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
    public func within(radiusKm: Double, of center: GeographicPoint) -> Self {
        var copy = self
        copy.spatialConstraint = SpatialConstraint(
            type: .withinDistance(
                center: center,
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
    public func within(polygon: [GeographicPoint], options: PolygonQueryOptions = PolygonQueryOptions()) -> Self {
        var copy = self
        copy.spatialConstraint = SpatialConstraint(type: .withinPolygon(points: polygon))
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
    /// - Parameter count: Maximum number of results (must be > 0)
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        guard count > 0 else {
            copy.configurationError = .invalidLimit(
                "limit must be positive, got \(count)"
            )
            copy.fetchLimit = nil
            return copy
        }
        copy.configurationError = nil
        copy.fetchLimit = count
        return copy
    }

    /// Execute the spatial search
    ///
    /// - Returns: SpatialQueryResult with items and metadata
    /// - Throws: Error if search fails or constraint not set
    public func execute() async throws -> SpatialQueryResult<T> {
        if let configurationError {
            throw configurationError
        }
        guard let constraint = spatialConstraint else {
            throw SpatialQueryError.noConstraint
        }

        if case .withinDistance(_, let radiusMeters) = constraint.type {
            guard radiusMeters.isFinite, radiusMeters >= 0 else {
                throw SpatialQueryError.invalidRadius(
                    "radius must be finite and non-negative"
                )
            }
        }

        // Validate polygon if applicable
        if case .withinPolygon(let points) = constraint.type {
            if polygonOptions.validateInput {
                try validatePolygon(points)
            }
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw SpatialQueryError.indexNotFound(requestedIndexIdentity)
        }

        let (encoding, level) = try spatialConfiguration(
            descriptor
        )

        let authorization = IndexReadAuthorization(
            limit: fetchLimit,
            offset: nil,
            orderBy: nil
        )
        return try await withAuthorizedSpatialIndexRead(
            descriptor: descriptor,
            authorization: authorization
        ) { readableIndex, transaction, execution in
            guard let readableIndex else {
                return SpatialQueryResult(items: [], limitReason: nil)
            }
            let scanResult = try await self.searchSpatial(
                constraint: constraint,
                level: level,
                encoding: encoding,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                workMeter: execution.workMeter
            )
            // Fetch and validate candidates on the same transaction snapshot as
            // the index scan. An index reference that has no target is physical
            // corruption, not an empty result.
            let items = try await fetchIndexedItems(
                primaryKeys: scanResult.keys,
                indexName: descriptor.name,
                transaction: transaction,
                execution: execution
            )
            var output = try DatabaseRetainedArrayBuilder<(
                item: T,
                distance: Double?
            )>(
                workMeter: execution.workMeter,
                stage: .projection,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(
                        for: (item: T, distance: Double?).self
                    ),
                expectedCount: items.count
            )
            for item in items {
                guard try matches(item, constraint: constraint) else {
                    continue
                }
                let distance = try referencePoint.flatMap { reference in
                    try extractGeographicPoint(from: item).map {
                        distanceInMeters(from: reference, to: $0)
                    }
                }
                let admission = try output.prepareAppend(
                    footprint: try CanonicalRelationalFootprintMeter.footprint(
                        of: item,
                        workMeter: execution.workMeter
                    ).adding(DatabaseIntermediateFootprint(bytes: 16)),
                    at: .projection
                )
                output.append((item: item, distance: distance), using: admission)
            }
            var retainedOutput = output.finish()
            if shouldOrderByDistance {
                retainedOutput = retainedOutput.sortingElements {
                    ($0.distance ?? Double.infinity)
                        < ($1.distance ?? Double.infinity)
                }
            }
            var limitReason = scanResult.limitReason
            if let fetchLimit, retainedOutput.count > fetchLimit {
                retainedOutput = retainedOutput.retainingSubrange(0..<fetchLimit)
                limitReason = .maxResultsReached(
                    returned: fetchLimit,
                    limit: fetchLimit
                )
            }
            guard let outputRows = UInt32(exactly: retainedOutput.count) else {
                throw DatabaseWorkLimitError.maximumRows(
                    stage: .resultMaterialization,
                    consumed: execution.workMeter.consumedRows,
                    requested: UInt32.max,
                    maximum: execution.workMeter.budget.maximumRows
                )
            }
            try execution.workMeter.recordOutputRows(outputRows)
            return SpatialQueryResult(
                items: retainedOutput.promoteToOutput(),
                limitReason: limitReason
            )
        }
    }

    private func withAuthorizedSpatialIndexRead<Result: Sendable>(
        descriptor: IndexDescriptor,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess,
            ReadExecutionContext
        ) async throws -> Result
    ) async throws -> Result {
        let execution = ReadExecutionContext(
            monotonicClock: queryContext.context.container.monotonicClock
        )
        return try await queryContext.context.withDataOperation {
            guard let entity = queryContext.schema.entity(
                named: T.persistableType
            ) else {
                throw SpatialQueryError.indexNotFound(
                    requestedIndexIdentity
                )
            }
            let admission = try queryContext.context.admitLogicalRead(
                listAuthorization: authorization,
                fieldPlan: .fullEntity(
                    entity,
                    including: Set(descriptor.fieldNames).union(
                        descriptor.includedFieldNames
                    )
                ),
                restrictingTo: [T.persistableType]
            )
            return try await queryContext.context
                .withReadAuthorizationAdmission(admission) {
                    try await queryContext.withReadableIndex(
                        named: descriptor.name,
                        indexType: descriptor.type,
                        for: T.self,
                        authorization: authorization
                    ) { readableIndex, transaction in
                        try await operation(
                            readableIndex,
                            transaction,
                            execution
                        )
                    }
                }
        }
    }

    private func fetchIndexedItems<PrimaryKeys>(
        primaryKeys: PrimaryKeys,
        indexName: String,
        transaction: any IndexQueryReadAccess,
        execution: ReadExecutionContext
    ) async throws -> DatabaseSharedRetainedArray<T>
    where PrimaryKeys: RandomAccessCollection & Sendable,
          PrimaryKeys.Element == Tuple {
        guard let entity = queryContext.schema.entity(
            named: T.persistableType
        ) else {
            throw SpatialQueryError.indexNotFound(requestedIndexIdentity)
        }
        let models = try await transaction
            .fetchPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: primaryKeys,
                partitions: queryContext.partitionValues,
                workMeter: execution.workMeter
            )
        var items = try DatabaseRetainedArrayBuilder<T>(
            workMeter: execution.workMeter,
            stage: .projection,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: T.self
            ),
            expectedCount: primaryKeys.count
        )
        for (primaryKey, model) in zip(primaryKeys, models) {
            guard let model else {
                throw SpatialQueryError.indexedItemMissing(
                    index: indexName,
                    primaryKey: primaryKey.pack()
                )
            }
            let admission = try items.prepareAppend(
                footprint: try CanonicalRelationalFootprintMeter.footprint(
                    of: model,
                    workMeter: execution.workMeter
                ),
                at: .projection
            )
            items.append(try model.decode(as: T.self), using: admission)
        }
        return try items.finish().moveToSharedOwnership(at: .projection)
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
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RetainedSpatialScanResult {
        let plan = try SpatialScanPlanner.plan(
            for: constraint,
            encoding: encoding,
            level: level,
            workMeter: workMeter
        )

        let scanner = SpatialCellScanner(
            indexSubspace: indexSubspace,
            encoding: encoding,
            level: level
        )

        return try await scanner.scanRetained(
            plan: plan,
            limit: SpatialScanBudget.candidateLimit(forFetchLimit: fetchLimit),
            transaction: transaction,
            workMeter: workMeter
        )
    }

    /// Execute and return only items (without distance)
    ///
    /// - Returns: Array of matching items
    /// - Throws: Error if search fails
    public func executeItems() async throws -> [T] {
        let results = try await execute()
        return results.items.map { $0.item }
    }

    /// Finds the spatial index descriptor for the requested field.
    private func findIndexDescriptor() -> IndexDescriptor? {
        queryContext.schema.indexDescriptors(for: T.persistableType).first {
            $0.type == .spatial
                && $0.fieldNames == [field.name]
        }
    }

    /// Identity used to diagnose a missing index declaration.
    private var requestedIndexIdentity: String {
        "\(T.persistableType).\(field.name)"
    }

    private func spatialConfiguration(
        _ descriptor: IndexDescriptor
    ) throws -> (encoding: SpatialEncoding, level: Int) {
        let definition = descriptor.declaration.definition
        guard case .spatial(_, let encoding, let level) = definition else {
            throw SpatialQueryError.indexNotFound(
                requestedIndexIdentity
            )
        }
        return (encoding, level)
    }

    private func extractGeographicPoint(from item: T) throws -> GeographicPoint? {
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

    private func matches(
        _ item: borrowing T,
        constraint: SpatialConstraint
    ) throws -> Bool {
        guard let location = try extractGeographicPoint(from: item) else {
            return false
        }
        switch constraint.type {
        case .withinDistance(let center, let radiusMeters):
            return distanceInMeters(
                from: center,
                to: location
            ) <= radiusMeters

        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            return location.latitude >= minLat &&
                location.latitude <= maxLat &&
                location.longitude >= minLon &&
                location.longitude <= maxLon

        case .withinPolygon(let points):
            return isPointInPolygon(
                point: location,
                polygon: points
            )
        }
    }

    // MARK: - Distance Calculation

    /// Calculate distance between two points in meters
    ///
    /// **Unit Convention**:
    /// Internal spatial operations use meters, matching the index scan contract.
    ///
    /// - Parameters:
    ///   - from: Source point
    ///   - to: Destination point
    /// - Returns: Distance in meters
    private func distanceInMeters(from: GeographicPoint, to: GeographicPoint) -> Double {
        CellDistanceCalculator.haversineDistance(from: from, to: to)
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
        point: GeographicPoint,
        polygon: [GeographicPoint]
    ) -> Bool {
        guard polygon.count >= 3 else { return false }

        // Select algorithm based on polygon type
        switch polygonOptions.type {
        case .convex:
            // Use optimized cross-product algorithm for convex polygons
            return isPointInConvexPolygon(point: point, polygon: polygon)

        case .complex:
            // Use Winding Number for complex/self-intersecting polygons
            // Also handles holes if specified
            if polygonOptions.holes.isEmpty {
                return WindingNumber.isPointInPolygon(point: point, polygon: polygon)
            } else {
                return WindingNumber.isPointInPolygonWithHoles(
                    point: point,
                    exterior: polygon,
                    holes: polygonOptions.holes
                )
            }

        case .simple:
            // Default: Ray Casting algorithm
            // Check holes first if specified
            if !polygonOptions.holes.isEmpty {
                for hole in polygonOptions.holes {
                    if isPointInSimplePolygon(point: point, polygon: hole) {
                        return false  // Inside a hole
                    }
                }
            }
            return isPointInSimplePolygon(point: point, polygon: polygon)
        }
    }

    /// Ray casting point-in-polygon for simple polygons
    private func isPointInSimplePolygon(
        point: GeographicPoint,
        polygon: [GeographicPoint]
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
        point: GeographicPoint,
        polygon: [GeographicPoint]
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
    /// Coordinate bounds are already guaranteed by `GeographicPoint`.
    ///
    /// - Parameter points: Polygon vertices
    /// - Throws: SpatialQueryError.invalidPolygon if validation fails
    private func validatePolygon(_ points: [GeographicPoint]) throws {
        guard points.count >= 3 else {
            throw SpatialQueryError.invalidPolygon("Polygon requires at least 3 points, got \(points.count)")
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
        from center: GeographicPoint,
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

    /// Applies explicit work limits to adaptive and best-first KNN execution.
    public func resourceLimits(
        _ limits: SpatialKNNResourceLimits
    ) -> Self {
        var copy = self
        copy.knnMaxIterations = limits.maximumIterations
        copy.knnMaxKeysPerIteration = limits.maximumKeysPerIteration
        copy.knnMaxTotalKeys = limits.maximumTotalKeys
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
        if let configurationError {
            throw configurationError
        }
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
            throw SpatialQueryError.indexNotFound(requestedIndexIdentity)
        }

        let (encoding, level) = try spatialConfiguration(
            descriptor
        )

        let authorization = IndexReadAuthorization(
            limit: k,
            offset: nil,
            orderBy: ["distance"]
        )
        return try await withAuthorizedSpatialIndexRead(
            descriptor: descriptor,
            authorization: authorization
        ) { readableIndex, transaction, execution in
            guard let readableIndex else {
                return SpatialKNNResult(
                    items: [],
                    k: k,
                    searchRadiusMeters: knnInitialRadiusKm * 1000.0,
                    limitReason: nil
                )
            }
            var currentRadiusMeters = knnInitialRadiusKm * 1000.0
            let maxRadiusMeters = knnMaxRadiusKm * 1000.0
            var allCandidates = try DatabaseRetainedArrayBuilder<(
                item: T,
                distance: Double
            )>(
                workMeter: execution.workMeter,
                stage: .projection,
                layout: try CanonicalRelationalFootprintMeter
                    .retainedArrayLayout(for: (item: T, distance: Double).self)
            )
            var seenIds = try SpatialRetainedIdentifierSet(
                workMeter: execution.workMeter
            )
            var iterations = 0
            var totalKeysScanned = 0
            var lastUsedRadiusMeters = currentRadiusMeters
            var limitReason: LimitReason?

            while allCandidates.count < k,
                  currentRadiusMeters <= maxRadiusMeters,
                  iterations < knnMaxIterations {
                iterations += 1
                lastUsedRadiusMeters = currentRadiusMeters
                let remainingKeyBudget = knnMaxTotalKeys - totalKeysScanned
                guard remainingKeyBudget > 0 else {
                    limitReason = .maxCandidatesReached(
                        scanned: totalKeysScanned,
                        limit: knnMaxTotalKeys
                    )
                    break
                }
                let radiusConstraint = SpatialConstraint(
                    type: .withinDistance(
                        center: center,
                        radiusMeters: currentRadiusMeters
                    )
                )
                let plan = try SpatialScanPlanner.plan(
                    for: radiusConstraint,
                    encoding: encoding,
                    level: level,
                    workMeter: execution.workMeter
                )
                let scanner = SpatialCellScanner(
                    indexSubspace: readableIndex.subspace,
                    encoding: encoding,
                    level: level
                )
                let scan = try await scanner.scanRetained(
                    plan: plan,
                    limit: min(knnMaxKeysPerIteration, remainingKeyBudget),
                    transaction: transaction,
                    workMeter: execution.workMeter
                )
                totalKeysScanned += scan.keys.count
                if let scanLimitReason = scan.limitReason {
                    switch scanLimitReason {
                    case .maxResultsReached(let returned, let limit):
                        limitReason = .maxCandidatesReached(
                            scanned: returned,
                            limit: limit
                        )
                    default:
                        limitReason = scanLimitReason
                    }
                }
                let items = try await fetchIndexedItems(
                    primaryKeys: scan.keys,
                    indexName: descriptor.name,
                    transaction: transaction,
                    execution: execution
                )
                for item in items {
                    guard let location = try extractGeographicPoint(
                        from: item
                    ) else {
                        continue
                    }
                    let distanceMeters = distanceInMeters(
                        from: center,
                        to: location
                    )
                    if distanceMeters <= currentRadiusMeters {
                        guard try seenIds.insert(
                            item.id.persistableIdentifierValue
                        ) else {
                            continue
                        }
                        let admission = try allCandidates.prepareAppend(
                            footprint: try CanonicalRelationalFootprintMeter
                                .footprint(
                                    of: item,
                                    workMeter: execution.workMeter
                                ).adding(
                                    DatabaseIntermediateFootprint(bytes: 8)
                                ),
                            at: .projection
                        )
                        allCandidates.append(
                            (item: item, distance: distanceMeters),
                            using: admission
                        )
                    }
                }
                if allCandidates.count >= k { break }
                if totalKeysScanned >= knnMaxTotalKeys {
                    limitReason = .maxCandidatesReached(
                        scanned: totalKeysScanned,
                        limit: knnMaxTotalKeys
                    )
                    break
                }
                currentRadiusMeters *= knnExpansionFactor
            }
            let sorted = allCandidates.finish().sortingElements {
                lhs,
                rhs in
                if lhs.distance != rhs.distance {
                    return lhs.distance < rhs.distance
                }
                return lhs.item.id.persistableIdentifierValue
                    < rhs.item.id.persistableIdentifierValue
            }
            let topKCount = min(k, sorted.count)
            let topK = sorted.retainingSubrange(0..<topKCount)
            if limitReason == nil && topK.count < k {
                if iterations >= knnMaxIterations {
                    limitReason = .maxIterationsReached(
                        iterations: iterations,
                        limit: knnMaxIterations
                    )
                } else {
                    limitReason = .maxRadiusReached(
                        radiusMeters: lastUsedRadiusMeters,
                        limitMeters: maxRadiusMeters
                    )
                }
            }
            guard let outputRows = UInt32(exactly: topK.count) else {
                throw DatabaseWorkLimitError.maximumRows(
                    stage: .resultMaterialization,
                    consumed: execution.workMeter.consumedRows,
                    requested: UInt32.max,
                    maximum: execution.workMeter.budget.maximumRows
                )
            }
            try execution.workMeter.recordOutputRows(outputRows)
            return SpatialKNNResult(
                items: topK.promoteToOutput(),
                k: k,
                searchRadiusMeters: lastUsedRadiusMeters,
                limitReason: limitReason
            )
        }
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
        guard knnMaxIterations > 0,
              knnMaxKeysPerIteration > 0,
              knnMaxTotalKeys > 0 else {
            throw SpatialQueryError.invalidKNNParameters(
                "KNN resource limits must all be positive"
            )
        }
    }

    // MARK: - Exact K-Nearest Neighbors

    /// Execute exact K-nearest-neighbor search over the spatial index snapshot.
    ///
    /// The implementation scans the index to exhaustion, or to the configured
    /// candidate cap, and computes distances from the retained snapshot. This
    /// reference path is encoding-independent and never claims completeness
    /// after approximate cell pruning.
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
        if let configurationError {
            throw configurationError
        }
        guard let k = knnK else {
            throw SpatialQueryError.noConstraint
        }
        guard let center = referencePoint else {
            throw SpatialQueryError.noConstraint
        }

        try validateKNNParameters(k: k)

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw SpatialQueryError.indexNotFound(requestedIndexIdentity)
        }

        let (encoding, level) = try spatialConfiguration(
            descriptor
        )

        return try await withAuthorizedSpatialIndexRead(
                descriptor: descriptor,
                authorization: IndexReadAuthorization(
                    limit: k,
                    offset: nil,
                    orderBy: ["distance"]
                )
            ) { readableIndex, transaction, execution in
            guard let readableIndex else {
                return SpatialKNNResult(
                    items: [],
                    k: k,
                    searchRadiusMeters: 0,
                    limitReason: nil
                )
            }
            guard let entity = queryContext.schema.entity(
                named: T.persistableType
            ) else {
                throw SpatialQueryError.indexNotFound(requestedIndexIdentity)
            }
            let knnSearch = SpatialKNNSearch<T>(
                entity: entity,
                indexName: descriptor.name,
                indexSubspace: readableIndex.subspace,
                encoding: encoding,
                level: level,
                fieldName: field.name,
                maximumCandidatesToScan: knnMaxTotalKeys
            )
            let retained = try await knnSearch.findKNearest(
                k: k,
                from: center,
                transaction: transaction,
                partitions: queryContext.partitionValues,
                workMeter: execution.workMeter
            )
            let outputCount = retained.count
            let limitReason = retained.limitReason
            guard let outputRows = UInt32(exactly: outputCount) else {
                throw DatabaseWorkLimitError.maximumRows(
                    stage: .resultMaterialization,
                    consumed: execution.workMeter.consumedRows,
                    requested: UInt32.max,
                    maximum: execution.workMeter.budget.maximumRows
                )
            }
            try execution.workMeter.recordOutputRows(outputRows)
            let items = retained.promoteToOutput()
            return SpatialKNNResult(
                items: items,
                k: k,
                searchRadiusMeters: items.last?.distance ?? 0,
                limitReason: limitReason
            )
        }
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
    /// - Parameter keyPath: KeyPath to the GeographicPoint field
    /// - Returns: Spatial query builder
    public func location(
        _ field: Field<T, GeographicPoint>
    ) -> SpatialQueryBuilder<T> {
        SpatialQueryBuilder(
            queryContext: queryContext,
            field: field.identity
        )
    }

    /// Specify the optional location field to search
    ///
    /// - Parameter keyPath: KeyPath to the optional GeographicPoint field
    /// - Returns: Spatial query builder
    public func location(
        _ field: Field<T, GeographicPoint?>
    ) -> SpatialQueryBuilder<T> {
        SpatialQueryBuilder(
            queryContext: queryContext,
            field: field.identity
        )
    }
}

// MARK: - DatabaseContext Extension

extension DatabaseContext {

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
public enum SpatialQueryError: Error, Sendable, CustomStringConvertible {
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

    /// The index referenced a model missing from the same read snapshot.
    case indexedItemMissing(index: String, primaryKey: ByteString)

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
        case .indexedItemMissing(let index, let primaryKey):
            return "Spatial index '\(index)' references missing item \(primaryKey)"
        }
    }
}
