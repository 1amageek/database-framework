// Nearby.swift
// SpatialIndex - Spatial search query for Fusion
//
// This file is part of SpatialIndex module, not DatabaseEngine.
// DatabaseEngine dispatches spatial semantics without depending on this module.

@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseKit
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

    private let queryContext: IndexQueryContext!
    private let field: FieldIdentity
    private var constraint: SpatialConstraint?
    private var referencePoint: GeographicPoint?
    private var configurationError: FusionQueryError?

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
        let context = FusionContext.current
        self.field = field.identity
        self.queryContext = context
        self.configurationError = nil
    }

    /// Create a Nearby query for an optional GeographicPoint field
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// - Parameter keyPath: KeyPath to the optional GeographicPoint field
    public init(_ field: Field<T, GeographicPoint?>) {
        let context = FusionContext.current
        self.field = field.identity
        self.queryContext = context
        self.configurationError = nil
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
        self.configurationError = nil
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
        self.configurationError = nil
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
        let radiusMeters = radiusKm * 1000.0
        guard radiusKm.isFinite,
              radiusKm >= 0,
              radiusMeters.isFinite else {
            copy.constraint = nil
            copy.referencePoint = nil
            copy.configurationError = .invalidConfiguration(
                "Nearby radius must be finite and non-negative"
            )
            return copy
        }
        copy.constraint = SpatialConstraint(
            type: .withinDistance(
                center: center,
                radiusMeters: radiusMeters
            )
        )
        copy.referencePoint = center
        copy.configurationError = nil
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
        copy.configurationError = nil
        return copy
    }

    // MARK: - Index Discovery

    /// Finds the spatial index descriptor for the requested field.
    private func findIndexDescriptor() -> IndexDescriptor? {
        queryContext.schema.indexDescriptors(for: T.persistableType).first {
            $0.type == .spatial
                && $0.fieldNames == [field.name]
        }
    }

    // MARK: - FusionQuery

    public var fusionQueryPlan: FusionQueryPlan<T> {
        if let configurationError {
            return FusionQueryPlan(configurationError: configurationError)
        }
        guard let queryContext else {
            return FusionQueryPlan(
                configurationError: .invalidConfiguration(
                    "Nearby requires an IndexQueryContext or context.fuse"
                )
            )
        }
        return FusionQueryPlan(
            context: queryContext,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["distance"]
            ),
            indexDescriptor: {
                guard let descriptor = self.findIndexDescriptor() else {
                    throw FusionQueryError.indexNotFound(
                        entity: T.persistableType,
                        field: self.field.name,
                        indexType: .spatial
                    )
                }
                return descriptor
            },
            operation: { [self] candidates, execution in
                try await executeBound(
                    candidates: candidates,
                    execution: execution
                )
            }
        )
    }

    private func executeBound(
        candidates: Set<T.ID>?,
        execution: ReadExecutionContext
    ) async throws -> FusionQueryResult<T> {
        guard let constraint = constraint else {
            throw FusionQueryError.invalidConfiguration("No spatial constraint specified")
        }

        // Find index descriptor
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: field.name,
                indexType: .spatial
            )
        }

        let (encoding, level) = try spatialConfiguration(
            descriptor
        )

        let indexName = descriptor.name

        guard let entity = queryContext.schema.entity(
            named: T.persistableType
        ) else {
            throw IndexQueryContextError.entityNotFound(T.persistableType)
        }

        // Keep the index keys and decoded storage rows in request-accounted
        // ownership on the same transaction snapshot until Fusion adopts them.
        return try await queryContext.withReadableIndex(
            named: indexName,
            indexType: descriptor.type,
            for: T.self,
            authorization: IndexReadAuthorization(
                limit: nil,
                offset: nil,
                orderBy: ["distance"]
            )
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return try FusionQueryResultBuilder<T>(
                    execution: execution
                ).finish()
            }
            let scan = try await self.searchSpatial(
                constraint: constraint,
                level: level,
                encoding: encoding,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                workMeter: execution.workMeter
            )
            let models = try await transaction
                .fetchPersistedModelsPreservingOrder(
                    entity: entity,
                    primaryKeys: scan.keys,
                    partitions: queryContext.partitionValues,
                    workMeter: execution.workMeter
                )
            return try buildFusionResult(
                primaryKeys: scan.keys,
                models: models,
                candidates: candidates,
                constraint: constraint,
                indexName: descriptor.name,
                execution: execution
            )
        }
    }

    private func buildFusionResult(
        primaryKeys: DatabaseSharedRetainedArray<Tuple>,
        models: DatabaseSharedRetainedArray<PersistedModel?>,
        candidates: Set<T.ID>?,
        constraint: SpatialConstraint,
        indexName: String,
        execution: ReadExecutionContext
    ) throws -> FusionQueryResult<T> {
        precondition(primaryKeys.count == models.count)

        let candidateReservation = try execution.workMeter.reserveIntermediate(
            bytes: candidates == nil
                ? 0
                : UInt64(MemoryLayout<Set<ByteString>>.stride),
            at: .indexScan
        )
        defer { candidateReservation.release() }
        var candidateKeys: Set<ByteString>?
        if let candidates {
            var keys: Set<ByteString> = []
            keys.reserveCapacity(candidates.count)
            for candidate in candidates {
                let packed = try PersistableIdentifierKeyCodec
                    .tuple(for: candidate).pack()
                guard !keys.contains(packed) else { continue }
                try candidateReservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(packed.count) + 64,
                    at: .indexScan
                )
                keys.insert(packed)
            }
            candidateKeys = keys
        }

        let reference = referencePoint
        var maximumDistance = 0.0
        if let reference {
            for index in models.indices {
                try execution.workMeter.consume(at: .indexScan)
                guard let model = models[index] else {
                    throw SpatialQueryError.indexedItemMissing(
                        index: indexName,
                        primaryKey: primaryKeys[index].pack()
                    )
                }
                let packedPrimaryKey = primaryKeys[index].pack()
                guard candidateKeys?.contains(packedPrimaryKey) ?? true,
                      let coordinate = try coordinate(from: model),
                      matches(coordinate, constraint: constraint)
                else {
                    continue
                }
                maximumDistance = max(
                    maximumDistance,
                    CellDistanceCalculator.haversineDistance(
                        from: reference,
                        to: coordinate
                    )
                )
            }
        }

        var output = try FusionQueryResultBuilder<T>(
            execution: execution,
            expectedCount: models.count
        )
        for index in models.indices {
            try execution.workMeter.consume(at: .indexScan)
            guard let model = models[index] else {
                throw SpatialQueryError.indexedItemMissing(
                    index: indexName,
                    primaryKey: primaryKeys[index].pack()
                )
            }
            let packedPrimaryKey = primaryKeys[index].pack()
            guard candidateKeys?.contains(packedPrimaryKey) ?? true,
                  let coordinate = try coordinate(from: model),
                  matches(coordinate, constraint: constraint)
            else {
                continue
            }
            let score: Double
            if let reference,
               maximumDistance > 0 {
                let distance = CellDistanceCalculator.haversineDistance(
                    from: reference,
                    to: coordinate
                )
                score = 1.0 - distance / maximumDistance
            } else {
                score = 1.0
            }
            try output.appendDecodedModel(model, score: score)
        }
        return try output.finish()
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
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RetainedSpatialScanResult {
        let plan = try SpatialScanPlanner.plan(
            for: constraint,
            encoding: encoding,
            level: level,
            workMeter: workMeter
        )
        let scanner = SpatialCellScanner(indexSubspace: indexSubspace, encoding: encoding, level: level)
        return try await scanner.scanRetained(
            plan: plan,
            limit: nil,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    private func matches(
        _ coordinate: GeographicPoint,
        constraint: SpatialConstraint
    ) -> Bool {
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

    private func coordinate(
        from model: borrowing PersistedModel
    ) throws -> GeographicPoint? {
        guard let value = model.value(for: field) else {
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
        _ descriptor: IndexDescriptor
    ) throws -> (encoding: SpatialEncoding, level: Int) {
        let definition = descriptor.declaration.definition
        guard case .spatial(_, let encoding, let level) = definition else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: field.name,
                indexType: .spatial
            )
        }
        return (encoding, level)
    }
}
