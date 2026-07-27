// SpatialIndexMaintainer.swift
// SpatialIndexLayer - Spatial index maintainer
//
// Maintains spatial indexes using S2 or Morton encoding.

import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit

/// Spatial index maintainer
///
/// **Functionality**:
/// - Encode coordinates to spatial codes (S2 or Morton)
/// - Index spatial data for range queries
/// - Support 2D and 3D coordinates
///
/// **Index Structure**:
/// ```
/// Key: [indexSubspace][spatialCode][primaryKey]
/// Value: '' (empty)
/// ```
///
/// **Usage**:
/// ```swift
/// let maintainer = SpatialIndexMaintainer<Restaurant>(
///     index: locationIndex,
///     kind: SpatialIndexKind(encoding: .s2, level: 15),
///     subspace: spatialSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id")
/// )
/// ```
public struct SpatialIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let encoding: SpatialEncoding
    private let level: Int

    public init(
        index: Index,
        encoding: SpatialEncoding,
        level: Int,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.encoding = encoding
        self.level = level
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        if let oldItem = oldItem {
            if let oldKey = try buildIndexKey(for: oldItem) {
                try transaction.clear(key: oldKey)
            }
        }

        if let newItem = newItem {
            if let newKey = try buildIndexKey(for: newItem) {
                let value = try CoveringValueBuilder.build(for: newItem, index: index)
                try transaction.setValue(value, for: newKey)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        if let indexKey = try buildIndexKey(for: item, id: id) {
            let value = try CoveringValueBuilder.build(for: item, index: index)
            try transaction.setValue(value, for: indexKey)
        }
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        if let key = try buildIndexKey(for: item, id: id) {
            return [key]
        }
        return []
    }

    /// Search for items within a radius
    ///
    /// - Parameters:
    ///   - latitude: Center latitude
    ///   - longitude: Center longitude
    ///   - radiusMeters: Search radius in meters
    ///   - limit: Optional maximum number of results
    ///   - transaction: FDB transaction
    /// - Returns: Scan result with primary keys and optional limit reason
    public func searchRadius(
        latitude: Double,
        longitude: Double,
        radiusMeters: Double,
        limit: Int? = nil,
        transaction: any TransactionAccess
    ) async throws -> SpatialScanResult {
        let plan = try SpatialScanPlanner.plan(
            for: SpatialConstraint(
                type: .withinDistance(
                    center: try GeographicPoint(
                        latitude: latitude,
                        longitude: longitude
                    ),
                    radiusMeters: radiusMeters
                )
            ),
            encoding: encoding,
            level: level
        )

        let scanner = SpatialCellScanner(
            indexSubspace: subspace,
            encoding: encoding,
            level: level
        )

        let (keys, limitReason) = try await scanner.scan(
            plan: plan,
            limit: limit,
            transaction: transaction
        )

        return SpatialScanResult(keys: keys, limitReason: limitReason)
    }

    /// Search for items within a bounding box
    ///
    /// - Parameters:
    ///   - minLat: Minimum latitude
    ///   - minLon: Minimum longitude
    ///   - maxLat: Maximum latitude
    ///   - maxLon: Maximum longitude
    ///   - limit: Optional maximum number of results
    ///   - transaction: FDB transaction
    /// - Returns: Scan result with primary keys and optional limit reason
    public func searchBoundingBox(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        limit: Int? = nil,
        transaction: any TransactionAccess
    ) async throws -> SpatialScanResult {
        let plan = try SpatialScanPlanner.plan(
            for: SpatialConstraint(
                type: .withinBounds(
                    minLat: minLat,
                    minLon: minLon,
                    maxLat: maxLat,
                    maxLon: maxLon
                )
            ),
            encoding: encoding,
            level: level
        )

        let scanner = SpatialCellScanner(
            indexSubspace: subspace,
            encoding: encoding,
            level: level
        )

        let (keys, limitReason) = try await scanner.scan(
            plan: plan,
            limit: limit,
            transaction: transaction
        )

        return SpatialScanResult(keys: keys, limitReason: limitReason)
    }

    // MARK: - Private Methods

    /// Build index key for spatial data
    ///
    /// **Sparse index behavior**:
    /// If the coordinate field is nil, returns nil (no index entry).
    ///
    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> ByteString? {
        guard let expression = index.rootExpression as? FieldKeyExpression,
              let fieldNumber = Item.fieldNumber(for: expression.fieldName) else {
            throw SpatialIndexMaintenanceError.invalidFieldExpression(
                indexName: index.name
            )
        }
        guard let value = try item.persistedFieldValue(
            for: FieldIdentity(
                name: expression.fieldName,
                number: fieldNumber
            )
        ) else {
            throw SpatialIndexMaintenanceError.missingCoordinate(
                fieldName: expression.fieldName
            )
        }
        guard value != .null else {
            return nil
        }

        let point: GeographicPoint
        let height: Double?
        switch value {
        case .geographicPoint(let coordinate):
            point = coordinate
            height = nil
        case .geographicPosition(let coordinate):
            point = coordinate.point
            height = coordinate.ellipsoidalHeightInMeters
        default:
            throw SpatialIndexMaintenanceError.unsupportedCoordinateValue(
                fieldName: expression.fieldName
            )
        }

        let spatialCode = encodeSpatialCode(point: point, height: height)

        // Extract primary key
        let primaryKeyTuple: Tuple
        if let providedId = id {
            primaryKeyTuple = providedId
        } else {
            primaryKeyTuple = try DataAccess.extractId(from: item, using: idExpression)
        }

        var allElements: [any TupleElement] = [spatialCode]
        for i in 0..<primaryKeyTuple.count {
            if let element = primaryKeyTuple[i] {
                allElements.append(element)
            }
        }

        return try packAndValidate(Tuple(allElements))
    }

    private func encodeSpatialCode(
        point: GeographicPoint,
        height: Double?
    ) -> UInt64 {
        switch encoding {
        case .s2:
            return S2Geometry.encode(
                latitude: point.latitude,
                longitude: point.longitude,
                level: level
            )

        case .morton:
            let x = MortonCode.normalize(
                point.longitude,
                min: -180,
                max: 180
            )
            let y = MortonCode.normalize(
                point.latitude,
                min: -90,
                max: 90
            )
            if let height {
                let z = MortonCode.normalize(
                    height,
                    min: -1000,
                    max: 10000
                )
                return MortonCode.encode3D(x: x, y: y, z: z, level: level)
            }
            return MortonCode.encode2D(x: x, y: y, level: level)
        }
    }
}
