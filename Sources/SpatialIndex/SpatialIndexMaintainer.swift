// SpatialIndexMaintainer.swift
// SpatialIndexLayer - Spatial index maintainer
//
// Maintains spatial indexes using S2 or Morton encoding.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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
        transaction: any TransactionProtocol
    ) async throws {
        if let oldItem = oldItem {
            if let oldKey = try buildIndexKey(for: oldItem) {
                transaction.clear(key: oldKey)
            }
        }

        if let newItem = newItem {
            if let newKey = try buildIndexKey(for: newItem) {
                transaction.setValue([], for: newKey)
            }
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws {
        if let indexKey = try buildIndexKey(for: item, id: id) {
            transaction.setValue([], for: indexKey)
        }
    }

    /// Compute expected index keys for this item
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [FDB.Bytes] {
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
        transaction: any TransactionProtocol
    ) async throws -> SpatialScanResult {
        // Get covering cells for the search area
        let coveringCells = S2Geometry.getCoveringCells(
            latitude: latitude,
            longitude: longitude,
            radiusMeters: radiusMeters,
            level: level
        )

        // Use SpatialCellScanner for efficient scanning
        let scanner = SpatialCellScanner(
            indexSubspace: subspace,
            encoding: encoding,
            level: level
        )

        let (keys, limitReason) = try await scanner.scanCells(
            cellIds: coveringCells,
            limit: limit,
            transaction: transaction
        )

        return SpatialScanResult(keys: keys, limitReason: limitReason)
    }

    /// Search for items within a radius (legacy API for backwards compatibility)
    ///
    /// - Parameters:
    ///   - latitude: Center latitude
    ///   - longitude: Center longitude
    ///   - radiusMeters: Search radius in meters
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys within the radius
    @available(*, deprecated, message: "Use searchRadius with SpatialScanResult instead")
    public func searchRadiusLegacy(
        latitude: Double,
        longitude: Double,
        radiusMeters: Double,
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        let result = try await searchRadius(
            latitude: latitude,
            longitude: longitude,
            radiusMeters: radiusMeters,
            limit: nil,
            transaction: transaction
        )

        return result.keys.map { tuple in
            var elements: [any TupleElement] = []
            for i in 0..<tuple.count {
                if let el = tuple[i] { elements.append(el) }
            }
            return elements
        }
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
        transaction: any TransactionProtocol
    ) async throws -> SpatialScanResult {
        // Get covering cells for the bounding box
        let coveringCells = S2Geometry.getCoveringCellsForBox(
            minLat: minLat,
            minLon: minLon,
            maxLat: maxLat,
            maxLon: maxLon,
            level: level
        )

        // Use SpatialCellScanner for efficient scanning
        let scanner = SpatialCellScanner(
            indexSubspace: subspace,
            encoding: encoding,
            level: level
        )

        let (keys, limitReason) = try await scanner.scanCells(
            cellIds: coveringCells,
            limit: limit,
            transaction: transaction
        )

        return SpatialScanResult(keys: keys, limitReason: limitReason)
    }

    /// Search for items within a bounding box (legacy API for backwards compatibility)
    ///
    /// - Parameters:
    ///   - minLat: Minimum latitude
    ///   - minLon: Minimum longitude
    ///   - maxLat: Maximum latitude
    ///   - maxLon: Maximum longitude
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys within the bounding box
    @available(*, deprecated, message: "Use searchBoundingBox with SpatialScanResult instead")
    public func searchBoundingBoxLegacy(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        let result = try await searchBoundingBox(
            minLat: minLat,
            minLon: minLon,
            maxLat: maxLat,
            maxLon: maxLon,
            limit: nil,
            transaction: transaction
        )

        return result.keys.map { tuple in
            var elements: [any TupleElement] = []
            for i in 0..<tuple.count {
                if let el = tuple[i] { elements.append(el) }
            }
            return elements
        }
    }

    // MARK: - Private Methods

    /// Build index key for spatial data
    ///
    /// **Sparse index behavior**:
    /// If the coordinate field is nil, returns nil (no index entry).
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> [UInt8]? {
        // Use optimized DataAccess method - KeyPath when available, falls back to KeyExpression
        // Sparse index: if coordinate field is nil, return nil (no index entry)
        let fieldValues: [any TupleElement]
        do {
            fieldValues = try DataAccess.evaluateIndexFields(
                from: item,
                keyPaths: index.keyPaths,
                expression: index.rootExpression
            )
        } catch DataAccessError.nilValueCannotBeIndexed {
            // Sparse index: nil coordinates are not indexed
            return nil
        }

        guard fieldValues.count >= 2 else {
            return nil
        }

        var coordinates: [Double] = []
        for value in fieldValues {
            if let d = TypeConversion.asDouble(value) {
                coordinates.append(d)
            } else {
                throw SpatialIndexError.invalidCoordinates("Spatial coordinates must be numeric")
            }
        }

        let spatialCode = try encodeSpatialCode(coordinates: coordinates)

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

    private func encodeSpatialCode(coordinates: [Double]) throws -> UInt64 {
        switch encoding {
        case .s2:
            guard coordinates.count == 2 else {
                throw SpatialIndexError.invalidCoordinates("S2 encoding requires 2 coordinates (latitude, longitude)")
            }
            return S2Geometry.encode(latitude: coordinates[0], longitude: coordinates[1], level: level)

        case .morton:
            if coordinates.count == 2 {
                let x = MortonCode.normalize(coordinates[0], min: -180, max: 180)
                let y = MortonCode.normalize(coordinates[1], min: -90, max: 90)
                return MortonCode.encode2D(x: x, y: y, level: level)
            } else if coordinates.count == 3 {
                let x = MortonCode.normalize(coordinates[0], min: -180, max: 180)
                let y = MortonCode.normalize(coordinates[1], min: -90, max: 90)
                let z = MortonCode.normalize(coordinates[2], min: -1000, max: 10000)
                return MortonCode.encode3D(x: x, y: y, z: z, level: level)
            } else {
                throw SpatialIndexError.invalidCoordinates("Morton encoding requires 2 or 3 coordinates")
            }
        }
    }
}
