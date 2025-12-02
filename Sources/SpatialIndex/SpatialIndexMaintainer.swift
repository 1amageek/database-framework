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
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys within the radius
    public func searchRadius(
        latitude: Double,
        longitude: Double,
        radiusMeters: Double,
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        // Get covering cells for the search area
        let coveringCells = S2Geometry.getCoveringCells(
            latitude: latitude,
            longitude: longitude,
            radiusMeters: radiusMeters,
            level: level
        )

        var results: [[any TupleElement]] = []

        for cellId in coveringCells {
            let cellTuple = Tuple(cellId)
            let cellSubspace = subspace.subspace(cellTuple)
            let (begin, end) = cellSubspace.range()

            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(begin),
                endSelector: .firstGreaterOrEqual(end),
                snapshot: true
            )

            for try await (key, _) in sequence {
                guard cellSubspace.contains(key) else { break }

                // Extract primary key from the key - skip corrupt entries
                guard let keyTuple = try? cellSubspace.unpack(key),
                      let elements = try? Tuple.unpack(from: keyTuple.pack()) else {
                    continue
                }
                results.append(elements)
            }
        }

        return results
    }

    /// Search for items within a bounding box
    ///
    /// - Parameters:
    ///   - minLat: Minimum latitude
    ///   - minLon: Minimum longitude
    ///   - maxLat: Maximum latitude
    ///   - maxLon: Maximum longitude
    ///   - transaction: FDB transaction
    /// - Returns: Array of primary keys within the bounding box
    public func searchBoundingBox(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        // Get covering cells for the bounding box
        let coveringCells = S2Geometry.getCoveringCellsForBox(
            minLat: minLat,
            minLon: minLon,
            maxLat: maxLat,
            maxLon: maxLon,
            level: level
        )

        var results: [[any TupleElement]] = []

        for cellId in coveringCells {
            let cellTuple = Tuple(cellId)
            let cellSubspace = subspace.subspace(cellTuple)
            let (begin, end) = cellSubspace.range()

            let sequence = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(begin),
                endSelector: .firstGreaterOrEqual(end),
                snapshot: true
            )

            for try await (key, _) in sequence {
                guard cellSubspace.contains(key) else { break }

                // Skip corrupt entries
                guard let keyTuple = try? cellSubspace.unpack(key),
                      let elements = try? Tuple.unpack(from: keyTuple.pack()) else {
                    continue
                }
                results.append(elements)
            }
        }

        return results
    }

    // MARK: - Private Methods

    /// Build index key for spatial data
    ///
    /// **KeyPath Optimization**:
    /// When `index.keyPaths` is available, uses direct KeyPath subscript access
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    private func buildIndexKey(for item: Item, id: Tuple? = nil) throws -> [UInt8]? {
        // Use optimized DataAccess method - KeyPath when available, falls back to KeyExpression
        let fieldValues = try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )

        guard fieldValues.count >= 2 else {
            return nil
        }

        var coordinates: [Double] = []
        for value in fieldValues {
            if let d = value as? Double {
                coordinates.append(d)
            } else if let f = value as? Float {
                coordinates.append(Double(f))
            } else if let i = value as? Int64 {
                coordinates.append(Double(i))
            } else if let i = value as? Int {
                coordinates.append(Double(i))
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
