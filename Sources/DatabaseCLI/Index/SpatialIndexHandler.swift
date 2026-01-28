import Foundation
import FoundationDB

/// Handler for spatial indexes (geo queries with S2/Morton encoding)
///
/// Storage layout:
/// - cells/<cellId>/<id> = empty
public struct SpatialIndexHandler: IndexHandler, Sendable {
    public let indexDefinition: IndexDefinition
    public let schemaName: String

    public init(indexDefinition: IndexDefinition, schemaName: String) {
        self.indexDefinition = indexDefinition
        self.schemaName = schemaName
    }

    public func updateIndex(
        oldItem: [String: Any]?,
        newItem: [String: Any]?,
        id: String,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws {
        guard let config = indexDefinition.config,
              case .spatial(let spatialConfig) = config else {
            return
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .spatial,
            indexName: indexDefinition.name
        )
        let cellsSubspace = indexSubspace.subspace(Tuple(["cells"]))

        let oldCoord = extractCoordinate(from: oldItem, config: spatialConfig)
        let newCoord = extractCoordinate(from: newItem, config: spatialConfig)

        // Remove old cell entries
        if let coord = oldCoord {
            let oldCells = computeCells(coord, config: spatialConfig)
            for cellId in oldCells {
                let key = cellsSubspace.pack(Tuple([cellId, id]))
                transaction.clear(key: key)
            }
        }

        // Add new cell entries
        if let coord = newCoord {
            let newCells = computeCells(coord, config: spatialConfig)
            for cellId in newCells {
                let key = cellsSubspace.pack(Tuple([cellId, id]))
                transaction.setValue([], for: key)
            }
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let config = indexDefinition.config,
              case .spatial(let spatialConfig) = config else {
            return []
        }

        guard let spatialQuery = query as? SpatialQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .spatial,
            indexName: indexDefinition.name
        )
        let cellsSubspace = indexSubspace.subspace(Tuple(["cells"]))

        var candidateIds = Set<String>()

        switch spatialQuery {
        case .near(let lat, let lon, let radiusMeters):
            // Get covering cells for the search area
            let coveringCells = computeCoveringCells(
                center: (lat, lon),
                radiusMeters: radiusMeters,
                config: spatialConfig
            )

            for cellId in coveringCells {
                let cellSubspace = cellsSubspace.subspace(Tuple([cellId]))
                let (begin, end) = cellSubspace.range()

                let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
                for try await (key, _) in sequence {
                    if let tuple = try? cellSubspace.unpack(key),
                       let id = tuple[0] as? String {
                        candidateIds.insert(id)
                    }
                }
            }

        case .bbox(let minLat, let minLon, let maxLat, let maxLon):
            // Get covering cells for the bounding box
            let coveringCells = computeBBoxCells(
                minLat: minLat, minLon: minLon,
                maxLat: maxLat, maxLon: maxLon,
                config: spatialConfig
            )

            for cellId in coveringCells {
                let cellSubspace = cellsSubspace.subspace(Tuple([cellId]))
                let (begin, end) = cellSubspace.range()

                let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
                for try await (key, _) in sequence {
                    if let tuple = try? cellSubspace.unpack(key),
                       let id = tuple[0] as? String {
                        candidateIds.insert(id)
                    }
                }
            }
        }

        return Array(candidateIds.prefix(limit))
    }

    // MARK: - Coordinate Extraction

    private func extractCoordinate(from item: [String: Any]?, config: SpatialIndexConfig) -> (lat: Double, lon: Double)? {
        guard let item = item,
              let lat = item[config.latField] as? Double,
              let lon = item[config.lonField] as? Double else {
            return nil
        }
        return (lat, lon)
    }

    // MARK: - Cell Computation (Simplified S2-like encoding)

    /// Compute cell IDs for a point at the configured level
    private func computeCells(_ coord: (lat: Double, lon: Double), config: SpatialIndexConfig) -> [String] {
        switch config.encoding {
        case .s2:
            return [computeS2Cell(lat: coord.lat, lon: coord.lon, level: config.level)]
        case .morton:
            return [computeMortonCell(lat: coord.lat, lon: coord.lon, level: config.level)]
        }
    }

    /// Compute covering cells for a circular area
    private func computeCoveringCells(
        center: (lat: Double, lon: Double),
        radiusMeters: Double,
        config: SpatialIndexConfig
    ) -> [String] {
        // Simplified: compute 9 cells around center (3x3 grid)
        let degreeOffset = radiusMeters / 111_000.0 // ~111km per degree

        var cells: [String] = []
        for latOffset in [-1, 0, 1] {
            for lonOffset in [-1, 0, 1] {
                let lat = center.lat + Double(latOffset) * degreeOffset
                let lon = center.lon + Double(lonOffset) * degreeOffset
                cells.append(contentsOf: computeCells((lat, lon), config: config))
            }
        }

        return Array(Set(cells))
    }

    /// Compute covering cells for a bounding box
    private func computeBBoxCells(
        minLat: Double, minLon: Double,
        maxLat: Double, maxLon: Double,
        config: SpatialIndexConfig
    ) -> [String] {
        // Compute cells at corners and center
        let latStep = (maxLat - minLat) / 2
        let lonStep = (maxLon - minLon) / 2

        var cells: [String] = []
        for latMult in [0.0, 1.0, 2.0] {
            for lonMult in [0.0, 1.0, 2.0] {
                let lat = minLat + latMult * latStep
                let lon = minLon + lonMult * lonStep
                cells.append(contentsOf: computeCells((lat, lon), config: config))
            }
        }

        return Array(Set(cells))
    }

    /// Simplified S2-like cell computation
    private func computeS2Cell(lat: Double, lon: Double, level: Int) -> String {
        // Normalize to 0-1 range
        let normalizedLat = (lat + 90) / 180
        let normalizedLon = (lon + 180) / 360

        // Compute cell coordinates at the given level
        let cellsPerSide = 1 << level
        let latCell = Int(normalizedLat * Double(cellsPerSide)) % cellsPerSide
        let lonCell = Int(normalizedLon * Double(cellsPerSide)) % cellsPerSide

        // Interleave bits for spatial locality
        let cellId = interleave(latCell, lonCell)
        return String(format: "s2_%d_%016llx", level, cellId)
    }

    /// Simplified Morton encoding
    private func computeMortonCell(lat: Double, lon: Double, level: Int) -> String {
        let normalizedLat = (lat + 90) / 180
        let normalizedLon = (lon + 180) / 360

        let cellsPerSide = 1 << level
        let latCell = Int(normalizedLat * Double(cellsPerSide)) % cellsPerSide
        let lonCell = Int(normalizedLon * Double(cellsPerSide)) % cellsPerSide

        let morton = interleave(latCell, lonCell)
        return String(format: "m_%d_%016llx", level, morton)
    }

    /// Interleave bits of two integers (Morton/Z-order curve)
    private func interleave(_ x: Int, _ y: Int) -> UInt64 {
        var result: UInt64 = 0
        for i in 0..<32 {
            result |= UInt64((x >> i) & 1) << (2 * i)
            result |= UInt64((y >> i) & 1) << (2 * i + 1)
        }
        return result
    }
}

// MARK: - Spatial Query

public enum SpatialQuery {
    case near(lat: Double, lon: Double, radiusMeters: Double)
    case bbox(minLat: Double, minLon: Double, maxLat: Double, maxLon: Double)
}
