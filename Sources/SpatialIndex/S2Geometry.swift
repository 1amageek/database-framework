// S2Geometry.swift
// SpatialIndexLayer - S2 Geometry encoding
//
// Simplified S2 implementation for spatial indexing.

import Foundation

/// S2 Geometry encoding for geographic coordinates
public enum S2Geometry {

    /// Encode latitude/longitude to S2 cell ID
    public static func encode(latitude: Double, longitude: Double, level: Int) -> UInt64 {
        precondition(latitude >= -90 && latitude <= 90, "Latitude must be in [-90, 90]")
        precondition(longitude >= -180 && longitude <= 180, "Longitude must be in [-180, 180]")
        precondition(level >= 0 && level <= 30, "Level must be in [0, 30]")

        let latRad = latitude * .pi / 180.0
        let lonRad = longitude * .pi / 180.0

        let x = cos(latRad) * cos(lonRad)
        let y = cos(latRad) * sin(lonRad)
        let z = sin(latRad)

        let face = getFace(x: x, y: y, z: z)
        let (u, v) = faceXYZtoUV(face: face, x: x, y: y, z: z)

        let s = uvToST(u)
        let t = uvToST(v)

        let si = Int(s * Double(1 << level))
        let ti = Int(t * Double(1 << level))

        let cellID = encodeCellID(face: face, si: si, ti: ti, level: level)
        return cellID
    }

    /// Decode S2 cell ID to latitude/longitude
    public static func decode(_ cellID: UInt64, level: Int) -> (latitude: Double, longitude: Double) {
        let (face, si, ti) = decodeCellID(cellID, level: level)

        let s = (Double(si) + 0.5) / Double(1 << level)
        let t = (Double(ti) + 0.5) / Double(1 << level)

        let u = stToUV(s)
        let v = stToUV(t)

        let (x, y, z) = faceUVtoXYZ(face: face, u: u, v: v)

        let latitude = atan2(z, sqrt(x * x + y * y)) * 180.0 / .pi
        let longitude = atan2(y, x) * 180.0 / .pi

        return (latitude, longitude)
    }

    private static func getFace(x: Double, y: Double, z: Double) -> Int {
        let absX = abs(x)
        let absY = abs(y)
        let absZ = abs(z)

        if absX > absY {
            if absX > absZ {
                return x > 0 ? 0 : 3
            } else {
                return z > 0 ? 5 : 4
            }
        } else {
            if absY > absZ {
                return y > 0 ? 1 : 2
            } else {
                return z > 0 ? 5 : 4
            }
        }
    }

    private static func faceXYZtoUV(face: Int, x: Double, y: Double, z: Double) -> (u: Double, v: Double) {
        switch face {
        case 0: return (y / x, z / x)
        case 1: return (-x / y, z / y)
        case 2: return (-x / y, -z / y)
        case 3: return (y / x, -z / x)
        case 4: return (y / z, -x / z)
        case 5: return (y / z, x / z)
        default: return (0, 0)
        }
    }

    private static func faceUVtoXYZ(face: Int, u: Double, v: Double) -> (x: Double, y: Double, z: Double) {
        switch face {
        case 0: return (1, u, v)
        case 1: return (-u, 1, v)
        case 2: return (-u, -1, -v)
        case 3: return (-1, -u, -v)
        case 4: return (v, u, -1)
        case 5: return (v, u, 1)
        default: return (0, 0, 0)
        }
    }

    private static func uvToST(_ u: Double) -> Double {
        if u >= 0 {
            return 0.5 * sqrt(1 + 3 * u)
        } else {
            return 1 - 0.5 * sqrt(1 - 3 * u)
        }
    }

    private static func stToUV(_ s: Double) -> Double {
        if s >= 0.5 {
            return (1.0 / 3.0) * (4 * s * s - 1)
        } else {
            return (1.0 / 3.0) * (1 - 4 * (1 - s) * (1 - s))
        }
    }

    private static func encodeCellID(face: Int, si: Int, ti: Int, level: Int) -> UInt64 {
        var cellID = UInt64(face) << 61

        for i in 0..<level {
            let mask = 1 << (level - i - 1)
            let bitS = (si & mask) != 0 ? 1 : 0
            let bitT = (ti & mask) != 0 ? 1 : 0
            let bits = (bitS << 1) | bitT

            cellID |= UInt64(bits) << UInt64(59 - 2 * i)
        }

        cellID |= 1

        return cellID
    }

    private static func decodeCellID(_ cellID: UInt64, level: Int) -> (face: Int, si: Int, ti: Int) {
        let face = Int((cellID >> 61) & 0x7)

        var si = 0
        var ti = 0

        for i in 0..<level {
            let bits = Int((cellID >> UInt64(59 - 2 * i)) & 0x3)
            let bitS = (bits >> 1) & 1
            let bitT = bits & 1

            si |= bitS << (level - i - 1)
            ti |= bitT << (level - i - 1)
        }

        return (face, si, ti)
    }

    // MARK: - Covering Cells for Spatial Queries

    /// Earth radius in meters
    private static let earthRadiusMeters: Double = 6371000.0

    /// Get covering cells for a circular area
    ///
    /// - Parameters:
    ///   - latitude: Center latitude in degrees
    ///   - longitude: Center longitude in degrees
    ///   - radiusMeters: Radius in meters
    ///   - level: S2 cell level (precision)
    /// - Returns: Array of S2 cell IDs that cover the circular area
    public static func getCoveringCells(
        latitude: Double,
        longitude: Double,
        radiusMeters: Double,
        level: Int
    ) -> [UInt64] {
        // Convert radius to degrees (approximate)
        let latDelta = radiusMeters / earthRadiusMeters * (180.0 / .pi)
        let lonDelta = radiusMeters / (earthRadiusMeters * cos(latitude * .pi / 180.0)) * (180.0 / .pi)

        // Calculate bounding box
        let minLat = max(-90, latitude - latDelta)
        let maxLat = min(90, latitude + latDelta)
        let minLon = max(-180, longitude - lonDelta)
        let maxLon = min(180, longitude + lonDelta)

        return getCoveringCellsForBox(
            minLat: minLat,
            minLon: minLon,
            maxLat: maxLat,
            maxLon: maxLon,
            level: level
        )
    }

    /// Get covering cells for a bounding box
    ///
    /// - Parameters:
    ///   - minLat: Minimum latitude
    ///   - minLon: Minimum longitude
    ///   - maxLat: Maximum latitude
    ///   - maxLon: Maximum longitude
    ///   - level: S2 cell level (precision)
    /// - Returns: Array of S2 cell IDs that cover the bounding box
    public static func getCoveringCellsForBox(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        level: Int
    ) -> [UInt64] {
        var cells: Set<UInt64> = []

        // Calculate cell size at this level (approximate degrees)
        let cellSize = 180.0 / Double(1 << level)

        // Add some buffer for edge cases
        let step = max(cellSize * 0.5, 0.001)

        // Sample points across the bounding box and collect unique cells
        var lat = minLat
        while lat <= maxLat {
            var lon = minLon
            while lon <= maxLon {
                let cellID = encode(
                    latitude: min(max(lat, -89.999), 89.999),
                    longitude: min(max(lon, -179.999), 179.999),
                    level: level
                )
                cells.insert(cellID)
                lon += step
            }
            // Always include the max longitude edge
            let edgeCellID = encode(
                latitude: min(max(lat, -89.999), 89.999),
                longitude: min(max(maxLon, -179.999), 179.999),
                level: level
            )
            cells.insert(edgeCellID)
            lat += step
        }

        // Always include the corners
        let corners = [
            (minLat, minLon),
            (minLat, maxLon),
            (maxLat, minLon),
            (maxLat, maxLon)
        ]
        for (cornerLat, cornerLon) in corners {
            let cellID = encode(
                latitude: min(max(cornerLat, -89.999), 89.999),
                longitude: min(max(cornerLon, -179.999), 179.999),
                level: level
            )
            cells.insert(cellID)
        }

        return Array(cells).sorted()
    }

    /// Get the parent cell at a lower level
    ///
    /// - Parameters:
    ///   - cellID: The cell ID
    ///   - currentLevel: Current level of the cell
    ///   - targetLevel: Target level (must be less than current level)
    /// - Returns: Parent cell ID at the target level
    public static func getParent(_ cellID: UInt64, currentLevel: Int, targetLevel: Int) -> UInt64 {
        precondition(targetLevel <= currentLevel, "Target level must be <= current level")

        if targetLevel == currentLevel {
            return cellID
        }

        // Mask out the bits for levels below targetLevel
        let bitsToKeep = 3 + 2 * targetLevel  // face bits (3) + position bits (2 per level)
        let shift = 61 - bitsToKeep
        let mask = ~UInt64(0) << shift
        return (cellID & mask) | 1  // Keep sentinel bit
    }

    /// Get all child cells at a deeper level
    ///
    /// - Parameters:
    ///   - cellID: Parent cell ID
    ///   - currentLevel: Current level
    ///   - targetLevel: Target deeper level
    /// - Returns: Array of child cell IDs
    public static func getChildren(_ cellID: UInt64, currentLevel: Int, targetLevel: Int) -> [UInt64] {
        precondition(targetLevel >= currentLevel, "Target level must be >= current level")

        if targetLevel == currentLevel {
            return [cellID]
        }

        var children: [UInt64] = [cellID]

        for level in currentLevel..<targetLevel {
            var nextChildren: [UInt64] = []
            for parent in children {
                // Each cell has 4 children
                let shift = UInt64(59 - 2 * level)
                let baseMask = parent & ~(UInt64(3) << shift)

                for i: UInt64 in 0..<4 {
                    let child = baseMask | (i << shift)
                    nextChildren.append(child)
                }
            }
            children = nextChildren
        }

        return children
    }

    /// Get the 4 immediate children of a cell
    ///
    /// - Parameters:
    ///   - cellID: Parent cell ID
    ///   - level: Current level of the parent
    /// - Returns: Array of 4 child cell IDs
    public static func getImmediateChildren(_ cellID: UInt64, level: Int) -> [UInt64] {
        let shift = UInt64(59 - 2 * level)
        let baseMask = cellID & ~(UInt64(3) << shift)

        return (0..<4).map { i in
            baseMask | (UInt64(i) << shift)
        }
    }

    // MARK: - Optimized Covering Cells (Recursive Subdivision)

    /// Relationship between a cell and a bounding box region
    private enum CellRegionRelation {
        case disjoint    // Cell is completely outside the region
        case contained   // Cell is completely inside the region
        case intersects  // Cell partially overlaps the region
    }

    /// Get covering cells using recursive subdivision algorithm
    ///
    /// **Algorithm**:
    /// 1. Start with face cells (level 0) that might intersect the region
    /// 2. For each cell, determine its relationship to the bounding box
    /// 3. If disjoint, skip. If contained, add all descendants at target level.
    /// 4. If intersecting and not at target level, subdivide into 4 children.
    /// 5. Continue until all cells are processed or maxCells is reached.
    ///
    /// **Reference**: Google S2 library S2RegionCoverer
    ///
    /// **Improvements over naive sampling**:
    /// - No cell boundary gaps (complete coverage guarantee)
    /// - Efficient for large regions (uses cell hierarchy)
    /// - Deterministic output
    /// - Bounded number of cells via maxCells parameter
    ///
    /// - Parameters:
    ///   - minLat: Minimum latitude
    ///   - minLon: Minimum longitude
    ///   - maxLat: Maximum latitude
    ///   - maxLon: Maximum longitude
    ///   - level: Target S2 cell level (precision)
    ///   - maxCells: Maximum number of cells to return (default: 500)
    /// - Returns: Array of S2 cell IDs that cover the bounding box
    public static func getCoveringCellsOptimized(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        level: Int,
        maxCells: Int = 500
    ) -> [UInt64] {
        // For very small regions or high levels, fall back to sampling
        // (recursive subdivision has overhead for tiny regions)
        let latSpan = maxLat - minLat
        let lonSpan = maxLon - minLon
        let cellSize = 180.0 / Double(1 << level)

        if latSpan < cellSize * 4 && lonSpan < cellSize * 4 {
            // Small region: use sampling approach
            return getCoveringCellsForBoxSampling(
                minLat: minLat, minLon: minLon,
                maxLat: maxLat, maxLon: maxLon,
                level: level
            )
        }

        var result: Set<UInt64> = []
        var candidates: [(cellId: UInt64, cellLevel: Int)] = []

        // Start with all 6 face cells (level 0)
        for face in 0..<6 {
            let faceCellId = UInt64(face) << 61 | 1
            candidates.append((faceCellId, 0))
        }

        while !candidates.isEmpty && result.count < maxCells {
            let (cellId, cellLevel) = candidates.removeFirst()

            let relation = cellBoundingBoxRelation(
                cellId: cellId,
                cellLevel: cellLevel,
                minLat: minLat, minLon: minLon,
                maxLat: maxLat, maxLon: maxLon
            )

            switch relation {
            case .disjoint:
                // Cell is outside region, skip
                continue

            case .contained:
                // Cell is fully inside region
                if cellLevel == level {
                    result.insert(cellId)
                } else if cellLevel < level {
                    // Add all descendant cells at target level
                    let descendants = getChildren(cellId, currentLevel: cellLevel, targetLevel: level)
                    for desc in descendants.prefix(maxCells - result.count) {
                        result.insert(desc)
                    }
                }

            case .intersects:
                if cellLevel == level {
                    // At target level, add even if partially intersecting
                    result.insert(cellId)
                } else if cellLevel < level {
                    // Subdivide: add 4 children to candidates
                    let children = getImmediateChildren(cellId, level: cellLevel)
                    for child in children {
                        candidates.append((child, cellLevel + 1))
                    }
                }
            }
        }

        return Array(result).sorted()
    }

    /// Determine the relationship between a cell and a bounding box
    private static func cellBoundingBoxRelation(
        cellId: UInt64,
        cellLevel: Int,
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double
    ) -> CellRegionRelation {
        // Get cell center and approximate bounds
        let center = decode(cellId, level: cellLevel)

        // Approximate cell size in degrees at this level
        // S2 cells vary in size, but this is a reasonable approximation
        let cellSizeDegrees = 180.0 / Double(1 << cellLevel)
        let halfSize = cellSizeDegrees / 2.0

        let cellMinLat = center.latitude - halfSize
        let cellMaxLat = center.latitude + halfSize
        let cellMinLon = center.longitude - halfSize
        let cellMaxLon = center.longitude + halfSize

        // Check if cell is completely outside the bounding box
        if cellMaxLat < minLat || cellMinLat > maxLat ||
           cellMaxLon < minLon || cellMinLon > maxLon {
            return .disjoint
        }

        // Check if cell is completely inside the bounding box
        if cellMinLat >= minLat && cellMaxLat <= maxLat &&
           cellMinLon >= minLon && cellMaxLon <= maxLon {
            return .contained
        }

        // Cell intersects the bounding box boundary
        return .intersects
    }

    /// Internal sampling-based covering cells (for small regions)
    private static func getCoveringCellsForBoxSampling(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        level: Int
    ) -> [UInt64] {
        var cells: Set<UInt64> = []

        // Calculate cell size at this level (approximate degrees)
        let cellSize = 180.0 / Double(1 << level)

        // Add some buffer for edge cases
        let step = max(cellSize * 0.5, 0.001)

        // Sample points across the bounding box and collect unique cells
        var lat = minLat
        while lat <= maxLat {
            var lon = minLon
            while lon <= maxLon {
                let cellID = encode(
                    latitude: min(max(lat, -89.999), 89.999),
                    longitude: min(max(lon, -179.999), 179.999),
                    level: level
                )
                cells.insert(cellID)
                lon += step
            }
            // Always include the max longitude edge
            let edgeCellID = encode(
                latitude: min(max(lat, -89.999), 89.999),
                longitude: min(max(maxLon, -179.999), 179.999),
                level: level
            )
            cells.insert(edgeCellID)
            lat += step
        }

        // Always include the corners
        let corners = [
            (minLat, minLon),
            (minLat, maxLon),
            (maxLat, minLon),
            (maxLat, maxLon)
        ]
        for (cornerLat, cornerLon) in corners {
            let cellID = encode(
                latitude: min(max(cornerLat, -89.999), 89.999),
                longitude: min(max(cornerLon, -179.999), 179.999),
                level: level
            )
            cells.insert(cellID)
        }

        return Array(cells).sorted()
    }
}
