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
}
