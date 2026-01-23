// WindingNumber.swift
// SpatialIndex - Robust point-in-polygon test using Winding Number algorithm
//
// Reference: Hormann, K. & Agathos, A. "The Point in Polygon Problem for Arbitrary Polygons",
// Computational Geometry, 2001

import Foundation
import DatabaseEngine
import Spatial

/// Winding Number algorithm for robust point-in-polygon testing
///
/// **Why Winding Number over Ray Casting**:
/// - Ray Casting fails on self-intersecting polygons
/// - Winding Number correctly handles complex polygons
/// - Better numerical stability at polygon edges
///
/// **Algorithm**:
/// The winding number counts how many times the polygon winds around the point.
/// - winding number = 0 → point is outside
/// - winding number ≠ 0 → point is inside
///
/// **Complexity**: O(n) where n = number of polygon vertices
///
/// **Reference**: Hormann & Agathos (2001)
public struct WindingNumber: Sendable {

    /// Check if a point is inside a polygon using the Winding Number algorithm
    ///
    /// **Algorithm**:
    /// For each edge of the polygon:
    /// 1. If edge crosses the upward ray from point, increment winding if crossing from left
    /// 2. If edge crosses the downward ray from point, decrement winding if crossing from right
    ///
    /// - Parameters:
    ///   - point: Point to test
    ///   - polygon: Array of polygon vertices
    /// - Returns: true if point is inside the polygon
    public static func isPointInPolygon(
        point: GeoPoint,
        polygon: [GeoPoint]
    ) -> Bool {
        guard polygon.count >= 3 else { return false }
        let wn = windingNumber(point: point, polygon: polygon)
        return wn != 0
    }

    /// Calculate the winding number of a point with respect to a polygon
    ///
    /// - Parameters:
    ///   - point: Test point
    ///   - polygon: Polygon vertices
    /// - Returns: Winding number (0 = outside, non-zero = inside)
    public static func windingNumber(
        point: GeoPoint,
        polygon: [GeoPoint]
    ) -> Int {
        guard polygon.count >= 3 else { return 0 }

        var wn = 0
        let n = polygon.count

        for i in 0..<n {
            let v1 = polygon[i]
            let v2 = polygon[(i + 1) % n]

            if v1.latitude <= point.latitude {
                // Start y <= P.y
                if v2.latitude > point.latitude {
                    // An upward crossing
                    if isLeft(p0: v1, p1: v2, p2: point) > 0 {
                        // P left of edge
                        wn += 1  // Have a valid up intersect
                    }
                }
            } else {
                // Start y > P.y (no test needed)
                if v2.latitude <= point.latitude {
                    // A downward crossing
                    if isLeft(p0: v1, p1: v2, p2: point) < 0 {
                        // P right of edge
                        wn -= 1  // Have a valid down intersect
                    }
                }
            }
        }

        return wn
    }

    /// Test if a point is left of, on, or right of an infinite line
    ///
    /// - Parameters:
    ///   - p0: First point on the line
    ///   - p1: Second point on the line
    ///   - p2: Point to test
    /// - Returns: > 0 for P2 left of the line, = 0 for on the line, < 0 for right
    private static func isLeft(p0: GeoPoint, p1: GeoPoint, p2: GeoPoint) -> Double {
        return (p1.longitude - p0.longitude) * (p2.latitude - p0.latitude) -
               (p2.longitude - p0.longitude) * (p1.latitude - p0.latitude)
    }

    /// Check if a point is inside a polygon with holes
    ///
    /// A point is inside if:
    /// 1. It is inside the exterior ring
    /// 2. It is NOT inside any of the holes
    ///
    /// **Conventions**:
    /// - Exterior ring: counter-clockwise (positive area)
    /// - Holes: clockwise (negative area)
    ///
    /// - Parameters:
    ///   - point: Point to test
    ///   - exterior: Exterior polygon ring (counter-clockwise)
    ///   - holes: Array of hole polygons (clockwise)
    /// - Returns: true if point is inside exterior but not inside any hole
    public static func isPointInPolygonWithHoles(
        point: GeoPoint,
        exterior: [GeoPoint],
        holes: [[GeoPoint]]
    ) -> Bool {
        // First check if inside exterior
        guard isPointInPolygon(point: point, polygon: exterior) else {
            return false
        }

        // Check if inside any hole
        for hole in holes {
            if isPointInPolygon(point: point, polygon: hole) {
                return false
            }
        }

        return true
    }

    /// Check if a polygon is wound counter-clockwise (positive area)
    ///
    /// Uses the shoelace formula to calculate signed area.
    /// - Positive area → counter-clockwise
    /// - Negative area → clockwise
    ///
    /// - Parameter polygon: Polygon vertices
    /// - Returns: true if counter-clockwise
    public static func isCounterClockwise(_ polygon: [GeoPoint]) -> Bool {
        return signedArea(polygon) > 0
    }

    /// Calculate the signed area of a polygon
    ///
    /// **Shoelace Formula**:
    /// Area = 1/2 × Σ(x[i] × y[i+1] - x[i+1] × y[i])
    ///
    /// - Parameter polygon: Polygon vertices
    /// - Returns: Signed area (positive = CCW, negative = CW)
    public static func signedArea(_ polygon: [GeoPoint]) -> Double {
        guard polygon.count >= 3 else { return 0 }

        var area: Double = 0
        let n = polygon.count

        for i in 0..<n {
            let j = (i + 1) % n
            area += polygon[i].longitude * polygon[j].latitude
            area -= polygon[j].longitude * polygon[i].latitude
        }

        return area / 2.0
    }

    /// Ensure polygon is wound in the correct direction
    ///
    /// - Parameters:
    ///   - polygon: Polygon vertices
    ///   - counterClockwise: Desired winding direction (true = CCW, false = CW)
    /// - Returns: Polygon with correct winding (may be reversed)
    public static func ensureWinding(
        _ polygon: [GeoPoint],
        counterClockwise: Bool
    ) -> [GeoPoint] {
        let isCCW = isCounterClockwise(polygon)
        if isCCW == counterClockwise {
            return polygon
        } else {
            return polygon.reversed()
        }
    }
}

// MARK: - Polygon with Holes

/// A polygon with optional interior holes
///
/// **Winding Conventions**:
/// - Exterior ring: counter-clockwise (CCW)
/// - Holes: clockwise (CW)
///
/// This is the standard convention used by GeoJSON, PostGIS, and most GIS systems.
///
/// **Usage**:
/// ```swift
/// let exterior = [
///     GeoPoint(0, 0), GeoPoint(10, 0), GeoPoint(10, 10), GeoPoint(0, 10)
/// ]
/// let hole = [
///     GeoPoint(2, 2), GeoPoint(2, 8), GeoPoint(8, 8), GeoPoint(8, 2)
/// ]
/// let polygon = PolygonWithHoles(exterior: exterior, holes: [hole])
///
/// let isInside = polygon.contains(GeoPoint(5, 5))  // false (in hole)
/// let isInside2 = polygon.contains(GeoPoint(1, 1)) // true
/// ```
public struct PolygonWithHoles: Sendable {
    /// Exterior ring (counter-clockwise)
    public let exterior: [GeoPoint]

    /// Interior holes (each clockwise)
    public let holes: [[GeoPoint]]

    /// Create a polygon with holes
    ///
    /// - Parameters:
    ///   - exterior: Exterior ring (will be normalized to CCW)
    ///   - holes: Interior holes (will be normalized to CW)
    ///   - normalizeWinding: Whether to normalize winding direction (default: true)
    public init(exterior: [GeoPoint], holes: [[GeoPoint]] = [], normalizeWinding: Bool = true) {
        if normalizeWinding {
            self.exterior = WindingNumber.ensureWinding(exterior, counterClockwise: true)
            self.holes = holes.map { WindingNumber.ensureWinding($0, counterClockwise: false) }
        } else {
            self.exterior = exterior
            self.holes = holes
        }
    }

    /// Check if a point is inside the polygon (outside all holes)
    ///
    /// - Parameter point: Point to test
    /// - Returns: true if point is inside exterior but not in any hole
    public func contains(_ point: GeoPoint) -> Bool {
        WindingNumber.isPointInPolygonWithHoles(
            point: point,
            exterior: exterior,
            holes: holes
        )
    }

    /// Calculate the area of the polygon (exterior minus holes)
    ///
    /// - Returns: Net area (positive value)
    public var area: Double {
        let exteriorArea = abs(WindingNumber.signedArea(exterior))
        let holesArea = holes.reduce(0.0) { $0 + abs(WindingNumber.signedArea($1)) }
        return exteriorArea - holesArea
    }

    /// Get the bounding box of the polygon
    ///
    /// - Returns: (minLat, minLon, maxLat, maxLon)
    public var boundingBox: (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
        let lats = exterior.map { $0.latitude }
        let lons = exterior.map { $0.longitude }
        return (
            minLat: lats.min() ?? 0,
            minLon: lons.min() ?? 0,
            maxLat: lats.max() ?? 0,
            maxLon: lons.max() ?? 0
        )
    }
}
