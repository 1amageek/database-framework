// CellDistanceCalculator.swift
// SpatialIndex - Cell-to-point distance calculation for KNN
//
// Reference: Samet, H. "Foundations of Multidimensional and Metric Data Structures", 2006

import Foundation
import DatabaseEngine
import Core
import FoundationDB
import Spatial

/// Calculator for cell-to-point distances
///
/// **Purpose**: Compute minimum and maximum distances between an S2 cell
/// and a query point for efficient KNN pruning.
///
/// **Algorithm**:
/// For a cell with known bounds and a query point Q:
/// - minDistance: Closest possible point in cell to Q
/// - maxDistance: Farthest possible point in cell from Q
///
/// **Pruning Rule**:
/// If minDistance(cell, Q) > kth-best-distance, skip the cell entirely.
///
/// **Reference**: Samet (2006) - "Foundations of Multidimensional and Metric Data Structures"
public struct CellDistanceCalculator: Sendable {
    /// Earth radius in meters
    private static let earthRadiusMeters: Double = 6371000.0

    /// Compute the minimum distance from a point to a cell (in meters)
    ///
    /// **Algorithm**:
    /// 1. Get cell bounds (bounding box)
    /// 2. Find the closest point on the cell boundary to the query
    /// 3. Compute haversine distance to that point
    ///
    /// - Parameters:
    ///   - cellId: S2 cell ID
    ///   - level: S2 level of the cell
    ///   - point: Query point
    /// - Returns: Minimum distance in meters
    public static func minDistance(
        cellId: UInt64,
        level: Int,
        to point: GeoPoint
    ) -> Double {
        let bounds = cellBounds(cellId: cellId, level: level)
        let closestPoint = closestPointOnBounds(bounds: bounds, to: point)
        return haversineDistance(from: point, to: closestPoint)
    }

    /// Compute the maximum distance from a point to a cell (in meters)
    ///
    /// **Algorithm**:
    /// Find the farthest corner of the cell from the query point.
    ///
    /// - Parameters:
    ///   - cellId: S2 cell ID
    ///   - level: S2 level of the cell
    ///   - point: Query point
    /// - Returns: Maximum distance in meters
    public static func maxDistance(
        cellId: UInt64,
        level: Int,
        to point: GeoPoint
    ) -> Double {
        let bounds = cellBounds(cellId: cellId, level: level)
        let farthestPoint = farthestPointOnBounds(bounds: bounds, from: point)
        return haversineDistance(from: point, to: farthestPoint)
    }

    /// Get the bounding box of an S2 cell
    ///
    /// - Parameters:
    ///   - cellId: S2 cell ID
    ///   - level: S2 level
    /// - Returns: Cell bounds as (minLat, minLon, maxLat, maxLon)
    public static func cellBounds(
        cellId: UInt64,
        level: Int
    ) -> (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
        // Get cell center
        let center = S2Geometry.decode(cellId, level: level)

        // Approximate cell size in degrees at this level
        // S2 cells at level L have approximately 2^(30-L) x 2^(30-L) area
        // At level 15: ~1km, at level 20: ~30m
        let cellSizeDegrees = 180.0 / Double(1 << level)
        let halfSize = cellSizeDegrees / 2.0

        // Adjust longitude span based on latitude (cells are narrower near poles)
        let latRad = abs(center.latitude) * .pi / 180.0
        let lonHalfSize = halfSize / max(cos(latRad), 0.1)

        return (
            minLat: max(-90, center.latitude - halfSize),
            minLon: max(-180, center.longitude - lonHalfSize),
            maxLat: min(90, center.latitude + halfSize),
            maxLon: min(180, center.longitude + lonHalfSize)
        )
    }

    /// Find the closest point on a bounding box to a query point
    ///
    /// - Parameters:
    ///   - bounds: Cell bounds
    ///   - to: Query point
    /// - Returns: Closest point on or inside the bounds
    private static func closestPointOnBounds(
        bounds: (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double),
        to point: GeoPoint
    ) -> GeoPoint {
        // If point is inside bounds, distance is 0
        if point.latitude >= bounds.minLat && point.latitude <= bounds.maxLat &&
           point.longitude >= bounds.minLon && point.longitude <= bounds.maxLon {
            return point
        }

        // Clamp point to bounds
        let closestLat = max(bounds.minLat, min(bounds.maxLat, point.latitude))
        let closestLon = max(bounds.minLon, min(bounds.maxLon, point.longitude))

        return GeoPoint(closestLat, closestLon)
    }

    /// Find the farthest point on a bounding box from a query point
    ///
    /// - Parameters:
    ///   - bounds: Cell bounds
    ///   - from: Query point
    /// - Returns: Farthest corner of the bounds
    private static func farthestPointOnBounds(
        bounds: (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double),
        from point: GeoPoint
    ) -> GeoPoint {
        // Check all 4 corners and find the farthest
        let corners = [
            GeoPoint(bounds.minLat, bounds.minLon),
            GeoPoint(bounds.minLat, bounds.maxLon),
            GeoPoint(bounds.maxLat, bounds.minLon),
            GeoPoint(bounds.maxLat, bounds.maxLon)
        ]

        var farthest = corners[0]
        var maxDist: Double = 0

        for corner in corners {
            let dist = haversineDistance(from: point, to: corner)
            if dist > maxDist {
                maxDist = dist
                farthest = corner
            }
        }

        return farthest
    }

    /// Haversine distance between two points in meters
    ///
    /// - Parameters:
    ///   - from: Source point
    ///   - to: Destination point
    /// - Returns: Distance in meters
    public static func haversineDistance(from: GeoPoint, to: GeoPoint) -> Double {
        let lat1 = from.latitude * .pi / 180.0
        let lat2 = to.latitude * .pi / 180.0
        let dLat = (to.latitude - from.latitude) * .pi / 180.0
        let dLon = (to.longitude - from.longitude) * .pi / 180.0

        let a = sin(dLat / 2) * sin(dLat / 2) +
                cos(lat1) * cos(lat2) * sin(dLon / 2) * sin(dLon / 2)
        let c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return earthRadiusMeters * c
    }
}
