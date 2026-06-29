// SpatialScanPlanner.swift
// SpatialIndex - Backend-independent scan planning for spatial encodings

import Foundation
import Core
import DatabaseEngine
import Geospatial

internal enum SpatialScanPlan: Sendable {
    case cells([UInt64])
    case codeRange(min: UInt64, max: UInt64)
}

internal enum SpatialScanPlanner {
    static func plan(
        for constraint: SpatialConstraint,
        encoding: SpatialEncoding,
        level: Int
    ) throws -> SpatialScanPlan {
        switch encoding {
        case .s2:
            return .cells(s2CoveringCells(for: constraint, level: level))
        case .morton:
            let range = try mortonRange(for: constraint, level: level)
            return .codeRange(min: range.min, max: range.max)
        }
    }

    static func mortonCode(latitude: Double, longitude: Double, level: Int) -> UInt64 {
        let x = MortonCode.normalize(longitude, min: -180, max: 180)
        let y = MortonCode.normalize(latitude, min: -90, max: 90)
        return MortonCode.encode2D(x: x, y: y, level: level)
    }

    private static func s2CoveringCells(
        for constraint: SpatialConstraint,
        level: Int
    ) -> [UInt64] {
        switch constraint.type {
        case .withinDistance(let center, let radiusMeters):
            return S2Geometry.getCoveringCells(
                latitude: center.latitude,
                longitude: center.longitude,
                radiusMeters: radiusMeters,
                level: level
            )
        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            return S2Geometry.getCoveringCellsForBox(
                minLat: minLat,
                minLon: minLon,
                maxLat: maxLat,
                maxLon: maxLon,
                level: level
            )
        case .withinPolygon(let points):
            let bounds = boundingBox(for: points)
            return S2Geometry.getCoveringCellsForBox(
                minLat: bounds.minLat,
                minLon: bounds.minLon,
                maxLat: bounds.maxLat,
                maxLon: bounds.maxLon,
                level: level
            )
        }
    }

    private static func mortonRange(
        for constraint: SpatialConstraint,
        level: Int
    ) throws -> (min: UInt64, max: UInt64) {
        let bounds: (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double)

        switch constraint.type {
        case .withinDistance(let center, let radiusMeters):
            bounds = boundingBox(center: center, radiusMeters: radiusMeters)
        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            bounds = (minLat: minLat, minLon: minLon, maxLat: maxLat, maxLon: maxLon)
        case .withinPolygon(let points):
            bounds = boundingBox(for: points)
        }

        let minCode = mortonCode(latitude: bounds.minLat, longitude: bounds.minLon, level: level)
        let maxCode = mortonCode(latitude: bounds.maxLat, longitude: bounds.maxLon, level: level)
        return (min: min(minCode, maxCode), max: max(minCode, maxCode))
    }

    private static func boundingBox(
        for points: [(latitude: Double, longitude: Double)]
    ) -> (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
        let latitudes = points.map { $0.latitude }
        let longitudes = points.map { $0.longitude }
        return (
            minLat: latitudes.min() ?? 0,
            minLon: longitudes.min() ?? 0,
            maxLat: latitudes.max() ?? 0,
            maxLon: longitudes.max() ?? 0
        )
    }

    private static func boundingBox(
        center: (latitude: Double, longitude: Double),
        radiusMeters: Double
    ) -> (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
        let radiusKm = radiusMeters / 1000.0
        let latDelta = radiusKm / 111.0
        let longitudeScale = max(cos(center.latitude * .pi / 180.0), 0.000001)
        let lonDelta = radiusKm / (111.0 * longitudeScale)

        return (
            minLat: max(-90.0, center.latitude - latDelta),
            minLon: max(-180.0, center.longitude - lonDelta),
            maxLat: min(90.0, center.latitude + latDelta),
            maxLon: min(180.0, center.longitude + lonDelta)
        )
    }
}
