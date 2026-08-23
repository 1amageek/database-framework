// SpatialScanPlanner.swift
// SpatialIndex - Backend-independent scan planning for spatial encodings

import DatabaseKit
import DatabaseMath
import DatabaseEngine
import DatabaseTypes

internal enum SpatialScanPlan: Sendable {
    case cells(SpatialCellPlan)
    case codeRanges([SpatialCodeRange])
}

internal struct SpatialCodeRange: Sendable, Equatable {
    let min: UInt64
    let max: UInt64
}

private struct SpatialPlanningBounds {
    let minLat: Double
    let maxLat: Double
    let longitudeIntervals: [ClosedRange<Double>]
}

internal struct SpatialCellPlan: Sendable {
    let cells: [UInt64]
    private let reservation: DatabaseIntermediateReservation?

    init(
        cells: consuming [UInt64],
        reservation: DatabaseIntermediateReservation?
    ) {
        self.cells = cells
        self.reservation = reservation
    }
}

internal enum SpatialScanPlanningError: Error, Sendable, Equatable {
    case invalidLevel(Int)
    case invalidRadius(Double)
    case invalidBounds
    case emptyPolygon
    case invalidMaximumCellCount(Int)
    case coveringCellLimitExceeded(required: UInt64, maximum: Int)
    case coveringCellCountOverflow
}

internal enum SpatialScanPlanner {
    static func plan(
        for constraint: SpatialConstraint,
        encoding: SpatialEncoding,
        level: Int,
        workMeter: DatabaseWorkMeter? = nil,
        maximumCells: Int = SpatialScanBudget.maximumCandidateLimit
    ) throws -> SpatialScanPlan {
        guard (0...30).contains(level) else {
            throw SpatialScanPlanningError.invalidLevel(level)
        }
        guard maximumCells > 0 else {
            throw SpatialScanPlanningError.invalidMaximumCellCount(
                maximumCells
            )
        }
        try validate(constraint)
        switch encoding {
        case .s2:
            return .cells(
                try s2CoveringCells(
                    for: constraint,
                    level: level,
                    maximumCells: maximumCells,
                    workMeter: workMeter
                )
            )
        case .morton:
            try workMeter?.checkpoint(at: .indexScan)
            try ensureDatabaseTaskIsActive()
            return .codeRanges(
                try mortonRanges(for: constraint, level: level)
            )
        }
    }

    static func mortonCode(latitude: Double, longitude: Double, level: Int) -> UInt64 {
        let x = MortonCode.normalize(longitude, min: -180, max: 180)
        let y = MortonCode.normalize(latitude, min: -90, max: 90)
        return MortonCode.encode2D(x: x, y: y, level: level)
    }

    private static func s2CoveringCells(
        for constraint: SpatialConstraint,
        level: Int,
        maximumCells: Int,
        workMeter: DatabaseWorkMeter?
    ) throws -> SpatialCellPlan {
        let bounds = planningBounds(for: constraint)

        let cellSize = 180.0 / Double(1 << level)
        let step = max(cellSize * 0.5, 0.001)
        let latitudeSegments = try sampleSegmentCount(
            minimum: bounds.minLat,
            maximum: bounds.maxLat,
            step: step
        )
        let (latitudeSamples, latitudeOverflow) = latitudeSegments
            .addingReportingOverflow(1)
        guard !latitudeOverflow else {
            throw SpatialScanPlanningError.coveringCellCountOverflow
        }
        var longitudeSamples: UInt64 = 0
        for interval in bounds.longitudeIntervals {
            let segments = try sampleSegmentCount(
                minimum: interval.lowerBound,
                maximum: interval.upperBound,
                step: step
            )
            let (samples, sampleOverflow) = segments.addingReportingOverflow(1)
            let (next, totalOverflow) = longitudeSamples
                .addingReportingOverflow(samples)
            guard !sampleOverflow, !totalOverflow else {
                throw SpatialScanPlanningError.coveringCellCountOverflow
            }
            longitudeSamples = next
        }
        let (gridSamples, gridOverflow) = latitudeSamples
            .multipliedReportingOverflow(by: longitudeSamples)
        guard !gridOverflow else {
            throw SpatialScanPlanningError.coveringCellCountOverflow
        }
        let requiredSamples = gridSamples
        guard requiredSamples <= UInt64(maximumCells) else {
            throw SpatialScanPlanningError.coveringCellLimitExceeded(
                required: requiredSamples,
                maximum: maximumCells
            )
        }

        let reservation: DatabaseIntermediateReservation?
        if let workMeter {
            let setFootprint = try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<Set<UInt64>>.stride)
            ).adding(
                try DatabaseIntermediateFootprint(bytes: 48)
                    .multiplied(by: requiredSamples)
            )
            let arrayFootprint = try DatabaseIntermediateCollectionMeter
                .arrayFootprint(
                    count: Int(requiredSamples),
                    element: UInt64.self
                )
            reservation = try workMeter.reserveIntermediate(
                rows: requiredSamples,
                bytes: try setFootprint.adding(arrayFootprint).bytes,
                at: .indexScan
            )
        } else {
            reservation = nil
        }
        var transferredReservation = false
        defer {
            if !transferredReservation { reservation?.release() }
        }

        var cells: Set<UInt64> = []
        cells.reserveCapacity(Int(requiredSamples))
        for latitudeIndex in 0...latitudeSegments {
            let latitude = interpolatedSample(
                minimum: bounds.minLat,
                maximum: bounds.maxLat,
                index: latitudeIndex,
                segmentCount: latitudeSegments
            )
            for interval in bounds.longitudeIntervals {
                let longitudeSegments = try sampleSegmentCount(
                    minimum: interval.lowerBound,
                    maximum: interval.upperBound,
                    step: step
                )
                for longitudeIndex in 0...longitudeSegments {
                    let longitude = interpolatedSample(
                        minimum: interval.lowerBound,
                        maximum: interval.upperBound,
                        index: longitudeIndex,
                        segmentCount: longitudeSegments
                    )
                    try workMeter?.consume(at: .indexScan)
                    try ensureDatabaseTaskIsActive()
                    cells.insert(
                        S2Geometry.encode(
                            latitude: clampedLatitude(latitude),
                            longitude: clampedLongitude(longitude),
                            level: level
                        )
                    )
                }
            }
        }
        var orderedCells = Array(cells)
        orderedCells.sort()
        transferredReservation = true
        return SpatialCellPlan(
            cells: orderedCells,
            reservation: reservation
        )
    }

    /// Divides every non-empty axis into at least two segments. Including the
    /// midpoint is required when a narrow region straddles an S2 cell boundary:
    /// sampling only its corners can omit the cell containing the query center.
    private static func sampleSegmentCount(
        minimum: Double,
        maximum: Double,
        step: Double
    ) throws -> UInt64 {
        let span = maximum - minimum
        guard span >= 0 else {
            throw SpatialScanPlanningError.coveringCellCountOverflow
        }
        guard span > 0 else { return 0 }
        let count = max(2, DatabaseMath.ceiling(span / step))
        guard count.isFinite, count <= Double(UInt64.max) else {
            throw SpatialScanPlanningError.coveringCellCountOverflow
        }
        return UInt64(count)
    }

    private static func interpolatedSample(
        minimum: Double,
        maximum: Double,
        index: UInt64,
        segmentCount: UInt64
    ) -> Double {
        guard segmentCount > 0 else { return minimum }
        if index == segmentCount { return maximum }
        return minimum
            + (maximum - minimum)
                * (Double(index) / Double(segmentCount))
    }

    private static func validate(_ constraint: SpatialConstraint) throws {
        switch constraint.type {
        case .withinDistance(_, let radiusMeters):
            guard radiusMeters.isFinite, radiusMeters >= 0 else {
                throw SpatialScanPlanningError.invalidRadius(radiusMeters)
            }
        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            guard minLat.isFinite, minLon.isFinite,
                  maxLat.isFinite, maxLon.isFinite,
                  (-90...90).contains(minLat),
                  (-90...90).contains(maxLat),
                  (-180...180).contains(minLon),
                  (-180...180).contains(maxLon),
                  minLat <= maxLat,
                  minLon <= maxLon else {
                throw SpatialScanPlanningError.invalidBounds
            }
        case .withinPolygon(let points):
            guard !points.isEmpty else {
                throw SpatialScanPlanningError.emptyPolygon
            }
        }
    }

    private static func clampedLatitude(_ value: Double) -> Double {
        min(max(value, -89.999), 89.999)
    }

    private static func clampedLongitude(_ value: Double) -> Double {
        min(max(value, -179.999), 179.999)
    }

    private static func mortonRanges(
        for constraint: SpatialConstraint,
        level: Int
    ) throws -> [SpatialCodeRange] {
        let bounds = planningBounds(for: constraint)
        return bounds.longitudeIntervals.map { interval in
            let minCode = mortonCode(
                latitude: bounds.minLat,
                longitude: interval.lowerBound,
                level: level
            )
            let maxCode = mortonCode(
                latitude: bounds.maxLat,
                longitude: interval.upperBound,
                level: level
            )
            return SpatialCodeRange(
                min: min(minCode, maxCode),
                max: max(minCode, maxCode)
            )
        }
    }

    private static func planningBounds(
        for constraint: SpatialConstraint
    ) -> SpatialPlanningBounds {
        switch constraint.type {
        case .withinDistance(let center, let radiusMeters):
            return boundingBox(
                center: center,
                radiusMeters: radiusMeters
            )
        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            return SpatialPlanningBounds(
                minLat: minLat,
                maxLat: maxLat,
                longitudeIntervals: [minLon...maxLon]
            )
        case .withinPolygon(let points):
            let bounds = boundingBox(for: points)
            return SpatialPlanningBounds(
                minLat: bounds.minLat,
                maxLat: bounds.maxLat,
                longitudeIntervals: [bounds.minLon...bounds.maxLon]
            )
        }
    }

    private static func boundingBox(
        for points: [GeographicPoint]
    ) -> (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
        var minLat = points[0].latitude
        var minLon = points[0].longitude
        var maxLat = minLat
        var maxLon = minLon
        for point in points.dropFirst() {
            minLat = min(minLat, point.latitude)
            minLon = min(minLon, point.longitude)
            maxLat = max(maxLat, point.latitude)
            maxLon = max(maxLon, point.longitude)
        }
        return (minLat, minLon, maxLat, maxLon)
    }

    private static func boundingBox(
        center: GeographicPoint,
        radiusMeters: Double
    ) -> SpatialPlanningBounds {
        let radiusKm = radiusMeters / 1000.0
        let latDelta = radiusKm / 111.0
        let longitudeScale = max(
            DatabaseMath.cosine(center.latitude * .pi / 180.0),
            0.000001
        )
        let lonDelta = radiusKm / (111.0 * longitudeScale)
        let longitudeIntervals: [ClosedRange<Double>]
        if lonDelta >= 180 {
            longitudeIntervals = [-180...180]
        } else {
            let minimum = center.longitude - lonDelta
            let maximum = center.longitude + lonDelta
            if minimum < -180 {
                longitudeIntervals = [
                    -180...maximum,
                    (minimum + 360)...180,
                ]
            } else if maximum > 180 {
                longitudeIntervals = [
                    -180...(maximum - 360),
                    minimum...180,
                ]
            } else {
                longitudeIntervals = [minimum...maximum]
            }
        }
        return SpatialPlanningBounds(
            minLat: max(-90.0, center.latitude - latDelta),
            maxLat: min(90.0, center.latitude + latDelta),
            longitudeIntervals: longitudeIntervals
        )
    }
}
