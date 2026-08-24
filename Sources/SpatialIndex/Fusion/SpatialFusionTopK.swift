import DatabaseEngine
import DatabaseTypes

struct SpatialFusionTopK {
    private struct Match: Sendable {
        let primaryKey: ByteString
        let distance: Double
    }

    private var matches: [Match] = []
    private let limit: Int
    private let reservation: DatabaseIntermediateReservation
    private let layout: DatabaseRetainedArrayLayout
    private let workMeter: DatabaseWorkMeter
    private var accountedCapacity = 0

    init(limit: Int, workMeter: DatabaseWorkMeter) throws {
        self.limit = limit
        self.workMeter = workMeter
        self.layout = try DatabaseRetainedArrayLayout.forElement(Match.self)
        self.reservation = try workMeter.reserveIntermediate(
            bytes: layout.containerByteCount,
            at: .indexScan
        )
    }

    mutating func consider(
        primaryKey: ByteString,
        distance: Double
    ) throws {
        // The heap outlives each physical cursor row. Only admitted winners
        // receive an exact, limit-bounded key copy with its own reservation.
        guard limit > 0 else { return }
        if matches.count < limit {
            let (requiredCapacity, overflow) = matches.count
                .addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: accountedCapacity
                )
            }
            let growth = try layout.growth(
                from: accountedCapacity,
                toFit: requiredCapacity
            )
            let keyReservation = try reservation.reserveChild(
                bytes: UInt64(primaryKey.count),
                at: .indexScan
            )
            let retainedPrimaryKey = try DatabaseRetainedByteString.copying(
                primaryKey,
                reservation: keyReservation,
                at: .indexScan
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: growth.additionalByteCount,
                at: .indexScan
            )
            if growth.capacity != accountedCapacity {
                matches.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
            }
            matches.append(
                Match(primaryKey: retainedPrimaryKey, distance: distance)
            )
            try siftUp(from: matches.count - 1)
            return
        }

        try workMeter.consume(at: .sortComparison)
        let proposed = Match(primaryKey: primaryKey, distance: distance)
        guard try isBetter(proposed, than: matches[0]) else { return }
        let keyReservation = try reservation.reserveChild(
            bytes: UInt64(primaryKey.count),
            at: .indexScan
        )
        let retainedPrimaryKey = try DatabaseRetainedByteString.copying(
            primaryKey,
            reservation: keyReservation,
            at: .indexScan
        )
        matches[0] = Match(
            primaryKey: retainedPrimaryKey,
            distance: distance
        )
        try siftDown(from: 0, through: matches.count - 1)
    }

    mutating func emit(to output: FusionMatchSink) throws {
        if matches.count > 1 {
            var end = matches.count - 1
            while end > 0 {
                matches.swapAt(0, end)
                end -= 1
                try siftDown(from: 0, through: end)
            }
        }
        for match in matches {
            try output.submit(
                primaryKey: match.primaryKey,
                numericSignal: match.distance
            )
        }
    }

    private mutating func siftUp(from start: Int) throws {
        var child = start
        while child > 0 {
            let parent = (child - 1) / 2
            try workMeter.consume(at: .sortComparison)
            guard try isBetter(matches[parent], than: matches[child]) else {
                return
            }
            matches.swapAt(parent, child)
            child = parent
        }
    }

    private mutating func siftDown(
        from start: Int,
        through end: Int
    ) throws {
        var parent = start
        while true {
            let left = parent * 2 + 1
            guard left <= end else { return }
            let right = left + 1
            var worseChild = left
            if right <= end {
                try workMeter.consume(at: .sortComparison)
                if try isBetter(matches[left], than: matches[right]) {
                    worseChild = right
                }
            }
            try workMeter.consume(at: .sortComparison)
            guard try isBetter(
                matches[parent],
                than: matches[worseChild]
            ) else {
                return
            }
            matches.swapAt(parent, worseChild)
            parent = worseChild
        }
    }

    private func isBetter(
        _ lhs: borrowing Match,
        than rhs: borrowing Match
    ) throws -> Bool {
        if lhs.distance != rhs.distance {
            return lhs.distance < rhs.distance
        }
        try DatabaseByteProcessingMeter.consume(
            byteCount: max(lhs.primaryKey.count, rhs.primaryKey.count),
            workMeter: workMeter,
            stage: .sortComparison
        )
        return lhs.primaryKey.lexicographicallyPrecedes(rhs.primaryKey)
    }
}
