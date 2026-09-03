import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// One vector-search match whose primary-key ownership remains coupled to the
/// request reservation that admitted it.
package struct VectorRetainedMatch: Sendable {
    fileprivate let primaryKey: DatabaseRetainedPrimaryKey
    fileprivate let distance: Double

    package static func make(
        packedPrimaryKey: ByteString,
        droppingFirstElement: Bool = false,
        distance: Double,
        workMeter: DatabaseWorkMeter
    ) throws -> VectorRetainedMatch {
        guard distance.isFinite else {
            throw VectorIndexError.invalidStructure(
                "Vector search produced a non-finite distance"
            )
        }
        guard let retainedByteCount = UInt64(exactly: packedPrimaryKey.count)
        else {
            throw VectorIndexError.invalidStructure(
                "Vector primary key exceeds the current platform limit"
            )
        }
        let reservation = try workMeter.reserveIntermediate(
            rows: 1,
            bytes: retainedByteCount,
            at: .indexScan
        )
        do {
            let retained = try DatabaseRetainedByteString.make(
                packedPrimaryKey,
                reservation: reservation,
                at: .indexScan
            )
            let decoded = try Tuple(packed: retained) { additionalByteCount in
                guard let additional = UInt64(exactly: additionalByteCount)
                else {
                    throw VectorIndexError.invalidStructure(
                        "Vector primary key tuple exceeds the current platform limit"
                    )
                }
                try reservation.reserveAdditional(
                    bytes: additional,
                    at: .indexScan
                )
            }
            let primaryKey: Tuple
            if droppingFirstElement {
                guard decoded.count > 0 else {
                    throw VectorIndexError.invalidStructure(
                        "IVF list key is missing its cluster identifier"
                    )
                }
                let elementCount = decoded.count - 1
                let elementBytes = try DatabaseIntermediateCollectionMeter
                    .arrayFootprint(
                        count: elementCount,
                        element: (any TupleElement).self
                    )
                try reservation.reserveAdditional(
                    bytes: elementBytes.bytes,
                    at: .indexScan
                )
                primaryKey = Tuple(
                    try decoded.elements(in: 1..<decoded.count)
                )
            } else {
                primaryKey = decoded
            }
            return VectorRetainedMatch(
                primaryKey: DatabaseRetainedPrimaryKey(
                    value: primaryKey,
                    reservation: reservation
                ),
                distance: distance
            )
        } catch {
            reservation.release()
            throw error
        }
    }
}

/// Request-owned top-k vector matches.
///
/// The private array is pre-admitted for the requested result capacity and is
/// never returned to a caller. Each retained key owns its own byte reservation
/// under the same work meter. The max-heap keeps the worst accepted match at
/// its root, so discarded candidates never allocate a retained tuple.
package struct VectorRetainedMatches: Sendable {
    package struct Builder: ~Copyable {
        private var matches: [VectorRetainedMatch]
        private let workMeter: DatabaseWorkMeter
        private let reservation: DatabaseIntermediateReservation
        private let layout: DatabaseRetainedArrayLayout
        private let limit: Int
        private var accountedCapacity: Int

        package init(
            limit: Int,
            workMeter: DatabaseWorkMeter
        ) throws {
            guard limit >= 0 else {
                throw VectorIndexError.invalidArgument(
                    "Vector result limit must not be negative"
                )
            }
            let layout = try DatabaseRetainedArrayLayout.forElement(
                VectorRetainedMatch.self
            )
            let growth = try layout.growth(
                from: 0,
                toFit: limit
            )
            let initialFootprint = try DatabaseIntermediateFootprint(
                bytes: layout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                )
            )
            let reservation = try workMeter.reserveIntermediate(
                bytes: initialFootprint.bytes,
                at: .indexScan
            )
            var matches: [VectorRetainedMatch] = []
            matches.reserveCapacity(growth.capacity)
            self.matches = matches
            self.workMeter = workMeter
            self.reservation = reservation
            self.layout = layout
            self.limit = limit
            self.accountedCapacity = growth.capacity
        }

        package var count: Int { matches.count }
        package var workMeterIdentity: DatabaseWorkMeter { workMeter }

        /// Considers one decoded candidate without retaining its tuple unless
        /// it belongs to the current top-k set.
        package mutating func consider(
            primaryKey: borrowing Tuple,
            distance: Double
        ) throws {
            guard distance.isFinite else {
                throw VectorIndexError.invalidStructure(
                    "Vector search produced a non-finite distance"
                )
            }
            guard limit > 0 else {
                return
            }

            if matches.count == limit {
                try workMeter.consume(at: .sortComparison)
                guard distance < matches[0].distance else {
                    return
                }
            }

            let retainedPrimaryKey = try retainPrimaryKey(primaryKey)
            let match = VectorRetainedMatch(
                primaryKey: retainedPrimaryKey,
                distance: distance
            )
            try insertAccepted(match)
        }

        package mutating func consider(
            _ match: consuming VectorRetainedMatch
        ) throws {
            guard match.primaryKey.workMeter === workMeter else {
                throw DatabaseIntermediateReservationError.workMeterMismatch
            }
            guard match.distance.isFinite else {
                throw VectorIndexError.invalidStructure(
                    "Vector search produced a non-finite distance"
                )
            }
            guard limit > 0 else { return }
            if matches.count == limit {
                try workMeter.consume(at: .sortComparison)
                guard match.distance < matches[0].distance else { return }
            }
            try insertAccepted(match)
        }

        /// Checks top-k membership before retaining or decoding the key.
        package mutating func consider(
            packedPrimaryKey: ByteString,
            droppingFirstElement: Bool = false,
            distance: Double
        ) throws {
            guard distance.isFinite else {
                throw VectorIndexError.invalidStructure(
                    "Vector search produced a non-finite distance"
                )
            }
            guard limit > 0 else { return }
            if matches.count == limit {
                try workMeter.consume(at: .sortComparison)
                guard distance < matches[0].distance else { return }
            }
            try insertAccepted(
                VectorRetainedMatch.make(
                    packedPrimaryKey: packedPrimaryKey,
                    droppingFirstElement: droppingFirstElement,
                    distance: distance,
                    workMeter: workMeter
                )
            )
        }

        private mutating func insertAccepted(
            _ match: consuming VectorRetainedMatch
        ) throws {
            if matches.count < limit {
                let (requiredCount, overflow) = matches.count
                    .addingReportingOverflow(1)
                guard !overflow else {
                    throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                        currentCapacity: accountedCapacity
                    )
                }
                let growth = try layout.growth(
                    from: accountedCapacity,
                    toFit: requiredCount
                )
                if growth.capacity != accountedCapacity {
                    // The requested capacity was admitted in init. This
                    // branch is defensive for a future caller that changes
                    // the initial-capacity policy.
                    try reservation.reserveAdditional(
                        bytes: growth.additionalByteCount,
                        at: .indexScan
                    )
                    matches.reserveCapacity(growth.capacity)
                    accountedCapacity = growth.capacity
                }
                matches.append(match)
                try siftUp(from: matches.count - 1)
                return
            }

            matches[0] = match
            try siftDown(from: 0, through: matches.count - 1)
        }

        /// Finishes the heap in ascending distance order and transfers the
        /// admitted storage to the read-only owner.
        package consuming func finish() throws -> VectorRetainedMatches {
            if matches.count > 1 {
                var end = matches.count - 1
                while end > 0 {
                    matches.swapAt(0, end)
                    end -= 1
                    try siftDown(from: 0, through: end)
                }
            }
            return VectorRetainedMatches(
                storage: Storage(
                    matches: matches,
                    reservation: reservation
                )
            )
        }

        private func retainPrimaryKey(
            _ primaryKey: borrowing Tuple
        ) throws -> DatabaseRetainedPrimaryKey {
            let packedByteCount = primaryKey.packedByteCount
            guard let retainedByteCount = UInt64(exactly: packedByteCount)
            else {
                throw VectorIndexError.invalidStructure(
                    "Vector primary key exceeds the current platform limit"
                )
            }
            let keyReservation = try reservation.reserveChild(
                rows: 1,
                bytes: retainedByteCount,
                at: .indexScan
            )
            do {
                let packed = primaryKey.pack()
                let retained = try DatabaseRetainedByteString.make(
                    packed,
                    reservation: keyReservation,
                    at: .indexScan
                )
                let tuple = try Tuple(packed: retained) {
                    additionalByteCount in
                    guard let additional = UInt64(exactly: additionalByteCount)
                    else {
                        throw VectorIndexError.invalidStructure(
                            "Vector primary key tuple exceeds the current platform limit"
                        )
                    }
                    try keyReservation.reserveAdditional(
                        bytes: additional,
                        at: .indexScan
                    )
                }
                return DatabaseRetainedPrimaryKey(
                    value: tuple,
                    reservation: keyReservation
                )
            } catch {
                keyReservation.release()
                throw error
            }
        }

        private mutating func siftUp(from start: Int) throws {
            var child = start
            while child > 0 {
                let parent = (child - 1) / 2
                try workMeter.consume(at: .sortComparison)
                guard matches[parent].distance < matches[child].distance else {
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
            guard end >= start else {
                return
            }
            var parent = start
            while true {
                let left = parent * 2 + 1
                guard left <= end else {
                    return
                }
                let right = left + 1
                var worseChild = left
                if right <= end {
                    try workMeter.consume(at: .sortComparison)
                    if matches[left].distance < matches[right].distance {
                        worseChild = right
                    }
                }
                try workMeter.consume(at: .sortComparison)
                guard matches[parent].distance < matches[worseChild].distance else {
                    return
                }
                matches.swapAt(parent, worseChild)
                parent = worseChild
            }
        }
    }

    private final class Storage: Sendable {
        let matches: [VectorRetainedMatch]
        let reservation: DatabaseIntermediateReservation

        init(
            matches: consuming [VectorRetainedMatch],
            reservation: consuming DatabaseIntermediateReservation
        ) {
            self.matches = matches
            self.reservation = reservation
        }
    }

    private let storage: Storage

    private init(storage: Storage) {
        self.storage = storage
    }

    package var count: Int { storage.matches.count }
    package var workMeter: DatabaseWorkMeter { storage.reservation.workMeter }

    /// Supplies the bounded owner used by the low-level maintainer facade.
    /// Canonical session reads always inject their session meter instead.
    package static func makeUnboundedWorkMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            unbounded: ExecutionBudget(
                maximumRows: UInt32.max,
                maximumWorkUnits: UInt64.max,
                maximumIntermediateRows: UInt32.max,
                maximumIntermediateBytes: UInt64.max,
                timeoutMilliseconds: UInt32.max
            )
        )
    }

    package borrowing func distance(at index: Int) -> Double {
        precondition(
            index >= storage.matches.startIndex
                && index < storage.matches.endIndex
        )
        return storage.matches[index].distance
    }

    private borrowing func withMatch<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing VectorRetainedMatch) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        precondition(
            index >= storage.matches.startIndex
                && index < storage.matches.endIndex
        )
        return try body(storage.matches[index])
    }

    private borrowing func withMatch<Result, Failure: Error>(
        at index: Int,
        _ body: (borrowing VectorRetainedMatch) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        precondition(
            index >= storage.matches.startIndex
                && index < storage.matches.endIndex
        )
        return try await body(storage.matches[index])
    }

    /// Promotes retained keys to the legacy low-level result shape.
    ///
    /// This is an explicit final boundary for maintainer APIs. The canonical
    /// read path consumes the retained owner directly and never calls this
    /// method.
    package func promotedOutput() throws -> [
        (primaryKey: [any TupleElement], distance: Double)
    ] {
        var output: [
            (primaryKey: [any TupleElement], distance: Double)
        ] = []
        output.reserveCapacity(storage.matches.count)
        for index in storage.matches.indices {
            let result = try withMatch(at: index) { match in
                var primaryKey: [any TupleElement] = []
                try match.primaryKey.withValue { value in
                    primaryKey = try value.elements()
                }
                return (primaryKey: primaryKey, distance: match.distance)
            }
            output.append(result)
        }
        return output
    }

}

package struct VectorSearchAccumulator: ~Copyable {
    private var builder: VectorRetainedMatches.Builder

    package init(k: Int, workMeter: DatabaseWorkMeter) throws {
        self.builder = try VectorRetainedMatches.Builder(
            limit: k,
            workMeter: workMeter
        )
    }

    package mutating func insert(
        packedPrimaryKey: ByteString,
        droppingFirstElement: Bool = false,
        distance: Double
    ) throws {
        try builder.consider(
            packedPrimaryKey: packedPrimaryKey,
            droppingFirstElement: droppingFirstElement,
            distance: distance
        )
    }

    package consuming func finish() throws -> VectorRetainedMatches {
        try builder.finish()
    }
}

extension VectorRetainedMatches: DatabaseRetainedPrimaryKeyCollection {
    package func withRetainedPrimaryKey(
        at position: Int,
        _ body: (borrowing Tuple) throws -> Void
    ) rethrows {
        func apply(
            _ match: borrowing VectorRetainedMatch
        ) throws {
            try match.primaryKey.withValue(body)
        }
        try withMatch(at: position, apply)
    }

    package func withRetainedPrimaryKey(
        at position: Int,
        _ body: (borrowing Tuple) async throws -> Void
    ) async rethrows {
        func apply(
            _ match: borrowing VectorRetainedMatch
        ) async throws {
            try await match.primaryKey.withValue(body)
        }
        try await withMatch(at: position, apply)
    }
}
