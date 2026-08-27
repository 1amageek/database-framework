import DatabaseEngine
import DatabaseTypes

/// Request-accounted hash workspace for one SPARQL hash-join build side.
struct SPARQLHashJoinIndex: ~Copyable {
    private struct Bucket {
        var indices: [Int]
        var accountedCapacity: Int
    }

    private static let dictionaryOwnerByteCount: UInt64 = 64
    private static let dictionaryCapacitySlotByteCount: UInt64 = 192
    private static let keyArrayOwnerByteCount: UInt64 = 64
    private static let keyValueCapacitySlotByteCount: UInt64 = 64
    private static let indexArrayOwnerByteCount: UInt64 = 64
    private static let indexCapacitySlotByteCount: UInt64 = 8

    private var buckets: [SPARQLQueryExecutor.JoinKey: Bucket]
    private let reservation: DatabaseIntermediateReservation
    private let footprintMeter: SPARQLBindingFootprintMeter
    private var accountedCapacity: Int

    private init(
        reservation: DatabaseIntermediateReservation,
        footprintMeter: SPARQLBindingFootprintMeter
    ) {
        self.buckets = [:]
        self.reservation = reservation
        self.footprintMeter = footprintMeter
        self.accountedCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLHashJoinIndex {
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .joinCandidate
        )
        do {
            return SPARQLHashJoinIndex(
                reservation: try workMeter.reserveIntermediate(
                    bytes: dictionaryOwnerByteCount,
                    at: .joinCandidate
                ),
                footprintMeter: footprintMeter
            )
        } catch {
            footprintMeter.shutdown()
            throw error
        }
    }

    var workMeter: DatabaseWorkMeter { reservation.workMeter }

    mutating func insert(
        index: Int,
        for binding: borrowing VariableBinding,
        variables: [String]
    ) throws {
        precondition(index >= 0)
        let keyFootprint = try footprint(
            binding: binding,
            variables: variables
        )
        let scratch = try workMeter.reserveIntermediate(
            bytes: keyFootprint.bytes,
            at: .joinCandidate
        )
        defer { scratch.release() }
        let key = SPARQLQueryExecutor.JoinKey(
            binding: binding,
            variables: variables
        )

        if buckets[key] != nil {
            let existingCount = buckets[key]!.indices.count
            let existingCapacity = buckets[key]!.accountedCapacity
            let targetCapacity = try Self.targetCapacity(
                current: existingCapacity,
                requiredCount: existingCount + 1
            )
            if targetCapacity != existingCapacity {
                try reservation.reserveAdditional(
                    bytes: try Self.checkedMultiply(
                        UInt64(targetCapacity),
                        Self.indexCapacitySlotByteCount
                    ),
                    at: .joinCandidate
                )
                buckets[key]!.indices.reserveCapacity(targetCapacity)
                reservation.releaseGuaranteedPartial(
                    bytes: try Self.checkedMultiply(
                        UInt64(existingCapacity),
                        Self.indexCapacitySlotByteCount
                    )
                )
            }
            buckets[key]!.indices.append(index)
            buckets[key]!.accountedCapacity = targetCapacity
            return
        }

        let dictionaryCapacity = try Self.targetCapacity(
            current: accountedCapacity,
            requiredCount: buckets.count + 1
        )
        let indexCapacity = try Self.targetCapacity(
            current: 0,
            requiredCount: 1
        )
        var destinationBytes = try Self.checkedMultiply(
            UInt64(dictionaryCapacity - accountedCapacity),
            Self.dictionaryCapacitySlotByteCount
        )
        destinationBytes = try Self.checkedAdd(
            destinationBytes,
            keyFootprint.bytes
        )
        destinationBytes = try Self.checkedAdd(
            destinationBytes,
            Self.indexArrayOwnerByteCount
        )
        destinationBytes = try Self.checkedAdd(
            destinationBytes,
            try Self.checkedMultiply(
                UInt64(indexCapacity),
                Self.indexCapacitySlotByteCount
            )
        )
        try reservation.reserveAdditional(
            bytes: destinationBytes,
            at: .joinCandidate
        )
        if dictionaryCapacity != accountedCapacity {
            buckets.reserveCapacity(dictionaryCapacity)
            accountedCapacity = dictionaryCapacity
        }
        var indices: [Int] = []
        indices.reserveCapacity(indexCapacity)
        indices.append(index)
        buckets[key] = Bucket(
            indices: indices,
            accountedCapacity: indexCapacity
        )
    }

    borrowing func withIndices<Failure: Error>(
        for binding: borrowing VariableBinding,
        variables: [String],
        _ body: (borrowing [Int]) async throws(Failure) -> Void
    ) async throws {
        let keyFootprint = try footprint(
            binding: binding,
            variables: variables
        )
        let scratch = try workMeter.reserveIntermediate(
            bytes: keyFootprint.bytes,
            at: .joinCandidate
        )
        defer { scratch.release() }
        let key = SPARQLQueryExecutor.JoinKey(
            binding: binding,
            variables: variables
        )
        guard let bucket = buckets[key] else { return }
        try await body(bucket.indices)
    }

    private func footprint(
        binding: borrowing VariableBinding,
        variables: [String]
    ) throws -> DatabaseIntermediateFootprint {
        let capacity = try Self.targetCapacity(
            current: 0,
            requiredCount: variables.count
        )
        var footprint = try DatabaseIntermediateFootprint(
            bytes: Self.keyArrayOwnerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: try Self.checkedMultiply(
                    UInt64(capacity),
                    Self.keyValueCapacitySlotByteCount
                )
            )
        )
        for variable in variables {
            let value = binding[variable] ?? .null
            footprint = try footprint.adding(
                try footprintMeter.footprint(of: value)
            )
        }
        return footprint
    }

    private static func targetCapacity(
        current: Int,
        requiredCount: Int
    ) throws -> Int {
        guard requiredCount > current else { return current }
        var capacity = max(1, current)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: current
                )
            }
            capacity = next
        }
        return capacity
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: left)
            .multiplied(by: right).bytes
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: left)
            .adding(DatabaseIntermediateFootprint(bytes: right)).bytes
    }
}
