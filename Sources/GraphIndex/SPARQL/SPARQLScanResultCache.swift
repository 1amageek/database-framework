import DatabaseEngine
import DatabaseTypes

/// Request-accounted owner for scan signatures and immutable result handles.
///
/// Binding payload remains in the shared snapshot. This owner accounts only
/// for the cache container, retained signature, and snapshot handle before the
/// Dictionary can allocate or extend their lifetime.
struct SPARQLScanResultCache: ~Copyable {
    private static let dictionaryOwnerByteCount: UInt64 = 64
    private static let dictionaryCapacitySlotByteCount: UInt64 = 256
    private static let entryOwnerByteCount: UInt64 = 64
    private static let signatureOwnerByteCount: UInt64 = 128
    private static let executionTermNodeByteCount: UInt64 = 32
    private static let stringOwnerByteCount: UInt64 = 16
    private static let namedGraphSetOwnerByteCount: UInt64 = 64
    private static let namedGraphCapacitySlotByteCount: UInt64 = 32

    private var entries: [
        SPARQLQueryExecutor.ScanSignature: SPARQLSharedBindingSnapshot
    ]
    private let reservation: DatabaseIntermediateReservation
    private let footprintMeter: SPARQLBindingFootprintMeter
    private var accountedCapacity: Int

    private init(
        reservation: DatabaseIntermediateReservation,
        footprintMeter: SPARQLBindingFootprintMeter
    ) {
        self.entries = [:]
        self.reservation = reservation
        self.footprintMeter = footprintMeter
        self.accountedCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter
    ) throws -> SPARQLScanResultCache {
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .joinCandidate
        )
        do {
            return SPARQLScanResultCache(
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

    borrowing func value(
        for signature: SPARQLQueryExecutor.ScanSignature
    ) -> SPARQLRetainedBindings? {
        entries[signature]?.retainedBindings()
    }

    mutating func insert(
        _ snapshot: SPARQLSharedBindingSnapshot,
        for signature: SPARQLQueryExecutor.ScanSignature,
        sourceWorkMeter: DatabaseWorkMeter
    ) throws {
        guard sourceWorkMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        if let originatingWorkMeter = snapshot.originatingWorkMeter,
           originatingWorkMeter !== workMeter {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        guard entries[signature] == nil else { return }

        let signatureFootprint = try footprint(of: signature)
        let targetCapacity = try Self.targetCapacity(
            current: accountedCapacity,
            requiredCount: entries.count + 1
        )
        var destinationBytes = try Self.checkedMultiply(
            UInt64(targetCapacity - accountedCapacity),
            Self.dictionaryCapacitySlotByteCount
        )
        destinationBytes = try Self.checkedAdd(
            destinationBytes,
            Self.entryOwnerByteCount
        )
        destinationBytes = try Self.checkedAdd(
            destinationBytes,
            signatureFootprint.bytes
        )
        try reservation.reserveAdditional(
            bytes: destinationBytes,
            at: .joinCandidate
        )
        if targetCapacity != accountedCapacity {
            entries.reserveCapacity(targetCapacity)
            accountedCapacity = targetCapacity
        }
        entries[signature] = snapshot
    }

    private func footprint(
        of signature: SPARQLQueryExecutor.ScanSignature
    ) throws -> DatabaseIntermediateFootprint {
        var accumulated = DatabaseIntermediateFootprint(
            bytes: Self.signatureOwnerByteCount
        )
        accumulated = try accumulated.adding(
            try footprint(of: signature.subject)
        ).adding(
            try footprint(of: signature.predicate)
        ).adding(
            try footprint(of: signature.object)
        ).adding(
            try footprint(of: signature.graphTarget)
        )
        return accumulated
    }

    /// Triple-term nesting is already bounded by SPARQL structural admission;
    /// this traversal adds no retained container of its own.
    private func footprint(
        of term: ExecutionTerm
    ) throws -> DatabaseIntermediateFootprint {
        var accumulated = DatabaseIntermediateFootprint(
            bytes: Self.executionTermNodeByteCount
        )
        switch term {
        case .variable(let variable):
            return try accumulated.adding(
                try Self.stringFootprint(variable)
            )
        case .value(let value):
            return try accumulated.adding(
                try footprintMeter.footprint(of: value)
            )
        case .wildcard:
            return accumulated
        case .tripleTerm(let subject, let predicate, let object):
            accumulated = try accumulated.adding(
                try self.footprint(of: subject)
            ).adding(
                try self.footprint(of: predicate)
            ).adding(
                try self.footprint(of: object)
            )
            return accumulated
        }
    }

    private func footprint(
        of graphTarget: RDFGraphScanTarget
    ) throws -> DatabaseIntermediateFootprint {
        switch graphTarget {
        case .empty, .defaultGraph, .allNamedGraphs:
            return DatabaseIntermediateFootprint()
        case .named(let graph):
            return try RDFTermRetainedFootprint.measure(graph.term)
        case .namedGraphUnion(let graphs):
            var footprint = DatabaseIntermediateFootprint(
                bytes: Self.namedGraphSetOwnerByteCount
            )
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: try Self.checkedMultiply(
                        UInt64(graphs.count),
                        Self.namedGraphCapacitySlotByteCount
                    )
                )
            )
            for graph in graphs {
                footprint = try footprint.adding(
                    try RDFTermRetainedFootprint.measure(graph.term)
                )
            }
            return footprint
        }
    }

    private static func stringFootprint(
        _ value: String
    ) throws -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(
            bytes: try checkedAdd(
                stringOwnerByteCount,
                UInt64(value.utf8.count)
            )
        )
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
