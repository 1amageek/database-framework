import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Linear ownership of sorted, unique named-graph discovery output.
public struct RDFDatasetNamedGraphs: ~Copyable, Sendable {
    private let owner: RDFDatasetNamedGraphOwner?
    package let workMeter: DatabaseWorkMeter

    fileprivate init(
        graphs: consuming [RDFGraphName],
        reservation: DatabaseIntermediateReservation?,
        workMeter: DatabaseWorkMeter
    ) {
        precondition(
            graphs.isEmpty == (reservation == nil),
            "Retained graph names and reservation must have the same lifetime"
        )
        if let reservation {
            precondition(
                reservation.workMeter === workMeter,
                "Named graphs and reservation must use the same work meter"
            )
        }
        self.owner = graphs.isEmpty
            ? nil
            : RDFDatasetNamedGraphOwner(
                graphs: graphs,
                reservation: reservation!
            )
        self.workMeter = workMeter
    }

    package init(
        graphs: consuming [RDFGraphName],
        workMeter: DatabaseWorkMeter
    ) throws {
        var builder = RDFDatasetNamedGraphBuilder(workMeter: workMeter)
        for graph in graphs {
            try builder.append(graph)
        }
        self = try builder.finish(limit: nil)
    }

    package static func empty(
        workMeter: DatabaseWorkMeter
    ) -> RDFDatasetNamedGraphs {
        RDFDatasetNamedGraphs(
            graphs: [],
            reservation: nil,
            workMeter: workMeter
        )
    }

    public var count: Int { owner?.graphs.count ?? 0 }
    public var isEmpty: Bool { owner == nil }

    package borrowing func withGraph<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFGraphName) throws(Failure) -> Void
    ) throws(Failure) {
        guard let owner else {
            preconditionFailure("Named-graph index is out of range")
        }
        precondition(owner.graphs.indices.contains(index))
        try body(owner.graphs[index])
    }

    package borrowing func withGraph<Failure: Error>(
        at index: Int,
        _ body: (borrowing RDFGraphName) async throws(Failure) -> Void
    ) async throws(Failure) {
        guard let owner else {
            preconditionFailure("Named-graph index is out of range")
        }
        precondition(owner.graphs.indices.contains(index))
        try await body(owner.graphs[index])
    }
}

private final class RDFDatasetNamedGraphOwner: Sendable {
    let graphs: [RDFGraphName]
    let reservation: DatabaseIntermediateReservation

    init(
        graphs: consuming [RDFGraphName],
        reservation: DatabaseIntermediateReservation
    ) {
        self.graphs = graphs
        self.reservation = reservation
    }
}

struct RDFDatasetNamedGraphRetainedMetrics: Sendable, Equatable {
    let retainedRows: UInt64
    let retainedArrayBytes: UInt64
    let retainedPayloadBytes: UInt64
    let constructionRows: UInt64
    let constructionBytes: UInt64

    private static let graphValueBaseline: UInt64 = 32
    static let resultOwnerOverhead: UInt64 = 64
    private static let setEntryOverhead: UInt64 = 32
    private static let geometricCapacityMultiplier: UInt64 = 2

    static func preflight(
        _ validation: RDFTermStorageValidation
    ) throws -> Self {
        try make(
            termBytes: RDFTermRetainedFootprint.measure(validation).bytes
        )
    }

    static func measure(
        _ graph: borrowing RDFGraphName
    ) throws -> Self {
        try make(
            termBytes: RDFTermRetainedFootprint.measure(graph.term).bytes
        )
    }

    private static func make(termBytes: UInt64) throws -> Self {
        let storage = try RDFDatasetScanRetainedMetrics.checkedMultiply(
            graphValueBaseline,
            geometricCapacityMultiplier
        )
        var constructionBytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
            storage,
            setEntryOverhead
        )
        constructionBytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
            constructionBytes,
            termBytes
        )
        return Self(
            retainedRows: 1,
            retainedArrayBytes: storage,
            retainedPayloadBytes: termBytes,
            constructionRows: 1,
            constructionBytes: constructionBytes
        )
    }

    func admissionRows() throws -> UInt64 {
        try RDFDatasetScanRetainedMetrics.checkedAdd(
            retainedRows,
            constructionRows
        )
    }

    func admissionBytes(includesOwner: Bool) throws -> UInt64 {
        var bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
            retainedArrayBytes,
            retainedPayloadBytes
        )
        bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
            bytes,
            constructionBytes
        )
        if includesOwner {
            bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
                bytes,
                Self.resultOwnerOverhead
            )
        }
        return bytes
    }

    func retainedBytes(includesOwner: Bool) throws -> UInt64 {
        var bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
            retainedArrayBytes,
            retainedPayloadBytes
        )
        if includesOwner {
            bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
                bytes,
                Self.resultOwnerOverhead
            )
        }
        return bytes
    }
}

struct RDFDatasetNamedGraphAdmission: ~Copyable {
    private let reservation: DatabaseIntermediateReservation
    private let metrics: RDFDatasetNamedGraphRetainedMetrics

    init(
        metrics: RDFDatasetNamedGraphRetainedMetrics,
        includesOwner: Bool,
        workMeter: DatabaseWorkMeter
    ) throws {
        self.metrics = metrics
        self.reservation = try workMeter.reserveIntermediate(
            rows: metrics.admissionRows(),
            bytes: metrics.admissionBytes(includesOwner: includesOwner),
            at: .deduplication
        )
    }

    consuming func commit(
        to retainedReservation: inout DatabaseIntermediateReservation?
    ) throws -> RDFDatasetNamedGraphRetainedMetrics {
        if let retainedReservation {
            try retainedReservation.absorbAll(from: reservation)
        } else {
            retainedReservation = reservation
        }
        return metrics
    }
}

struct RDFDatasetNamedGraphBuilder: ~Copyable {
    private var graphs: [RDFGraphName] = []
    private var seen = Set<RDFGraphName>()
    private var reservation: DatabaseIntermediateReservation?
    private var constructionRows: UInt64 = 0
    private var constructionBytes: UInt64 = 0
    private let workMeter: DatabaseWorkMeter

    init(workMeter: DatabaseWorkMeter) {
        self.workMeter = workMeter
    }

    var count: Int { graphs.count }

    func prepareAppend(
        _ metrics: RDFDatasetNamedGraphRetainedMetrics
    ) throws -> RDFDatasetNamedGraphAdmission {
        try RDFDatasetNamedGraphAdmission(
            metrics: metrics,
            includesOwner: graphs.isEmpty,
            workMeter: workMeter
        )
    }

    mutating func append(
        _ graph: borrowing RDFGraphName,
        using admission: consuming RDFDatasetNamedGraphAdmission
    ) throws {
        try workMeter.consume(at: .deduplication)
        guard seen.insert(copy graph).inserted else { return }
        graphs.append(copy graph)
        let metrics = try admission.commit(to: &reservation)
        constructionRows = try RDFDatasetScanRetainedMetrics.checkedAdd(
            constructionRows,
            metrics.constructionRows
        )
        constructionBytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
            constructionBytes,
            metrics.constructionBytes
        )
    }

    mutating func append(
        _ graph: borrowing RDFGraphName
    ) throws {
        let admission = try prepareAppend(
            RDFDatasetNamedGraphRetainedMetrics.measure(graph)
        )
        try append(graph, using: admission)
    }

    consuming func finish(
        limit: Int?
    ) throws -> RDFDatasetNamedGraphs {
        seen.removeAll(keepingCapacity: false)
        reservation?.releaseGuaranteedPartial(
            rows: constructionRows,
            bytes: constructionBytes
        )
        try workMeter.consume(UInt64(graphs.count), at: .sortInput)
        if graphs.count > 1 {
            try heapSort()
        }
        if let limit {
            let retainedCount = max(0, min(limit, graphs.count))
            if graphs.count > retainedCount {
                var releasedRows: UInt64 = 0
                var releasedPayloadBytes: UInt64 = 0
                for index in retainedCount..<graphs.count {
                    let metrics = try RDFDatasetNamedGraphRetainedMetrics
                        .measure(graphs[index])
                    releasedRows = try RDFDatasetScanRetainedMetrics.checkedAdd(
                        releasedRows,
                        metrics.retainedRows
                    )
                    releasedPayloadBytes = try RDFDatasetScanRetainedMetrics
                        .checkedAdd(
                            releasedPayloadBytes,
                            metrics.retainedPayloadBytes
                        )
                }
                graphs.removeLast(graphs.count - retainedCount)
                reservation?.releaseGuaranteedPartial(
                    rows: releasedRows,
                    bytes: releasedPayloadBytes
                )
            }
        }
        return RDFDatasetNamedGraphs(
            graphs: graphs,
            reservation: reservation,
            workMeter: workMeter
        )
    }

    private mutating func heapSort() throws {
        var start = graphs.count / 2
        while start > 0 {
            start -= 1
            try siftDown(from: start, through: graphs.count - 1)
        }
        var end = graphs.count - 1
        while end > 0 {
            graphs.swapAt(0, end)
            end -= 1
            try siftDown(from: 0, through: end)
        }
    }

    private mutating func siftDown(
        from start: Int,
        through end: Int
    ) throws {
        var root = start
        while true {
            let left = root * 2 + 1
            guard left <= end else { return }
            var selected = left
            let right = left + 1
            if right <= end {
                try workMeter.consume(2, at: .sortComparison)
                if graphs[selected] < graphs[right] { selected = right }
            }
            try workMeter.consume(2, at: .sortComparison)
            guard graphs[root] < graphs[selected] else { return }
            graphs.swapAt(root, selected)
            root = selected
        }
    }
}
