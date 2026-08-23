import DatabaseEngine
import DatabaseKit

/// Request-accounted named-graph discovery result.
///
/// Rows retain the owner so a graph value cannot outlive the intermediate
/// memory reservation that admitted it.
public struct RDFNamedGraphResult: Sendable, RandomAccessCollection {
    public typealias Element = RDFNamedGraphResultRow
    public typealias Index = Int

    private static let emptyOwner = RDFNamedGraphResultOwner(
        graphs: [],
        reservation: nil
    )

    private let owner: RDFNamedGraphResultOwner

    package init(buffer: consuming DatabaseRetainedBuffer<RDFGraphName>) {
        let retained = buffer.moveRetainingReservation()
        self.owner = RDFNamedGraphResultOwner(
            graphs: retained.elements,
            reservation: retained.reservation
        )
    }

    public static var empty: RDFNamedGraphResult {
        RDFNamedGraphResult(owner: emptyOwner)
    }

    private init(owner: RDFNamedGraphResultOwner) {
        self.owner = owner
    }

    public var startIndex: Int { owner.graphs.startIndex }
    public var endIndex: Int { owner.graphs.endIndex }

    public subscript(position: Int) -> RDFNamedGraphResultRow {
        precondition(owner.graphs.indices.contains(position))
        return RDFNamedGraphResultRow(owner: owner, position: position)
    }

    public func index(after index: Int) -> Int { index + 1 }
    public func index(before index: Int) -> Int { index - 1 }
    public func distance(from start: Int, to end: Int) -> Int { end - start }
    public func index(_ index: Int, offsetBy distance: Int) -> Int {
        index + distance
    }
}

public struct RDFNamedGraphResultRow: Sendable {
    private let owner: RDFNamedGraphResultOwner
    private let position: Int

    fileprivate init(owner: RDFNamedGraphResultOwner, position: Int) {
        self.owner = owner
        self.position = position
    }

    package var graph: RDFGraphName { owner.graphs[position] }

    @_spi(DatabaseExecution)
    public func ownedGraph() -> RDFGraphName { owner.graphs[position] }
}

private final class RDFNamedGraphResultOwner: Sendable {
    // Declaration order keeps the reservation alive until graph storage dies.
    let graphs: [RDFGraphName]
    let reservation: DatabaseIntermediateReservation?

    init(
        graphs: consuming [RDFGraphName],
        reservation: DatabaseIntermediateReservation?
    ) {
        self.graphs = graphs
        self.reservation = reservation
    }
}

package struct RDFNamedGraphResultBuilder: ~Copyable {
    private var builder: DatabaseRetainedArrayBuilder<RDFGraphName>
    private let workMeter: DatabaseWorkMeter

    package init(workMeter: DatabaseWorkMeter) throws {
        self.workMeter = workMeter
        self.builder = try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: .deduplication,
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: RDFGraphName.self)
        )
    }

    package mutating func append(_ graph: RDFGraphName) throws {
        try builder.append(
            footprint: try Self.footprint(of: graph),
            at: .deduplication
        ) {
            graph
        }
    }

    package consuming func finish(limit: Int?) throws -> RDFNamedGraphResult {
        let meter = workMeter
        try meter.consume(UInt64(builder.count), at: .sortInput)
        var buffer = try builder.finish().sortingElements { lhs, rhs in
            try meter.consume(2, at: .sortComparison)
            return lhs < rhs
        }

        let duplicateFootprint = try buffer.withSpan { graphs in
            var released = DatabaseIntermediateFootprint()
            guard graphs.count > 1 else { return released }
            for index in 1..<graphs.count where graphs[index] == graphs[index - 1] {
                released = try released.adding(Self.footprint(of: graphs[index]))
            }
            return released
        }
        buffer = try buffer.removingAdjacentDuplicates(by: ==)
            .releasingRetainedFootprint(duplicateFootprint)

        if let limit, buffer.count > limit {
            let released = try buffer.withSpan { graphs in
                var footprint = DatabaseIntermediateFootprint()
                for index in limit..<graphs.count {
                    footprint = try footprint.adding(
                        Self.footprint(of: graphs[index])
                    )
                }
                return footprint
            }
            buffer = try buffer.retainingSubrange(0..<limit)
                .releasingRetainedFootprint(released)
        }
        return RDFNamedGraphResult(buffer: buffer)
    }

    private static func footprint(
        of graph: RDFGraphName
    ) throws -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try RDFDatasetScanRetainedMetrics.checkedAdd(
                16,
                RDFTermRetainedFootprint.measure(graph.term).bytes
            )
        )
    }
}
