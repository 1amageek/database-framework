import DatabaseValue
import DatabaseEngine
import Graph
import StorageKit

/// Scans one logical RDF dataset assembled from canonical six-way quad indexes.
public struct IndexedRDFDatasetScanner: RDFDatasetScanner {
    private enum ScanControl: Error {
        case logicalLimitReached
    }

    private enum PhysicalGraphConstraint {
        case bound(
            component: RDFQuadIndexComponentWritePlan,
            term: DatabaseRDFTerm?
        )
        case anyNamedGraph

        var boundComponent: RDFQuadIndexComponentWritePlan? {
            switch self {
            case .bound(let component, _):
                component
            case .anyNamedGraph:
                nil
            }
        }
    }

    private enum PhysicalRequestedGraph {
        case defaultGraph
        case named(RDFGraphName)
        case allNamedGraphs
    }

    private let sources: [RDFDatasetSource]

    public init(sources: [RDFDatasetSource]) {
        self.sources = sources
    }

    public func scan(
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphScope: RDFGraphScanScope,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        if let limit, limit <= 0 {
            return .empty()
        }

        if case .empty = graphScope {
            return .empty()
        }

        let mergesNamedGraphs: Bool
        if case .namedGraphUnion = graphScope {
            mergesNamedGraphs = true
        } else {
            mergesNamedGraphs = false
        }

        // Keep the reservation lexically before every retained owner. Error
        // unwinding destroys local owners in reverse declaration order, so no
        // Set or Array storage can outlive its request ledger claim.
        var intermediateReservation: DatabaseIntermediateReservation?
        var quads: [RDFQuad] = []
        var seenQuads = Set<RDFQuad>()
        var seenMergedTriples = Set<RDFTriple>()
        var retainedMetricWorklist: [DatabaseRDFTerm] = []
        var modeledWorklistCapacity: UInt64 = 0
        var scratchReservation: DatabaseIntermediateReservation?
        defer {
            retainedMetricWorklist.removeAll(keepingCapacity: false)
            scratchReservation?.release()
        }
        var physicalScanCount = 0

        scanLoop: for source in sources {
            let graphConstraints = try graphConstraints(
                for: source.coverage,
                requested: graphScope
            )

            for graphConstraint in graphConstraints {
                try workMeter.consume(at: .indexScan)
                physicalScanCount += 1
                let ordering = GraphIndexScanPlanner.ordering(
                    strategy: .quadStore,
                    subjectBound: subject != nil,
                    predicateBound: predicate != nil,
                    objectBound: object != nil,
                    graphBound: graphConstraint.boundComponent != nil
                )
                let prefix = try scanPrefix(
                    subject: subject,
                    predicate: predicate,
                    object: object,
                    graphConstraint: graphConstraint,
                    ordering: ordering
                )
                let range: (begin: Bytes, end: Bytes)
                do {
                    range = try source.physicalCodec.range(
                        prefix: prefix,
                        ordering: ordering
                    )
                } catch let reason {
                    throw physicalIndexFailure(source, reason: reason)
                }

                let storageLimit = try workMeter.storageReadLimitWithSentinel()
                var reachedLogicalLimit = false
                do {
                    try await transaction.forEachInRange(
                        from: .firstGreaterOrEqual(range.begin),
                        to: .firstGreaterOrEqual(range.end),
                        limit: storageLimit,
                        snapshot: readMode.usesSnapshotReads,
                        streamingMode: .iterator
                    ) { key, _ in
                        try workMeter.consume(at: .storageRow)
                        let quad = try decodeQuad(
                            key: key,
                            source: source,
                            ordering: ordering
                        )
                        guard matches(
                            quad,
                            subject: subject,
                            predicate: predicate,
                            object: object,
                            graphConstraint: graphConstraint
                        ) else {
                            return
                        }
                        try workMeter.consume(at: .deduplication)
                        if mergesNamedGraphs {
                            let triple = quad.triple
                            if !seenMergedTriples.contains(triple) {
                                let metrics = try RDFDatasetScanRetainedMetrics.measure(
                                    quad,
                                    mergesNamedGraphs: true,
                                    worklist: &retainedMetricWorklist,
                                    modeledWorklistCapacity: &modeledWorklistCapacity,
                                    scratchReservation: &scratchReservation,
                                    workMeter: workMeter
                                )
                                try reserveIntermediate(
                                    metrics,
                                    workMeter: workMeter,
                                    reservation: &intermediateReservation
                                )
                                seenMergedTriples.insert(triple)
                                quads.append(triple.quad)
                            }
                        } else if !seenQuads.contains(quad) {
                            let metrics = try RDFDatasetScanRetainedMetrics.measure(
                                quad,
                                mergesNamedGraphs: false,
                                worklist: &retainedMetricWorklist,
                                modeledWorklistCapacity: &modeledWorklistCapacity,
                                scratchReservation: &scratchReservation,
                                workMeter: workMeter
                            )
                            try reserveIntermediate(
                                metrics,
                                workMeter: workMeter,
                                reservation: &intermediateReservation
                            )
                            seenQuads.insert(quad)
                            quads.append(quad)
                        }
                        if let limit, quads.count >= limit {
                            throw ScanControl.logicalLimitReached
                        }
                    }
                } catch ScanControl.logicalLimitReached {
                    reachedLogicalLimit = true
                }
                if reachedLogicalLimit {
                    break scanLoop
                }
            }
        }

        return RDFDatasetScanResult(
            quads: quads,
            physicalScanCount: physicalScanCount,
            intermediateReservation: intermediateReservation
        )
    }

    private func reserveIntermediate(
        _ metrics: RDFDatasetScanRetainedMetrics,
        workMeter: DatabaseWorkMeter,
        reservation: inout DatabaseIntermediateReservation?
    ) throws {
        if let reservation {
            try reservation.reserveAdditional(
                rows: metrics.rowCount,
                bytes: metrics.retainedByteCount,
                at: .deduplication
            )
        } else {
            reservation = try workMeter.reserveIntermediate(
                rows: metrics.rowCount,
                bytes: metrics.retainedByteCount,
                at: .deduplication
            )
        }
    }

    public func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [RDFGraphName] {
        if let limit, limit <= 0 { return [] }

        var seen = Set<RDFGraphName>()
        for source in sources {
            switch source.coverage {
            case .defaultGraph:
                continue
            case .namedGraph(let graph):
                try workMeter.consume(at: .deduplication)
                seen.insert(graph)
            case .dataset:
                try await collectNamedGraphs(
                    from: source,
                    readMode: readMode,
                    transaction: transaction,
                    workMeter: workMeter,
                    into: &seen
                )
            }
        }

        try workMeter.consume(UInt64(seen.count), at: .sortInput)
        var graphs = try seen.sorted { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            return lhs < rhs
        }
        if let limit, graphs.count > limit {
            graphs.removeLast(graphs.count - limit)
        }
        return graphs
    }

    public func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        for source in sources {
            switch source.coverage {
            case .defaultGraph:
                continue
            case .namedGraph(let available):
                if available == graph { return true }
            case .dataset:
                try workMeter.consume(at: .indexScan)
                let range = try graphRange(
                    graph,
                    source: source
                )
                try workMeter.consume(at: .storageRow)
                if let key = try await transaction.getKey(
                    selector: .firstGreaterOrEqual(range.begin),
                    snapshot: readMode.usesSnapshotReads
                ), key.lexicographicallyPrecedes(range.end) {
                    return true
                }
            }
        }
        return false
    }

    private func collectNamedGraphs(
        from source: RDFDatasetSource,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter,
        into seen: inout Set<RDFGraphName>
    ) async throws {
        try workMeter.consume(at: .indexScan)
        let ordering = GraphIndexOrdering.gspo
        let fullRange: (begin: Bytes, end: Bytes)
        do {
            fullRange = try source.physicalCodec.range(
                prefix: RDFQuadIndexPrefixWritePlan(),
                ordering: ordering
            )
        } catch let reason {
            throw physicalIndexFailure(source, reason: reason)
        }

        var begin = fullRange.begin
        while let key = try await transaction.getKey(
            selector: .firstGreaterOrEqual(begin),
            snapshot: readMode.usesSnapshotReads
        ), key.lexicographicallyPrecedes(fullRange.end) {
            try workMeter.consume(at: .storageRow)

            let encoded: RDFQuadIndexEncodedQuad
            do {
                encoded = try source.physicalCodec.decodeEncodedQuad(
                    key: key,
                    ordering: ordering
                )
            } catch let reason {
                throw physicalIndexFailure(source, reason: reason)
            }

            // The default-graph discriminator sorts after canonical RDF term
            // tags in the graph-first index, so no named graph can follow it.
            guard let encodedGraph = encoded.graph else { break }

            let graphTerm: DatabaseRDFTerm
            do {
                graphTerm = try source.physicalCodec.decodeGraphComponent(
                    encodedGraph
                )
            } catch let reason {
                switch reason {
                case .invalidComponent(let component, let error):
                    throw RDFDatasetScannerError.invalidRDFComponent(
                        source: sourceDescription(source),
                        component: component,
                        reason: error
                    )
                default:
                    throw physicalIndexFailure(source, reason: reason)
                }
            }
            let graph = try RDFGraphName(graphTerm)
            try workMeter.consume(at: .deduplication)
            seen.insert(graph)

            var prefix = RDFQuadIndexPrefixWritePlan()
            do {
                try prefix.append(
                    try RDFQuadIndexComponentWritePlan(
                        canonicalBytes: encodedGraph,
                        role: .graphName
                    )
                )
                begin = try source.physicalCodec.range(
                    prefix: prefix,
                    ordering: ordering
                ).end
            } catch let reason as RDFQuadIndexPhysicalCodecError {
                throw physicalIndexFailure(source, reason: reason)
            } catch let reason as DatabaseRDFTermCodecError {
                throw RDFDatasetScannerError.invalidRDFComponent(
                    source: sourceDescription(source),
                    component: .graph,
                    reason: reason
                )
            }
        }
    }

    private func graphRange(
        _ graph: RDFGraphName,
        source: RDFDatasetSource
    ) throws -> (begin: Bytes, end: Bytes) {
        var prefix = RDFQuadIndexPrefixWritePlan()
        try prefix.append(
            try RDFQuadIndexComponentWritePlan(
                term: graph.term,
                role: .graphName
            )
        )
        do {
            return try source.physicalCodec.range(
                prefix: prefix,
                ordering: .gspo
            )
        } catch let reason {
            throw physicalIndexFailure(source, reason: reason)
        }
    }

    private func graphConstraints(
        for coverage: RDFDatasetSourceCoverage,
        requested: RDFGraphScanScope
    ) throws -> [PhysicalGraphConstraint] {
        switch requested {
        case .empty:
            return []
        case .namedGraphUnion(let graphs):
            var constraints: [PhysicalGraphConstraint] = []
            constraints.reserveCapacity(graphs.count)
            for graph in graphs {
                if let constraint = try graphConstraint(
                    for: coverage,
                    requested: .named(graph)
                ) {
                    constraints.append(constraint)
                }
            }
            return constraints
        case .defaultGraph, .named, .allNamedGraphs:
            let physicalRequest: PhysicalRequestedGraph
            switch requested {
            case .defaultGraph:
                physicalRequest = .defaultGraph
            case .named(let graph):
                physicalRequest = .named(graph)
            case .allNamedGraphs:
                physicalRequest = .allNamedGraphs
            case .empty, .namedGraphUnion:
                return []
            }
            if let constraint = try graphConstraint(
                for: coverage,
                requested: physicalRequest
            ) {
                return [constraint]
            }
            return []
        }
    }

    private func graphConstraint(
        for coverage: RDFDatasetSourceCoverage,
        requested: PhysicalRequestedGraph
    ) throws -> PhysicalGraphConstraint? {
        switch (coverage, requested) {
        case (.defaultGraph, .defaultGraph), (.dataset, .defaultGraph):
            return .bound(component: .defaultGraph, term: nil)
        case (.namedGraph(let available), .named(let requestedGraph)):
            guard available == requestedGraph else { return nil }
            return try boundGraphConstraint(for: available.term)
        case (.dataset, .named(let requestedGraph)):
            return try boundGraphConstraint(for: requestedGraph.term)
        case (.namedGraph(let available), .allNamedGraphs):
            return try boundGraphConstraint(for: available.term)
        case (.dataset, .allNamedGraphs):
            return .anyNamedGraph
        case (.defaultGraph, .named),
             (.defaultGraph, .allNamedGraphs),
             (.namedGraph, .defaultGraph):
            return nil
        }
    }

    private func boundGraphConstraint(
        for term: DatabaseRDFTerm
    ) throws -> PhysicalGraphConstraint {
        .bound(
            component: try RDFQuadIndexComponentWritePlan(
                term: term,
                role: .graphName
            ),
            term: term
        )
    }

    private func scanPrefix(
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphConstraint: PhysicalGraphConstraint,
        ordering: GraphIndexOrdering
    ) throws -> RDFQuadIndexPrefixWritePlan {
        var prefix = RDFQuadIndexPrefixWritePlan()
        if ordering.isGraphFirst,
           let graph = graphConstraint.boundComponent {
            try prefix.append(graph)
        }

        switch ordering {
        case .spo, .gspo:
            try appendContiguousTerms(
                first: subject,
                firstRole: .subject,
                second: predicate,
                secondRole: .predicate,
                third: object,
                thirdRole: .object,
                to: &prefix
            )
        case .pos, .gpos:
            try appendContiguousTerms(
                first: predicate,
                firstRole: .predicate,
                second: object,
                secondRole: .object,
                third: subject,
                thirdRole: .subject,
                to: &prefix
            )
        case .osp, .gosp:
            try appendContiguousTerms(
                first: object,
                firstRole: .object,
                second: subject,
                secondRole: .subject,
                third: predicate,
                thirdRole: .predicate,
                to: &prefix
            )
        case .out, .in, .sop, .pso, .ops:
            throw RDFQuadIndexPhysicalCodecError.unsupportedOrdering(ordering)
        }
        return prefix
    }

    private func appendContiguousTerms(
        first: DatabaseRDFTerm?,
        firstRole: DatabaseRDFTermRole,
        second: DatabaseRDFTerm?,
        secondRole: DatabaseRDFTermRole,
        third: DatabaseRDFTerm?,
        thirdRole: DatabaseRDFTermRole,
        to prefix: inout RDFQuadIndexPrefixWritePlan
    ) throws {
        guard let first else { return }
        try prefix.append(try RDFQuadIndexComponentWritePlan(
            term: first,
            role: firstRole
        ))
        guard let second else { return }
        try prefix.append(try RDFQuadIndexComponentWritePlan(
            term: second,
            role: secondRole
        ))
        guard let third else { return }
        try prefix.append(try RDFQuadIndexComponentWritePlan(
            term: third,
            role: thirdRole
        ))
    }

    private func decodeQuad(
        key: Bytes,
        source: RDFDatasetSource,
        ordering: GraphIndexOrdering
    ) throws -> RDFQuad {
        do {
            return try source.physicalCodec.decodeQuad(
                key: key,
                ordering: ordering
            )
        } catch let reason {
            switch reason {
            case .invalidComponent(let component, let error):
                throw RDFDatasetScannerError.invalidRDFComponent(
                    source: sourceDescription(source),
                    component: component,
                    reason: error
                )
            default:
                throw physicalIndexFailure(source, reason: reason)
            }
        }
    }

    private func physicalIndexFailure(
        _ source: RDFDatasetSource,
        reason: RDFQuadIndexPhysicalCodecError
    ) -> RDFDatasetScannerError {
        .physicalIndexFailure(
            source: sourceDescription(source),
            reason: reason
        )
    }

    private func matches(
        _ quad: RDFQuad,
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphConstraint: PhysicalGraphConstraint
    ) -> Bool {
        if let subject, quad.subject != subject { return false }
        if let predicate, quad.predicate != predicate { return false }
        if let object, quad.object != object { return false }

        switch graphConstraint {
        case .bound(_, let graph):
            return quad.graph == graph
        case .anyNamedGraph:
            return quad.graph != nil
        }
    }

    private func sourceDescription(_ source: RDFDatasetSource) -> String {
        "\(source.entityName):\(source.indexName)"
    }
}
