import DatabaseTypes
import DatabaseEngine
import DatabaseKit
import StorageKit

/// Scans one logical RDF dataset assembled from canonical six-way quad indexes.
public struct IndexedRDFDatasetScanner: RDFDatasetScanner {
    private static let namedGraphMergeBlankNodeDomain: ByteString = [
        0x52, 0x44, 0x46, 0x4d, 0x42, 0x4e, 0x01,
    ]

    private enum ScanControl: Error {
        case logicalLimitReached
    }

    private enum PhysicalGraphConstraint {
        case bound(
            component: RDFQuadIndexComponentWritePlan,
            term: RDFTerm?
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

    private struct DigestSink: RDFTermStorageSink {
        var hasher: SHA256Accumulator

        mutating func write(_ byte: UInt8) {
            hasher.update(byte)
        }

        mutating func write(_ bytes: UnsafeRawBufferPointer) {
            hasher.update(bytes)
        }
    }

    private let sources: [RDFDatasetSource]

    public init(sources: [RDFDatasetSource]) {
        self.sources = sources
    }

    public func scan(
        subject: RDFTerm?,
        predicate: RDFTerm?,
        object: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        if let limit, limit <= 0 {
            return .empty()
        }

        if case .empty = graphTarget {
            return .empty()
        }

        let mergesNamedGraphs: Bool
        if case .namedGraphUnion = graphTarget {
            mergesNamedGraphs = true
        } else {
            mergesNamedGraphs = false
        }

        // Keep the reservation lexically before every retained owner. Error
        // unwinding destroys local owners in reverse declaration order, so no
        // Set or Array storage can outlive its request ledger claim.
        var intermediateReservation: DatabaseIntermediateReservation?
        var rows: [RDFDatasetScanStorageRow] = []
        var seenRows = Set<RDFDatasetScanStorageRow>()
        var seenMergedTriples = Set<RDFTriple>()
        var physicalScanCount = 0

        scanLoop: for source in sources {
            let graphConstraints = try graphConstraints(
                for: source.coverage,
                requested: graphTarget
            )

            for graphConstraint in graphConstraints {
                try workMeter.consume(at: .indexScan)
                physicalScanCount += 1
                let mergeBlankNodeHasher: SHA256Accumulator?
                if mergesNamedGraphs {
                    guard case .bound(_, let graph?) = graphConstraint else {
                        throw RDFDatasetScannerError
                            .namedGraphMergeRequiresBoundGraph
                    }
                    mergeBlankNodeHasher = try namedGraphMergeHasher(
                        graph: graph,
                        workMeter: workMeter
                    )
                } else {
                    mergeBlankNodeHasher = nil
                }
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
                let range: (begin: ByteString, end: ByteString)
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
                var cursor = transaction.rangeCursor(
                    from: .firstGreaterOrEqual(range.begin),
                    to: .firstGreaterOrEqual(range.end),
                    limit: storageLimit,
                    reverse: false,
                    snapshot: readMode.usesSnapshotReads,
                    streamingMode: .iterator
                )
                do {
                    while let (key, value) = try await cursor.next() {
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
                            continue
                        }
                        try workMeter.consume(at: .deduplication)
                        if mergesNamedGraphs {
                            guard let mergeBlankNodeHasher else {
                                throw RDFDatasetScannerError
                                    .namedGraphMergeRequiresBoundGraph
                            }
                            let triple = try mergedNamedGraphTriple(
                                quad,
                                hasher: mergeBlankNodeHasher,
                                workMeter: workMeter
                            )
                            guard seenMergedTriples.insert(triple).inserted else {
                                continue
                            }
                            let row = RDFDatasetScanStorageRow(
                                quad: triple.quad,
                                coveringValue: value,
                                storedFieldNames: source.storedFieldNames
                            )
                            let metrics = try RDFDatasetScanRetainedMetrics.measure(
                                row.quad,
                                mergesNamedGraphs: true,
                                coveringValueByteCount: value.count,
                                storedFieldNames: source.storedFieldNames
                            )
                            try reserveIntermediate(
                                metrics,
                                workMeter: workMeter,
                                reservation: &intermediateReservation
                            )
                            rows.append(row)
                        } else {
                            let row = RDFDatasetScanStorageRow(
                                quad: quad,
                                coveringValue: value,
                                storedFieldNames: source.storedFieldNames
                            )
                            guard !seenRows.contains(row) else {
                                continue
                            }
                            let metrics = try RDFDatasetScanRetainedMetrics.measure(
                                quad,
                                mergesNamedGraphs: false,
                                coveringValueByteCount: value.count,
                                storedFieldNames: source.storedFieldNames
                            )
                            try reserveIntermediate(
                                metrics,
                                workMeter: workMeter,
                                reservation: &intermediateReservation
                            )
                            seenRows.insert(row)
                            rows.append(row)
                        }
                        if let limit, rows.count >= limit {
                            reachedLogicalLimit = true
                            break
                        }
                    }
                } catch {
                    let iterationError = error
                    do {
                        try await cursor.finish()
                    } catch {
                        throw StorageRangeCleanupError(
                            iterationError: iterationError,
                            cleanupError: error
                        )
                    }
                    throw iterationError
                }
                try await cursor.finish()
                if reachedLogicalLimit {
                    break scanLoop
                }
            }
        }

        return RDFDatasetScanResult(
            rows: rows,
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

    private func namedGraphMergeHasher(
        graph: RDFTerm,
        workMeter: DatabaseWorkMeter
    ) throws -> SHA256Accumulator {
        let plan = try RDFTermStorageFormat.encodingPlan(
            graph,
            role: .graphName
        )
        try workMeter.consume(
            UInt64(
                Self.namedGraphMergeBlankNodeDomain.count
                    + MemoryLayout<UInt64>.size
                    + plan.byteCount
            ),
            at: .deduplication
        )
        var hasher = SHA256Accumulator()
        hasher.update(Self.namedGraphMergeBlankNodeDomain)
        Self.update(UInt64(plan.byteCount), hasher: &hasher)
        var sink = DigestSink(hasher: hasher)
        try RDFTermStorageFormat.encode(plan, into: &sink)
        return sink.hasher
    }

    private func mergedNamedGraphTriple(
        _ quad: RDFQuad,
        hasher: SHA256Accumulator,
        workMeter: DatabaseWorkMeter
    ) throws -> RDFTriple {
        RDFTriple(
            subject: try mergedNamedGraphSubject(
                quad.subject,
                hasher: hasher,
                workMeter: workMeter
            ),
            predicate: quad.predicate,
            object: try mergedNamedGraphTerm(
                quad.object,
                hasher: hasher,
                workMeter: workMeter
            )
        )
    }

    private func mergedNamedGraphSubject(
        _ subject: RDFSubject,
        hasher: SHA256Accumulator,
        workMeter: DatabaseWorkMeter
    ) throws -> RDFSubject {
        switch subject {
        case .iri:
            return subject
        case .blankNode(let identifier):
            return .blankNode(
                try mergedNamedGraphBlankNode(
                    identifier,
                    hasher: hasher,
                    workMeter: workMeter
                )
            )
        }
    }

    private func mergedNamedGraphTerm(
        _ term: RDFTerm,
        hasher: SHA256Accumulator,
        workMeter: DatabaseWorkMeter
    ) throws -> RDFTerm {
        switch term {
        case .iri, .literal:
            return term
        case .blankNode(let identifier):
            return .blankNode(
                try mergedNamedGraphBlankNode(
                    identifier,
                    hasher: hasher,
                    workMeter: workMeter
                )
            )
        case .tripleTerm(let subject, let predicate, let object):
            return .tripleTerm(
                subject: try mergedNamedGraphSubject(
                    subject,
                    hasher: hasher,
                    workMeter: workMeter
                ),
                predicate: predicate,
                object: try mergedNamedGraphTerm(
                    object,
                    hasher: hasher,
                    workMeter: workMeter
                )
            )
        }
    }

    private func mergedNamedGraphBlankNode(
        _ identifier: RDFBlankNodeIdentifier,
        hasher: SHA256Accumulator,
        workMeter: DatabaseWorkMeter
    ) throws -> RDFBlankNodeIdentifier {
        let labelByteCount = identifier.rawValue.utf8.count
        try workMeter.consume(
            UInt64(MemoryLayout<UInt64>.size + labelByteCount),
            at: .deduplication
        )
        var digest = hasher
        Self.update(UInt64(labelByteCount), hasher: &digest)
        digest.update(utf8: identifier.rawValue)
        // RDF merge changes blank-node identity, so the new owned label is the
        // required semantic output boundary; graph and source labels are fed
        // directly into the digest without an intermediate encoded payload.
        let value = digest.withUnsafeDigestBytes { bytes in
            "m" + Self.lowercaseHex(bytes)
        }
        return try RDFBlankNodeIdentifier(value)
    }

    private static func update(
        _ value: UInt64,
        hasher: inout SHA256Accumulator
    ) {
        var encoded = value.bigEndian
        withUnsafeBytes(of: &encoded) { bytes in
            hasher.update(bytes)
        }
    }

    private static func lowercaseHex(
        _ digest: UnsafeRawBufferPointer
    ) -> String {
        String(unsafeUninitializedCapacity: digest.count * 2) { output in
            var outputIndex = 0
            for byte in digest {
                let high = byte >> 4
                let low = byte & 0x0f
                output[outputIndex] = high < 10 ? high + 0x30 : high + 0x57
                output[outputIndex + 1] = low < 10 ? low + 0x30 : low + 0x57
                outputIndex += 2
            }
            return outputIndex
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
        let fullRange: (begin: ByteString, end: ByteString)
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
            do throws(RDFQuadIndexPhysicalCodecError) {
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

            let graphTerm: RDFTerm
            do throws(RDFQuadIndexPhysicalCodecError) {
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

            let graphComponent: RDFQuadIndexComponentWritePlan
            do throws(RDFTermStorageError) {
                graphComponent = try RDFQuadIndexComponentWritePlan(
                    canonicalBytes: encodedGraph,
                    role: .graphName
                )
            } catch let reason {
                throw RDFDatasetScannerError.invalidRDFComponent(
                    source: sourceDescription(source),
                    component: .graph,
                    reason: reason
                )
            }
            var prefix = RDFQuadIndexPrefixWritePlan()
            do throws(RDFQuadIndexPhysicalCodecError) {
                try prefix.append(graphComponent)
                begin = try source.physicalCodec.range(
                    prefix: prefix,
                    ordering: ordering
                ).end
            } catch let reason {
                throw physicalIndexFailure(source, reason: reason)
            }
        }
    }

    private func graphRange(
        _ graph: RDFGraphName,
        source: RDFDatasetSource
    ) throws -> (begin: ByteString, end: ByteString) {
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
        requested: RDFGraphScanTarget
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
        for term: RDFTerm
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
        subject: RDFTerm?,
        predicate: RDFTerm?,
        object: RDFTerm?,
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
        first: RDFTerm?,
        firstRole: RDFTermRole,
        second: RDFTerm?,
        secondRole: RDFTermRole,
        third: RDFTerm?,
        thirdRole: RDFTermRole,
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
        key: ByteString,
        source: RDFDatasetSource,
        ordering: GraphIndexOrdering
    ) throws -> RDFQuad {
        do throws(RDFQuadIndexPhysicalCodecError) {
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
        subject: RDFTerm?,
        predicate: RDFTerm?,
        object: RDFTerm?,
        graphConstraint: PhysicalGraphConstraint
    ) -> Bool {
        if let subject, quad.subject.term != subject { return false }
        if let predicate, quad.predicate.term != predicate { return false }
        if let object, quad.object != object { return false }

        switch graphConstraint {
        case .bound(_, let graph):
            return quad.graph?.term == graph
        case .anyNamedGraph:
            return quad.graph != nil
        }
    }

    private func sourceDescription(_ source: RDFDatasetSource) -> String {
        "\(source.entityName):\(source.indexName)"
    }
}
