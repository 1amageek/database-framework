import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import GraphIndex

@Suite("Graph physical read budget")
struct GraphPhysicalReadBudgetTests {
    @Test("zero budget performs no backend range read")
    func zeroBudgetSkipsRangeRead() async throws {
        let seededIndex = try await makeSequentialAdjacencyIndex(edgeCount: 3)

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 0)
            let snapshot = GraphReadSnapshot(
                transaction: recording,
                monotonicClock: TestProcessMonotonicClock(),
                workBudget: budget
            )
            let scanner = GraphEdgeScanner(
                indexSubspace: seededIndex.subspace,
                strategy: .adjacency,
                graphTarget: .all,
                snapshot: snapshot
            )

            var count = 0
            var cursor = scanner.scanAllEdges(
                edgeLabel: nil,
                transaction: recording
            ).makeCursor()
            while try await cursor.next() != nil {
                count += 1
            }

            #expect(count == 0)
            #expect(metrics.snapshot.callLimits.isEmpty)
            #expect(metrics.snapshot.yieldedRows == 0)
            #expect(budget.consumedWorkUnits == 0)
            #expect(budget.limitReason != nil)
        }
    }

    @Test("backend limit bounds physical rows")
    func rangeLimitBoundsPhysicalRows() async throws {
        let seededIndex = try await makeSequentialAdjacencyIndex(edgeCount: 7)

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 3)
            let snapshot = GraphReadSnapshot(
                transaction: recording,
                monotonicClock: TestProcessMonotonicClock(),
                workBudget: budget
            )
            let scanner = GraphEdgeScanner(
                indexSubspace: seededIndex.subspace,
                strategy: .adjacency,
                graphTarget: .all,
                snapshot: snapshot
            )

            var count = 0
            var cursor = scanner.scanAllEdges(
                edgeLabel: nil,
                transaction: recording
            ).makeCursor()
            while try await cursor.next() != nil {
                count += 1
            }

            #expect(count == 3)
            #expect(metrics.snapshot.callLimits == [3])
            #expect(metrics.snapshot.yieldedRows == 3)
            #expect(budget.consumedWorkUnits == 3)
            #expect(budget.limitReason != nil)
        }
    }

    @Test("abandoned cursor charges its reservation without another range")
    func abandonedCursorChargesReservation() async throws {
        let seededIndex = try await makeSequentialAdjacencyIndex(edgeCount: 7)

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 5)
            let snapshot = GraphReadSnapshot(
                transaction: recording,
                monotonicClock: TestProcessMonotonicClock(),
                workBudget: budget
            )
            let scanner = GraphEdgeScanner(
                indexSubspace: seededIndex.subspace,
                strategy: .adjacency,
                graphTarget: .all,
                snapshot: snapshot
            )

            let edge = try await firstEdge(
                scanner.scanAllEdges(
                    edgeLabel: nil,
                    transaction: recording
                )
            )

            #expect(edge != nil)
            #expect(metrics.snapshot.callLimits == [5])
            #expect(metrics.snapshot.yieldedRows == 1)
            #expect(budget.consumedWorkUnits == 5)
            #expect(budget.limitReason != nil)
        }
    }

    @Test("abandoned batch cursor does not open the next identity range")
    func abandonedBatchCursorDoesNotReadNextIdentity() async throws {
        let seededIndex = try await makeSequentialAdjacencyIndex(edgeCount: 3)

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let scanner = GraphEdgeScanner(
                indexSubspace: seededIndex.subspace,
                strategy: .adjacency,
                graphTarget: .all
            )
            var iterator = scanner.batchScanOutgoing(
                from: [.identifier("source-0"), .identifier("source-1")],
                edgeLabel: .identifier("edge"),
                transaction: recording
            ).makeCursor()

            let edge = try await iterator.next()

            #expect(edge?.source == .identifier("source-0"))
            #expect(metrics.snapshot.callLimits == [0])
            #expect(metrics.snapshot.yieldedRows == 1)
        }
    }

    @Test("batch scan opens one point range for each unique identity")
    func batchScanDeduplicatesPointRanges() async throws {
        let seededIndex = try await makeAdjacencyIndex(
            edges: [
                ("source-0", "edge", "target-0"),
                ("source-1", "edge", "target-1")
            ]
        )

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let scanner = GraphEdgeScanner(
                indexSubspace: seededIndex.subspace,
                strategy: .adjacency,
                graphTarget: .all
            )
            var edges: [EdgeInfo] = []

            var cursor = scanner.batchScanOutgoing(
                from: [
                    .identifier("source-1"),
                    .identifier("source-0"),
                    .identifier("source-1")
                ],
                edgeLabel: .identifier("edge"),
                transaction: recording
            ).makeCursor()
            while let edge = try await cursor.next() {
                edges.append(edge)
            }

            #expect(edges.map(\.source) == [
                .identifier("source-0"),
                .identifier("source-1")
            ])
            #expect(metrics.snapshot.callLimits == [0, 0])
            #expect(metrics.snapshot.yieldedRows == 2)
        }
    }

    @Test("pull traversal stops physical reads when its consumer stops")
    func pullTraversalStopsWithConsumer() async throws {
        let seededIndex = try await makeAdjacencyIndex(
            edges: [
                ("source", "edge", "target-0"),
                ("source", "edge", "target-1")
            ]
        )

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let traverser = try GraphTraverser(
                snapshot: GraphReadSnapshot(
                    transaction: recording,
                    monotonicClock: TestProcessMonotonicClock()
                ),
                subspace: seededIndex.subspace
            )
            var iterator = traverser.neighbors(
                from: .identifier("source"),
                edgeLabel: .identifier("edge")
            ).makeCursor()

            let edge = try await iterator.next()

            #expect(edge?.target == .identifier("target-0"))
            #expect(metrics.snapshot.callLimits == [0])
            #expect(metrics.snapshot.yieldedRows == 1)
        }
    }

    @Test("breadth-first traversal consumes one stable transaction snapshot")
    func breadthFirstTraversalUsesStableSnapshot() async throws {
        let seededIndex = try await makeAdjacencyIndex(
            edges: [
                ("A", "edge", "B"),
                ("A", "edge", "C"),
                ("B", "edge", "D"),
                ("C", "edge", "D")
            ]
        )

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let traverser = try GraphTraverser(
                snapshot: GraphReadSnapshot(
                    transaction: recording,
                    monotonicClock: TestProcessMonotonicClock()
                ),
                subspace: seededIndex.subspace,
                configuration: GraphTraverserConfiguration(
                    maximumDepth: 2,
                    maximumNodes: 10
                )
            )
            var steps: [GraphTraversalStep] = []

            var cursor = traverser.traverse(
                from: .identifier("A"),
                edgeLabel: .identifier("edge")
            ).makeCursor()
            while let step = try await cursor.next() {
                steps.append(step)
            }

            #expect(steps == [
                GraphTraversalStep(depth: 0, node: .identifier("A")),
                GraphTraversalStep(depth: 1, node: .identifier("B")),
                GraphTraversalStep(depth: 1, node: .identifier("C")),
                GraphTraversalStep(depth: 2, node: .identifier("D"))
            ])
            #expect(metrics.snapshot.yieldedRows == 4)
        }
    }

    @Test("traversal reports a typed node limit instead of a partial success")
    func traversalReportsNodeLimit() async throws {
        let seededIndex = try await makeAdjacencyIndex(
            edges: [
                ("source", "edge", "target-0"),
                ("source", "edge", "target-1")
            ]
        )

        try await seededIndex.engine.withTransaction { transaction in
            let traverser = try GraphTraverser(
                snapshot: GraphReadSnapshot(
                    transaction: transaction,
                    monotonicClock: TestProcessMonotonicClock()
                ),
                subspace: seededIndex.subspace,
                configuration: GraphTraverserConfiguration(
                    maximumDepth: 1,
                    maximumNodes: 2
                )
            )
            var iterator = traverser.traverse(
                from: .identifier("source"),
                edgeLabel: .identifier("edge")
            ).makeCursor()

            #expect(try await iterator.next()?.node == .identifier("source"))
            #expect(try await iterator.next()?.node == .identifier("target-0"))
            do {
                _ = try await iterator.next()
                Issue.record("Expected a typed traversal node limit")
            } catch let error as GraphTraversalError {
                #expect(error == .maximumNodesReached(explored: 2, limit: 2))
            }
        }
    }

    @Test("neighbor sequence reports an exhausted physical read budget")
    func neighborSequenceReportsPhysicalBudget() async throws {
        let seededIndex = try await makeAdjacencyIndex(
            edges: [
                ("source", "edge", "target-0"),
                ("source", "edge", "target-1")
            ]
        )

        try await seededIndex.engine.withTransaction { transaction in
            let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 1)
            let traverser = try GraphTraverser(
                snapshot: GraphReadSnapshot(
                    transaction: transaction,
                    monotonicClock: TestProcessMonotonicClock(),
                    workBudget: budget
                ),
                subspace: seededIndex.subspace
            )
            var iterator = traverser.neighbors(
                from: .identifier("source"),
                edgeLabel: .identifier("edge")
            ).makeCursor()

            #expect(try await iterator.next()?.target == .identifier("target-0"))
            do {
                _ = try await iterator.next()
                Issue.record("Expected an exhausted physical read budget")
            } catch let error as GraphTraversalError {
                guard case .incomplete(.maxWorkUnitsReached) = error else {
                    Issue.record("Expected a work-unit limit, received \(error)")
                    return
                }
            }
        }
    }

    @Test("shortest path stops the physical scan at the first matching edge")
    func shortestPathStopsAtFirstMatch() async throws {
        let seededIndex = try await makeAdjacencyIndex(
            edges: [
                ("source", "edge", "target-0"),
                ("source", "edge", "target-1")
            ]
        )

        try await seededIndex.engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let finder = ShortestPathFinder(
                snapshot: GraphReadSnapshot(
                    transaction: recording,
                    monotonicClock: TestProcessMonotonicClock()
                ),
                subspace: seededIndex.subspace,
                configuration: ShortestPathConfiguration(
                    maxDepth: 1,
                    bidirectional: false,
                    batchSize: 100,
                    maxNodesExplored: 100
                )
            )

            let result = try await finder.findShortestPath(
                from: .identifier("source"),
                to: .identifier("target-0"),
                edgeLabel: .identifier("edge")
            )

            #expect(result.isConnected)
            #expect(result.path?.nodeIDs == [
                .identifier("source"),
                .identifier("target-0")
            ])
            #expect(metrics.snapshot.callLimits == [0])
            #expect(metrics.snapshot.yieldedRows == 1)
        }
    }

    @Test("connectivity reports an inconclusive work limit as a typed error")
    func connectivityReportsIncompleteSearch() async throws {
        let seededIndex = try await makeAdjacencyIndex(
            edges: [("source", "edge", "target")]
        )

        try await seededIndex.engine.withTransaction { transaction in
            let budget = GraphAlgorithmWorkBudget(maximumWorkUnits: 0)
            let finder = ShortestPathFinder(
                snapshot: GraphReadSnapshot(
                    transaction: transaction,
                    monotonicClock: TestProcessMonotonicClock(),
                    workBudget: budget
                ),
                subspace: seededIndex.subspace,
                configuration: ShortestPathConfiguration(
                    maxDepth: 1,
                    bidirectional: false,
                    batchSize: 1,
                    maxNodesExplored: 10
                )
            )

            do {
                _ = try await finder.isConnected(
                    from: .identifier("source"),
                    to: .identifier("target"),
                    edgeLabel: .identifier("edge")
                )
                Issue.record("Expected a typed incomplete connectivity error")
            } catch let error as ShortestPathError {
                #expect(error == .incomplete(
                    .maxWorkUnitsReached(consumed: 0, limit: 0)
                ))
            }
        }
    }

    @Test("RDF identities retain physical key storage through the scanner")
    func rdfIdentityRetainsPhysicalKeyStorage() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("rdf-storage-sharing-scan").pack())
        let subject = try RDFTermStorageFormat.encode(
            .blankNode(identifier: "event")
        )
        let predicate = try RDFTermStorageFormat.encode(
            .iri(validating: "urn:contains")
        )
        let object = try RDFTermStorageFormat.encode(
            .iri(validating: "urn:item")
        )
        let key = subspace.subspace(Int64(8)).pack(
            Tuple(
                RDFQuadIndexPhysicalLayout.defaultGraphDiscriminator,
                subject,
                predicate,
                object
            )
        )
        try await engine.withTransaction { transaction in
            try transaction.setValue(ByteString(), for: key)
        }

        try await engine.withTransaction { transaction in
            let metrics = RangeMetrics()
            let recording = RecordingTransaction(
                underlying: transaction,
                metrics: metrics
            )
            let scanner = GraphEdgeScanner(
                indexSubspace: subspace,
                strategy: .quadStore,
                graphTarget: .defaultGraph
            )
            let edge = try #require(
                try await firstEdge(
                    scanner.scanAllEdges(
                        edgeLabel: nil,
                        transaction: recording
                    )
                )
            )
            let physicalKeyRange = try #require(metrics.snapshot.lastKeyAddressRange)

            try expectIdentity(
                edge.source,
                isRetainedWithin: physicalKeyRange,
                decodesAs: try .blankNode(identifier: "event")
            )
            try expectIdentity(
                edge.edgeLabel,
                isRetainedWithin: physicalKeyRange,
                decodesAs: try .iri(validating: "urn:contains")
            )
            try expectIdentity(
                edge.target,
                isRetainedWithin: physicalKeyRange,
                decodesAs: try .iri(validating: "urn:item")
            )
        }
    }

    private func firstEdge(
        _ sequence: GraphEdgeScan
    ) async throws -> EdgeInfo? {
        var iterator = sequence.makeCursor()
        return try await iterator.next()
    }

    private func expectIdentity(
        _ identity: GraphIdentity,
        isRetainedWithin keyRange: Range<UInt>,
        decodesAs expectedTerm: RDFTerm
    ) throws {
        let bytes = try #require(identity.canonicalRDFBytes)
        bytes.withUnsafeBytes { buffer in
            let start = buffer.baseAddress.map(UInt.init(bitPattern:))
            #expect(start != nil)
            if let start {
                #expect(start >= keyRange.lowerBound)
                #expect(start + UInt(buffer.count) <= keyRange.upperBound)
            }
        }
        #expect(try identity.decodeRDFTerm() == expectedTerm)
    }

    private func makeSequentialAdjacencyIndex(
        edgeCount: Int
    ) async throws -> (engine: InMemoryEngine, subspace: Subspace) {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("physical-read-budget").pack())
        try await engine.withTransaction { transaction in
            for index in 0..<edgeCount {
                let source = "source-\(index)"
                let target = "target-\(index)"
                try transaction.setValue(
                    ByteString(),
                    for: subspace.subspace(Int64(0)).pack(
                        Tuple(source, "edge", target)
                    )
                )
            }
        }
        return (engine, subspace)
    }

    private func makeAdjacencyIndex(
        edges: [(source: String, label: String, target: String)]
    ) async throws -> (engine: InMemoryEngine, subspace: Subspace) {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("pull-graph-traversal").pack())
        try await engine.withTransaction { transaction in
            for edge in edges {
                try transaction.setValue(
                    ByteString(),
                    for: subspace.subspace(Int64(0)).pack(
                        Tuple(edge.source, edge.label, edge.target)
                    )
                )
            }
        }
        return (engine, subspace)
    }

    private final class RangeMetrics: Sendable {
        struct Snapshot: Sendable {
            let callLimits: [Int]
            let yieldedRows: Int
            let lastKeyAddressRange: Range<UInt>?
        }

        private struct State: Sendable {
            var callLimits: [Int] = []
            var yieldedRows = 0
            var lastKeyAddressRange: Range<UInt>?
        }

        private let state = Mutex(State())

        var snapshot: Snapshot {
            state.withLock {
                Snapshot(
                    callLimits: $0.callLimits,
                    yieldedRows: $0.yieldedRows,
                    lastKeyAddressRange: $0.lastKeyAddressRange
                )
            }
        }

        func recordCall(limit: Int) {
            state.withLock { $0.callLimits.append(limit) }
        }

        func recordYield(key: ByteString) {
            key.withUnsafeBytes { buffer in
                let range = buffer.baseAddress.map { address in
                    let start = UInt(bitPattern: address)
                    return start..<(start + UInt(buffer.count))
                }
                state.withLock {
                    $0.yieldedRows += 1
                    $0.lastKeyAddressRange = range
                }
            }
        }
    }

    private final class RecordingTransaction: TransactionAccess, Sendable {
        struct RangeResult: TransactionRangeResult {
            let underlying: any TransactionAccess
            let begin: KeySelector
            let end: KeySelector
            let limit: Int
            let reverse: Bool
            let snapshot: Bool
            let streamingMode: StreamingMode
            let metrics: RangeMetrics

            func makeCursor() -> Cursor {
                Cursor(
                    cursor: underlying.rangeCursor(
                        from: begin,
                        to: end,
                        limit: limit,
                        reverse: reverse,
                        snapshot: snapshot,
                        streamingMode: streamingMode
                    ),
                    metrics: metrics
                )
            }

            struct Cursor: TransactionRangeCursor {
                var cursor: KeyValueCursor
                let metrics: RangeMetrics

                mutating func next() async throws -> (ByteString, ByteString)? {
                    guard let element = try await cursor.next() else {
                        return nil
                    }
                    metrics.recordYield(key: element.0)
                    return element
                }

                mutating func finish(
                    isolation actor: isolated (any Actor)?
                ) async throws {
                    try await cursor.finish()
                }
            }
        }

        let underlying: any TransactionAccess
        let metrics: RangeMetrics

        init(
            underlying: any TransactionAccess,
            metrics: RangeMetrics
        ) {
            self.underlying = underlying
            self.metrics = metrics
        }

        var capabilities: TransactionCapabilities {
            underlying.capabilities
        }

        func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
            try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getValue(for key: ByteString) async throws -> ByteString? {
            try await underlying.getValue(for: key)
        }

        func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? {
            try await underlying.getKey(selector: selector, snapshot: snapshot)
        }

        func rangeCursor(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> KeyValueCursor {
            metrics.recordCall(limit: limit)
            return KeyValueCursor(consuming: RangeResult(
                underlying: underlying,
                begin: begin,
                end: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode,
                metrics: metrics
            ))
        }

        func setValue(_ value: ByteString, for key: ByteString) throws {
            try underlying.setValue(value, for: key)
        }

        func clear(key: ByteString) throws {
            try underlying.clear(key: key)
        }

        func clearRange(beginKey: ByteString, endKey: ByteString) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
        }

        func atomicOp(
            key: ByteString,
            param: ByteString,
            mutationType: MutationType
        ) throws {
            try underlying.atomicOp(
                key: key,
                param: param,
                mutationType: mutationType
            )
        }

        func setReadVersion(_ version: Int64) throws {
            try underlying.setReadVersion(version)
        }

        func getReadVersion() async throws -> Int64 {
            try await underlying.getReadVersion()
        }

        func setOption(forOption option: TransactionOption) throws {
            try underlying.setOption(forOption: option)
        }

        func setOption(to value: ByteString?, forOption option: TransactionOption) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func setOption(to value: Int, forOption option: TransactionOption) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func addConflictRange(
            beginKey: ByteString,
            endKey: ByteString,
            type: ConflictRangeType
        ) throws {
            try underlying.addConflictRange(
                beginKey: beginKey,
                endKey: endKey,
                type: type
            )
        }

        func getEstimatedRangeSizeBytes(
            beginKey: ByteString,
            endKey: ByteString
        ) async throws -> Int {
            try await underlying.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }

        func getRangeSplitPoints(
            beginKey: ByteString,
            endKey: ByteString,
            chunkSize: Int
        ) async throws -> [ByteString] {
            try await underlying.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
        }

        func requestVersionstamp() -> any PendingTransactionVersionstamp {
            underlying.requestVersionstamp()
        }
    }
}
