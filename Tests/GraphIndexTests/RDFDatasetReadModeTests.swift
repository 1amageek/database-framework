import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import StorageKit
import Synchronization
import TestHeartbeat
import Testing
@testable import GraphIndex
#if FOUNDATION_DB
import TestSupport
#endif

@Suite("RDF dataset read isolation", .heartbeat)
struct RDFDatasetReadModeTests {
    @Test("SPARQL executor propagates default and mutation read modes")
    func sparqlExecutorPropagatesReadMode() async throws {
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/sparql-read-mode"
        )

        let defaultObservations = ScannerCallObservations()
        let defaultExecutor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            datasetScanner: ReadModeSpyScanner(
                graph: graph,
                observations: defaultObservations
            )
        )
        try await exerciseAllDatasetReadPaths(
            executor: defaultExecutor,
            graph: graph
        )
        #expect(defaultObservations.calls == [
            .scan(.snapshot),
            .containsNamedGraph(.snapshot),
            .namedGraphs(.snapshot),
        ])

        let mutationObservations = ScannerCallObservations()
        let mutationExecutor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            datasetScanner: ReadModeSpyScanner(
                graph: graph,
                observations: mutationObservations
            ),
            readMode: .serializable
        )
        try await exerciseAllDatasetReadPaths(
            executor: mutationExecutor,
            graph: graph
        )
        #expect(mutationObservations.calls == [
            .scan(.serializable),
            .containsNamedGraph(.serializable),
            .namedGraphs(.serializable),
        ])
    }

    @Test("Scanner maps explicit read modes to storage snapshot flags")
    func scannerPropagatesReadMode() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/read-isolation"
        )
        let quad = makeQuad(graph: graph)

        try await engine.withTransaction(configuration: .batch) { transaction in
            _ = try await store.insert(
                quad,
                transaction: transaction,
                workMeter: makeMeter()
            )
        }

        let observations = ReadObservations()
        let indexedScanner = IndexedRDFDatasetScanner(
            sources: [store.datasetSource]
        )
        try await engine.withTransaction(configuration: .default) { transaction in
            let recording = RecordingTransaction(
                underlying: transaction,
                observations: observations
            )
            for readMode in [RDFDatasetReadMode.snapshot, .serializable] {
                _ = try await store.scan(
                    subject: nil,
                    predicate: nil,
                    object: nil,
                    graphScope: .named(graph),
                    limit: nil,
                    readMode: readMode,
                    transaction: recording,
                    workMeter: makeMeter()
                )
                _ = try await store.namedGraphs(
                    limit: nil,
                    readMode: readMode,
                    transaction: recording,
                    workMeter: makeMeter()
                )
                _ = try await store.containsNamedGraph(
                    graph,
                    readMode: readMode,
                    transaction: recording,
                    workMeter: makeMeter()
                )
                _ = try await indexedScanner.containsNamedGraph(
                    graph,
                    readMode: readMode,
                    transaction: recording,
                    workMeter: makeMeter()
                )
            }
        }

        #expect(observations.rangeSnapshots == [true, true, false, false])
        #expect(observations.valueSnapshots == [true, false])
        #expect(observations.keySnapshots == [true, false])
    }

    @Test("Implicit graph creation uses one conflict-tracked single-row probe")
    func insertionProtectsEmptyGraphRange() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/range-probe"
        )
        let observations = ReadObservations()

        try await engine.withTransaction(configuration: .batch) { transaction in
            let recording = RecordingTransaction(
                underlying: transaction,
                observations: observations
            )
            let result = try await store.insert(
                makeQuad(graph: graph),
                transaction: recording,
                workMeter: makeMeter()
            )
            #expect(result == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
        }

        #expect(observations.rangeCalls == [
            RangeCall(limit: 1, snapshot: false)
        ])
    }

#if FOUNDATION_DB
    @Test("Orphan insertion conflicts with the protected graph range")
    func orphanInsertionConflictsWithGraphCreation() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let engine = try await FoundationDBScenarioCoordinator.shared.makeEngine()
            let root = Subspace(
                prefix: Tuple("rdf-graph-range-conflict-tests").pack()
            )
            let rootRange = root.range()
            try await engine.withTransaction { transaction in
                try transaction.clearRange(
                    beginKey: rootRange.begin,
                    endKey: rootRange.end
                )
            }

            let store = CanonicalRDFGraphStore(rootSubspace: root)
            let graph = try RDFGraphName(
                iri: "https://example.com/graph/concurrent-orphan"
            )
            let protectedTransaction = try engine.createTransaction()
            let protectedResult = try await store.insert(
                makeQuad(graph: graph, suffix: "protected"),
                transaction: protectedTransaction,
                workMeter: makeMeter()
            )
            #expect(protectedResult == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))

            let orphanTransaction = try engine.createTransaction()
            try writePhysicalQuad(
                makeQuad(graph: graph, suffix: "orphan"),
                root: root,
                transaction: orphanTransaction
            )
            try await orphanTransaction.commit()

            var protectedTransactionCommitted = false
            do {
                try await protectedTransaction.commit()
                protectedTransactionCommitted = true
                Issue.record(
                    "Expected the graph range read to conflict with the orphan insertion"
                )
            } catch let error as StorageError {
                #expect(error.code == .transactionConflict)
            } catch {
                Issue.record("Unexpected transaction failure: \(error)")
            }
            if !protectedTransactionCommitted {
                try await protectedTransaction.cancel()
            }

            try await engine.withTransaction { transaction in
                try transaction.clearRange(
                    beginKey: rootRange.begin,
                    endKey: rootRange.end
                )
            }
        }
    }
#endif

    private func makeRoot() -> Subspace {
        Subspace(prefix: Tuple("rdf-read-mode-tests").pack())
    }

    private func exerciseAllDatasetReadPaths(
        executor: SPARQLQueryExecutor,
        graph: RDFGraphName
    ) async throws {
        let triple = ExecutionTriple(
            subject: .variable("?subject"),
            predicate: .variable("?predicate"),
            object: .variable("?object")
        )
        for pattern in [
            ExecutionPattern.basic([triple]),
            .graph(.named(graph), .basic([])),
            .graph(.variable("?graph"), .basic([])),
        ] {
            _ = try await executor.execute(
                pattern: pattern,
                limit: nil,
                offset: 0,
                workMeter: makeMeter()
            )
        }
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            )
        )
    }

    private func makeQuad(
        graph: RDFGraphName,
        suffix: String = "value"
    ) -> RDFQuad {
        RDFQuad(
            subject: .iri("https://example.com/subject/\(suffix)"),
            predicate: .iri("https://example.com/predicate"),
            object: .iri("https://example.com/object/\(suffix)"),
            graph: graph.term
        )
    }

    private func writePhysicalQuad(
        _ quad: RDFQuad,
        root: Subspace,
        transaction: any TransactionAccess
    ) throws {
        let codec = RDFQuadIndexPhysicalCodec(
            baseSubspace: root.subspace(Int64(1))
        )
        try RDFQuadIndexWritePlan(quad: quad).forEachEntry { entry in
            try transaction.setValue([], for: codec.encode(entry))
        }
    }

    private struct RangeCall: Sendable, Equatable {
        let limit: Int
        let snapshot: Bool
    }

    private enum ScannerCall: Sendable, Equatable {
        case scan(RDFDatasetReadMode)
        case namedGraphs(RDFDatasetReadMode)
        case containsNamedGraph(RDFDatasetReadMode)
    }

    private final class ScannerCallObservations: Sendable {
        private let state = Mutex<[ScannerCall]>([])

        var calls: [ScannerCall] {
            state.withLock { $0 }
        }

        func record(_ call: ScannerCall) {
            state.withLock { $0.append(call) }
        }
    }

    private struct ReadModeSpyScanner: RDFDatasetScanner {
        let graph: RDFGraphName
        let observations: ScannerCallObservations

        func scan(
            subject: DatabaseRDFTerm?,
            predicate: DatabaseRDFTerm?,
            object: DatabaseRDFTerm?,
            graphScope: RDFGraphScanScope,
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionAccess,
            workMeter: DatabaseWorkMeter
        ) async throws -> RDFDatasetScanResult {
            observations.record(.scan(readMode))
            return RDFDatasetScanResult(quads: [], physicalScanCount: 1)
        }

        func namedGraphs(
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionAccess,
            workMeter: DatabaseWorkMeter
        ) async throws -> [RDFGraphName] {
            observations.record(.namedGraphs(readMode))
            return [graph]
        }

        func containsNamedGraph(
            _ graph: RDFGraphName,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionAccess,
            workMeter: DatabaseWorkMeter
        ) async throws -> Bool {
            observations.record(.containsNamedGraph(readMode))
            return graph == self.graph
        }
    }

    private final class ReadObservations: Sendable {
        private struct State: Sendable {
            var rangeCalls: [RangeCall] = []
            var valueSnapshots: [Bool] = []
            var keySnapshots: [Bool] = []
        }

        private let state = Mutex(State())

        var rangeCalls: [RangeCall] {
            state.withLock { $0.rangeCalls }
        }

        var rangeSnapshots: [Bool] {
            state.withLock { $0.rangeCalls.map(\.snapshot) }
        }

        var valueSnapshots: [Bool] {
            state.withLock { $0.valueSnapshots }
        }

        var keySnapshots: [Bool] {
            state.withLock { $0.keySnapshots }
        }

        func recordRange(limit: Int, snapshot: Bool) {
            state.withLock {
                $0.rangeCalls.append(RangeCall(limit: limit, snapshot: snapshot))
            }
        }

        func recordValue(snapshot: Bool) {
            state.withLock { $0.valueSnapshots.append(snapshot) }
        }

        func recordKey(snapshot: Bool) {
            state.withLock { $0.keySnapshots.append(snapshot) }
        }
    }

    private final class RecordingTransaction: TransactionAccess, Sendable {
        struct RangeResult: TransactionRangeResult {
            typealias Element = (Bytes, Bytes)

            let cursor: KeyValueCursor

            func makeAsyncIterator() -> AsyncIterator {
                AsyncIterator(cursor: cursor)
            }

            struct AsyncIterator: TransactionRangeIterator {
                var cursor: KeyValueCursor

                mutating func next() async throws -> Element? {
                    try await cursor.next()
                }

                mutating func finish(
                    isolation actor: isolated (any Actor)?
                ) async throws {
                    try await cursor.finish()
                }
            }
        }

        let underlying: any TransactionAccess
        let observations: ReadObservations

        init(
            underlying: any TransactionAccess,
            observations: ReadObservations
        ) {
            self.underlying = underlying
            self.observations = observations
        }

        var capabilities: TransactionCapabilities {
            underlying.capabilities
        }

        func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
            observations.recordValue(snapshot: snapshot)
            return try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getKey(selector: KeySelector, snapshot: Bool) async throws -> Bytes? {
            observations.recordKey(snapshot: snapshot)
            return try await underlying.getKey(
                selector: selector,
                snapshot: snapshot
            )
        }

        func getRange(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> RangeResult {
            observations.recordRange(limit: limit, snapshot: snapshot)
            return RangeResult(cursor: underlying.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            ))
        }

        func setValue(_ value: Bytes, for key: Bytes) throws {
            try underlying.setValue(value, for: key)
        }

        func clear(key: Bytes) throws {
            try underlying.clear(key: key)
        }

        func clearRange(beginKey: Bytes, endKey: Bytes) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
        }

        func atomicOp(
            key: Bytes,
            param: Bytes,
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

        func setOption(
            to value: Bytes?,
            forOption option: TransactionOption
        ) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func setOption(to value: Int, forOption option: TransactionOption) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func addConflictRange(
            beginKey: Bytes,
            endKey: Bytes,
            type: ConflictRangeType
        ) throws {
            try underlying.addConflictRange(
                beginKey: beginKey,
                endKey: endKey,
                type: type
            )
        }

        func getEstimatedRangeSizeBytes(
            beginKey: Bytes,
            endKey: Bytes
        ) async throws -> Int {
            try await underlying.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }

        func getRangeSplitPoints(
            beginKey: Bytes,
            endKey: Bytes,
            chunkSize: Int
        ) async throws -> [Bytes] {
            try await underlying.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
        }
    }
}
