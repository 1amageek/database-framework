import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import Synchronization
import TestHeartbeat
import TestSupport
import Testing
@testable import GraphIndex

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
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(
                now: Timestamp(secondsSinceUnixEpoch: 0)
            ),
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
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(
                now: Timestamp(secondsSinceUnixEpoch: 0)
            ),
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
        let quad = try makeQuad(graph: graph)

        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .batch,
            clock: TestProcessMonotonicClock()
        ) { transaction in
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
        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            let recording = RecordingTransaction(
                underlying: transaction,
                observations: observations
            )
            for readMode in [RDFDatasetReadMode.snapshot, .serializable] {
                _ = try await store.scan(
                    subject: nil,
                    predicate: nil,
                    object: nil,
                    graphTarget: .named(graph),
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

        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .batch,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            let recording = RecordingTransaction(
                underlying: transaction,
                observations: observations
            )
            let result = try await store.insert(
                try makeQuad(graph: graph),
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
                try makeQuad(graph: graph, suffix: "protected"),
                transaction: protectedTransaction,
                workMeter: makeMeter()
            )
            #expect(protectedResult == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))

            let orphanTransaction = try engine.createTransaction()
            try writePhysicalQuad(
                try makeQuad(graph: graph, suffix: "orphan"),
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
            budget: ExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func makeQuad(
        graph: RDFGraphName,
        suffix: String = "value"
    ) throws -> RDFQuad {
        RDFQuad(
            subject: .iri(
                try RDFIRI(
                    "https://example.com/subject/\(suffix)"
                )
            ),
            predicate: try RDFPredicateIRI(
                "https://example.com/predicate"
            ),
            object: try .iri(
                validating:
                    "https://example.com/object/\(suffix)"
            ),
            graph: graph
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
            subject: RDFTerm?,
            predicate: RDFTerm?,
            object: RDFTerm?,
            graphTarget: RDFGraphScanTarget,
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionReadAccess,
            workMeter: DatabaseWorkMeter
        ) async throws -> RDFDatasetScanResult {
            observations.record(.scan(readMode))
            return .empty(
                physicalScanCount: 1,
                workMeter: workMeter
            )
        }

        func namedGraphs(
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionReadAccess,
            workMeter: DatabaseWorkMeter
        ) async throws -> RDFDatasetNamedGraphs {
            observations.record(.namedGraphs(readMode))
            return try RDFDatasetNamedGraphs(
                graphs: [graph],
                workMeter: workMeter
            )
        }

        func containsNamedGraph(
            _ graph: RDFGraphName,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionReadAccess,
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

        func entityRange(limit: Int, snapshot: Bool) {
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

        var transactionDomain: StorageTransactionDomain {
            underlying.transactionDomain
        }

        func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
            observations.recordValue(snapshot: snapshot)
            return try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getValue(for key: ByteString) async throws -> ByteString? {
            try await underlying.getValue(for: key)
        }

        func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? {
            observations.recordKey(snapshot: snapshot)
            return try await underlying.getKey(
                selector: selector,
                snapshot: snapshot
            )
        }

        func rangeCursor(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> KeyValueCursor {
            observations.entityRange(limit: limit, snapshot: snapshot)
            return underlying.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
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

        func setOption(
            to value: ByteString?,
            forOption option: TransactionOption
        ) throws {
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
    }
}
