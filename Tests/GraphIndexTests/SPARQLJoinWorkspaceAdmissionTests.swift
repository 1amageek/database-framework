import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
import TestSupport
@_spi(DatabaseExecution) @testable import GraphIndex

@Suite("SPARQL join workspace admission")
struct SPARQLJoinWorkspaceAdmissionTests {
    @Test("Hash strategy executes against retained left bindings and releases its workspace")
    func hashStrategyRetainsAndReleasesWorkspace() async throws {
        let meter = makeJoinWorkspaceMeter()
        let scanner = JoinDatasetScanner(
            quads: [
                try makeJoinQuad(key: 0),
                try makeJoinQuad(key: 64),
            ]
        )

        do {
            let executor = try makeJoinExecutor(
                scanner: scanner,
                workMeter: meter
            )
            let leftBindings = try makeJoinLeftBindings(
                keys: Array(0...64),
                workMeter: meter
            )
            let retainedLeftRows = meter.retainedIntermediateRows
            let retainedLeftBytes = meter.retainedIntermediateBytes
            let engine = InMemoryEngine()
            let transaction = try engine.createTransaction()
            let evaluation = try await executor.evaluateHashJoinWithFallback(
                pattern: try makeJoinPattern(),
                leftBindings: leftBindings,
                joinVariables: ["?key"],
                transaction: transaction,
                activeGraph: executor.initialActiveGraph,
                filter: nil,
                resultLimit: nil
            )

            switch consume evaluation {
            case .executed(let retainedResults, let statistics):
                #expect(statistics.joinStrategies == [.hashJoin])
                #expect(statistics.indexScans == 1)
                let results = (consume retainedResults).promoteToOutput()
                #expect(results.count == 2)
                #expect(
                    Set(results.compactMap { $0["?left"] })
                        == [.int64(0), .int64(64)]
                )
            case .fallback(let reason, _):
                Issue.record("Unexpected hash fallback: \(reason)")
            }
            #expect(await scanner.callCount == 1)
            #expect(meter.retainedIntermediateRows == retainedLeftRows)
            #expect(meter.retainedIntermediateBytes == retainedLeftBytes)
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Failed hash entry admission publishes no partial bucket")
    func failedHashEntryAdmissionRollsBack() async throws {
        let maximumBytes: UInt64 = 4_096
        let meter = makeJoinWorkspaceMeter(
            maximumIntermediateBytes: maximumBytes
        )

        do {
            var index = try SPARQLHashJoinIndex.make(workMeter: meter)
            let retainedBeforeBlocker = meter.retainedIntermediateBytes
            let blocker = try meter.reserveIntermediate(
                bytes: maximumBytes - retainedBeforeBlocker,
                at: .joinCandidate
            )
            let binding = VariableBinding(["?key": .string("rejected")])

            do {
                try index.insert(
                    index: 0,
                    for: binding,
                    variables: ["?key"]
                )
                Issue.record("Expected hash entry admission to fail")
            } catch let error as DatabaseWorkLimitError {
                guard case .maximumIntermediateBytes(
                    stage: .joinCandidate,
                    consumed: maximumBytes,
                    requested: _,
                    maximum: maximumBytes
                ) = error else {
                    Issue.record("Unexpected hash admission error: \(error)")
                    return
                }
            }

            blocker.release()
            var observedMatch = false
            try await index.withIndices(
                for: binding,
                variables: ["?key"]
            ) { _ in
                observedMatch = true
            }
            #expect(!observedMatch)

            try index.insert(
                index: 0,
                for: binding,
                variables: ["?key"]
            )
            let retainedBeforeGrowth = meter.retainedIntermediateBytes
            let saturation = try meter.reserveIntermediate(
                bytes: maximumBytes - retainedBeforeGrowth,
                at: .joinCandidate
            )
            var keyScratchByteCount: UInt64?
            do {
                try await index.withIndices(
                    for: binding,
                    variables: ["?key"]
                ) { _ in
                    Issue.record("A saturated meter admitted hash-key scratch")
                }
            } catch let error as DatabaseWorkLimitError {
                guard case .maximumIntermediateBytes(
                    stage: .joinCandidate,
                    consumed: maximumBytes,
                    requested: let requested,
                    maximum: maximumBytes
                ) = error else {
                    Issue.record("Unexpected hash scratch error: \(error)")
                    return
                }
                keyScratchByteCount = requested
            }
            saturation.release()
            let keyScratch = try #require(keyScratchByteCount)
            let previouslyClaimedGrowth = UInt64(MemoryLayout<Int>.stride)
            let growthAllowance = keyScratch + previouslyClaimedGrowth
            let growthBlocker = try meter.reserveIntermediate(
                bytes: maximumBytes
                    - retainedBeforeGrowth
                    - growthAllowance,
                at: .joinCandidate
            )

            #expect(throws: DatabaseWorkLimitError.self) {
                try index.insert(
                    index: 1,
                    for: binding,
                    variables: ["?key"]
                )
            }

            growthBlocker.release()
            var indicesAfterRejection: [Int] = []
            try await index.withIndices(
                for: binding,
                variables: ["?key"]
            ) { indices in
                indicesAfterRejection = Array(indices)
            }
            #expect(indicesAfterRejection == [0])

            try index.insert(
                index: 1,
                for: binding,
                variables: ["?key"]
            )
            var indicesAfterRetry: [Int] = []
            try await index.withIndices(
                for: binding,
                variables: ["?key"]
            ) { indices in
                indicesAfterRetry = Array(indices)
            }
            #expect(indicesAfterRetry == [0, 1])
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Batched and OPTIONAL strategies reuse exact signatures without changing semantics")
    func batchedAndOptionalStrategiesReuseScans() async throws {
        let batchedMeter = makeJoinWorkspaceMeter()
        do {
            let scanner = JoinDatasetScanner(
                quads: [try makeJoinQuad(key: 7)]
            )
            let executor = try makeJoinExecutor(
                scanner: scanner,
                workMeter: batchedMeter
            )
            let leftBindings = try makeJoinLeftBindings(
                keys: [7, 7, 7],
                workMeter: batchedMeter
            )
            let retainedLeftBytes = batchedMeter.retainedIntermediateBytes
            let transaction = try InMemoryEngine().createTransaction()
            do {
                let result = try await executor
                    .evaluateBatchedNestedLoopJoinStep(
                        pattern: try makeJoinPattern(),
                        leftBindings: leftBindings,
                        transaction: transaction,
                        activeGraph: executor.initialActiveGraph,
                        filter: nil,
                        resultLimit: nil
                    )
                #expect(result.stats.joinStrategies == [.batchedNestedLoop])
                #expect(result.stats.indexScans == 1)
                #expect(result.bindings.count == 3)
            }
            #expect(await scanner.callCount == 1)
            #expect(
                batchedMeter.retainedIntermediateBytes
                    == retainedLeftBytes
            )
        }
        #expect(batchedMeter.retainedIntermediateRows == 0)
        #expect(batchedMeter.retainedIntermediateBytes == 0)

        let optionalMeter = makeJoinWorkspaceMeter()
        do {
            let scanner = JoinDatasetScanner(
                quads: [try makeJoinQuad(key: 7)]
            )
            let executor = try makeJoinExecutor(
                scanner: scanner,
                workMeter: optionalMeter
            )
            let leftBindings = try makeJoinLeftBindings(
                keys: [7, 7, 99],
                workMeter: optionalMeter
            )
            let retainedLeftBytes = optionalMeter.retainedIntermediateBytes
            let transaction = try InMemoryEngine().createTransaction()
            do {
                let result = try await executor
                    .evaluateOptionalBatchedSingleTriple(
                        leftBindings: leftBindings,
                        rightTriple: try makeJoinPattern(),
                        transaction: transaction,
                        activeGraph: executor.initialActiveGraph,
                        resultLimit: nil
                    )
                #expect(result.stats.indexScans == 2)
                #expect(result.stats.optionalMisses == 1)
                #expect(result.bindings.count == 3)
                var observedLeftValues: Set<FieldValue> = []
                for index in 0..<result.bindings.count {
                    result.bindings.withElement(at: index) { binding in
                        if let value = binding["?left"] {
                            observedLeftValues.insert(value)
                        }
                    }
                }
                #expect(
                    observedLeftValues
                        == [.int64(0), .int64(1), .int64(2)]
                )
            }
            #expect(await scanner.callCount == 2)
            #expect(
                optionalMeter.retainedIntermediateBytes
                    == retainedLeftBytes
            )
        }
        #expect(optionalMeter.retainedIntermediateRows == 0)
        #expect(optionalMeter.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation releases partial hash, cache, and output state in one request")
    func cancellationReleasesPartialStrategyState() async throws {
        let hashMeter = makeJoinWorkspaceMeter()
        do {
            let scanner = JoinDatasetScanner(
                quads: [try makeJoinQuad(key: 0)],
                cancellationCalls: [1]
            )
            let executor = try makeJoinExecutor(
                scanner: scanner,
                workMeter: hashMeter
            )
            let leftBindings = try makeJoinLeftBindings(
                keys: Array(0...64),
                workMeter: hashMeter
            )
            let retainedLeftBytes = hashMeter.retainedIntermediateBytes
            let transaction = try InMemoryEngine().createTransaction()
            do {
                let unexpected = try await executor
                    .evaluateHashJoinWithFallback(
                        pattern: try makeJoinPattern(),
                        leftBindings: leftBindings,
                        joinVariables: ["?key"],
                        transaction: transaction,
                        activeGraph: executor.initialActiveGraph,
                        filter: nil,
                        resultLimit: nil
                    )
                _ = consume unexpected
                Issue.record("Expected hash scan cancellation")
            } catch is CancellationError {
            }
            #expect(
                hashMeter.retainedIntermediateBytes == retainedLeftBytes
            )
        }
        #expect(hashMeter.retainedIntermediateRows == 0)
        #expect(hashMeter.retainedIntermediateBytes == 0)

        let batchedMeter = makeJoinWorkspaceMeter()
        do {
            let scanner = JoinDatasetScanner(
                quads: [
                    try makeJoinQuad(key: 1),
                    try makeJoinQuad(key: 2),
                ],
                cancellationCalls: [2]
            )
            let executor = try makeJoinExecutor(
                scanner: scanner,
                workMeter: batchedMeter
            )
            let leftBindings = try makeJoinLeftBindings(
                keys: [1, 2],
                workMeter: batchedMeter
            )
            let retainedLeftBytes = batchedMeter.retainedIntermediateBytes
            let transaction = try InMemoryEngine().createTransaction()
            do {
                let unexpected = try await executor
                    .evaluateBatchedNestedLoopJoinStep(
                        pattern: try makeJoinPattern(),
                        leftBindings: leftBindings,
                        transaction: transaction,
                        activeGraph: executor.initialActiveGraph,
                        filter: nil,
                        resultLimit: nil
                    )
                _ = consume unexpected
                Issue.record("Expected batched scan cancellation")
            } catch is CancellationError {
            }
            #expect(
                batchedMeter.retainedIntermediateBytes
                    == retainedLeftBytes
            )

            do {
                let retry = try await executor
                    .evaluateBatchedNestedLoopJoinStep(
                        pattern: try makeJoinPattern(),
                        leftBindings: leftBindings,
                        transaction: transaction,
                        activeGraph: executor.initialActiveGraph,
                        filter: nil,
                        resultLimit: nil
                    )
                #expect(retry.bindings.count == 2)
            }
            #expect(await scanner.callCount == 4)
            #expect(
                batchedMeter.retainedIntermediateBytes
                    == retainedLeftBytes
            )
        }
        #expect(batchedMeter.retainedIntermediateRows == 0)
        #expect(batchedMeter.retainedIntermediateBytes == 0)
    }
}

private func makeJoinWorkspaceMeter(
    maximumIntermediateBytes: UInt64 = 1_000_000
) -> DatabaseWorkMeter {
    DatabaseWorkMeter(
        budget: ExecutionBudget(
            maximumRows: 100,
            maximumWorkUnits: 10_000,
            maximumIntermediateRows: 1_000,
            maximumIntermediateBytes: maximumIntermediateBytes,
            timeoutMilliseconds: 30_000
        ),
        monotonicClock: TestProcessMonotonicClock()
    )
}

private func makeJoinExecutor(
    scanner: any RDFDatasetScanner,
    workMeter: DatabaseWorkMeter
) throws -> SPARQLQueryExecutor {
    try SPARQLQueryExecutor(
        monotonicClock: TestProcessMonotonicClock(),
        wallClock: FixedTestWallClock(),
        datasetScanner: scanner
    )
    .requestScoped(by: workMeter)
    .transactionAttemptScoped()
}

private func makeJoinLeftBindings(
    keys: [Int],
    workMeter: DatabaseWorkMeter
) throws -> SPARQLRetainedBindings {
    var builder = try SPARQLRetainedBindingBuilder.make(
        workMeter: workMeter,
        stage: .joinCandidate,
        expectedCount: keys.count
    )
    for (offset, key) in keys.enumerated() {
        try builder.append(
            VariableBinding([
                "?key": try makeJoinKey(key),
                "?left": .int64(Int64(offset)),
            ])
        )
    }
    return builder.finish()
}

private func makeJoinPattern() throws -> ExecutionTriple {
    ExecutionTriple(
        subject: .variable("?key"),
        predicate: .value(
            .rdfTerm(.iri(try RDFIRI("https://example.com/join")))
        ),
        object: .value(
            .rdfTerm(
                .literal(
                    RDFLiteral(
                        lexicalForm: "matched",
                        datatype: .xsdString
                    )
                )
            )
        )
    )
}

private func makeJoinKey(_ key: Int) throws -> FieldValue {
    .rdfTerm(
        .iri(try RDFIRI("https://example.com/key/\(key)"))
    )
}

private func makeJoinQuad(key: Int) throws -> RDFQuad {
    RDFQuad(
        subject: .iri(
            try RDFIRI("https://example.com/key/\(key)")
        ),
        predicate: RDFPredicateIRI(
            try RDFIRI("https://example.com/join")
        ),
        object: .literal(
            RDFLiteral(
                lexicalForm: "matched",
                datatype: .xsdString
            )
        )
    )
}

private actor JoinDatasetScannerState {
    private var calls = 0
    private let cancellationCalls: Set<Int>

    init(cancellationCalls: Set<Int>) {
        self.cancellationCalls = cancellationCalls
    }

    func beginScan() throws {
        calls += 1
        if cancellationCalls.contains(calls) {
            throw CancellationError()
        }
    }

    var callCount: Int { calls }
}

private struct JoinDatasetScanner: RDFDatasetScanner {
    private let quads: [RDFQuad]
    private let state: JoinDatasetScannerState

    init(
        quads: [RDFQuad],
        cancellationCalls: Set<Int> = []
    ) {
        self.quads = quads
        self.state = JoinDatasetScannerState(
            cancellationCalls: cancellationCalls
        )
    }

    var callCount: Int {
        get async { await state.callCount }
    }

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
        try await state.beginScan()
        let matched = quads.lazy.filter { quad in
            (subject == nil || subject == quad.subject.term)
                && (predicate == nil || predicate == quad.predicate.term)
                && (object == nil || object == quad.object)
        }
        let selected = limit.map {
            Array(matched.prefix($0))
        } ?? Array(matched)
        return try RDFDatasetScanResult(
            quads: selected,
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
        .empty(workMeter: workMeter)
    }

    func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        false
    }
}
