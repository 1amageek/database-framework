import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import StorageKit
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("RDF dataset scan reservations", .heartbeat)
struct RDFDatasetScanReservationTests {
    private struct ReservedResultScanner: RDFDatasetScanner {
        let quad: RDFQuad

        func scan(
            subject: DatabaseRDFTerm?,
            predicate: DatabaseRDFTerm?,
            object: DatabaseRDFTerm?,
            graphScope: RDFGraphScanScope,
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any Transaction,
            workMeter: DatabaseWorkMeter
        ) async throws -> RDFDatasetScanResult {
            let reservation = try workMeter.reserveIntermediate(
                rows: 2,
                bytes: 256,
                at: .deduplication
            )
            return RDFDatasetScanResult(
                quads: [quad],
                physicalScanCount: 1,
                intermediateReservation: reservation
            )
        }

        func namedGraphs(
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any Transaction,
            workMeter: DatabaseWorkMeter
        ) async throws -> [RDFGraphName] {
            []
        }

        func containsNamedGraph(
            _ graph: RDFGraphName,
            readMode: RDFDatasetReadMode,
            transaction: any Transaction,
            workMeter: DatabaseWorkMeter
        ) async throws -> Bool {
            false
        }
    }

    @Test("Scan result owns reservations until its last copy is released")
    func resultLifetimeOwnsReservation() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("lifetime")
        let quad = makeQuad(graph: graph, suffix: "retained")
        try await insert([quad], into: store, database: engine)

        let meter = makeMeter()
        var result: RDFDatasetScanResult? = try await scan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource]
            ),
            graphScope: .named(graph),
            database: engine,
            workMeter: meter
        )
        let metrics = try measure(
            quad,
            mergesNamedGraphs: false
        )
        let scratchRows = RDFDatasetScanRetainedMetrics.initialWorklistCapacity
        let scratchBytes = try scratchByteCount(capacity: scratchRows)

        #expect(result?.count == 1)
        var retainedRow = result?.first
        #expect(retainedRow?.quad == quad)
        #expect(meter.retainedIntermediateRows == metrics.rowCount)
        #expect(meter.retainedIntermediateBytes == metrics.retainedByteCount)
        #expect(meter.peakIntermediateRows == scratchRows + metrics.rowCount)
        #expect(
            meter.peakIntermediateBytes
                == scratchBytes + metrics.retainedByteCount
        )

        var resultCopy = result
        var iterator = result?.makeIterator()
        result = nil
        #expect(resultCopy?.first?.quad == quad)
        #expect(meter.retainedIntermediateRows == metrics.rowCount)
        #expect(meter.retainedIntermediateBytes == metrics.retainedByteCount)

        resultCopy = nil
        var iteratorRow = iterator?.next()
        #expect(iteratorRow?.quad == quad)
        #expect(meter.retainedIntermediateRows == metrics.rowCount)
        iterator = nil
        retainedRow = nil
        #expect(meter.retainedIntermediateRows == metrics.rowCount)
        iteratorRow = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("SPARQL consumption releases the scan owner after projection")
    func sparqlConsumptionRetainsResultOwner() async throws {
        let quad = RDFQuad(
            subject: .iri("https://example.com/subject"),
            predicate: .iri("https://example.com/predicate"),
            object: .iri("https://example.com/object")
        )
        let meter = makeMeter()
        let executor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            datasetScanner: ReservedResultScanner(quad: quad)
        )
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?subject"),
                predicate: .variable("?predicate"),
                object: .variable("?object")
            ),
        ])

        let result = try await executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: meter
        )

        #expect(result.0.count == 1)
        #expect(result.0[0]["?subject"] == .rdfTerm(quad.subject))
        #expect(meter.peakIntermediateRows >= 2)
        #expect(meter.peakIntermediateBytes >= 256)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Aggregate row admission rejects the second unique quad")
    func aggregateRowLimitRejectsScan() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("row-budget")
        let first = makeQuad(graph: graph, suffix: "first")
        let second = makeQuad(graph: graph, suffix: "other")
        try await insert([first, second], into: store, database: engine)

        let metrics = try measure(first, mergesNamedGraphs: false)
        let scratchRows = RDFDatasetScanRetainedMetrics.initialWorklistCapacity
        let maximumRows = scratchRows + metrics.rowCount + 1
        let meter = makeMeter(
            maximumIntermediateRows: UInt32(maximumRows)
        )
        do {
            _ = try await scan(
                scanner: IndexedRDFDatasetScanner(
                    sources: [store.datasetSource]
                ),
                graphScope: .named(graph),
                database: engine,
                workMeter: meter
            )
            Issue.record("Expected aggregate intermediate row rejection")
        } catch let error as DatabaseWorkLimitError {
            #expect(error == .maximumIntermediateRows(
                stage: .deduplication,
                consumed: scratchRows + metrics.rowCount,
                requested: metrics.rowCount,
                maximum: maximumRows
            ))
        }

        #expect(meter.peakIntermediateRows == scratchRows + metrics.rowCount)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Aggregate byte admission rejects the second unique quad")
    func aggregateByteLimitRejectsScan() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("byte-budget")
        let first = makeQuad(graph: graph, suffix: "first")
        let second = makeQuad(graph: graph, suffix: "other")
        try await insert([first, second], into: store, database: engine)

        let metrics = try measure(
            first,
            mergesNamedGraphs: false
        )
        let secondMetrics = try measure(
            second,
            mergesNamedGraphs: false
        )
        #expect(metrics == secondMetrics)
        let scratchBytes = try scratchByteCount(
            capacity: RDFDatasetScanRetainedMetrics.initialWorklistCapacity
        )
        let maximumBytes = scratchBytes + metrics.retainedByteCount * 2 - 1
        let meter = makeMeter(maximumIntermediateBytes: maximumBytes)

        do {
            _ = try await scan(
                scanner: IndexedRDFDatasetScanner(
                    sources: [store.datasetSource]
                ),
                graphScope: .named(graph),
                database: engine,
                workMeter: meter
            )
            Issue.record("Expected aggregate intermediate byte rejection")
        } catch let error as DatabaseWorkLimitError {
            #expect(error == .maximumIntermediateBytes(
                stage: .deduplication,
                consumed: scratchBytes + metrics.retainedByteCount,
                requested: metrics.retainedByteCount,
                maximum: maximumBytes
            ))
        }

        #expect(
            meter.peakIntermediateBytes
                == scratchBytes + metrics.retainedByteCount
        )
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Duplicate physical rows do not reserve twice")
    func duplicatesDoNotGrowReservation() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("duplicate")
        let quad = makeQuad(graph: graph, suffix: "shared")
        try await insert([quad], into: store, database: engine)

        let meter = makeMeter()
        var result: RDFDatasetScanResult? = try await scan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource, store.datasetSource]
            ),
            graphScope: .named(graph),
            database: engine,
            workMeter: meter
        )
        let metrics = try measure(
            quad,
            mergesNamedGraphs: false
        )
        let scratchRows = RDFDatasetScanRetainedMetrics.initialWorklistCapacity
        let scratchBytes = try scratchByteCount(capacity: scratchRows)

        #expect(result?.count == 1)
        #expect(result?.first?.quad == quad)
        #expect(result?.physicalScanCount == 2)
        #expect(meter.peakIntermediateRows == scratchRows + metrics.rowCount)
        #expect(
            meter.peakIntermediateBytes
                == scratchBytes + metrics.retainedByteCount
        )

        result = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Named graph union reserves one triple Set and one quad Array row")
    func namedGraphUnionAccountsForBothOwners() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let firstGraph = try makeGraph("union-a")
        let secondGraph = try makeGraph("union-b")
        let first = makeQuad(graph: firstGraph, suffix: "merged")
        let second = RDFQuad(
            subject: first.subject,
            predicate: first.predicate,
            object: first.object,
            graph: secondGraph.term
        )
        try await insert([first, second], into: store, database: engine)

        let meter = makeMeter()
        var result: RDFDatasetScanResult? = try await scan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource]
            ),
            graphScope: .namedGraphUnion([firstGraph, secondGraph]),
            database: engine,
            workMeter: meter
        )
        let metrics = try measure(
            first,
            mergesNamedGraphs: true
        )
        let scratchRows = RDFDatasetScanRetainedMetrics.initialWorklistCapacity
        let scratchBytes = try scratchByteCount(capacity: scratchRows)

        #expect(result?.count == 1)
        #expect(result?.first?.quad == first.triple.quad)
        #expect(result?.physicalScanCount == 2)
        #expect(meter.peakIntermediateRows == scratchRows + metrics.rowCount)
        #expect(
            meter.peakIntermediateBytes
                == scratchBytes + metrics.retainedByteCount
        )

        result = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Nested RDF-star terms are measured with an iterative worklist")
    func deeplyNestedTripleTermsAreMeasuredIteratively() throws {
        var nested = DatabaseRDFTerm.iri("https://example.com/root")
        for index in 0..<512 {
            nested = .tripleTerm(
                subject: nested,
                predicate: .iri("https://example.com/predicate/\(index)"),
                object: .blankNode("node-\(index)")
            )
        }
        let quad = RDFQuad(
            subject: nested,
            predicate: .iri("https://example.com/predicate"),
            object: .iri("https://example.com/object")
        )

        let metrics = try measure(
            quad,
            mergesNamedGraphs: false
        )
        #expect(metrics.rowCount == 2)
        #expect(metrics.retainedByteCount > 50_000)
    }

    @Test("RDF-star traversal admits scratch growth before allocation")
    func deeplyNestedTripleTermScratchRejectsBeforeGrowth() throws {
        var nested = DatabaseRDFTerm.iri("https://example.com/root")
        for index in 0..<512 {
            nested = .tripleTerm(
                subject: nested,
                predicate: .iri("https://example.com/predicate/\(index)"),
                object: .blankNode("node-\(index)")
            )
        }
        let quad = RDFQuad(
            subject: nested,
            predicate: .iri("https://example.com/predicate"),
            object: .iri("https://example.com/object")
        )
        let admittedCapacity: UInt64 = 8
        let admittedBytes = try scratchByteCount(
            capacity: admittedCapacity
        )
        let rejectedBytes = try RDFDatasetScanRetainedMetrics.checkedMultiply(
            admittedCapacity,
            RDFDatasetScanRetainedMetrics.worklistSlotByteCount
        )
        let meter = makeMeter(maximumIntermediateBytes: admittedBytes)

        #expect(throws: DatabaseWorkLimitError.maximumIntermediateBytes(
            stage: .deduplication,
            consumed: admittedBytes,
            requested: rejectedBytes,
            maximum: admittedBytes
        )) {
            _ = try measure(
                quad,
                mergesNamedGraphs: false,
                workMeter: meter
            )
        }
        #expect(meter.peakIntermediateRows == 8)
        #expect(meter.peakIntermediateBytes == admittedBytes)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Retained byte arithmetic rejects addition and multiplication overflow")
    func retainedByteArithmeticRejectsOverflow() {
        #expect(throws: RDFDatasetScannerError.retainedByteCountOverflow(
            operation: .addition,
            left: UInt64.max,
            right: 1
        )) {
            try RDFDatasetScanRetainedMetrics.checkedAdd(UInt64.max, 1)
        }
        #expect(throws: RDFDatasetScannerError.retainedByteCountOverflow(
            operation: .multiplication,
            left: UInt64.max,
            right: 2
        )) {
            try RDFDatasetScanRetainedMetrics.checkedMultiply(UInt64.max, 2)
        }
    }

    private func measure(
        _ quad: RDFQuad,
        mergesNamedGraphs: Bool,
        workMeter: DatabaseWorkMeter? = nil
    ) throws -> RDFDatasetScanRetainedMetrics {
        let meter = workMeter ?? makeMeter()
        var worklist: [DatabaseRDFTerm] = []
        var modeledCapacity: UInt64 = 0
        var scratchReservation: DatabaseIntermediateReservation?
        defer {
            worklist.removeAll(keepingCapacity: false)
            scratchReservation?.release()
        }
        return try RDFDatasetScanRetainedMetrics.measure(
            quad,
            mergesNamedGraphs: mergesNamedGraphs,
            worklist: &worklist,
            modeledWorklistCapacity: &modeledCapacity,
            scratchReservation: &scratchReservation,
            workMeter: meter
        )
    }

    private func scratchByteCount(capacity: UInt64) throws -> UInt64 {
        try RDFDatasetScanRetainedMetrics.checkedAdd(
            RDFDatasetScanRetainedMetrics.worklistContainerByteCount,
            RDFDatasetScanRetainedMetrics.checkedMultiply(
                capacity,
                RDFDatasetScanRetainedMetrics.worklistSlotByteCount
            )
        )
    }

    private func scan(
        scanner: IndexedRDFDatasetScanner,
        graphScope: RDFGraphScanScope,
        database: InMemoryEngine,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        try await database.withTransaction(configuration: .default) {
            transaction in
            try await scanner.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphScope: graphScope,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    private func insert(
        _ quads: [RDFQuad],
        into store: CanonicalRDFGraphStore,
        database: InMemoryEngine
    ) async throws {
        let workMeter = makeMeter()
        try await database.withTransaction(configuration: .batch) {
            transaction in
            for quad in quads {
                _ = try await store.insert(
                    quad,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
        }
    }

    private func makeMeter(
        maximumIntermediateRows: UInt32 = 10_000,
        maximumIntermediateBytes: UInt64 = 16 * 1_024 * 1_024
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            )
        )
    }

    private func makeRoot() -> Subspace {
        Subspace(prefix: Tuple("rdf-scan-reservation-tests").pack())
    }

    private func makeGraph(_ suffix: String) throws -> RDFGraphName {
        try RDFGraphName(iri: "https://example.com/graph/\(suffix)")
    }

    private func makeQuad(
        graph: RDFGraphName,
        suffix: String
    ) -> RDFQuad {
        RDFQuad(
            subject: .iri("https://example.com/subject/\(suffix)"),
            predicate: .iri("https://example.com/predicate"),
            object: .iri("https://example.com/object/\(suffix)"),
            graph: graph.term
        )
    }
}
