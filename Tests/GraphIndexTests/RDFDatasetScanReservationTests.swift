import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import TestHeartbeat
import TestSupport
import Testing
@testable import GraphIndex

@Suite("RDF dataset scan reservations", .heartbeat)
struct RDFDatasetScanReservationTests {
    private struct ReservedResultScanner: RDFDatasetScanner {
        let quad: RDFQuad

        func scan(
            subject: RDFTerm?,
            predicate: RDFTerm?,
            object: RDFTerm?,
            graphScope: RDFGraphScanScope,
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionAccess,
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
            transaction: any TransactionAccess,
            workMeter: DatabaseWorkMeter
        ) async throws -> [RDFGraphName] {
            []
        }

        func containsNamedGraph(
            _ graph: RDFGraphName,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionAccess,
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
        let quad = try makeQuad(graph: graph, suffix: "retained")
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
        #expect(result?.count == 1)
        var retainedRow = result?.first
        #expect(retainedRow?.quad == quad)
        #expect(meter.retainedIntermediateRows == metrics.rowCount)
        #expect(meter.retainedIntermediateBytes == metrics.retainedByteCount)
        #expect(meter.peakIntermediateRows == metrics.rowCount)
        #expect(meter.peakIntermediateBytes == metrics.retainedByteCount)

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
            subject: .iri(.xsdString),
            predicate: RDFPredicateIRI(.rdfLanguageString),
            object: .iri(.rdfDirectionalLanguageString)
        )
        let meter = makeMeter()
        let executor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            wallClock: FixedTestWallClock(
                now: Timestamp(secondsSinceUnixEpoch: 0)
            ),
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
        #expect(
            result.0[0]["?subject"]
                == .rdfTerm(quad.subject.term)
        )
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
        let first = try makeQuad(graph: graph, suffix: "first")
        let second = try makeQuad(graph: graph, suffix: "other")
        try await insert([first, second], into: store, database: engine)

        let metrics = try measure(first, mergesNamedGraphs: false)
        let maximumRows = metrics.rowCount + 1
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
                consumed: metrics.rowCount,
                requested: metrics.rowCount,
                maximum: maximumRows
            ))
        }

        #expect(meter.peakIntermediateRows == metrics.rowCount)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Aggregate byte admission rejects the second unique quad")
    func aggregateByteLimitRejectsScan() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("byte-budget")
        let first = try makeQuad(graph: graph, suffix: "first")
        let second = try makeQuad(graph: graph, suffix: "other")
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
        let maximumBytes = metrics.retainedByteCount * 2 - 1
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
                consumed: metrics.retainedByteCount,
                requested: metrics.retainedByteCount,
                maximum: maximumBytes
            ))
        }

        #expect(meter.peakIntermediateBytes == metrics.retainedByteCount)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Duplicate physical rows do not reserve twice")
    func duplicatesDoNotGrowReservation() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("duplicate")
        let quad = try makeQuad(graph: graph, suffix: "shared")
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
        #expect(result?.count == 1)
        #expect(result?.first?.quad == quad)
        #expect(result?.physicalScanCount == 2)
        #expect(meter.peakIntermediateRows == metrics.rowCount)
        #expect(meter.peakIntermediateBytes == metrics.retainedByteCount)

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
        let first = try makeQuad(
            graph: firstGraph,
            suffix: "merged"
        )
        let second = RDFQuad(
            subject: first.subject,
            predicate: first.predicate,
            object: first.object,
            graph: secondGraph
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
        #expect(result?.count == 1)
        #expect(result?.first?.quad == first.triple.quad)
        #expect(result?.physicalScanCount == 2)
        #expect(meter.peakIntermediateRows == metrics.rowCount)
        #expect(meter.peakIntermediateBytes == metrics.retainedByteCount)

        result = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Nested RDF-star terms are measured with a constant-space cursor")
    func deeplyNestedTripleTermsUseConstantTraversalStorage() throws {
        let tripleSubject = RDFSubject.iri(.xsdString)
        var nested = RDFTerm.iri(.rdfDirectionalLanguageString)
        for index in 0..<512 {
            nested = .tripleTerm(
                subject: tripleSubject,
                predicate: try RDFPredicateIRI(
                    "https://example.com/predicate/\(index)"
                ),
                object: nested
            )
        }
        let quad = RDFQuad(
            subject: tripleSubject,
            predicate: RDFPredicateIRI(.rdfLanguageString),
            object: nested
        )

        let metrics = try measure(
            quad,
            mergesNamedGraphs: false
        )
        #expect(metrics.rowCount == 2)
        #expect(metrics.retainedByteCount > 50_000)
    }

    @Test("RDF-star traversal does not reserve depth-proportional scratch")
    func deeplyNestedTripleTermDoesNotReserveScratch() throws {
        let tripleSubject = RDFSubject.iri(.xsdString)
        var nested = RDFTerm.iri(.rdfDirectionalLanguageString)
        for index in 0..<512 {
            nested = .tripleTerm(
                subject: tripleSubject,
                predicate: try RDFPredicateIRI(
                    "https://example.com/predicate/\(index)"
                ),
                object: nested
            )
        }
        let quad = RDFQuad(
            subject: tripleSubject,
            predicate: RDFPredicateIRI(.rdfLanguageString),
            object: nested
        )
        let meter = makeMeter(maximumIntermediateBytes: 1)

        let metrics = try measure(
            quad,
            mergesNamedGraphs: false
        )
        #expect(metrics.retainedByteCount > 50_000)
        #expect(meter.peakIntermediateRows == 0)
        #expect(meter.peakIntermediateBytes == 0)
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
        mergesNamedGraphs: Bool
    ) throws -> RDFDatasetScanRetainedMetrics {
        try RDFDatasetScanRetainedMetrics.measure(
            quad,
            mergesNamedGraphs: mergesNamedGraphs
        )
    }

    private func scan(
        scanner: IndexedRDFDatasetScanner,
        graphScope: RDFGraphScanScope,
        database: InMemoryEngine,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        try await StorageTransactionExecutor(engine: database).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) {
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
        try await StorageTransactionExecutor(engine: database).withTransaction(
            configuration: .batch,
            clock: TestProcessMonotonicClock()
        ) {
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
            budget: ExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
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
}
