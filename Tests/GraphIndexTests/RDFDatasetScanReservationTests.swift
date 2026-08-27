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
        var resultWorkMeter: DatabaseWorkMeter?

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
            return try RDFDatasetScanResult(
                quads: [quad],
                physicalScanCount: 1,
                workMeter: resultWorkMeter ?? workMeter
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

    @Test("RDFS rejects a scan retained by another work meter")
    func rdfsRejectsForeignScanMeter() async throws {
        let quad = RDFQuad(
            subject: .iri(try RDFIRI("https://example.com/Child")),
            predicate: try RDFPredicateIRI(
                "http://www.w3.org/2000/01/rdf-schema#subClassOf"
            ),
            object: .iri(try RDFIRI("https://example.com/Parent"))
        )
        let engine = InMemoryEngine()
        let requestedMeter = makeMeter()
        let foreignMeter = makeMeter()
        let executor = SPARQLQueryExecutor(
            database: engine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(
                now: Timestamp(secondsSinceUnixEpoch: 0)
            ),
            datasetScanner: ReservedResultScanner(
                quad: quad,
                resultWorkMeter: foreignMeter
            )
        )

        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            await #expect(
                throws: DatabaseIntermediateReservationError
                    .workMeterMismatch
            ) {
                _ = try await RDFSGraphEntailment.resolve(
                    executor: executor,
                    dataGraph: .defaultGraph,
                    transaction: transaction,
                    budget: SHACLValidationWorkBudget(
                        workMeter: requestedMeter
                    )
                )
            }
        }

        #expect(requestedMeter.retainedIntermediateRows == 0)
        #expect(requestedMeter.retainedIntermediateBytes == 0)
        #expect(foreignMeter.retainedIntermediateRows == 0)
        #expect(foreignMeter.retainedIntermediateBytes == 0)
    }

    @Test("Scan result owns reservations until its linear owner is released")
    func resultLifetimeOwnsReservation() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("lifetime")
        let quad = try makeQuad(graph: graph, suffix: "retained")
        try await insert([quad], into: store, database: engine)

        let meter = makeMeter()
        let metrics = try measure(
            quad,
            mergesNamedGraphs: false
        )
        try await withScan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource]
            ),
            graphTarget: .named(graph),
            database: engine,
            workMeter: meter
        ) { result in
            #expect(result.count == 1)
            result.withQuad(at: 0) { retained in
                #expect(retained == quad)
                #expect(
                    meter.retainedIntermediateRows == metrics.rowCount
                )
                #expect(
                    meter.retainedIntermediateBytes
                        == metrics.retainedByteCount
                )
            }
            #expect(meter.peakIntermediateRows == metrics.rowCount)
            #expect(meter.peakIntermediateBytes == metrics.retainedByteCount)
        }
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
            monotonicClock: TestProcessMonotonicClock(),
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

    @Test("RDFS closure admits its destination before the scan owner releases")
    func rdfsClosureOwnsAdmittedDestination() async throws {
        let quad = RDFQuad(
            subject: .iri(try RDFIRI("https://example.com/Child")),
            predicate: try RDFPredicateIRI(
                "http://www.w3.org/2000/01/rdf-schema#subClassOf"
            ),
            object: .iri(try RDFIRI("https://example.com/Parent"))
        )
        let engine = InMemoryEngine()
        let meter = makeMeter()
        let scanMetrics = try measure(quad, mergesNamedGraphs: false)
        let executor = SPARQLQueryExecutor(
            database: engine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(
                now: Timestamp(secondsSinceUnixEpoch: 0)
            ),
            datasetScanner: ReservedResultScanner(quad: quad)
        )

        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            do {
                let entailment = try await RDFSGraphEntailment.resolve(
                    executor: executor,
                    dataGraph: .defaultGraph,
                    transaction: transaction,
                    budget: SHACLValidationWorkBudget(workMeter: meter)
                )
                #expect(
                    entailment.subsumes(
                        superClass: "https://example.com/Parent",
                        subClass: "https://example.com/Child"
                    )
                )
                #expect(meter.retainedIntermediateBytes > 0)
                #expect(
                    meter.peakIntermediateBytes
                        >= meter.retainedIntermediateBytes
                            + scanMetrics.retainedByteCount
                )
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Extracted RDFS ontology keeps its claims and failed admission cleans up")
    func rdfsOntologyLifetimeAndFailureCleanup() throws {
        let quad = RDFQuad(
            subject: .iri(try RDFIRI("https://example.com/childProperty")),
            predicate: try RDFPredicateIRI(
                "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
            ),
            object: .iri(try RDFIRI("https://example.com/parentProperty"))
        )
        let measuringMeter = makeMeter()
        var ontology: OntologyContext?
        do {
            let entailment = try RDFSGraphEntailment(
                quads: [quad],
                budget: SHACLValidationWorkBudget(
                    workMeter: measuringMeter
                )
            )
            ontology = entailment.ontologyContext
            #expect(
                ontology?.subProperties(
                    of: "https://example.com/parentProperty"
                ).contains("https://example.com/childProperty") == true
            )
            #expect(
                ontology?.knownEntailedPropertyScans(
                    of: "https://example.com/parentProperty"
                )?.map(\.predicateIRI)
                    == [
                        "https://example.com/childProperty",
                        "https://example.com/parentProperty",
                    ]
            )
        }
        let admittedPeak = measuringMeter.peakIntermediateBytes
        #expect(measuringMeter.retainedIntermediateBytes > 0)
        ontology = nil
        #expect(measuringMeter.retainedIntermediateRows == 0)
        #expect(measuringMeter.retainedIntermediateBytes == 0)

        let rejectingMeter = makeMeter(
            maximumIntermediateBytes: admittedPeak - 1
        )
        do {
            _ = try RDFSGraphEntailment(
                quads: [quad],
                budget: SHACLValidationWorkBudget(
                    workMeter: rejectingMeter
                )
            )
            Issue.record("Expected RDFS destination admission to fail")
        } catch is DatabaseWorkLimitError {
            #expect(rejectingMeter.retainedIntermediateRows == 0)
            #expect(rejectingMeter.retainedIntermediateBytes == 0)
        }
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
            try await withScan(
                scanner: IndexedRDFDatasetScanner(
                    sources: [store.datasetSource]
                ),
                graphTarget: .named(graph),
                database: engine,
                workMeter: meter
            ) { _ in
                Issue.record("Expected aggregate intermediate row rejection")
            }
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
            try await withScan(
                scanner: IndexedRDFDatasetScanner(
                    sources: [store.datasetSource]
                ),
                graphTarget: .named(graph),
                database: engine,
                workMeter: meter
            ) { _ in
                Issue.record("Expected aggregate intermediate byte rejection")
            }
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
        let metrics = try measure(
            quad,
            mergesNamedGraphs: false
        )
        let admittedRows = try metrics.admittedRowCount
        let admittedBytes = try metrics.admittedByteCount
        try await withScan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource, store.datasetSource]
            ),
            graphTarget: .named(graph),
            database: engine,
            workMeter: meter
        ) { result in
            #expect(result.count == 1)
            result.withQuad(at: 0) { retained in
                #expect(retained == quad)
            }
            #expect(result.physicalScanCount == 2)
            #expect(
                meter.peakIntermediateRows
                    == metrics.rowCount + admittedRows
            )
            #expect(
                meter.peakIntermediateBytes
                    == metrics.retainedByteCount
                        + admittedBytes
            )
            #expect(meter.retainedIntermediateRows == metrics.rowCount)
        }
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
        let metrics = try measure(
            first,
            mergesNamedGraphs: true
        )
        let admittedRows = try metrics.admittedRowCount
        let admittedBytes = try metrics.admittedByteCount
        try await withScan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource]
            ),
            graphTarget: .namedGraphUnion(
                RDFNamedGraphSet([firstGraph, secondGraph])
            ),
            database: engine,
            workMeter: meter
        ) { result in
            #expect(result.count == 1)
            result.withQuad(at: 0) { retained in
                #expect(retained == first.triple.quad)
            }
            #expect(result.physicalScanCount == 2)
            #expect(
                meter.peakIntermediateRows
                    == metrics.rowCount + admittedRows
            )
            #expect(
                meter.peakIntermediateBytes
                    == metrics.retainedByteCount
                        + admittedBytes
            )
            #expect(meter.retainedIntermediateRows == metrics.rowCount)
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Named graph union standardizes blank nodes apart by source graph")
    func namedGraphUnionSeparatesBlankNodeIdentity() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let firstGraph = try makeGraph("blank-union-a")
        let secondGraph = try makeGraph("blank-union-b")
        let shared = try RDFBlankNodeIdentifier("shared")
        let predicate = try RDFPredicateIRI(
            "https://example.com/predicate/blank-union"
        )
        let nestedPredicate = try RDFPredicateIRI(
            "https://example.com/predicate/nested"
        )
        let object = RDFTerm.tripleTerm(
            subject: .blankNode(shared),
            predicate: nestedPredicate,
            object: .blankNode(shared)
        )
        let first = RDFQuad(
            subject: .blankNode(shared),
            predicate: predicate,
            object: object,
            graph: firstGraph
        )
        let second = RDFQuad(
            subject: .blankNode(shared),
            predicate: predicate,
            object: object,
            graph: secondGraph
        )
        try await insert([first, second], into: store, database: engine)

        try await withScan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource]
            ),
            graphTarget: .namedGraphUnion(
                RDFNamedGraphSet([secondGraph, firstGraph, firstGraph])
            ),
            database: engine,
            workMeter: makeMeter()
        ) { result in
            #expect(result.count == 2)
            var sourceIdentifiers = Set<RDFBlankNodeIdentifier>()
            for index in 0..<result.count {
                result.withQuad(at: index) { quad in
                    #expect(quad.graph == nil)
                    guard case .blankNode(let subjectIdentifier) = quad.subject,
                          case .tripleTerm(
                            let nestedSubject,
                            _,
                            let nestedObject
                          ) = quad.object,
                          case .blankNode(let nestedSubjectIdentifier) = nestedSubject,
                          case .blankNode(let nestedObjectIdentifier) = nestedObject else {
                        Issue.record("Expected relabelled nested blank nodes")
                        return
                    }
                    #expect(subjectIdentifier == nestedSubjectIdentifier)
                    #expect(subjectIdentifier == nestedObjectIdentifier)
                    sourceIdentifiers.insert(subjectIdentifier)
                }
            }
            #expect(sourceIdentifiers.count == 2)
        }
    }

    @Test("Named graph union admits the decoded source and merged output peak")
    func namedGraphUnionAdmitsTransientSourceFootprint() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph(String(repeating: "g", count: 2_048))
        let blank = try RDFBlankNodeIdentifier(
            String(repeating: "b", count: 2_048)
        )
        let quad = RDFQuad(
            subject: .blankNode(blank),
            predicate: try RDFPredicateIRI(
                "https://example.com/predicate/transient-union"
            ),
            object: .blankNode(blank),
            graph: graph
        )
        try await insert([quad], into: store, database: engine)
        let metrics = try measure(quad, mergesNamedGraphs: true)
        let admittedByteCount = try metrics.admittedByteCount
        #expect(metrics.transientRowCount == 1)
        #expect(metrics.transientByteCount > 6_000)

        let rejectingMeter = makeMeter(
            maximumIntermediateBytes: admittedByteCount - 1
        )
        do {
            try await withScan(
                scanner: IndexedRDFDatasetScanner(
                    sources: [store.datasetSource]
                ),
                graphTarget: .namedGraphUnion(RDFNamedGraphSet([graph])),
                database: engine,
                workMeter: rejectingMeter
            ) { _ in
                Issue.record("Expected transient union admission rejection")
            }
        } catch let error as DatabaseWorkLimitError {
            #expect(error == .maximumIntermediateBytes(
                stage: .deduplication,
                consumed: 0,
                requested: admittedByteCount,
                maximum: admittedByteCount - 1
            ))
        }
        #expect(rejectingMeter.peakIntermediateBytes == 0)

        let admittedMeter = makeMeter(
            maximumIntermediateBytes: admittedByteCount
        )
        try await withScan(
            scanner: IndexedRDFDatasetScanner(
                sources: [store.datasetSource]
            ),
            graphTarget: .namedGraphUnion(RDFNamedGraphSet([graph])),
            database: engine,
            workMeter: admittedMeter
        ) { result in
            #expect(result.count == 1)
            #expect(
                admittedMeter.peakIntermediateBytes
                    == admittedByteCount
            )
            #expect(
                admittedMeter.retainedIntermediateBytes
                    == metrics.retainedByteCount
            )
        }
        #expect(admittedMeter.retainedIntermediateBytes == 0)
    }

    @Test("Named graph set is sorted and deduplicated")
    func namedGraphSetIsCanonical() throws {
        let firstGraph = try makeGraph("canonical-a")
        let secondGraph = try makeGraph("canonical-b")
        let graphs = RDFNamedGraphSet([
            secondGraph,
            firstGraph,
            secondGraph,
        ])

        #expect(Array(graphs) == [firstGraph, secondGraph])
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

    @Test("Physical preflight matches materialized nested RDF footprint")
    func physicalPreflightMatchesMaterializedFootprint() throws {
        let blank = try RDFBlankNodeIdentifier("preflight")
        let predicate = try RDFPredicateIRI("https://example.com/nested")
        let graph = try makeGraph("preflight")
        let quad = RDFQuad(
            subject: .blankNode(blank),
            predicate: predicate,
            object: .tripleTerm(
                subject: .blankNode(blank),
                predicate: predicate,
                object: .literal(RDFLiteral(
                    lexicalForm: "before\0after",
                    datatype: .xsdString
                ))
            ),
            graph: graph
        )
        let codec = RDFQuadIndexPhysicalCodec(baseSubspace: makeRoot())
        let entry = try RDFQuadIndexWritePlan(quad: quad).primaryEntry
        let preflight = try codec.preflightQuad(
            key: codec.encode(entry),
            ordering: .spo
        )
        let decoded = try codec.decodeQuad(preflight)

        #expect(decoded == quad)
        #expect(
            try RDFDatasetScanRetainedMetrics.preflight(
                preflight,
                mergesNamedGraphs: false
            ) == RDFDatasetScanRetainedMetrics.measure(
                decoded,
                mergesNamedGraphs: false
            )
        )
    }

    @Test("First-row admission rejection retains no decoded scan row")
    func firstRowAdmissionRejectsBeforeRetention() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("first-admission")
        let quad = try makeQuad(graph: graph, suffix: "first-admission")
        try await insert([quad], into: store, database: engine)
        let metrics = try measure(quad, mergesNamedGraphs: false)
        let meter = makeMeter(
            maximumIntermediateBytes: metrics.retainedByteCount - 1
        )

        do {
            try await withScan(
                scanner: IndexedRDFDatasetScanner(
                    sources: [store.datasetSource]
                ),
                graphTarget: .named(graph),
                database: engine,
                workMeter: meter
            ) { _ in
                Issue.record("Expected first-row admission rejection")
            }
        } catch let error as DatabaseWorkLimitError {
            #expect(error == .maximumIntermediateBytes(
                stage: .deduplication,
                consumed: 0,
                requested: metrics.retainedByteCount,
                maximum: metrics.retainedByteCount - 1
            ))
        }
        #expect(meter.peakIntermediateRows == 0)
        #expect(meter.peakIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Named graph discovery owns and releases its reservation")
    func namedGraphDiscoveryOwnsReservation() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("named-owner")
        try await insert(
            [try makeQuad(graph: graph, suffix: "named-owner")],
            into: store,
            database: engine
        )
        let meter = makeMeter()
        let metrics = try RDFDatasetNamedGraphRetainedMetrics.measure(graph)
        let retainedBytes = try metrics.retainedBytes(includesOwner: true)

        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            do {
                let graphs = try await store.namedGraphs(
                    limit: nil,
                    readMode: .snapshot,
                    transaction: transaction,
                    workMeter: meter
                )
                #expect(graphs.count == 1)
                graphs.withGraph(at: 0) { retained in
                    #expect(retained == graph)
                }
                #expect(
                    meter.retainedIntermediateRows == metrics.retainedRows
                )
                #expect(
                    meter.retainedIntermediateBytes
                        == retainedBytes
                )
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Duplicate named graph admission is released after deduplication")
    func duplicateNamedGraphAdmissionIsReleased() async throws {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(rootSubspace: makeRoot())
        let graph = try makeGraph("named-duplicate")
        try await insert(
            [try makeQuad(graph: graph, suffix: "named-duplicate")],
            into: store,
            database: engine
        )
        let meter = makeMeter()
        let metrics = try RDFDatasetNamedGraphRetainedMetrics.measure(graph)
        let admissionRows = try metrics.admissionRows()
        let peakBytes = try metrics.admissionBytes(includesOwner: true)
            + metrics.admissionBytes(includesOwner: false)
        let retainedBytes = try metrics.retainedBytes(includesOwner: true)
        let scanner = IndexedRDFDatasetScanner(
            sources: [store.datasetSource, store.datasetSource]
        )

        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            do {
                let graphs = try await scanner.namedGraphs(
                    limit: nil,
                    readMode: .snapshot,
                    transaction: transaction,
                    workMeter: meter
                )
                #expect(graphs.count == 1)
                #expect(
                    meter.peakIntermediateRows
                        == admissionRows * 2
                )
                #expect(
                    meter.peakIntermediateBytes
                        == peakBytes
                )
                #expect(
                    meter.retainedIntermediateRows == metrics.retainedRows
                )
                #expect(
                    meter.retainedIntermediateBytes
                        == retainedBytes
                )
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Named graph limit releases construction and truncated payload claims")
    func namedGraphLimitRetainsOnlyVisiblePayloadAndArrayCapacity() async throws {
        let engine = InMemoryEngine()
        let first = try makeGraph("limit-a")
        let second = try makeGraph("limit-b")
        let firstMetrics = try RDFDatasetNamedGraphRetainedMetrics
            .measure(first)
        let secondMetrics = try RDFDatasetNamedGraphRetainedMetrics
            .measure(second)
        let scanner = IndexedRDFDatasetScanner(sources: [
            RDFDatasetSource(
                entityName: "First",
                indexName: "first",
                indexSubspace: makeRoot().subspace("first"),
                coverage: .namedGraph(second)
            ),
            RDFDatasetSource(
                entityName: "Second",
                indexName: "second",
                indexSubspace: makeRoot().subspace("second"),
                coverage: .namedGraph(first)
            ),
        ])
        let meter = makeMeter()
        let expectedRetainedBytes = try firstMetrics.retainedBytes(
            includesOwner: true
        ) + secondMetrics.retainedArrayBytes

        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            do {
                let graphs = try await scanner.namedGraphs(
                    limit: 1,
                    readMode: .snapshot,
                    transaction: transaction,
                    workMeter: meter
                )
                #expect(graphs.count == 1)
                graphs.withGraph(at: 0) { graph in
                    #expect(graph == first)
                }
                #expect(meter.retainedIntermediateRows == 1)
                #expect(
                    meter.retainedIntermediateBytes == expectedRetainedBytes
                )
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
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

    private func withScan(
        scanner: IndexedRDFDatasetScanner,
        graphTarget: RDFGraphScanTarget,
        database: InMemoryEngine,
        workMeter: DatabaseWorkMeter,
        _ body: @escaping @Sendable (
            borrowing RDFDatasetScanResult
        ) throws -> Void
    ) async throws {
        try await StorageTransactionExecutor(engine: database).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) {
            transaction in
            let result = try await scanner.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: graphTarget,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: workMeter
            )
            try body(result)
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
