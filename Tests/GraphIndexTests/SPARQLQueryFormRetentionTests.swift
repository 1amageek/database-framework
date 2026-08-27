import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Testing
import TestSupport
@_spi(DatabaseExecution) @testable import GraphIndex

@Suite("SPARQL query-form retained output")
struct SPARQLQueryFormRetentionTests {
    private struct StaticScanner: RDFDatasetScanner {
        let quad: RDFQuad

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
            guard subject == nil || subject == quad.subject.term else {
                return .empty(workMeter: workMeter)
            }
            return try RDFDatasetScanResult(
                quads: [quad],
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

    private struct ForeignMeterScanner: RDFDatasetScanner {
        let quad: RDFQuad
        let foreignMeter: DatabaseWorkMeter

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
            try RDFDatasetScanResult(
                quads: [quad],
                physicalScanCount: 1,
                workMeter: foreignMeter
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

    @Test("ASK keeps its solution relation inside the request meter")
    func askUsesRetainedSolution() async throws {
        let meter = makeMeter()
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let result = try await makeExecutor().executeAskInTransaction(
            AskQuery(
                pattern: .values(
                    variables: ["value"],
                    bindings: [[.int(1)]]
                )
            ),
            structuralLimits: .default,
            transaction: transaction,
            workMeter: meter
        )

        #expect(result)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let constrainedMeter = makeMeter(maximumIntermediateRows: 0)
        await #expect(
            throws: DatabaseWorkLimitError.maximumIntermediateRows(
                stage: .bindingCandidate,
                consumed: 0,
                requested: 1,
                maximum: 0
            )
        ) {
            try await makeExecutor().executeAskInTransaction(
                AskQuery(
                    pattern: .values(
                        variables: ["value"],
                        bindings: [[.int(1)]]
                    )
                ),
                structuralLimits: .default,
                transaction: transaction,
                workMeter: constrainedMeter
            )
        }
        #expect(constrainedMeter.retainedIntermediateRows == 0)
        #expect(constrainedMeter.retainedIntermediateBytes == 0)
    }

    @Test("CONSTRUCT admits quads before materializing retained output")
    func constructPreadmitsOutput() async throws {
        let meter = makeMeter()
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let query = ConstructQuery(
            template: [
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("https://example.com/predicate"),
                    object: .variable("object")
                ),
            ],
            pattern: .values(
                variables: ["subject", "object"],
                bindings: [[
                    .iri("https://example.com/subject"),
                    .string("object"),
                ]]
            )
        )
        let graph = try await makeExecutor().executeConstructInTransaction(
            query,
            nodeNamespace: try GraphResultNodeNamespace(
                ByteString(repeating: 0x21, count: 32)
            ),
            structuralLimits: .default,
            transaction: transaction,
            workMeter: meter
        )

        #expect(graph.count == 1)
        #expect(meter.retainedIntermediateRows == 1)
        let output = graph.promoteToOutput()
        #expect(output.count == 1)
        #expect(
            output[0].subject.term
                == .iri(try RDFIRI("https://example.com/subject"))
        )
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let foreignFootprintMeterWork = makeMeter()
        let foreignFootprintMeter = try DatabaseRDFQuadFootprintMeter.make(
            workMeter: foreignFootprintMeterWork,
            stage: .resultMaterialization
        )
        let foreignMaximum = try foreignFootprintMeter.footprint(of: output[0])
        let requestRowsBeforeMismatch = meter.retainedIntermediateRows
        let requestBytesBeforeMismatch = meter.retainedIntermediateBytes
        let foreignRowsBeforeMismatch = foreignFootprintMeterWork
            .retainedIntermediateRows
        let foreignBytesBeforeMismatch = foreignFootprintMeterWork
            .retainedIntermediateBytes
        var mismatchedProducerDidRun = false
        #expect(
            throws: DatabaseIntermediateReservationError.workMeterMismatch
        ) {
            _ = try DatabaseQueryScopedRDFQuad.producingOptional(
                maximumFootprint: foreignMaximum,
                footprintMeter: foreignFootprintMeter,
                workMeter: meter,
                stage: .resultMaterialization
            ) {
                mismatchedProducerDidRun = true
                return output[0]
            }
        }
        #expect(!mismatchedProducerDidRun)
        #expect(meter.retainedIntermediateRows == requestRowsBeforeMismatch)
        #expect(meter.retainedIntermediateBytes == requestBytesBeforeMismatch)
        #expect(
            foreignFootprintMeterWork.retainedIntermediateRows
                == foreignRowsBeforeMismatch
        )
        #expect(
            foreignFootprintMeterWork.retainedIntermediateBytes
                == foreignBytesBeforeMismatch
        )
        foreignFootprintMeter.shutdown()
        #expect(foreignFootprintMeterWork.retainedIntermediateRows == 0)
        #expect(foreignFootprintMeterWork.retainedIntermediateBytes == 0)

        let shrinkMeter = makeMeter()
        let shrinkFootprintMeter = try DatabaseRDFQuadFootprintMeter.make(
            workMeter: shrinkMeter,
            stage: .resultMaterialization
        )
        let exactFootprint = try shrinkFootprintMeter.footprint(of: output[0])
        let safeMaximum = try exactFootprint.adding(
            DatabaseIntermediateFootprint(bytes: 4_096)
        )
        let shrinkBaselineRows = shrinkMeter.retainedIntermediateRows
        let shrinkBaselineBytes = shrinkMeter.retainedIntermediateBytes
        do {
            let makeQuad: () throws -> RDFQuad? = { output[0] }
            guard let produced = try DatabaseQueryScopedRDFQuad
                .producingOptional(
                    maximumFootprint: safeMaximum,
                    footprintMeter: shrinkFootprintMeter,
                    workMeter: shrinkMeter,
                    stage: .resultMaterialization,
                    makeQuad
                ) else {
                Issue.record("Expected one produced RDF quad")
                return
            }
            #expect(
                shrinkMeter.retainedIntermediateRows
                    == shrinkBaselineRows + exactFootprint.rows
            )
            #expect(
                shrinkMeter.retainedIntermediateBytes
                    == shrinkBaselineBytes + exactFootprint.bytes
            )
            produced.withQuad { quad in
                #expect(quad == output[0])
            }
        }
        #expect(shrinkMeter.retainedIntermediateRows == shrinkBaselineRows)
        #expect(shrinkMeter.retainedIntermediateBytes == shrinkBaselineBytes)
        shrinkFootprintMeter.shutdown()
        #expect(shrinkMeter.retainedIntermediateRows == 0)
        #expect(shrinkMeter.retainedIntermediateBytes == 0)

        let complexTemplate = TriplePattern(
            subject: .blankNode("root"),
            predicate: .iri("https://example.com/links"),
            object: .reifiedTriple(
                subject: .iri("https://example.com/statement-subject"),
                predicate: .iri("https://example.com/statement-predicate"),
                object: .tripleTerm(
                    subject: .iri("https://example.com/inner-subject"),
                    predicate: .iri("https://example.com/inner-predicate"),
                    object: .literal(.string("nested"))
                ),
                reifier: .blankNode("statement")
            )
        )
        let complexMeter = makeMeter()
        let complexGraph = try await makeExecutor()
            .executeConstructInTransaction(
                ConstructQuery(
                    template: [complexTemplate],
                    pattern: .values(
                        variables: ["seed"],
                        bindings: [[.int(1)]]
                    )
                ),
                nodeNamespace: try GraphResultNodeNamespace(
                    ByteString(repeating: 0x22, count: 32)
                ),
                structuralLimits: .default,
                transaction: transaction,
                workMeter: complexMeter
            )
        #expect(complexGraph.count == 2)
        let complexOutput = complexGraph.promoteToOutput()
        #expect(
            Set(complexOutput.map { $0.predicate.iri.rawValue })
                == [
                    "https://example.com/links",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies",
                ]
        )
        #expect(complexMeter.retainedIntermediateRows == 0)
        #expect(complexMeter.retainedIntermediateBytes == 0)

        let plannedMaximum = try SPARQLConstructFootprintPlanner
            .maximumQuadFootprint(
                complexTemplate,
                binding: VariableBinding()
            )
        let maximum = try #require(
            plannedMaximum
        )
        let namespace = try GraphResultNodeNamespace(
            ByteString(repeating: 0x23, count: 32)
        )
        let fingerprint = ByteString(repeating: 0x24, count: 32)
        let calibrationMeter = makeMeter()
        let baselineBytes: UInt64
        do {
            let calibrationOutput = try DatabaseRetainedRDFGraphBuilder(
                workMeter: calibrationMeter
            )
            let calibrationResolver = SPARQLConstructBlankNodeResolver(
                nodeNamespace: namespace,
                bindingFingerprint: fingerprint,
                occurrence: 0,
                workMeter: calibrationMeter
            )
            baselineBytes = calibrationMeter.retainedIntermediateBytes
            _ = consume calibrationOutput
            _ = consume calibrationResolver
        }
        #expect(calibrationMeter.retainedIntermediateBytes == 0)

        let shortMeter = makeMeter(
            maximumIntermediateBytes: baselineBytes + maximum.bytes
        )
        do {
            var shortOutput = try DatabaseRetainedRDFGraphBuilder(
                workMeter: shortMeter
            )
            var shortResolver = SPARQLConstructBlankNodeResolver(
                nodeNamespace: namespace,
                bindingFingerprint: fingerprint,
                occurrence: 0,
                workMeter: shortMeter
            )
            let retainedBeforeProduction = shortMeter
                .retainedIntermediateBytes
            #expect(retainedBeforeProduction == baselineBytes)
            #expect(throws: DatabaseWorkLimitError.self) {
                try SPARQLConstructTemplateInstantiator.append(
                    complexTemplate,
                    binding: VariableBinding(),
                    blankNodeResolver: &shortResolver,
                    to: &shortOutput
                )
            }
            #expect(
                shortMeter.retainedIntermediateBytes
                    == retainedBeforeProduction
            )
            #expect(shortMeter.retainedIntermediateRows == 0)
            _ = shortOutput.finish()
        }
        #expect(shortMeter.retainedIntermediateRows == 0)
        #expect(shortMeter.retainedIntermediateBytes == 0)

        let cancelledMeter = makeMeter()
        let cancelledAttempt = Task { () -> Bool in
            do {
                let output = try DatabaseRetainedRDFGraphBuilder(
                    workMeter: cancelledMeter
                )
                var resolver = SPARQLConstructBlankNodeResolver(
                    nodeNamespace: namespace,
                    bindingFingerprint: fingerprint,
                    occurrence: 0,
                    workMeter: cancelledMeter
                )
                guard let maximum = try SPARQLConstructFootprintPlanner
                    .maximumQuadFootprint(
                        complexTemplate,
                        binding: VariableBinding()
                    ) else {
                    _ = output.finish()
                    _ = consume resolver
                    return false
                }
                let retainedBeforeProduction = cancelledMeter
                    .retainedIntermediateBytes
                var producerEntered = false
                var producerObservedMaximum = false
                do {
                    _ = try DatabaseQueryScopedRDFQuad.producingOptional(
                        maximumFootprint: maximum,
                        footprintMeter: output.producerFootprintMeter,
                        workMeter: cancelledMeter,
                        stage: .resultMaterialization
                    ) {
                        producerEntered = true
                        producerObservedMaximum = cancelledMeter
                            .retainedIntermediateBytes
                            == retainedBeforeProduction + maximum.bytes
                        _ = try resolver.identifier(for: "cancelled")
                        withUnsafeCurrentTask { $0?.cancel() }
                        try cancelledMeter.checkpoint(
                            at: .resultMaterialization
                        )
                        return nil
                    }
                    return false
                } catch is CancellationError {
                    let releasedProducerClaim = producerEntered
                        && producerObservedMaximum
                        && cancelledMeter.retainedIntermediateBytes
                            == retainedBeforeProduction
                    _ = output.finish()
                    _ = consume resolver
                    return releasedProducerClaim
                }
            } catch {
                return false
            }
        }
        #expect(await cancelledAttempt.value)
        #expect(cancelledMeter.retainedIntermediateRows == 0)
        #expect(cancelledMeter.retainedIntermediateBytes == 0)
    }

    @Test("DESCRIBE overlaps scan ownership with admitted graph output")
    func describePreadmitsBorrowedScanQuad() async throws {
        let quad = RDFQuad(
            subject: .iri(try RDFIRI("https://example.com/resource")),
            predicate: try RDFPredicateIRI("https://example.com/predicate"),
            object: .literal(
                RDFLiteral(
                    lexicalForm: "retained",
                    datatype: .xsdString
                )
            )
        )
        let meter = makeMeter()
        let engine = InMemoryEngine()
        let transaction = try engine.createTransaction()
        let graph = try await makeExecutor(
            scanner: StaticScanner(quad: quad)
        ).executeDescribeInTransaction(
            DescribeQuery(
                selection: .resources(
                    first: .iri("https://example.com/resource"),
                    additional: []
                )
            ),
            structuralLimits: .default,
            transaction: transaction,
            workMeter: meter
        )

        #expect(graph.count == 1)
        let retainedGraphBytes = meter.retainedIntermediateBytes
        #expect(retainedGraphBytes > 0)
        #expect(meter.peakIntermediateBytes > retainedGraphBytes)
        #expect(graph.promoteToOutput() == [quad])
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let foreignMeter = makeMeter()
        await #expect(
            throws: DatabaseIntermediateReservationError.workMeterMismatch
        ) {
            _ = try await makeExecutor(
                scanner: ForeignMeterScanner(
                    quad: quad,
                    foreignMeter: foreignMeter
                )
            ).executeDescribeInTransaction(
                DescribeQuery(
                    selection: .resources(
                        first: .iri("https://example.com/resource"),
                        additional: []
                    )
                ),
                structuralLimits: .default,
                transaction: transaction,
                workMeter: meter
            )
        }
        #expect(foreignMeter.retainedIntermediateRows == 0)
        #expect(foreignMeter.retainedIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeExecutor(
        scanner: any RDFDatasetScanner = IndexedRDFDatasetScanner(sources: [])
    ) -> SPARQLQueryExecutor {
        SPARQLQueryExecutor(
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: scanner
        )
    }

    private func makeMeter(
        maximumIntermediateRows: UInt32 = 64,
        maximumIntermediateBytes: UInt64 = 1_000_000
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 32,
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
