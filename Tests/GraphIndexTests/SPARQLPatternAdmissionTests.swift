import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import Testing
import TestSupport
@testable import GraphIndex

@Suite("SPARQL pattern admission")
struct SPARQLPatternAdmissionTests {
    private final class InvocationCounter: Sendable {
        private let count = Mutex(0)

        func record() {
            count.withLock { $0 += 1 }
        }

        var value: Int {
            count.withLock { $0 }
        }
    }

    private struct StaticCoveringScanner: RDFDatasetScanner {
        let row: RDFDatasetScanStorageRow
        let scanResultWorkMeter: DatabaseWorkMeter?
        let namedGraph: RDFGraphName?
        let namedGraphResultWorkMeter: DatabaseWorkMeter?

        init(
            row: RDFDatasetScanStorageRow,
            scanResultWorkMeter: DatabaseWorkMeter? = nil,
            namedGraph: RDFGraphName? = nil,
            namedGraphResultWorkMeter: DatabaseWorkMeter? = nil
        ) {
            self.row = row
            self.scanResultWorkMeter = scanResultWorkMeter
            self.namedGraph = namedGraph
            self.namedGraphResultWorkMeter = namedGraphResultWorkMeter
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
            let resultWorkMeter = scanResultWorkMeter ?? workMeter
            let reservation = try resultWorkMeter.reserveIntermediate(
                rows: 1,
                bytes: 64,
                at: .deduplication
            )
            return RDFDatasetScanResult(
                rows: [row],
                physicalScanCount: 1,
                intermediateReservation: reservation,
                workMeter: resultWorkMeter
            )
        }

        func namedGraphs(
            limit: Int?,
            readMode: RDFDatasetReadMode,
            transaction: any TransactionReadAccess,
            workMeter: DatabaseWorkMeter
        ) async throws -> RDFDatasetNamedGraphs {
            let resultWorkMeter = namedGraphResultWorkMeter ?? workMeter
            guard let namedGraph else {
                return .empty(workMeter: resultWorkMeter)
            }
            return try RDFDatasetNamedGraphs(
                graphs: [namedGraph],
                workMeter: resultWorkMeter
            )
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

    @Test("Decoded properties overlap admitted binding output and reject before decode")
    func decodedPropertiesPreserveTwoStageAdmission() async throws {
        let coveringValue = try PersistableFieldFrameCodec.encode(
            magic: [0x44, 0x42, 0x49, 0x58],
            version: CoveringValueBuilder.formatVersion,
            entity: "Fixture",
            fields: [
                PersistableField(
                    number: 1,
                    name: "label",
                    value: .string(String(repeating: "x", count: 256))
                )
            ]
        )
        let row = RDFDatasetScanStorageRow(
            quad: RDFQuad(
                subject: .iri(try RDFIRI("did:example:subject")),
                predicate: try RDFPredicateIRI("did:example:predicate"),
                object: try .iri(validating: "did:example:object")
            ),
            coveringValue: coveringValue,
            includedFieldNames: ["label"]
        )
        let workspace = try CoveringValueBuilder
            .decodedPropertiesWorkspaceFootprint(
                coveringValue,
                includedFieldNames: ["label"]
            )

        let shortMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateBytes: workspace.bytes - 1
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let counter = InvocationCounter()
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await row.withDecodedProperties(
                workMeter: shortMeter,
                stage: .bindingCandidate
            ) { _ in
                counter.record()
            }
        }
        #expect(counter.value == 0)
        #expect(shortMeter.retainedIntermediateBytes == 0)

        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let engine = InMemoryEngine()
        let executor = try SPARQLQueryExecutor(
            database: engine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: StaticCoveringScanner(row: row)
        ).requestScoped(by: meter)
        let transaction = try engine.createTransaction()

        do {
            let result = try await executor.executePattern(
                ExecutionTriple("?subject", "?predicate", "?object"),
                transaction: transaction,
                activeGraph: .defaultGraph
            )
            #expect(result.bindings.count == 1)
            result.bindings.withElement(at: 0) { binding in
                #expect(
                    binding["?label"]
                        == .string(String(repeating: "x", count: 256))
                )
            }
            let retainedOutputBytes = meter.retainedIntermediateBytes
            #expect(retainedOutputBytes > 0)
            #expect(
                meter.peakIntermediateBytes
                    >= retainedOutputBytes + workspace.bytes + 64
            )
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let predicate = try RDFPredicateIRI(
            "https://example.com/property-path-meter"
        )
        let propertyPaths: [ExecutionPropertyPath] = [
            .iri(predicate),
            .negatedPropertySet(
                try PropertyPathNegatedSet(
                    forward: [predicate],
                    inverse: []
                )
            ),
            .empty,
        ]
        for path in propertyPaths {
            let requestedMeter = DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
            let foreignMeter = DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: TestProcessMonotonicClock()
            )
            let executor = SPARQLQueryExecutor(
                database: InMemoryEngine(),
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(),
                datasetScanner: StaticCoveringScanner(
                    row: row,
                    scanResultWorkMeter: foreignMeter
                )
            )
            await #expect(
                throws: DatabaseIntermediateReservationError
                    .workMeterMismatch
            ) {
                _ = try await executor.execute(
                    pattern: .propertyPath(
                        subject: .variable("?subject"),
                        path: path,
                        object: .variable("?object")
                    ),
                    limit: nil,
                    offset: 0,
                    workMeter: requestedMeter
                )
            }
            #expect(requestedMeter.retainedIntermediateRows == 0)
            #expect(requestedMeter.retainedIntermediateBytes == 0)
            #expect(foreignMeter.retainedIntermediateRows == 0)
            #expect(foreignMeter.retainedIntermediateBytes == 0)
        }

        let graph = try RDFGraphName(
            iri: "https://example.com/graph-variable-meter"
        )
        let graphRequestedMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let graphForeignMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let graphExecutor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: StaticCoveringScanner(
                row: row,
                scanResultWorkMeter: graphRequestedMeter,
                namedGraph: graph,
                namedGraphResultWorkMeter: graphForeignMeter
            )
        )
        await #expect(
            throws: DatabaseIntermediateReservationError.workMeterMismatch
        ) {
            _ = try await graphExecutor.execute(
                pattern: .graph(
                    .variable("?graph"),
                    .basic([
                        ExecutionTriple(
                            subject: .variable("?subject"),
                            predicate: .variable("?predicate"),
                            object: .variable("?object")
                        ),
                    ])
                ),
                limit: nil,
                offset: 0,
                workMeter: graphRequestedMeter
            )
        }
        #expect(graphRequestedMeter.retainedIntermediateRows == 0)
        #expect(graphRequestedMeter.retainedIntermediateBytes == 0)
        #expect(graphForeignMeter.retainedIntermediateRows == 0)
        #expect(graphForeignMeter.retainedIntermediateBytes == 0)
    }
}
