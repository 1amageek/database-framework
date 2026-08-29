#if MultiBase
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine
@_spi(DatabaseExecution) @testable import GraphIndex

private enum CompositionSPARQLAuthorizationProbeError: Error {
    case missingAuthorizationEvidence
}

private actor CompositionSPARQLRowProbe {
    private var rows: [CompositionQueryRow] = []

    func receive(_ event: CompositionQueryEvent) -> Bool {
        if case .row(let row) = event {
            rows.append(row)
        }
        return true
    }

    func values() -> [CompositionQueryRow] {
        rows
    }
}

private actor CompositionRDFQuadProbe {
    private var quads: [RDFQuad] = []

    func receive(_ event: CompositionRDFQueryEvent) -> Bool {
        if case .quad(let result) = event {
            quads.append(result.quad)
        }
        return true
    }

    func values() -> [RDFQuad] {
        quads
    }
}

private final class CompositionSPARQLWorkMeterControl: Sendable {
    private struct State: Sendable {
        var workMeter: DatabaseWorkMeter? = nil
        var row: QueryRow? = nil
        var quad: RDFQuad? = nil
    }

    private let state = Mutex(State())

    func output(
        defaultWorkMeter: DatabaseWorkMeter
    ) -> (workMeter: DatabaseWorkMeter, row: QueryRow?) {
        state.withLock {
            ($0.workMeter ?? defaultWorkMeter, $0.row)
        }
    }

    func set(_ workMeter: DatabaseWorkMeter?) {
        state.withLock { $0.workMeter = workMeter }
    }

    func setRow(_ row: QueryRow?) {
        state.withLock { $0.row = row }
    }

    func graphOutput(
        defaultWorkMeter: DatabaseWorkMeter
    ) -> (workMeter: DatabaseWorkMeter, quad: RDFQuad?) {
        state.withLock {
            ($0.workMeter ?? defaultWorkMeter, $0.quad)
        }
    }

    func setQuad(_ quad: RDFQuad?) {
        state.withLock { $0.quad = quad }
    }
}

private struct CompositionSPARQLAuthorizationProbe:
    SPARQLSourceExecutor
{
    let outputWorkMeter: CompositionSPARQLWorkMeterControl

    func executeInTransaction(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedQueryRows {
        guard session.transaction.authorization != nil else {
            throw CompositionSPARQLAuthorizationProbeError
                .missingAuthorizationEvidence
        }
        let output = outputWorkMeter.output(
            defaultWorkMeter: options.workMeter
        )
        var rows = try DatabaseRetainedQueryRowsBuilder(
            workMeter: output.workMeter,
            stage: .resultMaterialization
        )
        if let row = output.row {
            try rows.append(row)
        }
        return rows.finish()
    }

    func executeAskInTransaction(
        session: DatabaseReadSession,
        askQuery: AskQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> Bool {
        guard session.transaction.authorization != nil else {
            throw CompositionSPARQLAuthorizationProbeError
                .missingAuthorizationEvidence
        }
        return true
    }

    func executeConstructInTransaction(
        session: DatabaseReadSession,
        constructQuery: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        try retainedGraph(session: session, options: options)
    }

    func executeDescribeInTransaction(
        session: DatabaseReadSession,
        describeQuery: DescribeQuery,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> DatabaseRetainedRDFGraph {
        try retainedGraph(session: session, options: options)
    }

    private func retainedGraph(
        session: DatabaseReadSession,
        options: ReadExecutionContext
    ) throws -> DatabaseRetainedRDFGraph {
        guard session.transaction.authorization != nil else {
            throw CompositionSPARQLAuthorizationProbeError
                .missingAuthorizationEvidence
        }
        let output = outputWorkMeter.graphOutput(
            defaultWorkMeter: options.workMeter
        )
        var graph = try DatabaseRetainedRDFGraphBuilder(
            workMeter: output.workMeter
        )
        if let quad = output.quad {
            try graph.append(quad)
        }
        return graph.finish()
    }
}

@Suite("Composition SPARQL authorization")
struct CompositionSPARQLAuthorizationTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let baseID: Base.ID
        let readerAuthorization: AuthorizationContext
        let outputWorkMeter: CompositionSPARQLWorkMeterControl
    }

    @Test("Composition dispatch seals SPARQL authorization evidence")
    func compositionDispatchSealsSPARQLAuthorization() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let source = try fixture.container.session(
            authorization: fixture.readerAuthorization
        ).composition(bases: [fixture.baseID])
        let readContext = ReadExecutionContext(
            monotonicClock: fixture.container.monotonicClock
        )

        try await CompositionSPARQLQueryPlanner(
            structuralLimits: readContext.queryStructuralLimits
        ).execute(
            SelectQuery(
                projection: .all,
                source: .graphPattern(.basic([]))
            ),
            source: source,
            graphPartitions: FieldObject(),
            pageSize: 1,
            readContext: readContext
        ) { _ in true }

        let askResult = try await CompositionRDFQueryPlanner().executeAsk(
            AskQuery(pattern: .basic([])),
            source: source,
            graphPartitions: FieldObject(),
            readContext: readContext
        )
        #expect(askResult.value)
        #expect(askResult.metadata.composition.bases == [fixture.baseID])
        #expect(
            askResult.metadata.basePlacementGenerations[fixture.baseID]
                != nil
        )
        #expect(
            askResult.metadata.schemaGeneration
                == fixture.container.schemaGeneration
        )
        #expect(
            askResult.origin
                == .derived(contributors: [fixture.baseID])
        )
        #expect(readContext.workMeter.retainedIntermediateRows == 0)
        #expect(readContext.workMeter.retainedIntermediateBytes == 0)

        let foreignMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: fixture.container.monotonicClock
        )
        fixture.outputWorkMeter.set(foreignMeter)
        await #expect(
            throws: DatabaseIntermediateReservationError.workMeterMismatch
        ) {
            try await CompositionSPARQLQueryPlanner(
                structuralLimits: readContext.queryStructuralLimits
            ).execute(
                SelectQuery(
                    projection: .all,
                    source: .graphPattern(.basic([]))
                ),
                source: source,
                graphPartitions: FieldObject(),
                pageSize: 1,
                readContext: readContext
            ) { _ in true }
        }
        #expect(foreignMeter.retainedIntermediateRows == 0)
        #expect(foreignMeter.retainedIntermediateBytes == 0)

        await #expect(
            throws: DatabaseIntermediateReservationError.workMeterMismatch
        ) {
            try await CompositionRDFQueryPlanner().execute(
                .construct(
                    ConstructQuery(
                        template: [
                            TriplePattern(
                                subject: .iri("https://example.com/subject"),
                                predicate: .iri("https://example.com/predicate"),
                                object: .literal(.string("object"))
                            )
                        ],
                        pattern: .basic([])
                    )
                ),
                source: source,
                graphPartitions: FieldObject(),
                nodeNamespace: try GraphResultNodeNamespace(
                    ByteString(repeating: 0x31, count: 32)
                ),
                readContext: readContext
            ) { _ in true }
        }
        #expect(foreignMeter.retainedIntermediateRows == 0)
        #expect(foreignMeter.retainedIntermediateBytes == 0)

        fixture.outputWorkMeter.set(nil)
        let sourceQuad = RDFQuad(
            subject: .blankNode(
                try RDFBlankNodeIdentifier("rdf-subject")
            ),
            predicate: try RDFPredicateIRI(
                "https://example.com/rdf-predicate"
            ),
            object: .blankNode(
                try RDFBlankNodeIdentifier("rdf-object")
            )
        )
        fixture.outputWorkMeter.setQuad(sourceQuad)
        let emittedQuads = CompositionRDFQuadProbe()
        try await CompositionRDFQueryPlanner().execute(
            .construct(
                ConstructQuery(
                    template: [],
                    pattern: .basic([])
                )
            ),
            source: source,
            graphPartitions: FieldObject(),
            nodeNamespace: try GraphResultNodeNamespace(
                ByteString(repeating: 0x32, count: 32)
            ),
            readContext: readContext
        ) { event in
            await emittedQuads.receive(event)
        }
        let expectedQuad = try CompositionRDFIdentity.qualifyBlankNodes(
            in: sourceQuad,
            baseID: fixture.baseID
        )
        #expect(await emittedQuads.values() == [expectedQuad])
        #expect(readContext.workMeter.retainedIntermediateRows == 0)
        #expect(readContext.workMeter.retainedIntermediateBytes == 0)

        let describedQuads = CompositionRDFQuadProbe()
        try await CompositionRDFQueryPlanner().execute(
            .describe(
                DescribeQuery(
                    selection: .resources(
                        first: .iri("https://example.com/resource"),
                        additional: []
                    )
                )
            ),
            source: source,
            graphPartitions: FieldObject(),
            readContext: readContext
        ) { event in
            await describedQuads.receive(event)
        }
        #expect(await describedQuads.values() == [expectedQuad])
        #expect(readContext.workMeter.retainedIntermediateRows == 0)
        #expect(readContext.workMeter.retainedIntermediateBytes == 0)

        let sourceRow = QueryRow(
            fields: [
                "subject": .rdfTerm(
                    .blankNode(try RDFBlankNodeIdentifier("subject"))
                ),
            ],
            annotations: [
                "nested": .array([
                    .rdfTerm(
                        .blankNode(try RDFBlankNodeIdentifier("nested"))
                    ),
                ]),
            ]
        )
        fixture.outputWorkMeter.setRow(sourceRow)
        let distinctQuery = SelectQuery(
            projection: .all,
            source: .graphPattern(.basic([])),
            distinct: true
        )
        func context(maximumIntermediateBytes: UInt64) -> ReadExecutionContext {
            ReadExecutionContext(
                options: ReadExecutionOptions(
                    pageSize: 1,
                    budget: ExecutionBudget(
                        maximumRows: 100,
                        maximumWorkUnits: 100_000,
                        maximumIntermediateRows: 16,
                        maximumIntermediateBytes: maximumIntermediateBytes,
                        timeoutMilliseconds: 30_000
                    )
                ),
                monotonicClock: fixture.container.monotonicClock
            )
        }

        let calibrationContext = context(
            maximumIntermediateBytes: 1 * 1_024 * 1_024
        )
        let emittedRows = CompositionSPARQLRowProbe()
        try await CompositionSPARQLQueryPlanner(
            structuralLimits: calibrationContext.queryStructuralLimits
        ).execute(
            distinctQuery,
            source: source,
            graphPartitions: FieldObject(),
            pageSize: 1,
            readContext: calibrationContext
        ) { event in
            await emittedRows.receive(event)
        }
        let expectedRow = try CompositionRDFIdentity.qualifyBlankNodes(
            in: sourceRow,
            baseID: fixture.baseID
        )
        #expect(await emittedRows.values().map(\.row) == [expectedRow])
        #expect(calibrationContext.workMeter.retainedIntermediateRows == 0)
        #expect(calibrationContext.workMeter.retainedIntermediateBytes == 0)
        let successfulPeak = calibrationContext.workMeter
            .peakIntermediateBytes
        #expect(successfulPeak > 0)

        let constrainedContext = context(
            maximumIntermediateBytes: successfulPeak - 1
        )
        let constrainedEmissions = CompositionSPARQLRowProbe()
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await CompositionSPARQLQueryPlanner(
                structuralLimits: constrainedContext.queryStructuralLimits
            ).execute(
                distinctQuery,
                source: source,
                graphPartitions: FieldObject(),
                pageSize: 1,
                readContext: constrainedContext
            ) { event in
                await constrainedEmissions.receive(event)
            }
        }
        #expect(await constrainedEmissions.values().isEmpty)
        #expect(constrainedContext.workMeter.retainedIntermediateRows == 0)
        #expect(constrainedContext.workMeter.retainedIntermediateBytes == 0)
    }

    private func makeFixture() async throws -> Fixture {
        let domainID = try DatabaseStorageDomain.ID(
            "composition-sparql-authorization"
        )
        let placementID = try Base.Placement.ID(
            "composition-sparql-authorization"
        )
        let baseID = try Base.ID("composition-sparql-authorization")
        let owner = Principal(identifier: "composition-sparql-owner")
        let reader = Principal(identifier: "composition-sparql-reader")
        let engine = InMemoryEngine()
        let topology = try DatabaseStorageTopology(
            controlDomainID: domainID,
            domains: [
                try DatabaseStorageDomain(
                    id: domainID,
                    rootPath: [
                        "tests", "composition-sparql-authorization"
                    ],
                    storageEngine: engine
                )
            ],
            placements: [
                DatabaseStoragePlacement(
                    id: placementID,
                    domainID: domainID
                )
            ],
            defaultPlacementID: placementID
        )
        let outputWorkMeter = CompositionSPARQLWorkMeterControl()
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(
                name: "composition-sparql-authorization",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "graph-index-tests",
                    revision: 1
                ),
                sparqlSourceExecutor: CompositionSPARQLAuthorizationProbe(
                    outputWorkMeter: outputWorkMeter
                ),
                entityRuntimes: [
                    try EntityRuntimeDefinition(Anchor.self).registration()
                ]
            ),
            security: .testingDisabled
        )
        do {
            _ = try await container.provisionBase(
                baseID,
                placementID: placementID,
                initialGrants: [
                    Security.Grant(
                        subject: .principal(owner.identifier),
                        resource: .base(baseID),
                        access: .all
                    ),
                    Security.Grant(
                        subject: .principal(reader.identifier),
                        resource: .base(baseID),
                        access: .read
                    ),
                ],
                expectedRevision: 0
            )
        } catch {
            await container.shutdown()
            throw error
        }
        return Fixture(
            container: container,
            baseID: baseID,
            readerAuthorization: .authenticated(reader),
            outputWorkMeter: outputWorkMeter
        )
    }
}
#endif
