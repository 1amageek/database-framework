import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import Synchronization
import TestHeartbeat
import Testing
import TestSupport
@testable import GraphIndex
@testable import QueryAST

@Suite("SPARQL SubSelect execution", .heartbeat)
struct SPARQLSubSelectExecutionTests {
    private struct ScanObservationState: Sendable {
        var callCount = 0
        var transactionIdentifiers: Set<ObjectIdentifier> = []
        var workMeterIdentifiers: Set<ObjectIdentifier> = []
    }

    private final class ScanObservations: Sendable {
        private let state = Mutex(ScanObservationState())

        var callCount: Int {
            state.withLock { $0.callCount }
        }

        var transactionCount: Int {
            state.withLock { $0.transactionIdentifiers.count }
        }

        var workMeterIdentifiers: Set<ObjectIdentifier> {
            state.withLock { $0.workMeterIdentifiers }
        }

        func record(
            transaction: any TransactionAccess,
            workMeter: DatabaseWorkMeter
        ) {
            state.withLock {
                $0.callCount += 1
                $0.transactionIdentifiers.insert(
                    ObjectIdentifier(transaction as AnyObject)
                )
                $0.workMeterIdentifiers.insert(ObjectIdentifier(workMeter))
            }
        }
    }

    private struct RecordingScanner: RDFDatasetScanner {
        let observations: ScanObservations

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
            observations.record(
                transaction: transaction,
                workMeter: workMeter
            )
            return RDFDatasetScanResult(
                quads: [
                    RDFQuad(
                        subject: .iri(try RDFIRI("urn:subject")),
                        predicate: try RDFPredicateIRI("urn:predicate"),
                        object: try .iri(validating: "urn:object")
                    )
                ],
                physicalScanCount: 1
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

    private struct RetryScanObservationState: Sendable {
        var innerScanCount = 0
        var rightScanCount = 0
        var transactions: [ObjectIdentifier: any TransactionAccess] = [:]
        var workMeterIdentifiers: Set<ObjectIdentifier> = []
    }

    private final class RetryScanObservations: Sendable {
        private let state = Mutex(RetryScanObservationState())

        var innerScanCount: Int {
            state.withLock { $0.innerScanCount }
        }

        var rightScanCount: Int {
            state.withLock { $0.rightScanCount }
        }

        var transactionCount: Int {
            state.withLock { $0.transactions.count }
        }

        var workMeterIdentifiers: Set<ObjectIdentifier> {
            state.withLock { $0.workMeterIdentifiers }
        }

        func record(
            predicate: String,
            transaction: any TransactionAccess,
            workMeter: DatabaseWorkMeter
        ) -> Int {
            state.withLock { state in
                let transactionIdentifier = ObjectIdentifier(
                    transaction as AnyObject
                )
                state.transactions[transactionIdentifier] = transaction
                state.workMeterIdentifiers.insert(ObjectIdentifier(workMeter))
                switch predicate {
                case "urn:inner":
                    state.innerScanCount += 1
                    return state.innerScanCount
                case "urn:right":
                    state.rightScanCount += 1
                    return state.rightScanCount
                default:
                    return 0
                }
            }
        }
    }

    private struct RetryAwareScanner: RDFDatasetScanner {
        let observations: RetryScanObservations

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
            guard case .iri(let predicateIRI) = predicate else {
                throw StorageError(
                    code: .invalidOperation,
                    operation: .read,
                    backend: .inMemory,
                    message: "Retry scanner requires a bound predicate"
                )
            }
            let call = observations.record(
                predicate: predicateIRI.rawValue,
                transaction: transaction,
                workMeter: workMeter
            )

            switch predicateIRI.rawValue {
            case "urn:inner":
                let value = call == 1 ? "urn:first" : "urn:second"
                return RDFDatasetScanResult(
                    quads: [
                        RDFQuad(
                            subject: .iri(
                                try RDFIRI("urn:inner-subject")
                            ),
                            predicate: RDFPredicateIRI(predicateIRI),
                            object: try .iri(validating: value)
                        )
                    ],
                    physicalScanCount: 1
                )
            case "urn:right":
                if call == 1 {
                    throw StorageError.transactionConflict
                }
                return RDFDatasetScanResult(
                    quads: [
                        RDFQuad(
                            subject: .iri(
                                try RDFIRI("urn:right-subject")
                            ),
                            predicate: RDFPredicateIRI(predicateIRI),
                            object: try .iri(validating: "urn:second")
                        )
                    ],
                    physicalScanCount: 1
                )
            default:
                throw StorageError(
                    code: .invalidOperation,
                    operation: .read,
                    backend: .inMemory,
                    message: "Unexpected predicate \(predicateIRI)"
                )
            }
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

    private struct FollowingBindingScanner: RDFDatasetScanner {
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
            guard case .iri(let predicateIRI) = predicate else {
                return RDFDatasetScanResult(quads: [], physicalScanCount: 1)
            }

            let quad: RDFQuad
            switch predicateIRI.rawValue {
            case "urn:p":
                quad = RDFQuad(
                    subject: .iri(try RDFIRI("urn:subject")),
                    predicate: RDFPredicateIRI(predicateIRI),
                    object: try .iri(validating: "urn:object")
                )
            case "urn:q":
                quad = RDFQuad(
                    subject: .iri(try RDFIRI("urn:subject")),
                    predicate: RDFPredicateIRI(predicateIRI),
                    object: .literal(
                        try RDFLiteral(
                            lexicalForm: "1",
                            datatype: "http://www.w3.org/2001/XMLSchema#integer"
                        )
                    )
                )
            default:
                return RDFDatasetScanResult(quads: [], physicalScanCount: 1)
            }
            return RDFDatasetScanResult(
                quads: [quad],
                physicalScanCount: 1
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

    @Test("A SubSelect exposes only projected variables")
    func hiddenVariablesDoNotEscape() async throws {
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("visible")))
            ]),
            source: .graphPattern(
                .values(
                    variables: ["visible", "hidden"],
                    bindings: [[.string("public"), .string("private")]]
                )
            )
        )
        let pattern = try GraphPatternConverter.convert(.subquery(query))

        #expect(pattern.outputVariables == ["?visible"])
        let rows = try await execute(pattern)
        #expect(rows.count == 1)
        #expect(lexicalForm(rows[0], variable: "?visible") == "public")
        #expect(!rows[0].isBound("?hidden"))
    }

    @Test("A normal join matches only projected SubSelect variables")
    func projectedVariablesParticipateInOuterJoin() async throws {
        let inner = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("identifier"))),
                ProjectionItem(.variable(Variable("value"))),
            ]),
            source: .graphPattern(
                .values(
                    variables: ["identifier", "value"],
                    bindings: [
                        [.string("a"), .string("one")],
                        [.string("c"), .string("three")],
                    ]
                )
            )
        )
        let graphPattern = GraphPattern.join(
            .values(
                variables: ["identifier"],
                bindings: [[.string("a")], [.string("b")]]
            ),
            .subquery(inner)
        )

        let rows = try await execute(
            GraphPatternConverter.convert(graphPattern)
        )

        #expect(rows.count == 1)
        #expect(lexicalForm(rows[0], variable: "?identifier") == "a")
        #expect(lexicalForm(rows[0], variable: "?value") == "one")
    }

    @Test("An isolated SubSelect occurrence is scanned once in one transaction")
    func isolatedOccurrenceUsesOneScanAndSharedRuntimeContext() async throws {
        let observations = ScanObservations()
        let scanner = RecordingScanner(observations: observations)
        let inner = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("subject")))
            ]),
            source: .graphPattern(
                .basic([
                    TriplePattern(
                        subject: .variable("subject"),
                        predicate: .iri("urn:predicate"),
                        object: .variable("object")
                    )
                ])
            )
        )
        let pattern = try GraphPatternConverter.convert(
            .join(
                .values(
                    variables: ["outer"],
                    bindings: [[.string("first")], [.string("second")]]
                ),
                .subquery(inner)
            )
        )
        let meter = makeWorkMeter()

        let result = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: scanner
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: meter
        )

        #expect(result.0.count == 2)
        #expect(observations.callCount == 1)
        #expect(observations.transactionCount == 1)
        #expect(
            observations.workMeterIdentifiers == [ObjectIdentifier(meter)]
        )
    }

    @Test("Isolated SubSelect execution enforces the request row budget")
    func isolatedExecutionEnforcesRequestRowBudget() async throws {
        let first = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("first")))
            ]),
            source: .graphPattern(
                .values(
                    variables: ["first"],
                    bindings: [[.string("one")]]
                )
            )
        )
        let second = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("second")))
            ]),
            source: .graphPattern(
                .values(
                    variables: ["second"],
                    bindings: [[.string("two")]]
                )
            )
        )
        let pattern = try GraphPatternConverter.convert(
            .join(.subquery(first), .subquery(second))
        )

        do {
            _ = try await SPARQLQueryExecutor(
                database: InMemoryEngine(),
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(),
                sources: []
            ).execute(
                pattern: pattern,
                limit: nil,
                offset: 0,
                workMeter: makeWorkMeter(maximumIntermediateRows: 1)
            )
            Issue.record("Expected the SubSelect request row budget to fail")
        } catch let error as DatabaseWorkLimitError {
            #expect(error == .maximumIntermediateRows(
                stage: .bindingCandidate,
                consumed: 1,
                requested: 1,
                maximum: 1
            ))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("The isolated SubSelect cache enforces its retained byte budget")
    func isolatedCacheEnforcesRetainedByteBudget() async throws {
        let inner = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("value")))
            ]),
            source: .graphPattern(
                .values(
                    variables: ["value"],
                    bindings: [[.string(String(repeating: "x", count: 128))]]
                )
            )
        )
        let pattern = try GraphPatternConverter.convert(.subquery(inner))

        do {
            _ = try await SPARQLQueryExecutor(
                database: InMemoryEngine(),
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(),
                sources: []
            ).execute(
                pattern: pattern,
                limit: nil,
                offset: 0,
                workMeter: makeWorkMeter(maximumIntermediateBytes: 1)
            )
            Issue.record("Expected the SubSelect byte budget to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(
                stage: .subqueryCache,
                consumed: 0,
                requested: let requested,
                maximum: 1
            ) = error else {
                Issue.record("Unexpected work limit: \(error)")
                return
            }
            #expect(requested > 1)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("An isolated SubSelect cache never crosses request execution")
    func isolatedCacheIsRequestScoped() async throws {
        let observations = ScanObservations()
        let inner = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("subject")))
            ]),
            source: .graphPattern(
                .basic([
                    TriplePattern(
                        subject: .variable("subject"),
                        predicate: .iri("urn:predicate"),
                        object: .variable("object")
                    )
                ])
            )
        )
        let pattern = try GraphPatternConverter.convert(.subquery(inner))
        let executor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: RecordingScanner(observations: observations)
        )

        _ = try await executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )
        _ = try await executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )

        #expect(observations.callCount == 2)
    }

    @Test("A transaction retry rebuilds the isolated SubSelect cache")
    func isolatedCacheIsFreshForEveryTransactionAttempt() async throws {
        let observations = RetryScanObservations()
        let innerQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("value")))
            ]),
            source: .graphPattern(
                .basic([
                    TriplePattern(
                        subject: .variable("innerSubject"),
                        predicate: .iri("urn:inner"),
                        object: .variable("value")
                    )
                ])
            )
        )
        let pattern = try GraphPatternConverter.convert(
            .join(
                .subquery(innerQuery),
                .basic([
                    TriplePattern(
                        subject: .variable("rightSubject"),
                        predicate: .iri("urn:right"),
                        object: .variable("value")
                    )
                ])
            )
        )
        let meter = makeWorkMeter()

        let result = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: RetryAwareScanner(observations: observations)
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: meter
        )

        #expect(result.0.count == 1)
        let expectedValue = FieldValue.rdfTerm(
            try .iri(validating: "urn:second")
        )
        #expect(result.0[0]["?value"] == expectedValue)
        #expect(observations.innerScanCount == 2)
        #expect(observations.rightScanCount == 2)
        #expect(observations.transactionCount == 2)
        #expect(
            observations.workMeterIdentifiers == [ObjectIdentifier(meter)]
        )
    }

    @Test("A parsed FILTER sees a binding produced by a following triple")
    func parsedFilterSeesFollowingTripleBinding() async throws {
        let query = try SPARQLParser().parseSelect(
            """
            SELECT ?s WHERE {
                ?s <urn:p> ?x .
                FILTER(?y = 1) .
                ?s <urn:q> ?y
            }
            """
        )
        guard case .graphPattern(let graphPattern) = query.source else {
            Issue.record("Expected a graph-pattern query source")
            return
        }

        let result = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: FollowingBindingScanner()
        ).execute(
            pattern: GraphPatternConverter.convert(graphPattern),
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )

        #expect(result.0.count == 1)
        let expectedSubject = FieldValue.rdfTerm(
            try .iri(validating: "urn:subject")
        )
        #expect(result.0[0]["?s"] == expectedSubject)
        #expect(lexicalForm(result.0[0], variable: "?y") == "1")
    }

    @Test("A normal SubSelect cannot read an outer binding")
    func isolatedSubSelectDoesNotSeeOuterBinding() async throws {
        let rows = try await execute(
            GraphPatternConverter.convert(
                .join(
                    .values(
                        variables: ["identifier"],
                        bindings: [[.string("a")]]
                    ),
                    .subquery(correlatedQuery())
                )
            )
        )

        #expect(rows.isEmpty)
    }

    @Test("A LATERAL SubSelect receives the current outer binding")
    func lateralSubSelectSeesOuterBinding() async throws {
        let rows = try await execute(
            GraphPatternConverter.convert(
                .lateral(
                    .values(
                        variables: ["identifier"],
                        bindings: [[.string("a")], [.string("b")]]
                    ),
                    .subquery(correlatedQuery())
                )
            )
        )

        #expect(rows.count == 1)
        #expect(lexicalForm(rows[0], variable: "?identifier") == "a")
    }

    @Test("ORDER BY and Slice are local to the SubSelect")
    func localOrderAndSliceAreAppliedBeforeOuterEvaluation() async throws {
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("visible")))
            ]),
            source: .graphPattern(
                .values(
                    variables: ["visible", "rank"],
                    bindings: [
                        [.string("second"), .int(2)],
                        [.string("first"), .int(1)],
                    ]
                )
            ),
            orderBy: [SortKey(.variable(Variable("rank")))],
            limit: 1
        )

        let rows = try await execute(
            GraphPatternConverter.convert(.subquery(query))
        )

        #expect(rows.count == 1)
        #expect(lexicalForm(rows[0], variable: "?visible") == "first")
        #expect(!rows[0].isBound("?rank"))
    }

    @Test("DISTINCT observes projected SubSelect solutions")
    func projectionPrecedesDistinct() async throws {
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("visible")))
            ]),
            source: .graphPattern(
                .values(
                    variables: ["visible", "hidden"],
                    bindings: [
                        [.string("same"), .int(1)],
                        [.string("same"), .int(2)],
                    ]
                )
            ),
            distinct: true
        )

        let rows = try await execute(
            GraphPatternConverter.convert(.subquery(query))
        )

        #expect(rows.count == 1)
        #expect(lexicalForm(rows[0], variable: "?visible") == "same")
    }

    @Test("Aggregation is completed inside the SubSelect")
    func localAggregationIsProjected() async throws {
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .aggregate(
                        .count(
                            .variable(Variable("value")),
                            distinct: false
                        )
                    ),
                    alias: "count"
                )
            ]),
            source: .graphPattern(
                .values(
                    variables: ["value"],
                    bindings: [[.string("one")], [.string("two")]]
                )
            )
        )

        let rows = try await execute(
            GraphPatternConverter.convert(.subquery(query))
        )

        #expect(rows.count == 1)
        #expect(lexicalForm(rows[0], variable: "?count") == "2")
        #expect(!rows[0].isBound("?value"))
    }

    @Test("Unexecutable SubSelect modifiers and dataset clauses fail at compilation")
    func invalidSubSelectPlanFailsBeforeExecution() {
        let source = DataSource.graphPattern(GraphPattern.basic([]))

        #expect(
            throws: SPARQLSelectPlanCompilationError
                .solutionModifierExceedsExecutionRange(
                    name: "LIMIT",
                    value: .max
                )
        ) {
            try GraphPatternConverter.convert(
                .subquery(
                    SelectQuery(
                        projection: .all,
                        source: source,
                        limit: .max
                    )
                )
            )
        }
        #expect(
            throws: SPARQLSelectPlanCompilationError
                .explicitDatasetInSubquery
        ) {
            try GraphPatternConverter.convert(
                .subquery(
                    SelectQuery(
                        projection: .all,
                        source: source,
                        dataset: .explicit(
                            defaultGraphs: ["urn:default"],
                            namedGraphs: []
                        )
                    )
                )
            )
        }
    }

    @Test("The optimizer preserves the SubSelect algebra boundary")
    func optimizerPreservesSubSelectBoundary() async throws {
        let subquery = try GraphPatternConverter.convert(
            .subquery(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(.variable(Variable("visible")))
                    ]),
                    source: .graphPattern(
                        .values(
                            variables: ["visible", "hidden"],
                            bindings: [[.string("public"), .string("private")]]
                        )
                    )
                )
            )
        )
        let optimized = SPARQLQueryOptimizer().optimize(
            .filter(subquery, .bound("?visible"))
        )

        guard case .filter(
            .subquery(let optimizedPlan),
            .bound(let variable)
        ) = optimized,
        case .subquery(let originalPlan) = subquery else {
            Issue.record("Expected the filter to remain outside the SubSelect")
            return
        }
        #expect(variable == "?visible")
        #expect(
            optimizedPlan.occurrenceIdentifier
                == originalPlan.occurrenceIdentifier
        )
        #expect(optimizedPlan.inputPolicy == originalPlan.inputPolicy)
        let optimizedResults = try await execute(optimized)
        let originalResults = try await execute(subquery)
        #expect(optimizedResults == originalResults)
    }

    private func correlatedQuery() -> SelectQuery {
        SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("identifier")))
            ]),
            source: .graphPattern(
                .filter(
                    .basic([]),
                    .equal(
                        .variable(Variable("identifier")),
                        .literal(.string("a"))
                    )
                )
            )
        )
    }

    private func execute(
        _ pattern: ExecutionPattern
    ) async throws -> [VariableBinding] {
        let result = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeWorkMeter()
        )
        return result.0
    }

    private func makeWorkMeter(
        maximumIntermediateRows: UInt32 = 1_000,
        maximumIntermediateBytes: UInt64 = 16 * 1_024 * 1_024
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: maximumIntermediateRows,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func lexicalForm(
        _ binding: VariableBinding,
        variable: String
    ) -> String? {
        guard case .rdfTerm(.literal(let literal)) = binding[variable] else {
            return nil
        }
        return literal.lexicalForm
    }
}
