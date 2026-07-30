import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import GraphIndex
import StorageKit
import TestHeartbeat
import Testing
import TestSupport

@Suite("SPARQL runtime expression execution", .heartbeat)
struct SPARQLRuntimeExpressionExecutionTests {
    private struct ExistsLimitScanner: RDFDatasetScanner {
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
            guard limit == 1 else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "EXISTS did not request a first-match physical scan"
                )
            }
            guard readMode == .snapshot else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "SPARQL query reads must use snapshot isolation"
                )
            }
            return RDFDatasetScanResult(
                quads: [
                    RDFQuad(
                        subject: .iri(
                            try RDFIRI("did:example:subject")
                        ),
                        predicate: try RDFPredicateIRI(
                            "did:example:predicate"
                        ),
                        object: try .iri(
                            validating: "did:example:object"
                        )
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

    private struct EchoFunction: SPARQLFunction {
        let identifier: RDFIRI

        func evaluate(
            arguments: [FieldValue]
        ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
            guard arguments.count == 1 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(
                    identifier.rawValue
                )
            }
            return arguments[0]
        }
    }

    private struct FailureFunction: SPARQLFunction {
        let identifier: RDFIRI

        func evaluate(
            arguments: [FieldValue]
        ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "failure function was evaluated"
            )
        }
    }

    @Test("BIND evaluates a dataset-dependent EXISTS expression")
    func bindEvaluatesExists() async throws {
        let existsQuery = SelectQuery(
            projection: .all,
            source: .graphPattern(.basic([]))
        )
        let queryPattern = GraphPattern.bind(
            .basic([]),
            variable: "exists",
            expression: .exists(existsQuery)
        )

        let result = try await execute(
            GraphPatternConverter.convert(queryPattern)
        )

        #expect(result.count == 1)
        guard case .rdfTerm(.literal(let literal)) = result[0]["?exists"] else {
            Issue.record("BIND did not produce a canonical RDF literal")
            return
        }
        #expect(
            literal.datatypeIRI.rawValue
                == "http://www.w3.org/2001/XMLSchema#boolean"
        )
        #expect(literal.lexicalForm == "true")
    }

    @Test("Correlated EXISTS sees the outer binding inside its FILTER")
    func correlatedExistsSeesOuterBinding() async throws {
        let outer = GraphPattern.bind(
            .basic([]),
            variable: "flag",
            expression: .literal(.bool(true))
        )
        let inner = GraphPattern.filter(
            .basic([]),
            .variable(Variable("flag"))
        )
        let existsQuery = SelectQuery(
            projection: .all,
            source: .graphPattern(inner)
        )
        let queryPattern = GraphPattern.filter(
            outer,
            .exists(existsQuery)
        )

        let result = try await execute(
            GraphPatternConverter.convert(queryPattern)
        )

        #expect(result.count == 1)
        #expect(result[0].isBound("?flag"))
    }

    @Test("A local BIND expression error leaves its target unbound")
    func bindExpressionErrorLeavesTargetUnbound() async throws {
        let queryPattern = GraphPattern.bind(
            .basic([]),
            variable: "value",
            expression: .variable(Variable("missing"))
        )

        let result = try await execute(
            GraphPatternConverter.convert(queryPattern)
        )

        #expect(result.count == 1)
        #expect(!result[0].isBound("?value"))
    }

    @Test("Boolean short circuit does not execute an unreachable function")
    func booleanShortCircuitSkipsUnreachableFunction() async throws {
        let identifier = try RDFIRI("did:example:fail")
        let registry = try SPARQLFunctionRegistry([
            FailureFunction(identifier: identifier)
        ])
        let failure = Expression.function(
            FunctionCall(
                name: identifier.rawValue,
                arguments: []
            )
        )
        let falseAndFailure = ExecutionPattern.filter(
            .basic([]),
            try GraphPatternConverter.convertFilter(
                .and(.literal(.bool(false)), failure)
            )
        )
        let trueOrFailure = ExecutionPattern.filter(
            .basic([]),
            try GraphPatternConverter.convertFilter(
                .or(.literal(.bool(true)), failure)
            )
        )

        #expect(
            try await execute(
                falseAndFailure,
                functionRegistry: registry
            ).isEmpty
        )
        #expect(
            try await execute(
                trueOrFailure,
                functionRegistry: registry
            ).count == 1
        )
    }

    @Test("IF and COALESCE dispatch ASCII case-insensitively")
    func conditionalFunctionsDispatchIgnoringASCIICase() async throws {
        let ifRows = try await execute(
            .extend(
                .basic([]),
                variable: "?value",
                expression: try SPARQLExpressionPlan(
                    .function(
                        FunctionCall(
                            name: "if",
                            arguments: [
                                .literal(.bool(true)),
                                .literal(.string("selected")),
                                .literal(.string("fallback")),
                            ]
                        )
                    )
                )
            )
        )
        let coalesceRows = try await execute(
            .extend(
                .basic([]),
                variable: "?value",
                expression: try SPARQLExpressionPlan(
                    .function(
                        FunctionCall(
                            name: "coalesce",
                            arguments: [
                                .variable(Variable("missing")),
                                .literal(.string("available")),
                            ]
                        )
                    )
                )
            )
        )

        guard case .rdfTerm(.literal(let ifValue)) = ifRows.first?["?value"],
              case .rdfTerm(.literal(let coalesceValue)) = coalesceRows.first?["?value"] else {
            Issue.record("Conditional functions did not produce RDF literals")
            return
        }
        #expect(ifValue.lexicalForm == "selected")
        #expect(coalesceValue.lexicalForm == "available")
    }

    @Test("A runtime expression failure is not converted to an unbound BIND")
    func bindRuntimeFailurePropagates() async throws {
        let identifier = try RDFIRI("did:example:fail")
        let registry = try SPARQLFunctionRegistry([
            FailureFunction(identifier: identifier)
        ])
        let queryPattern = GraphPattern.bind(
            .basic([]),
            variable: "value",
            expression: .function(
                FunctionCall(name: identifier.rawValue, arguments: [])
            )
        )
        let executionPattern = try GraphPatternConverter.convert(queryPattern)

        await #expect(throws: SPARQLExpressionEvaluationError.self) {
            try await execute(
                executionPattern,
                functionRegistry: registry
            )
        }
    }

    @Test("An invalid EXISTS source fails during compilation")
    func invalidExistsSourceFailsCompilation() {
        let invalid = Expression.exists(
            SelectQuery(
                projection: .all,
                source: .table(TableRef("not-a-graph"))
            )
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            _ = try SPARQLExpressionPlan(invalid)
        }
    }

    @Test("EXISTS compiles once and requests only its first physical match")
    func existsUsesFirstMatchExecution() async throws {
        let exists = SelectQuery(
            projection: .all,
            source: .graphPattern(
                .basic([
                    TriplePattern(
                        subject: .variable("subject"),
                        predicate: .variable("predicate"),
                        object: .variable("object")
                    )
                ])
            )
        )
        let pattern = ExecutionPattern.extend(
            .basic([]),
            variable: "?exists",
            expression: try SPARQLExpressionPlan(.exists(exists))
        )
        let result = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            wallClock: FixedTestWallClock(),
            datasetScanner: ExistsLimitScanner()
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 100,
                    maximumWorkUnits: 1_000,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
        )

        #expect(result.0.count == 1)
        guard case .rdfTerm(.literal(let value)) = result.0[0]["?exists"] else {
            Issue.record("EXISTS did not produce an RDF boolean")
            return
        }
        #expect(value.lexicalForm == "true")
    }

    @Test("NOW is stable for every solution in one query")
    func nowIsQueryStable() async throws {
        let rows = try await execute(
            .extend(
                .union(.basic([]), .basic([])),
                variable: "?now",
                expression: try SPARQLExpressionPlan(
                    .function(FunctionCall(name: "NOW", arguments: []))
                )
            )
        )

        #expect(rows.count == 2)
        #expect(rows[0]["?now"] == rows[1]["?now"])
        guard case .rdfTerm(.literal(let literal)) = rows[0]["?now"] else {
            Issue.record("NOW did not return an RDF literal")
            return
        }
        #expect(
            literal.datatypeIRI.rawValue
                == "http://www.w3.org/2001/XMLSchema#dateTime"
        )
    }

    @Test("BNODE labels are stable per solution occurrence, not query-global")
    func blankNodeScopeFollowsQuerySemantics() async throws {
        let source = ExecutionPattern.union(.basic([]), .basic([]))
        let labeled = try await execute(
            .extend(
                source,
                variable: "?node",
                expression: try SPARQLExpressionPlan(
                    .function(
                        FunctionCall(
                            name: "BNODE",
                            arguments: [.literal(.string("label"))]
                        )
                    )
                )
            )
        )
        let repeatedWithinOneSolution = try await execute(
            .extend(
                .extend(
                    .basic([]),
                    variable: "?first",
                    expression: try SPARQLExpressionPlan(
                        .function(
                            FunctionCall(
                                name: "BNODE",
                                arguments: [.literal(.string("label"))]
                            )
                        )
                    )
                ),
                variable: "?second",
                expression: try SPARQLExpressionPlan(
                    .function(
                        FunctionCall(
                            name: "BNODE",
                            arguments: [.literal(.string("label"))]
                        )
                    )
                )
            )
        )
        let fresh = try await execute(
            .extend(
                source,
                variable: "?node",
                expression: try SPARQLExpressionPlan(
                    .function(FunctionCall(name: "BNODE", arguments: []))
                )
            )
        )

        #expect(labeled.count == 2)
        #expect(labeled[0]["?node"] != labeled[1]["?node"])
        #expect(fresh.count == 2)
        #expect(fresh[0]["?node"] != fresh[1]["?node"])
        #expect(repeatedWithinOneSolution.count == 1)
        #expect(
            repeatedWithinOneSolution[0]["?first"]
                == repeatedWithinOneSolution[0]["?second"]
        )
    }

    @Test("Executor uses its injected extension function registry")
    func injectedFunctionRegistryIsExecuted() async throws {
        let identifier = try RDFIRI("did:example:echo")
        let registry = try SPARQLFunctionRegistry([
            EchoFunction(identifier: identifier)
        ])
        let rows = try await execute(
            .extend(
                .basic([]),
                variable: "?value",
                expression: try SPARQLExpressionPlan(
                    .function(
                        FunctionCall(
                            name: identifier.rawValue,
                            arguments: [.literal(.string("registered"))]
                        )
                    )
                )
            ),
            functionRegistry: registry
        )

        guard case .rdfTerm(.literal(let value)) = rows.first?["?value"] else {
            Issue.record("Injected extension function did not produce an RDF literal")
            return
        }
        #expect(value.lexicalForm == "registered")
    }

    @Test("RAND and UUID return typed values")
    func randomFunctionsReturnTypedValues() async throws {
        let random = try await execute(
            .extend(
                .basic([]),
                variable: "?value",
                expression: try SPARQLExpressionPlan(
                    .function(FunctionCall(name: "RAND", arguments: []))
                )
            )
        )
        let uuid = try await execute(
            .extend(
                .basic([]),
                variable: "?value",
                expression: try SPARQLExpressionPlan(
                    .function(FunctionCall(name: "UUID", arguments: []))
                )
            )
        )

        guard case .rdfTerm(.literal(let randomLiteral)) = random[0]["?value"],
              let randomValue = Double(randomLiteral.lexicalForm) else {
            Issue.record("RAND did not return a numeric RDF literal")
            return
        }
        #expect(
            randomLiteral.datatypeIRI.rawValue
                == "http://www.w3.org/2001/XMLSchema#double"
        )
        #expect(randomValue >= 0 && randomValue < 1)
        guard case .rdfTerm(.iri(let uuidIRI)) = uuid[0]["?value"] else {
            Issue.record("UUID did not return an IRI")
            return
        }
        #expect(uuidIRI.rawValue.hasPrefix("urn:uuid:"))
        #expect(uuidIRI.rawValue.count == 45)
    }

    @Test("Projection expressions compile to Extend and bind their alias")
    func projectionExpressionBindsAlias() async throws {
        let source = ExecutionPattern.extend(
            .basic([]),
            variable: "?source",
            expression: try SPARQLExpressionPlan(.literal(.int(4)))
        )
        let projection = Projection.items([
            ProjectionItem(
                .add(.variable(Variable("source")), .literal(.int(1))),
                alias: "result"
            )
        ])
        let pattern = try GraphPatternConverter.applyingProjectionExpressions(
            projection,
            to: source
        )

        let rows = try await execute(pattern)

        #expect(rows.count == 1)
        guard case .rdfTerm(.literal(let result)) = rows[0]["?result"] else {
            Issue.record("Projection expression did not bind a canonical literal")
            return
        }
        #expect(result.lexicalForm == "5")
        #expect(
            result.datatypeIRI.rawValue
                == "http://www.w3.org/2001/XMLSchema#integer"
        )
    }

    @Test("A projection expression can use a preceding alias")
    func projectionCanUsePrecedingAlias() throws {
        let projection = Projection.items([
            ProjectionItem(.literal(.int(1)), alias: "first"),
            ProjectionItem(
                .variable(Variable("first")),
                alias: "second"
            ),
        ])

        let pattern = try GraphPatternConverter.applyingProjectionExpressions(
            projection,
            to: .basic([])
        )
        #expect(pattern.outputVariables.contains("?first"))
        #expect(pattern.outputVariables.contains("?second"))
    }

    @Test("A projection expression cannot use a following alias")
    func projectionForwardAliasFailsCompilation() {
        let projection = Projection.items([
            ProjectionItem(.variable(Variable("second")), alias: "first"),
            ProjectionItem(.literal(.int(1)), alias: "second"),
        ])

        #expect(
            throws: GraphPatternConversionError
                .projectionAliasDependency("?second")
        ) {
            try GraphPatternConverter.applyingProjectionExpressions(
                projection,
                to: .basic([])
            )
        }
    }

    private func execute(
        _ pattern: ExecutionPattern,
        functionRegistry: SPARQLFunctionRegistry = .empty
    ) async throws -> [VariableBinding] {
        let result = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            wallClock: FixedTestWallClock(),
            sources: [],
            functionRegistry: functionRegistry
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 1_000,
                    maximumWorkUnits: 10_000,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
        )
        return result.0
    }
}
