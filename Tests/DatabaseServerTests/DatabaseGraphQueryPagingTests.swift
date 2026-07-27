import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import DatabaseKit
import StorageKit
import Testing

@Suite("Canonical graph query paging")
struct DatabaseGraphQueryPagingTests {
    @Test("CONSTRUCT pages the canonical graph without gaps or duplicates")
    func constructPagesCanonicalGraph() async throws {
        let container = try await makeContainer()
        let query = constructQuery()
        let budget = executionBudget()
        var continuation: ByteString?
        var snapshots = Set<Int64>()
        var triples: [RDFQuad] = []

        for _ in 0..<8 {
            let page = try graphPage(
                try await execute(
                    request(
                        .construct(query),
                        limit: 1,
                        continuation: continuation,
                        budget: budget
                    ),
                    container: container
                )
            )
            triples.append(contentsOf: page.triples)
            if let snapshotVersion = page.snapshotVersion {
                snapshots.insert(snapshotVersion)
            }
            continuation = page.continuation
            if continuation == nil { break }
        }

        #expect(continuation == nil)
        #expect(triples.count == 4)
        #expect(Set(triples).count == 4)
        #expect(snapshots.count == 1)
    }

    @Test("CONSTRUCT deduplicates globally and scopes template blank nodes per binding")
    func constructDeduplicatesAndScopesBlankNodes() async throws {
        let container = try await makeContainer()
        let duplicate = TriplePattern(
            subject: .variable("subject"),
            predicate: .iri("urn:derived"),
            object: .variable("object")
        )
        let duplicateQuery = ConstructQuery(
            template: [duplicate, duplicate],
            pattern: sourcePattern
        )
        let duplicatePage = try graphPage(
            try await execute(
                request(.construct(duplicateQuery), limit: 10),
                container: container
            )
        )
        #expect(duplicatePage.triples.count == 2)
        #expect(Set(duplicatePage.triples).count == 2)

        let blankQuery = ConstructQuery(
            template: [
                TriplePattern(
                    subject: .blankNode("result"),
                    predicate: .iri("urn:value"),
                    object: .variable("object")
                )
            ],
            pattern: sourcePattern
        )
        let first = try graphPage(
            try await execute(
                request(.construct(blankQuery), limit: 10),
                container: container
            )
        )
        let second = try graphPage(
            try await execute(
                request(.construct(blankQuery), limit: 10),
                container: container
            )
        )
        let blankNodes = Set(first.triples.compactMap { triple -> String? in
            guard case .blankNode(let identifier) = triple.subject else {
                return nil
            }
            return identifier.rawValue
        })

        #expect(blankNodes.count == 2)
        #expect(first.triples == second.triples)
    }

    @Test("CONSTRUCT blank nodes remain distinct for duplicate solutions")
    func constructScopesBlankNodesPerDuplicateSolution() async throws {
        let container = try await makeContainer()
        let query = ConstructQuery(
            template: [
                TriplePattern(
                    subject: .blankNode("result"),
                    predicate: .iri("urn:value"),
                    object: .variable("object")
                )
            ],
            pattern: .union(sourcePattern, sourcePattern)
        )
        let page = try graphPage(
            try await execute(
                request(.construct(query), limit: 10),
                container: container
            )
        )
        let blankNodes = Set(page.triples.compactMap { quad -> String? in
            guard case .blankNode(let identifier) = quad.subject else {
                return nil
            }
            return identifier.rawValue
        })

        #expect(page.triples.count == 4)
        #expect(blankNodes.count == 4)
    }

    @Test("CONSTRUCT omission is local and retains reification output")
    func constructOmissionIsLocal() async throws {
        let container = try await makeContainer()
        let query = ConstructQuery(
            template: [
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:retained"),
                    object: .variable("object")
                ),
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:omitted"),
                    object: .variable("unbound")
                ),
                TriplePattern(
                    subject: .reifiedTriple(
                        subject: .variable("subject"),
                        predicate: .iri(Self.sourcePredicate),
                        object: .variable("object"),
                        reifier: .blankNode("statement")
                    ),
                    predicate: .iri("urn:outer-omitted"),
                    object: .variable("unbound")
                ),
            ],
            pattern: sourcePattern
        )
        let page = try graphPage(
            try await execute(
                request(.construct(query), limit: 10),
                container: container
            )
        )
        let retainedPredicate = try RDFPredicateIRI("urn:retained")
        let reifiesPredicate = try RDFPredicateIRI(Self.reifiesPredicate)
        let retainedCount = page.triples.count {
            $0.predicate == retainedPredicate
        }
        let reificationCount = page.triples.count {
            $0.predicate == reifiesPredicate
        }

        #expect(page.triples.count == 4)
        #expect(retainedCount == 2)
        #expect(reificationCount == 2)
        let omittedPredicate = try RDFPredicateIRI("urn:omitted")
        let outerOmittedPredicate = try RDFPredicateIRI(
            "urn:outer-omitted"
        )
        #expect(!page.triples.contains {
            $0.predicate == omittedPredicate
                || $0.predicate == outerOmittedPredicate
        })
    }

    @Test("DESCRIBE pages all outgoing triples exactly once")
    func describePagesOutgoingTriples() async throws {
        let container = try await makeContainer()
        let query = DescribeQuery(
            selection: .resources(
                first: .iri(Self.describedSubject),
                additional: []
            )
        )
        let budget = executionBudget()
        var continuation: ByteString?
        var triples: [RDFQuad] = []

        for _ in 0..<6 {
            let page = try graphPage(
                try await execute(
                    request(
                        .describe(query),
                        limit: 1,
                        continuation: continuation,
                        budget: budget
                    ),
                    container: container
                )
            )
            triples.append(contentsOf: page.triples)
            continuation = page.continuation
            if continuation == nil { break }
        }

        #expect(continuation == nil)
        #expect(triples.count == 3)
        #expect(Set(triples).count == 3)
        let describedIRI = try RDFIRI(Self.describedSubject)
        #expect(triples.allSatisfy { $0.subject == .iri(describedIRI) })
    }

    @Test("DESCRIBE scans a blank-node subject bound through a variable")
    func describeFixedBlankNode() async throws {
        let container = try await makeContainer()
        let query = DescribeQuery(
            selection: .resources(
                first: .variable("resource"),
                additional: []
            ),
            pattern: .basic([
                TriplePattern(
                    subject: .variable("resource"),
                    predicate: .iri("urn:blank-detail"),
                    object: .variable("detail")
                )
            ])
        )
        let page = try graphPage(
            try await execute(
                request(.describe(query), limit: 10),
                container: container
            )
        )

        #expect(page.triples.count == 1)
        let describedBlankNode = try RDFBlankNodeIdentifier(
            Self.describedBlankNode
        )
        #expect(
            page.triples[0].subject
                == .blankNode(describedBlankNode)
        )
    }

    @Test("Explicit DESCRIBE resources are independent of a zero solution limit")
    func describeExplicitResourceWithZeroLimit() async throws {
        let container = try await makeContainer()
        let query = DescribeQuery(
            selection: .resources(
                first: .iri(Self.describedSubject),
                additional: []
            ),
            pattern: sourcePattern,
            modifiers: SPARQLSolutionModifiers(limit: 0)
        )
        let page = try graphPage(
            try await execute(
                request(.describe(query), limit: 10),
                container: container
            )
        )

        #expect(page.triples.count == 3)
        let describedIRI = try RDFIRI(Self.describedSubject)
        #expect(page.triples.allSatisfy {
            $0.subject == .iri(describedIRI)
        })
    }

    @Test("DESCRIBE all exposes only variables visible from a subquery")
    func describeAllUsesVisibleVariables() async throws {
        let container = try await makeContainer()
        let subquery = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("subject")))
            ]),
            source: .graphPattern(sourcePattern)
        )
        let query = DescribeQuery(
            selection: .all,
            pattern: .subquery(subquery)
        )
        let page = try graphPage(
            try await execute(
                request(.describe(query), limit: 10),
                container: container
            )
        )

        let sourceOne = RDFSubject.iri(try RDFIRI("urn:source:1"))
        let sourceTwo = RDFSubject.iri(try RDFIRI("urn:source:2"))
        #expect(Set(page.triples.map(\.subject)) == [sourceOne, sourceTwo])
        let objectOne = RDFSubject.iri(try RDFIRI("urn:object:1"))
        let objectTwo = RDFSubject.iri(try RDFIRI("urn:object:2"))
        #expect(!page.triples.contains {
            $0.subject == objectOne || $0.subject == objectTwo
        })
    }

    @Test("continuations are bound to query kind and request")
    func continuationRejectsDifferentQuery() async throws {
        let container = try await makeContainer()
        let budget = executionBudget()
        let first = try graphPage(
            try await execute(
                request(.construct(constructQuery()), limit: 1, budget: budget),
                container: container
            )
        )
        let continuation = try #require(first.continuation)
        let changed = ConstructQuery(
            template: [
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:changed"),
                    object: .variable("object")
                )
            ],
            pattern: sourcePattern
        )

        await expectGraphError(.continuationDoesNotMatchRequest) {
            try await execute(
                request(
                    .construct(changed),
                    limit: 1,
                    continuation: continuation,
                    budget: budget
                ),
                container: container
            )
        }
        await expectGraphError(.invalidContinuation) {
            try await execute(
                request(
                    .describe(
                        DescribeQuery(
                            selection: .resources(
                                first: .iri(Self.describedSubject),
                                additional: []
                            )
                        )
                    ),
                    limit: 1,
                    continuation: continuation,
                    budget: budget
                ),
                container: container
            )
        }
        var corruptedBytes = continuation.copyBytes()
        corruptedBytes.append(0)
        let corruptedContinuation = ByteString(corruptedBytes)
        await expectGraphError(.invalidContinuation) {
            try await execute(
                request(
                    .construct(constructQuery()),
                    limit: 1,
                    continuation: corruptedContinuation,
                    budget: budget
                ),
                container: container
            )
        }
    }

    @Test("continuation rejects a changed snapshot")
    func continuationRejectsChangedSnapshot() async throws {
        let container = try await makeContainer()
        let budget = executionBudget()
        let first = try graphPage(
            try await execute(
                request(.construct(constructQuery()), limit: 1, budget: budget),
                container: container
            )
        )
        let continuation = try #require(first.continuation)
        let context = container.newContext()
        try context.insert(
            try statement(
                id: "source-3",
                subject: "urn:source:3",
                predicate: Self.sourcePredicate,
                object: "urn:object:3"
            )
        )
        try await context.save()

        await expectGraphError(.continuationSnapshotChanged) {
            try await execute(
                request(
                    .construct(constructQuery()),
                    limit: 1,
                    continuation: continuation,
                    budget: budget
                ),
                container: container
            )
        }
    }

    @Test("page and work limits fail before returning partial graphs")
    func graphLimitsAreEnforced() async throws {
        let container = try await makeContainer()
        await expectGraphError(.pageLimitExceedsMaximum) {
            try await execute(
                request(
                    .construct(constructQuery()),
                    limit: 3,
                    budget: ExecutionBudget(
                        maximumRows: 2,
                        maximumWorkUnits: 100,
                        timeoutMilliseconds: 1_000
                    )
                ),
                container: container
            )
        }
        do {
            _ = try await execute(
                request(
                    .construct(constructQuery()),
                    limit: 1,
                    budget: ExecutionBudget(
                        maximumRows: 10,
                        maximumWorkUnits: 1,
                        timeoutMilliseconds: 1_000
                    )
                ),
                container: container
            )
            Issue.record("Expected a work limit error")
        } catch is DatabaseWorkLimitError {
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("graph pages traverse the canonical endpoint envelope")
    func graphPageTraversesEndpoint() async throws {
        let container = try await makeContainer()
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(QueryExecuteHandler())],
            requiredOperations: [.queryExecute]
        )
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            )
        )
        let operationRequest = request(.construct(constructQuery()), limit: 2)
        let encoder = DatabaseWireEncoder()
        let decoder = DatabaseWireDecoder()
        let frame = try encoder.encodeRequest(
            DatabaseOperations.queryExecute,
            requestID: 77,
            metadata: OperationRequestMetadata(traceID: "graph-page"),
            request: operationRequest
        )

        let responseFrame = try await endpoint.execute(frame)
        let header = try decoder.decodeResponseHeader(responseFrame)
        let decoded = try decoder.decodeResponse(
            DatabaseOperations.queryExecute,
            from: responseFrame,
            matching: 77
        )
        guard case .success(let response) = decoded else {
            Issue.record("Expected a successful graph response")
            return
        }
        let page = try graphPage(response)

        #expect(header.requestID == 77)
        #expect(page.triples.count == 2)
        #expect(page.continuation != nil)
        #expect(page.snapshotVersion != nil)
    }

    @Test("cold SPARQL resolution uses one read-only caller transaction")
    func coldSPARQLResolutionUsesCallerTransaction() async throws {
        let engine = TransactionCountingInMemoryEngine()
        _ = try await makeContainer(engine: engine)
        let readContainer = try await makeEmptyContainer(engine: engine)
        let transactionCountBeforeRead = engine.transactionCount
        let keyCountBeforeRead = engine.keyCount

        let result = try boolean(
            try await execute(
                request(.ask(AskQuery(pattern: sourcePattern)), limit: 1),
                container: readContainer
            )
        )

        #expect(result)
        #expect(engine.transactionCount - transactionCountBeforeRead == 1)
        #expect(engine.keyCount == keyCountBeforeRead)
    }

    @Test("ASK rejects a continuation instead of silently ignoring it")
    func askRejectsContinuation() async throws {
        let container = try await makeContainer()
        let query = AskQuery(pattern: sourcePattern)
        do {
            _ = try await execute(
                request(
                    .ask(query),
                    limit: 1,
                    continuation: [1]
                ),
                container: container
            )
            Issue.record("Expected ASK to reject the continuation")
        } catch DatabaseQueryExecutionError.continuationNotSupported(let statement) {
            #expect(statement == "ASK")
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Explicit datasets control default and named graph visibility")
    func explicitDatasetExecution() async throws {
        let container = try await makeContainer()
        let dataset = SPARQLDataset.explicit(
            defaultGraphs: [Self.namedGraphOne, Self.namedGraphTwo],
            namedGraphs: []
        )
        let selectedRows = try rowPage(
            try await execute(
                request(
                    .select(
                        SelectQuery(
                            projection: .items([
                                ProjectionItem(.variable(Variable("subject"))),
                                ProjectionItem(.variable(Variable("object"))),
                            ]),
                            source: .graphPattern(sourcePattern),
                            dataset: dataset
                        )
                    ),
                    limit: 10
                ),
                container: container
            )
        )
        #expect(selectedRows.rows.count == 2)

        let construct = ConstructQuery(
            template: [
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:selected"),
                    object: .variable("object")
                )
            ],
            pattern: sourcePattern,
            dataset: dataset
        )
        let page = try graphPage(
            try await execute(
                request(.construct(construct), limit: 10),
                container: container
            )
        )
        #expect(page.triples.count == 2)
        let expectedNamedSubjects: Set<RDFSubject> = [
            .iri(try RDFIRI("urn:named:shared")),
            .iri(try RDFIRI("urn:named:unique")),
        ]
        #expect(Set(page.triples.map(\.subject)) == expectedNamedSubjects)

        let namedPattern = GraphPattern.graph(
            name: .iri(Self.namedGraphOne),
            pattern: sourcePattern
        )
        let hidden = AskQuery(
            pattern: namedPattern,
            dataset: .explicit(
                defaultGraphs: [],
                namedGraphs: [Self.namedGraphTwo]
            )
        )
        let visible = AskQuery(
            pattern: namedPattern,
            dataset: .explicit(
                defaultGraphs: [],
                namedGraphs: [Self.namedGraphOne]
            )
        )
        #expect(try boolean(
            try await execute(
                request(.ask(hidden), limit: 1),
                container: container
            )
        ) == false)
        #expect(try boolean(
            try await execute(
                request(.ask(visible), limit: 1),
                container: container
            )
        ) == true)

        let graphVariableQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("graph"))),
                ProjectionItem(.variable(Variable("subject"))),
            ]),
            source: .graphPattern(
                .graph(
                    name: .variable("graph"),
                    pattern: sourcePattern
                )
            ),
            dataset: .explicit(
                defaultGraphs: [],
                namedGraphs: [Self.namedGraphOne]
            )
        )
        let graphRows = try rowPage(
            try await execute(
                request(.select(graphVariableQuery), limit: 10),
                container: container
            )
        )
        #expect(graphRows.rows.count == 1)
        let graphValue = rowValue(
            named: "graph",
            in: graphRows.rows[0],
            columns: graphRows.columns
        )
        let expectedGraphValue = FieldValue.rdfTerm(
            try .iri(validating: Self.namedGraphOne)
        )
        #expect(
            graphValue == expectedGraphValue
        )

        let describedNamedResource = DescribeQuery(
            selection: .resources(
                first: .iri("urn:named:shared"),
                additional: []
            ),
            dataset: .explicit(
                defaultGraphs: [Self.namedGraphOne],
                namedGraphs: []
            )
        )
        let describedPage = try graphPage(
            try await execute(
                request(.describe(describedNamedResource), limit: 10),
                container: container
            )
        )
        #expect(describedPage.triples.count == 1)
        let describedNamedSubject = RDFSubject.iri(
            try RDFIRI("urn:named:shared")
        )
        #expect(
            describedPage.triples[0].subject
                == describedNamedSubject
        )
    }

    @Test("ASK and DESCRIBE execute their solution modifiers")
    func nonSelectSolutionModifiers() async throws {
        let container = try await makeContainer()
        let skippedAsk = AskQuery(
            pattern: sourcePattern,
            modifiers: SPARQLSolutionModifiers(offset: 2)
        )
        let zeroLimitAsk = AskQuery(
            pattern: sourcePattern,
            modifiers: SPARQLSolutionModifiers(limit: 0)
        )
        #expect(try boolean(
            try await execute(
                request(.ask(skippedAsk), limit: 1),
                container: container
            )
        ) == false)
        #expect(try boolean(
            try await execute(
                request(.ask(zeroLimitAsk), limit: 1),
                container: container
            )
        ) == false)

        let describe = DescribeQuery(
            selection: .all,
            pattern: sourcePattern,
            modifiers: SPARQLSolutionModifiers(
                orderBy: [
                    SortKey(.variable(Variable("subject")))
                ],
                limit: 1
            )
        )
        let page = try graphPage(
            try await execute(
                request(.describe(describe), limit: 10),
                container: container
            )
        )
        #expect(page.triples.count == 2)
        let expectedSubjects: Set<RDFSubject> = [
            .iri(try RDFIRI("urn:source:1")),
            .iri(try RDFIRI("urn:object:1")),
        ]
        #expect(Set(page.triples.map(\.subject)) == expectedSubjects)
    }

    @Test("ASK applies implicit and explicit grouping before existence")
    func askAppliesGroupingAndHaving() async throws {
        let container = try await makeContainer()
        let having = Expression.greaterThan(
            .aggregate(
                .count(
                    .variable(Variable("object")),
                    distinct: false
                )
            ),
            .literal(.int(1))
        )
        let implicitGroup = AskQuery(
            pattern: sourcePattern,
            modifiers: SPARQLSolutionModifiers(having: [having])
        )
        let explicitGroups = AskQuery(
            pattern: sourcePattern,
            modifiers: SPARQLSolutionModifiers(
                groupBy: [.variable(Variable("subject"))],
                having: [having]
            )
        )

        #expect(try boolean(
            try await execute(
                request(.ask(implicitGroup), limit: 1),
                container: container
            )
        ))
        #expect(try boolean(
            try await execute(
                request(.ask(explicitGroups), limit: 1),
                container: container
            )
        ) == false)
    }

    @Test("Text SPARQL executes a SubSelect through the canonical endpoint path")
    func textSPARQLExecutesSubSelect() async throws {
        let container = try await makeContainer()
        let response = try await execute(
            QueryExecuteOperation.Request(
                input: .text(
                    language: .sparql,
                    statement: """
                        SELECT ?subject WHERE {
                            {
                                SELECT ?subject ?object WHERE {
                                    ?subject <urn:source> ?object
                                }
                                ORDER BY ?subject
                                LIMIT 1
                            }
                        }
                        """
                ),
                page: QueryExecuteOperation.Page(limit: 10),
                budget: executionBudget()
            ),
            container: container
        )
        let page = try rowPage(response)

        #expect(page.rows.count == 1)
        let subject = rowValue(
            named: "subject",
            in: page.rows[0],
            columns: page.columns
        )
        let expectedSubject = FieldValue.rdfTerm(
            try .iri(validating: "urn:source:1")
        )
        #expect(subject == expectedSubject)
        #expect(
            rowValue(
                named: "object",
                in: page.rows[0],
                columns: page.columns
            ) == nil
        )
    }

    @Test("Text SPARQL LATERAL SubSelect receives each outer solution")
    func textSPARQLExecutesLateralSubSelect() async throws {
        let container = try await makeContainer()
        let response = try await execute(
            QueryExecuteOperation.Request(
                input: .text(
                    language: .sparql,
                    statement: """
                        SELECT ?subject ?object WHERE {
                            VALUES ?subject {
                                <urn:source:1>
                                <urn:source:2>
                            }
                            LATERAL {
                                SELECT ?subject ?object WHERE {
                                    ?subject <urn:source> ?object
                                }
                                LIMIT 1
                            }
                        }
                        ORDER BY ?subject
                        """
                ),
                page: QueryExecuteOperation.Page(limit: 10),
                budget: executionBudget()
            ),
            container: container
        )
        let page = try rowPage(response)

        #expect(page.rows.count == 2)
        let allRowsContainProjectedValues = page.rows.allSatisfy { row in
            rowValue(
                named: "subject",
                in: row,
                columns: page.columns
            ) != nil
                && rowValue(
                    named: "object",
                    in: row,
                    columns: page.columns
                ) != nil
        }
        #expect(allRowsContainProjectedValues)
    }

    private func makeContainer() async throws -> DBContainer {
        try await makeContainer(engine: InMemoryEngine())
    }

    private func makeContainer(
        engine: any StorageEngine
    ) async throws -> DBContainer {
        let container = try await makeEmptyContainer(engine: engine)
        let context = container.newContext()
        try context.insert(
            try statement(
                id: "source-1",
                subject: "urn:source:1",
                predicate: Self.sourcePredicate,
                object: "urn:object:1"
            )
        )
        try context.insert(
            try statement(
                id: "named-1",
                subject: "urn:named:shared",
                predicate: Self.sourcePredicate,
                object: "urn:named:object",
                graph: Self.namedGraphOne
            )
        )
        try context.insert(
            try statement(
                id: "named-duplicate",
                subject: "urn:named:shared",
                predicate: Self.sourcePredicate,
                object: "urn:named:object",
                graph: Self.namedGraphTwo
            )
        )
        try context.insert(
            try statement(
                id: "named-unique",
                subject: "urn:named:unique",
                predicate: Self.sourcePredicate,
                object: "urn:named:other",
                graph: Self.namedGraphTwo
            )
        )
        try context.insert(
            try statement(
                id: "source-2",
                subject: "urn:source:2",
                predicate: Self.sourcePredicate,
                object: "urn:object:2"
            )
        )
        try context.insert(
            try statement(
                id: "object-detail-1",
                subject: "urn:object:1",
                predicate: "urn:object-detail",
                object: "urn:detail:1"
            )
        )
        try context.insert(
            try statement(
                id: "object-detail-2",
                subject: "urn:object:2",
                predicate: "urn:object-detail",
                object: "urn:detail:2"
            )
        )
        var blankNodeStatement = try statement(
            id: "blank-node-detail",
            subject: "urn:placeholder",
            predicate: "urn:blank-detail",
            object: "urn:blank-object"
        )
        blankNodeStatement.subject = .blankNode(
            try RDFBlankNodeIdentifier(Self.describedBlankNode)
        )
        try context.insert(blankNodeStatement)
        for index in 1...3 {
            try context.insert(
                try statement(
                    id: "describe-\(index)",
                    subject: Self.describedSubject,
                    predicate: "urn:describe:\(index)",
                    object: "urn:described-object:\(index)"
                )
            )
        }
        try await context.save()
        return container
    }

    private func makeEmptyContainer(
        engine: any StorageEngine
    ) async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseGraphQueryStatement.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseGraphQueryStatement.self]
            ),
            security: .disabled
        )
    }

    private func statement(
        id: String,
        subject: String,
        predicate: String,
        object: String,
        graph: String? = nil
    ) throws -> DatabaseGraphQueryStatement {
        DatabaseGraphQueryStatement(
            id: id,
            subject: try .iri(validating: subject),
            predicate: try .iri(validating: predicate),
            object: try .iri(validating: object),
            graph: try graph.map { try .iri(validating: $0) }
        )
    }

    private func constructQuery() -> ConstructQuery {
        ConstructQuery(
            template: [
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:derived:a"),
                    object: .variable("object")
                ),
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:derived:b"),
                    object: .variable("object")
                ),
            ],
            pattern: sourcePattern
        )
    }

    private var sourcePattern: GraphPattern {
        .basic([
            TriplePattern(
                subject: .variable("subject"),
                predicate: .iri(Self.sourcePredicate),
                object: .variable("object")
            )
        ])
    }

    private func request(
        _ statement: QueryStatement,
        limit: UInt32,
        continuation: ByteString? = nil,
        budget: ExecutionBudget? = nil
    ) -> QueryExecuteOperation.Request {
        QueryExecuteOperation.Request(
            input: .ir(statement),
            page: QueryExecuteOperation.Page(
                limit: limit,
                continuation: continuation
            ),
            budget: budget ?? executionBudget()
        )
    }

    private func executionBudget() -> ExecutionBudget {
        ExecutionBudget(
            maximumRows: 100,
            maximumWorkUnits: 10_000,
            timeoutMilliseconds: 1_000
        )
    }

    private func execute(
        _ request: QueryExecuteOperation.Request,
        container: DBContainer
    ) async throws -> QueryExecuteOperation.Response {
        try await QueryExecuteHandler().handle(
            request,
            context: DatabaseOperationContext(
                container: container,
                requestID: 1,
                metadata: OperationRequestMetadata(),
                requestPayload: try DatabaseWireEncoder().encodeRequestPayload(
                    DatabaseOperations.queryExecute,
                    request: request
                )
            )
        )
    }

    private func graphPage(
        _ response: QueryExecuteOperation.Response
    ) throws -> MaterializedGraphPage {
        guard case .rdfGraph(let page) = response else {
            throw GraphQueryResponseAssertionError.expectedGraphPage
        }
        return MaterializedGraphPage(
            triples: try page.materializedQuads(
                maximumCount: page.quadCount
            ),
            continuation: page.continuation,
            snapshotVersion: page.snapshotVersion
        )
    }

    private func rowPage(
        _ response: QueryExecuteOperation.Response
    ) throws -> MaterializedRowPage {
        guard case .rows(let page) = response else {
            throw GraphQueryResponseAssertionError.expectedRowPage
        }
        return MaterializedRowPage(
            columns: page.columns,
            rows: try page.materializedRows(
                maximumCount: page.rowCount
            ),
            continuation: page.continuation,
            snapshotVersion: page.snapshotVersion
        )
    }

    private func rowValue(
        named name: String,
        in row: DatabaseWire.QueryRow,
        columns: [QueryColumn]
    ) -> FieldValue? {
        guard let index = columns.firstIndex(where: { $0.name == name }),
              row.values.indices.contains(index) else {
            return nil
        }
        return row.values[index]
    }

    private struct MaterializedGraphPage {
        let triples: [RDFQuad]
        let continuation: ByteString?
        let snapshotVersion: Int64?
    }

    private struct MaterializedRowPage {
        let columns: [QueryColumn]
        let rows: [DatabaseWire.QueryRow]
        let continuation: ByteString?
        let snapshotVersion: UInt64?
    }

    private func boolean(
        _ response: QueryExecuteOperation.Response
    ) throws -> Bool {
        guard case .boolean(let value) = response else {
            throw GraphQueryResponseAssertionError.expectedBoolean
        }
        return value
    }

    private static let namedGraphOne = "urn:graph:one"
    private static let namedGraphTwo = "urn:graph:two"

    private func expectGraphError(
        _ expected: ExpectedGraphError,
        operation: () async throws -> QueryExecuteOperation.Response
    ) async {
        do {
            _ = try await operation()
            Issue.record("Expected graph query error \(expected)")
        } catch let error as DatabaseGraphQueryError {
            #expect(expected.matches(error))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    private enum ExpectedGraphError: CustomStringConvertible {
        case invalidContinuation
        case continuationDoesNotMatchRequest
        case continuationSnapshotChanged
        case pageLimitExceedsMaximum

        var description: String {
            switch self {
            case .invalidContinuation: "invalidContinuation"
            case .continuationDoesNotMatchRequest: "continuationDoesNotMatchRequest"
            case .continuationSnapshotChanged: "continuationSnapshotChanged"
            case .pageLimitExceedsMaximum: "pageLimitExceedsMaximum"
            }
        }

        func matches(_ error: DatabaseGraphQueryError) -> Bool {
            switch (self, error) {
            case (.invalidContinuation, .invalidContinuation),
                 (.continuationDoesNotMatchRequest, .continuationDoesNotMatchRequest),
                 (.continuationSnapshotChanged, .continuationSnapshotChanged),
                 (.pageLimitExceedsMaximum, .pageLimitExceedsMaximum):
                true
            default:
                false
            }
        }
    }

    private enum GraphQueryResponseAssertionError: Error {
        case expectedGraphPage
        case expectedRowPage
        case expectedBoolean
    }

    private static let sourcePredicate = "urn:source"
    private static let describedSubject = "urn:described"
    private static let describedBlankNode = "described-blank"
    private static let reifiesPredicate =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
}
