import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import DatabaseTypes
import DatabaseWire
import GraphIndex
import StorageKit
import Synchronization
import Testing
@testable import DatabaseServer

@Suite("SPARQL statement mutation executor")
struct SPARQLStatementMutationExecutorTests {
    @Test("Configured structural limits reach mutation graph compilation")
    func configuredStructuralLimitsReachMutationCompiler() async throws {
        let container = try await makeContainer()
        var expression = Expression.literal(.bool(true))
        for _ in 0..<63 {
            expression = .not(expression)
        }
        let pattern = GraphPattern.filter(.basic([]), expression)
        #expect(
            throws: SPARQLSemanticValidationError.structural(
                .resourceLimitExceeded(
                    resource: .nestingDepth,
                    actual: 65,
                    maximum: 64
                )
            )
        ) {
            try SPARQLSemanticValidator.validate(pattern)
        }
        let structuralLimits = QueryStructuralLimits(maximumNestingDepth: 67)
        try SPARQLSemanticValidator.validate(
            pattern,
            limits: structuralLimits
        )
        // Admission evidence, rather than executor construction defaults,
        // must govern every downstream compiler in this prepared request.
        let executor = CanonicalDatabaseStatementMutationExecutor()

        let effect = try await executeRDF(
            .modify(
                SPARQLModifyOperation(
                    action: .delete([]),
                    wherePattern: pattern
                )
            ),
            executor: executor,
            context: context(
                container,
                idempotencyKey: "configured-structural-limits"
            ),
            structuralLimits: structuralLimits
        )
        #expect(effect.deletedQuads == 0)
        #expect(effect.insertedQuads == 0)
    }

    @Test("INSERT DATA and DELETE DATA mutate the canonical RDF store")
    func groundDataLifecycle() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let graphIRI = "https://example.test/graphs/events"
        let defaultQuad = Quad(
            triple: TriplePattern(
                subject: .iri("https://example.test/events/1"),
                predicate: .iri("https://example.test/title"),
                object: .literal(.string("Runtime"))
            )
        )
        let namedQuad = Quad(
            graph: .iri(graphIRI),
            triple: TriplePattern(
                subject: .iri("https://example.test/events/2"),
                predicate: .iri("https://example.test/title"),
                object: .literal(.string("Graph"))
            )
        )

        let inserted = try await executeRDF(
            .insertData(InsertDataQuery(quads: [defaultQuad, namedQuad])),
            executor: executor,
            context: context(container, idempotencyKey: "insert-ground")
        )
        #expect(inserted.insertedQuads == 2)
        #expect(inserted.deletedQuads == 0)

        let store = CanonicalRDFGraphStore()
        let defaultRows = try await scan(
            store,
            scope: .defaultGraph,
            container: container
        )
        let namedRows = try await scan(
            store,
            scope: .named(try RDFGraphName(iri: graphIRI)),
            container: container
        )
        #expect(defaultRows.count == 1)
        #expect(namedRows.count == 1)

        let deleted = try await executeRDF(
            .deleteData(DeleteDataQuery(quads: [namedQuad])),
            executor: executor,
            context: context(container, idempotencyKey: "delete-ground")
        )
        #expect(deleted.deletedQuads == 1)
        #expect(try await scan(
            store,
            scope: .named(try RDFGraphName(iri: graphIRI)),
            container: container
        ).isEmpty)
    }

    @Test("DELETE INSERT WHERE reads and writes in one transaction")
    func deleteInsertWhereLifecycle() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let subject = "https://example.test/events/replace"
        let predicate = "https://example.test/status"
        let original = Quad(
            triple: TriplePattern(
                subject: .iri(subject),
                predicate: .iri(predicate),
                object: .literal(.string("old"))
            )
        )
        _ = try await executeRDF(
            .insertData(InsertDataQuery(quads: [original])),
            executor: executor,
            context: context(container, idempotencyKey: "modify-seed")
        )

        let match = TriplePattern(
            subject: .variable("subject"),
            predicate: .iri(predicate),
            object: .variable("old")
        )
        let effect = try await executeRDF(
            .modify(
                SPARQLModifyOperation(
                    action: .deleteAndInsert(
                        delete: [Quad(triple: match)],
                        insert: [
                        Quad(
                            triple: TriplePattern(
                                subject: .variable("subject"),
                                predicate: .iri(predicate),
                                object: .literal(.string("new"))
                            )
                        ),
                    ]),
                    wherePattern: .basic([match])
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "modify-update")
        )
        #expect(effect.deletedQuads == 1)
        #expect(effect.insertedQuads == 1)

        let rows = try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        )
        #expect(rows.count == 1)
        #expect(rows[0].subject == .iri(try RDFIRI(subject)))
        guard case .literal(let literal) = rows[0].object else {
            Issue.record("Expected the replacement RDF literal")
            return
        }
        #expect(literal.lexicalForm == "new")
    }

    @Test("An explicitly created empty graph is visible to GRAPH variable evaluation")
    func emptyGraphCatalogIsQueryable() async throws {
        let container = try await makeContainer()
        let graphIRI = "https://example.test/graphs/empty"
        let effect = try await executeRDF(
            .createGraph(
                CreateSPARQLGraphQuery(graph: graphIRI, silent: false)
            ),
            executor: CanonicalDatabaseStatementMutationExecutor(),
            context: context(container, idempotencyKey: "create-empty")
        )
        #expect(effect.createdGraphs == 1)

        guard let queryExecutor = container.runtimeConfiguration
            .logicalSourceExecutors.sparqlExecutor else {
            Issue.record("Expected the SPARQL runtime executor")
            return
        }
        let response = try await queryExecutor.execute(
            context: container.newContext(),
            selectQuery: SelectQuery(
                projection: .all,
                source: .graphPattern(
                    .graph(
                        name: .variable("graph"),
                        pattern: .basic([])
                    )
                )
            ),
            options: ReadExecutionContext(),
            partitions: FieldObject()
        )
        #expect(response.continuation == nil)
        #expect(response.rows.count == 1)
        #expect(
            response.rows[0].fields["graph"]
                == .rdfTerm(try RDFTerm.iri(validating: graphIRI))
        )
    }

    @Test("A mutation limit failure rolls back earlier RDF writes")
    func mutationLimitRollsBack() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor(
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10,
                maximumWorkUnits: 10_000,
                maximumTimeoutMilliseconds: 30_000,
                maximumMutations: 1
            )
        )
        let quads = ["1", "2"].map { suffix in
            Quad(
                triple: TriplePattern(
                    subject: .iri("https://example.test/events/\(suffix)"),
                    predicate: .iri("https://example.test/value"),
                    object: .literal(.string(suffix))
                )
            )
        }
        do {
            _ = try await executeRDF(
                .insertData(InsertDataQuery(quads: quads)),
                executor: executor,
                context: context(container, idempotencyKey: "limited-insert")
            )
            Issue.record("Expected the mutation limit to reject the update")
        } catch DatabaseMutationError.mutationLimitExceeded(
            let actual,
            let maximum
        ) {
            #expect(actual == 2)
            #expect(maximum == 1)
        }

        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        ).isEmpty)
    }

    @Test("INSERT DATA blank node labels are scoped to one idempotent update")
    func blankNodeScopeIsStableAndFresh() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let predicate = "https://example.test/value"
        func quad(_ value: String) -> Quad {
            Quad(
                triple: TriplePattern(
                    subject: .blankNode("event"),
                    predicate: .iri(predicate),
                    object: .literal(.string(value))
                )
            )
        }

        _ = try await executeRDF(
            .insertData(InsertDataQuery(quads: [quad("a"), quad("b")])),
            executor: executor,
            context: context(container, idempotencyKey: "blank-scope-a")
        )
        _ = try await executeRDF(
            .insertData(InsertDataQuery(quads: [quad("c")])),
            executor: executor,
            context: context(container, idempotencyKey: "blank-scope-b")
        )

        let rows = try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        )
        let subjects = Set(rows.map(\.subject))
        #expect(rows.count == 3)
        #expect(subjects.count == 2)
    }

    @Test("DELETE WHERE uses one quad pattern for matching and deletion")
    func deleteWhereUsesCanonicalPattern() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let predicate = "https://example.test/status"
        let oldQuad = Quad(
            triple: TriplePattern(
                subject: .iri("https://example.test/events/old"),
                predicate: .iri(predicate),
                object: .literal(.string("old"))
            )
        )
        let currentQuad = Quad(
            triple: TriplePattern(
                subject: .iri("https://example.test/events/current"),
                predicate: .iri(predicate),
                object: .literal(.string("current"))
            )
        )
        _ = try await executeRDF(
            .insertData(InsertDataQuery(quads: [oldQuad, currentQuad])),
            executor: executor,
            context: context(container, idempotencyKey: "delete-where-seed")
        )

        let effect = try await executeRDF(
            .deleteWhere(
                DeleteWhereQuery(
                    pattern: [
                        Quad(
                            triple: TriplePattern(
                                subject: .variable("subject"),
                                predicate: .iri(predicate),
                                object: .literal(.string("old"))
                            )
                        ),
                    ]
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "delete-where")
        )

        #expect(effect.deletedQuads == 1)
        let rows = try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        )
        #expect(rows.count == 1)
        #expect(
            rows[0].subject
                == .iri(try RDFIRI("https://example.test/events/current"))
        )
    }

    @Test("WITH controls templates while USING controls the WHERE dataset")
    func withAndUsingHaveDistinctRoles() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let target = "https://example.test/graphs/target"
        let source = "https://example.test/graphs/source"
        let subject = "https://example.test/events/with-using"
        let predicate = "https://example.test/status"
        let targetQuad = Quad(
            graph: .iri(target),
            triple: TriplePattern(
                subject: .iri(subject),
                predicate: .iri(predicate),
                object: .literal(.string("old"))
            )
        )
        let sourceQuad = Quad(
            graph: .iri(source),
            triple: TriplePattern(
                subject: .iri(subject),
                predicate: .iri(predicate),
                object: .literal(.string("new"))
            )
        )
        _ = try await executeRDF(
            .insertData(InsertDataQuery(quads: [targetQuad, sourceQuad])),
            executor: executor,
            context: context(container, idempotencyKey: "with-using-seed")
        )

        let effect = try await executeRDF(
            .modify(
                SPARQLModifyOperation(
                    withGraph: target,
                    action: .deleteAndInsert(
                        delete: [
                            Quad(
                                triple: TriplePattern(
                                    subject: .iri(subject),
                                    predicate: .iri(predicate),
                                    object: .literal(.string("old"))
                                )
                            ),
                        ],
                        insert: [
                            Quad(
                                triple: TriplePattern(
                                    subject: .iri(subject),
                                    predicate: .iri(predicate),
                                    object: .variable("value")
                                )
                            ),
                        ]
                    ),
                    using: [GraphRef(iri: source)],
                    wherePattern: .basic([
                        TriplePattern(
                            subject: .iri(subject),
                            predicate: .iri(predicate),
                            object: .variable("value")
                        ),
                    ])
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "with-using")
        )

        #expect(effect.deletedQuads == 1)
        #expect(effect.insertedQuads == 1)
        let targetRows = try await scan(
            CanonicalRDFGraphStore(),
            scope: .named(try RDFGraphName(iri: target)),
            container: container
        )
        let sourceRows = try await scan(
            CanonicalRDFGraphStore(),
            scope: .named(try RDFGraphName(iri: source)),
            container: container
        )
        #expect(targetRows.count == 1)
        #expect(sourceRows.count == 1)
        #expect(targetRows[0].object == sourceRows[0].object)
    }

    @Test("CLEAR NAMED preserves graph identity and DROP NAMED removes it")
    func clearAndDropNamedGraphSet() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let emptyGraph = "https://example.test/graphs/empty-drop"
        let populatedGraph = "https://example.test/graphs/populated-drop"
        _ = try await executeRDF(
            .createGraph(
                CreateSPARQLGraphQuery(graph: emptyGraph, silent: false)
            ),
            executor: executor,
            context: context(container, idempotencyKey: "clear-drop-create")
        )
        _ = try await executeRDF(
            .insertData(
                InsertDataQuery(
                    quads: [
                        Quad(
                            graph: .iri(populatedGraph),
                            triple: TriplePattern(
                                subject: .iri("https://example.test/events/drop"),
                                predicate: .iri("https://example.test/value"),
                                object: .literal(.string("value"))
                            )
                        ),
                    ]
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "clear-drop-seed")
        )

        let cleared = try await executeRDF(
            .clear(ClearQuery(target: .named)),
            executor: executor,
            context: context(container, idempotencyKey: "clear-named")
        )
        #expect(cleared.deletedQuads == 1)
        #expect(cleared.droppedGraphs == 0)
        #expect(try await containsGraph(
            emptyGraph,
            container: container
        ))
        #expect(try await containsGraph(
            populatedGraph,
            container: container
        ))

        let dropped = try await executeRDF(
            .drop(DropQuery(target: .named)),
            executor: executor,
            context: context(container, idempotencyKey: "drop-named")
        )
        #expect(dropped.deletedQuads == 0)
        #expect(dropped.droppedGraphs == 2)
        #expect(try await !containsGraph(
            emptyGraph,
            container: container
        ))
        #expect(try await !containsGraph(
            populatedGraph,
            container: container
        ))
    }

    @Test("COPY and MOVE preserve graph-store semantics including empty graphs")
    func graphTransferLifecycle() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let source = "https://example.test/graphs/transfer-source"
        let destination = "https://example.test/graphs/transfer-destination"
        let emptySource = "https://example.test/graphs/empty-source"
        let emptyDestination = "https://example.test/graphs/empty-destination"
        _ = try await executeRDF(
            .insertData(
                InsertDataQuery(
                    quads: [
                        Quad(
                            graph: .iri(source),
                            triple: TriplePattern(
                                subject: .iri("https://example.test/events/source"),
                                predicate: .iri("https://example.test/value"),
                                object: .literal(.string("source"))
                            )
                        ),
                        Quad(
                            graph: .iri(destination),
                            triple: TriplePattern(
                                subject: .iri("https://example.test/events/old"),
                                predicate: .iri("https://example.test/value"),
                                object: .literal(.string("old"))
                            )
                        ),
                    ]
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "transfer-seed")
        )
        _ = try await executeRDF(
            .createGraph(
                CreateSPARQLGraphQuery(graph: emptySource, silent: false)
            ),
            executor: executor,
            context: context(container, idempotencyKey: "empty-transfer-seed")
        )

        let copied = try await executeRDF(
            .graphTransfer(
                GraphTransferQuery(
                    operation: .copy,
                    source: .graph(source),
                    destination: .graph(destination)
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "copy-graph")
        )
        #expect(copied.insertedQuads == 1)
        #expect(copied.deletedQuads == 1)
        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .named(try RDFGraphName(iri: source)),
            container: container
        ).count == 1)
        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .named(try RDFGraphName(iri: destination)),
            container: container
        ).count == 1)

        let moved = try await executeRDF(
            .graphTransfer(
                GraphTransferQuery(
                    operation: .move,
                    source: .graph(source),
                    destination: .default
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "move-graph")
        )
        #expect(moved.insertedQuads == 1)
        #expect(moved.deletedQuads == 1)
        #expect(moved.droppedGraphs == 1)
        #expect(try await !containsGraph(source, container: container))
        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        ).count == 1)

        let emptyCopy = try await executeRDF(
            .graphTransfer(
                GraphTransferQuery(
                    operation: .copy,
                    source: .graph(emptySource),
                    destination: .graph(emptyDestination)
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "copy-empty-graph")
        )
        #expect(emptyCopy.insertedQuads == 0)
        #expect(emptyCopy.createdGraphs == 1)
        #expect(try await containsGraph(
            emptyDestination,
            container: container
        ))
    }

    @Test("Graph transfer retains its scan owner through destination writes")
    func graphTransferRetainsScanOwner() async throws {
        let container = try await makeContainer()
        let store = TrackingRDFGraphMutationStore()
        let executor = CanonicalDatabaseStatementMutationExecutor(
            graphStore: store
        )
        let source = "https://example.test/graphs/owned-transfer-source"
        let destination =
            "https://example.test/graphs/owned-transfer-destination"
        _ = try await executeRDF(
            .insertData(
                InsertDataQuery(
                    quads: [
                        Quad(
                            graph: .iri(source),
                            triple: TriplePattern(
                                subject: .iri(
                                    "https://example.test/events/owned-source"
                                ),
                                predicate: .iri(
                                    "https://example.test/value"
                                ),
                                object: .literal(.string("source"))
                            )
                        ),
                    ]
                )
            ),
            executor: executor,
            context: context(
                container,
                idempotencyKey: "owned-transfer-seed"
            )
        )
        let seedInsertCount = store.insertRetainedIntermediateRows.count
        let seedScanCount = store.scanRetainedIntermediateRows.count

        let copied = try await executeRDF(
            .graphTransfer(
                GraphTransferQuery(
                    operation: .copy,
                    source: .graph(source),
                    destination: .graph(destination)
                )
            ),
            executor: executor,
            context: context(
                container,
                idempotencyKey: "owned-transfer-copy"
            )
        )

        #expect(copied.insertedQuads == 1)
        let transferRows = store.insertRetainedIntermediateRows.dropFirst(
            seedInsertCount
        )
        let scanRows = store.scanRetainedIntermediateRows.dropFirst(
            seedScanCount
        )
        #expect(transferRows.count == 1)
        #expect(scanRows.count == 1)
        #expect(scanRows.allSatisfy { $0 > 0 })
        #expect(zip(transferRows, scanRows).allSatisfy { pair in
            pair.0 >= pair.1
        })
    }

    @Test("An update sequence executes in order inside one transaction")
    func updateSequenceExecutesInOrder() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor()
        let predicate = "https://example.test/value"
        let first = Quad(
            triple: TriplePattern(
                subject: .iri("https://example.test/events/first"),
                predicate: .iri(predicate),
                object: .literal(.string("first"))
            )
        )
        let second = Quad(
            triple: TriplePattern(
                subject: .iri("https://example.test/events/second"),
                predicate: .iri(predicate),
                object: .literal(.string("second"))
            )
        )

        let effect = try await executeRDF(
            SPARQLUpdateRequest(
                firstOperation: .insertData(InsertDataQuery(quads: [first])),
                additionalOperations: [
                    .deleteData(DeleteDataQuery(quads: [first])),
                    .insertData(InsertDataQuery(quads: [second])),
                ]
            ),
            executor: executor,
            context: context(container, idempotencyKey: "ordered-sequence")
        )

        #expect(effect.insertedQuads == 2)
        #expect(effect.deletedQuads == 1)
        let rows = try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        )
        #expect(rows.count == 1)
        #expect(
            rows[0].subject ==
                .iri(try RDFIRI("https://example.test/events/second"))
        )
    }

    @Test("A request-wide mutation limit rolls back every prior operation")
    func updateSequenceSharesMutationLimitAndRollsBack() async throws {
        let container = try await makeContainer()
        let executor = CanonicalDatabaseStatementMutationExecutor(
            runtimeLimits: try DatabaseRuntimeLimits(
                maximumRows: 10,
                maximumWorkUnits: 10_000,
                maximumTimeoutMilliseconds: 30_000,
                maximumMutations: 1
            )
        )
        func quad(_ suffix: String) -> Quad {
            Quad(
                triple: TriplePattern(
                    subject: .iri("https://example.test/events/\(suffix)"),
                    predicate: .iri("https://example.test/value"),
                    object: .literal(.string(suffix))
                )
            )
        }

        do {
            _ = try await executeRDF(
                SPARQLUpdateRequest(
                    firstOperation: .insertData(
                        InsertDataQuery(quads: [quad("first")])
                    ),
                    additionalOperations: [
                        .insertData(
                            InsertDataQuery(quads: [quad("second")])
                        ),
                    ]
                ),
                executor: executor,
                context: context(container, idempotencyKey: "limited-sequence")
            )
            Issue.record("Expected the shared mutation limit to fail")
        } catch DatabaseMutationError.mutationLimitExceeded(
            let actual,
            let maximum
        ) {
            #expect(actual == 2)
            #expect(maximum == 1)
        }

        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        ).isEmpty)
    }

    @Test("Direct QueryIR rejects a blank node label reused by INSERT DATA operations")
    func updateSequenceRejectsBlankNodeLabelReuse() async throws {
        let container = try await makeContainer()
        let quad = Quad(
            triple: TriplePattern(
                subject: .blankNode("event"),
                predicate: .iri("https://example.test/value"),
                object: .literal(.string("value"))
            )
        )

        do {
            _ = try await executeRDF(
                SPARQLUpdateRequest(
                    firstOperation: .insertData(InsertDataQuery(quads: [quad])),
                    additionalOperations: [
                        .insertData(InsertDataQuery(quads: [quad])),
                    ]
                ),
                executor: CanonicalDatabaseStatementMutationExecutor(),
                context: context(container, idempotencyKey: "blank-sequence")
            )
            Issue.record("Expected request-level blank node validation to fail")
        } catch let error as SPARQLSemanticValidationError {
            #expect(error == .labelCrossesInsertDataOperations("event"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        let rows = try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        )
        #expect(rows.isEmpty)
    }

    @Test("Request-level DELETE validation runs before external LOAD preparation")
    func deleteValidationPrecedesLoadPreparation() async throws {
        let container = try await makeContainer()
        let source = RecordingSPARQLLoadSource { _ in
            SPARQLLoadDocument(byteCount: 0, triples: [])
        }
        let executor = CanonicalDatabaseStatementMutationExecutor(
            loadSource: AnySPARQLLoadSource(source)
        )
        let invalidDelete = Quad(
            triple: TriplePattern(
                subject: .blankNode("deleted"),
                predicate: .iri("https://example.test/value"),
                object: .literal(.string("value"))
            )
        )

        do {
            _ = try await executeRDF(
                SPARQLUpdateRequest(
                    firstOperation: .load(
                        LoadQuery(source: "https://source.test/data")
                    ),
                    additionalOperations: [
                        .deleteData(DeleteDataQuery(quads: [invalidDelete])),
                    ]
                ),
                executor: executor,
                context: context(container, idempotencyKey: "validate-before-load")
            )
            Issue.record("Expected request-level DELETE validation to fail")
        } catch let error as SPARQLSemanticValidationError {
            #expect(
                error == .blankNodeNotAllowed(
                    context: .deleteData,
                    label: "deleted"
                )
            )
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(source.callCount == 0)
        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        ).isEmpty)
    }

    @Test("LOAD prepares once and commits its document atomically")
    func loadDocumentLifecycle() async throws {
        let container = try await makeContainer()
        let graph = "https://example.test/graphs/load"
        let source = RecordingSPARQLLoadSource { _ in
            SPARQLLoadDocument(
                byteCount: 128,
                triples: [
                    RDFTriple(
                        subject: .iri(
                            try RDFIRI("https://example.test/events/load")
                        ),
                        predicate: try RDFPredicateIRI(
                            "https://example.test/value"
                        ),
                        object: .literal(.string("loaded"))
                    ),
                ]
            )
        }
        let executor = CanonicalDatabaseStatementMutationExecutor(
            loadSource: AnySPARQLLoadSource(source)
        )

        let effect = try await executeRDF(
            .load(LoadQuery(source: "https://source.test/data", destination: graph)),
            executor: executor,
            context: context(container, idempotencyKey: "load-document")
        )

        #expect(source.callCount == 1)
        #expect(effect.insertedQuads == 1)
        #expect(effect.createdGraphs == 1)
        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .named(try RDFGraphName(iri: graph)),
            container: container
        ).count == 1)
    }

    @Test("LOAD preserves an explicitly created empty destination graph")
    func loadEmptyDocumentCreatesDestinationGraph() async throws {
        let container = try await makeContainer()
        let graph = "https://example.test/graphs/empty-load"
        let source = RecordingSPARQLLoadSource { _ in
            SPARQLLoadDocument(byteCount: 0, triples: [])
        }

        let effect = try await executeRDF(
            .load(LoadQuery(source: "https://source.test/empty", destination: graph)),
            executor: CanonicalDatabaseStatementMutationExecutor(
                loadSource: AnySPARQLLoadSource(source)
            ),
            context: context(container, idempotencyKey: "load-empty")
        )

        #expect(effect.insertedQuads == 0)
        #expect(effect.createdGraphs == 1)
        #expect(try await containsGraph(graph, container: container))
    }

    @Test("LOAD SILENT suppresses source failures but never authorization failures")
    func loadSilentBoundary() async throws {
        let container = try await makeContainer()
        let missingSource = RecordingSPARQLLoadSource { request in
            throw SPARQLLoadSourceError.sourceNotFound(request.sourceIRI)
        }
        let silentEffect = try await executeRDF(
            .load(
                LoadQuery(
                    source: "https://source.test/missing",
                    silent: true
                )
            ),
            executor: CanonicalDatabaseStatementMutationExecutor(
                loadSource: AnySPARQLLoadSource(missingSource)
            ),
            context: context(container, idempotencyKey: "load-silent-missing")
        )
        #expect(silentEffect == MutationExecuteOperation.RDFEffect())

        let deniedSource = RecordingSPARQLLoadSource { request in
            throw SPARQLLoadSourceError.accessDenied(request.sourceIRI)
        }
        do {
            _ = try await executeRDF(
                .load(
                    LoadQuery(
                        source: "https://source.test/private",
                        silent: true
                    )
                ),
                executor: CanonicalDatabaseStatementMutationExecutor(
                    loadSource: AnySPARQLLoadSource(deniedSource)
                ),
                context: context(container, idempotencyKey: "load-silent-denied")
            )
            Issue.record("Expected authorization failure to remain visible")
        } catch SPARQLLoadSourceError.accessDenied(let sourceIRI) {
            #expect(sourceIRI == "https://source.test/private")
        }
    }

    @Test("LOAD validates byte and triple limits before opening a mutation transaction")
    func loadResourceLimits() async throws {
        let container = try await makeContainer()
        let oversized = RecordingSPARQLLoadSource { _ in
            SPARQLLoadDocument(byteCount: 17, triples: [])
        }
        let runtimeLimits = try DatabaseRuntimeLimits(
            maximumRows: 10,
            maximumWorkUnits: 10_000,
            maximumTimeoutMilliseconds: 30_000,
            maximumMutations: 1,
            maximumLoadDocumentBytes: 16
        )
        do {
            _ = try await executeRDF(
                .load(LoadQuery(source: "https://source.test/oversized")),
                executor: CanonicalDatabaseStatementMutationExecutor(
                    runtimeLimits: runtimeLimits,
                    loadSource: AnySPARQLLoadSource(oversized)
                ),
                context: context(container, idempotencyKey: "load-oversized")
            )
            Issue.record("Expected the LOAD byte limit to fail")
        } catch SPARQLLoadSourceError.documentTooLarge(
            let actual,
            let maximum
        ) {
            #expect(actual == 17)
            #expect(maximum == 16)
        }

        let tooManyTriples = RecordingSPARQLLoadSource { _ in
            let triple = try RDFTriple(
                subject: .iri(
                    RDFIRI("https://example.test/events/limit")
                ),
                predicate: RDFPredicateIRI(
                    "https://example.test/value"
                ),
                object: .literal(.string("value"))
            )
            return SPARQLLoadDocument(
                byteCount: 16,
                triples: [triple, triple]
            )
        }
        do {
            _ = try await executeRDF(
                .load(LoadQuery(source: "https://source.test/triples")),
                executor: CanonicalDatabaseStatementMutationExecutor(
                    runtimeLimits: runtimeLimits,
                    loadSource: AnySPARQLLoadSource(tooManyTriples)
                ),
                context: context(container, idempotencyKey: "load-triples")
            )
            Issue.record("Expected the LOAD triple limit to fail")
        } catch SPARQLLoadSourceError.tripleLimitExceeded(
            let actual,
            let maximum
        ) {
            #expect(actual == 2)
            #expect(maximum == 1)
        }

        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        ).isEmpty)
    }

    @Test("An idempotent endpoint replay does not fetch a LOAD source again")
    func loadEndpointReplaySkipsSourceFetch() async throws {
        let container = try await makeContainer()
        let source = RecordingSPARQLLoadSource { _ in
            SPARQLLoadDocument(
                byteCount: 32,
                triples: [
                    RDFTriple(
                        subject: .iri(
                            try RDFIRI("https://example.test/events/replay")
                        ),
                        predicate: try RDFPredicateIRI(
                            "https://example.test/value"
                        ),
                        object: .literal(.string("value"))
                    ),
                ]
            )
        }
        let handler = MutationExecuteHandler(
            stateStore: try await DatabaseMutationStateStore(container: container),
            statementExecutor: AnyDatabaseStatementMutationExecutor(
                CanonicalDatabaseStatementMutationExecutor(
                    loadSource: AnySPARQLLoadSource(source)
                )
            )
        )
        let registry = try DatabaseOperationRegistry(
            handlers: [AnyDatabaseOperationHandler(handler)],
            requiredOperations: [.mutationExecute]
        )
        let endpoint = DatabaseEndpoint(
            container: container,
            registry: registry,
            admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                UnrestrictedDatabaseOperationAdmissionPolicy()
            )
        )
        let payload = MutationExecuteOperation.Request(
            input: .statement(
                .ir(
                    .sparqlUpdate(
                        SPARQLUpdateRequest(
                            firstOperation: .load(
                                LoadQuery(source: "https://source.test/replay")
                            )
                        )
                    )
                ),
                parameters: []
            )
        )

        let first = try await invokeMutation(
            payload,
            requestID: 41,
            idempotencyKey: "load-endpoint-replay",
            endpoint: endpoint
        )
        let replay = try await invokeMutation(
            payload,
            requestID: 42,
            idempotencyKey: "load-endpoint-replay",
            endpoint: endpoint
        )

        #expect(source.callCount == 1)
        #expect(first == replay)
        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        ).count == 1)
    }

    @Test("UPDATE WHERE reads the injected authoritative store serializably")
    func updateWhereUsesInjectedSerializableStore() async throws {
        let container = try await makeContainer()
        let store = TrackingRDFGraphMutationStore()
        let executor = CanonicalDatabaseStatementMutationExecutor(
            graphStore: store
        )
        let subject = "https://example.test/events/injected-store"
        let predicate = "https://example.test/status"
        _ = try await executeRDF(
            .insertData(
                InsertDataQuery(
                    quads: [
                        Quad(
                            triple: TriplePattern(
                                subject: .iri(subject),
                                predicate: .iri(predicate),
                                object: .literal(.string("old"))
                            )
                        ),
                    ]
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "injected-seed")
        )

        _ = try await executeRDF(
            .modify(
                SPARQLModifyOperation(
                    action: .insert([
                        Quad(
                            triple: TriplePattern(
                                subject: .variable("subject"),
                                predicate: .iri(predicate),
                                object: .literal(.string("new"))
                            )
                        ),
                    ]),
                    wherePattern: .basic([
                        TriplePattern(
                            subject: .variable("subject"),
                            predicate: .iri(predicate),
                            object: .literal(.string("old"))
                        ),
                    ])
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "injected-modify")
        )

        #expect(!store.scanReadModes.isEmpty)
        #expect(store.scanReadModes.allSatisfy { $0 == .serializable })
        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .defaultGraph,
            container: container
        ).count == 2)
    }

    @Test("SILENT never hides a failure after graph mutation has begun")
    func silentPostMutationFailureRollsBackAndPropagates() async throws {
        let container = try await makeContainer()
        let graphIRI = "https://example.test/graphs/silent-rollback"
        let graph = try RDFGraphName(iri: graphIRI)
        let store = TrackingRDFGraphMutationStore(
            clearBehavior: .delegateThenFail(.graphNotFound(graph))
        )
        let executor = CanonicalDatabaseStatementMutationExecutor(
            graphStore: store
        )
        _ = try await executeRDF(
            .insertData(
                InsertDataQuery(
                    quads: [
                        Quad(
                            graph: .iri(graphIRI),
                            triple: TriplePattern(
                                subject: .iri("https://example.test/events/silent"),
                                predicate: .iri("https://example.test/value"),
                                object: .literal(.string("value"))
                            )
                        ),
                    ]
                )
            ),
            executor: executor,
            context: context(container, idempotencyKey: "silent-seed")
        )

        do {
            _ = try await executeRDF(
                .clear(ClearQuery(target: .graph(graphIRI), silent: true)),
                executor: executor,
                context: context(container, idempotencyKey: "silent-clear")
            )
            Issue.record("Expected the post-mutation failure to propagate")
        } catch RDFGraphStoreError.graphNotFound(let failedGraph) {
            #expect(failedGraph == graph)
        }

        #expect(try await scan(
            CanonicalRDFGraphStore(),
            scope: .named(graph),
            container: container
        ).count == 1)
    }

    @Test("Storage retries share the original request work budget")
    func storageRetrySharesRequestWorkMeter() async throws {
        let container = try await makeContainer()
        let store = ConflictOnceRDFGraphMutationStore()
        let executor = CanonicalDatabaseStatementMutationExecutor(
            graphStore: store
        )
        let quad = Quad(
            triple: TriplePattern(
                subject: .iri("https://example.test/events/retry"),
                predicate: .iri("https://example.test/value"),
                object: .literal(.string("value"))
            )
        )

        do {
            _ = try await executeRDF(
                .insertData(InsertDataQuery(quads: [quad])),
                executor: executor,
                context: context(container, idempotencyKey: "meter-retry"),
                budget: ExecutionBudget(
                    maximumRows: 1,
                    maximumWorkUnits: 2,
                    timeoutMilliseconds: 30_000
                )
            )
            Issue.record("Expected the retry to exhaust the request budget")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumWorkUnits(
                stage: .mutationPlanning,
                consumed: 2,
                requested: 1,
                maximum: 2
            ) = error else {
                Issue.record("Unexpected work limit error: \(error)")
                return
            }
        }

        #expect(store.insertAttemptCount == 1)
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [DatabaseEndpointEntity.self]
            ),
            security: .disabled
        )
    }

    private func context(
        _ container: DBContainer,
        idempotencyKey: String
    ) -> DatabaseOperationContext {
        DatabaseOperationContext(
            container: container,
            requestID: 1,
            metadata: OperationRequestMetadata(
                idempotencyKey: idempotencyKey
            ),
            requestPayload: []
        )
    }

    private func executeRDF(
        _ operation: SPARQLUpdateOperation,
        executor: CanonicalDatabaseStatementMutationExecutor,
        context: DatabaseOperationContext,
        budget: ExecutionBudget = ExecutionBudget(),
        structuralLimits: QueryStructuralLimits = .default
    ) async throws -> MutationExecuteOperation.RDFEffect {
        try await executeRDF(
            SPARQLUpdateRequest(firstOperation: operation),
            executor: executor,
            context: context,
            budget: budget,
            structuralLimits: structuralLimits
        )
    }

    private func executeRDF(
        _ request: SPARQLUpdateRequest,
        executor: CanonicalDatabaseStatementMutationExecutor,
        context: DatabaseOperationContext,
        budget: ExecutionBudget = ExecutionBudget(),
        structuralLimits: QueryStructuralLimits = .default
    ) async throws -> MutationExecuteOperation.RDFEffect {
        let statement = try DatabaseStatementAdmission(
            structuralLimits: structuralLimits
        ).admit(
            .ir(.sparqlUpdate(request)),
            parameters: []
        )
        let prepared = try await executor.prepare(
            statement,
            budget: budget,
            context: context
        )
        let database = context.container.newContext()
        let result = try await database.withTransaction(
            configuration: .batch
        ) { transaction in
            try await executor.execute(
                prepared,
                preconditions: [],
                graphPartitions: FieldObject(),
                context: context,
                transaction: transaction
            )
        }
        guard case .rdf(let effect) = result else {
            throw DatabaseMutationError.unsupportedStatement(
                "Expected an RDF mutation result"
            )
        }
        return effect
    }

    private func invokeMutation(
        _ payload: MutationExecuteOperation.Request,
        requestID: UInt64,
        idempotencyKey: String,
        endpoint: DatabaseEndpoint
    ) async throws -> MutationExecuteOperation.Response {
        let request = try DatabaseWireEncoder().encodeRequest(
            DatabaseOperations.mutationExecute,
            requestID: requestID,
            metadata: OperationRequestMetadata(
                idempotencyKey: idempotencyKey
            ),
            request: payload
        )
        let response = try DatabaseWireDecoder().decodeResponse(
            DatabaseOperations.mutationExecute,
            from: try await endpoint.execute(request),
            matching: requestID
        )
        guard case .success(let value) = response else {
            throw EndpointInvocationFailure.remoteFailure
        }
        return value
    }

    private func scan(
        _ store: CanonicalRDFGraphStore,
        scope: RDFGraphScanScope,
        container: DBContainer
    ) async throws -> RDFDatasetScanResult {
        try await container.engine.withTransaction { transaction in
            try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphScope: scope,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: DatabaseWorkMeter(
                    budget: ExecutionBudget()
                )
            )
        }
    }

    private func containsGraph(
        _ iri: String,
        container: DBContainer
    ) async throws -> Bool {
        try await container.engine.withTransaction { transaction in
            try await CanonicalRDFGraphStore().containsGraph(
                try RDFGraphName(iri: iri),
                readMode: .snapshot,
                transaction: transaction,
                workMeter: DatabaseWorkMeter(
                    budget: ExecutionBudget()
                )
            )
        }
    }
}

private final class RecordingSPARQLLoadSource: SPARQLLoadSource, Sendable {
    private let calls = Mutex(0)
    private let loadClosure: @Sendable (
        SPARQLLoadRequest
    ) throws -> SPARQLLoadDocument

    init(
        load: @Sendable @escaping (
            SPARQLLoadRequest
        ) throws -> SPARQLLoadDocument
    ) {
        self.loadClosure = load
    }

    var callCount: Int {
        calls.withLock { $0 }
    }

    func load(_ request: SPARQLLoadRequest) async throws -> SPARQLLoadDocument {
        calls.withLock { count in
            count += 1
        }
        return try loadClosure(request)
    }
}
