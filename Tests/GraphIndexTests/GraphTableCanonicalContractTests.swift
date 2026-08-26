import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import GraphIndex

@Persistable(type: "GraphTableContractEdge")
private struct GraphTableContractEdge {
    #Directory<GraphTableContractEdge>("graph_table_contract_edges")
    #Index(
        .graph(
            name: "graph_table_contract_index",
            definition: .property(
                source: \GraphTableContractEdge.source,
                label: .field(\GraphTableContractEdge.label),
                target: \GraphTableContractEdge.target,
                graph: nil,
                strategy: .tripleStore
            ),
            includedFields: [\GraphTableContractEdge.weight]
        )
    )

    var id: String = ""
    var source: String = ""
    var label: String = ""
    var target: String = ""
    var weight: Int64 = 0
}

@Suite("GRAPH_TABLE canonical contract")
struct GraphTableCanonicalContractTests {
    @Test("Direct graph reads reject an unresolved graph source")
    func directGraphReadsRejectUnresolvedGraphSource() async throws {
        let scenario = try await Self.makeScenario(edgeCount: 1)
        defer { await scenario.container.shutdown() }
        let rows = try await scenario.context.graphTable(
            GraphTableContractEdge.self,
            source: Self.source(path: Self.linearPath())
        )
        #expect(rows.count == 1)
        #expect(rows[0].source == "source")
        #expect(rows[0].target == "target-0")

        let unresolved = GraphTableSource(
            graphName: "missing_graph_table_contract_index",
            matchPattern: MatchPattern(paths: [Self.linearPath()])
        )

        do {
            _ = try await scenario.context.graphTable(
                GraphTableContractEdge.self,
                source: unresolved
            )
            Issue.record("Expected the unresolved graph source to fail")
        } catch let GraphTableError.indexNotFound(reason) {
            let expectedReason =
                "Property graph 'missing_graph_table_contract_index' "
                + "could not be resolved. Available indexes: "
                + "GraphTableContractEdge:graph_table_contract_index"
            #expect(
                reason == expectedReason
            )
        } catch {
            Issue.record("Expected GraphTableError.indexNotFound, got \(error)")
        }
    }

    @Test("Unsupported path and node semantics fail before an empty scan")
    func unsupportedSemanticsFailBeforeEmptyScan() async throws {
        let cases: [(GraphTableSource, String)] = [
            (
                Self.source(
                    path: Self.linearPath(
                        left: NodePattern(
                            variable: "source",
                            labels: ["Person"]
                        )
                    )
                ),
                "GRAPH_TABLE node labels are not supported"
            ),
            (
                Self.source(
                    path: PathPattern(
                        pathVariable: "path",
                        elements: Self.linearPath().elements
                    )
                ),
                "GRAPH_TABLE path variables are not supported"
            ),
            (
                Self.source(path: Self.linearPath(mode: .trail)),
                "TRAIL GRAPH_TABLE path mode is not supported"
            ),
            (
                Self.source(path: Self.linearPath(mode: .acyclic)),
                "ACYCLIC GRAPH_TABLE path mode is not supported"
            ),
            (
                Self.source(path: Self.linearPath(mode: .simple)),
                "SIMPLE GRAPH_TABLE path mode is not supported"
            ),
            (
                Self.source(
                    path: PathPattern(
                        elements: [
                            .quantified(
                                Self.linearPath(),
                                quantifier: .oneOrMore
                            )
                        ]
                    )
                ),
                "Quantified GRAPH_TABLE paths are not supported"
            ),
            (
                Self.source(
                    path: PathPattern(
                        elements: [
                            .alternation([
                                Self.linearPath(),
                                Self.linearPath(),
                            ])
                        ]
                    )
                ),
                "Alternating GRAPH_TABLE paths are not supported"
            ),
        ]

        let descriptor = try #require(
            try GraphTableContractEdge.indexDescriptors.first
        )
        for (graphTableSource, expectedReason) in cases {
            let transaction = try InMemoryEngine().createTransaction()
            let executor = GraphTableExecutor(
                indexDescriptor: descriptor,
                indexSubspace: Subspace(),
                graphTableSource: graphTableSource
            )
            do {
                _ = try await executor.execute(
                    transaction: transaction,
                    workMeter: DatabaseWorkMeter(
                        budget: ExecutionBudget(),
                        monotonicClock: TestProcessMonotonicClock()
                    )
                )
                Issue.record("Expected unsupported GRAPH_TABLE semantics")
            } catch let GraphTableError.invalidGraphPattern(reason) {
                #expect(reason == expectedReason)
            } catch {
                Issue.record("Expected GraphTableError, got \(error)")
            }
        }
    }

    @Test("Runtime GRAPH_TABLE observes an uncommitted caller transaction")
    func runtimeGraphTableObservesCallerTransaction() async throws {
        let scenario = try await Self.makeScenario(edgeCount: 0)
        defer { await scenario.container.shutdown() }

        let response = try await scenario.context.withTransaction {
            transaction in
            try await transaction.save(
                GraphTableContractEdge(
                    id: "uncommitted-edge",
                    source: "uncommitted-source",
                    label: "KNOWS",
                    target: "uncommitted-target",
                    weight: 1
                ),
                precondition: .notExists
            )
            return try await scenario.context.query(
                SelectQuery(
                    projection: .all,
                    source: .graphTable(
                        Self.source(path: Self.linearPath())
                    )
                )
            )
        }

        #expect(response.rows.count == 1)
        #expect(
            response.rows[0].fields["source_id"]
                == .string("uncommitted-source")
        )
        #expect(
            response.rows[0].fields["target_id"]
                == .string("uncommitted-target")
        )
    }

    @Test("Graph expansion owns its intermediate row budget")
    func graphExpansionOwnsIntermediateRowBudget() async throws {
        let scenario = try await Self.makeScenario(edgeCount: 3)
        defer { await scenario.container.shutdown() }
        let descriptor = try #require(
            try GraphTableContractEdge.indexDescriptors.first
        )
        try await scenario.container.withTestBaseTransaction { transaction in
            guard let readableIndex = try await scenario.context
                .indexQueryContext.readableIndex(
                    named: descriptor.name,
                    indexType: descriptor.type,
                    forEntityName: GraphTableContractEdge.persistableType,
                    partitions: FieldObject(),
                    transaction: transaction
                ) else {
                Issue.record("Expected a readable graph index")
                return
            }

            do {
                let rows = try await GraphTableExecutor(
                    indexDescriptor: descriptor,
                    indexSubspace: readableIndex.subspace,
                    graphTableSource: Self.source(path: Self.linearPath())
                ).execute(
                    transaction: transaction,
                    workMeter: DatabaseWorkMeter(
                        budget: ExecutionBudget(
                            maximumRows: 10,
                            maximumWorkUnits: 10_000,
                            maximumIntermediateRows: 2,
                            maximumIntermediateBytes: 1_024 * 1_024,
                            timeoutMilliseconds: 30_000
                        ),
                        monotonicClock: TestProcessMonotonicClock()
                    )
                )
                _ = rows.count
                Issue.record(
                    "Expected graph expansion to exceed its retained-row budget"
                )
            } catch let error as DatabaseWorkLimitError {
                #expect(
                    error == .maximumIntermediateRows(
                        stage: .pathExpansion,
                        consumed: 2,
                        requested: 1,
                        maximum: 2
                    )
                )
            }
        }
    }

    @Test("Direct graph expansion observes task cancellation")
    func directGraphExpansionObservesCancellation() async throws {
        let control = StorageTransactionControl()
        let scenario = try await Self.makeScenario(
            edgeCount: 1,
            storageEngine: ControlledStorageEngine(
                base: InMemoryEngine(),
                control: control
            )
        )
        defer { await scenario.container.shutdown() }

        let descriptor = try #require(
            try GraphTableContractEdge.indexDescriptors.first
        )
        try await scenario.container.withTestBaseTransaction { transaction in
            guard let readableIndex = try await scenario.context
                .indexQueryContext.readableIndex(
                    named: descriptor.name,
                    indexType: descriptor.type,
                    forEntityName: GraphTableContractEdge.persistableType,
                    partitions: FieldObject(),
                    transaction: transaction
                ) else {
                Issue.record("Expected a readable graph index")
                return
            }
            let executor = GraphTableExecutor(
                indexDescriptor: descriptor,
                indexSubspace: readableIndex.subspace,
                graphTableSource: Self.source(path: Self.linearPath())
            )
            let rangeBarrier = control.suspendNextRangeAdvance()
            let execution = Task {
                let rows = try await executor.execute(
                    transaction: transaction,
                    workMeter: DatabaseWorkMeter(
                        budget: ExecutionBudget(),
                        monotonicClock: TestProcessMonotonicClock()
                    )
                )
                return rows.count
            }
            await rangeBarrier.waitUntilEntered()

            execution.cancel()
            rangeBarrier.release()

            do {
                _ = try await execution.value
                Issue.record("Expected graph expansion cancellation")
            } catch is CancellationError {
                // Expected.
            } catch {
                Issue.record("Expected CancellationError, got \(error)")
            }
        }
    }

    private static func makeScenario(
        edgeCount: Int,
        storageEngine: any StorageEngine = InMemoryEngine()
    ) async throws -> (
        container: DBContainer,
        context: DatabaseContext
    ) {
        let provider = GraphIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(
            GraphTableContractEdge.self
        )
        try runtime.register(provider)
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try GraphTableContractEdge.schemaEntity]
            ),
            configuration: .testing(storageEngine: storageEngine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "graph-table-contract-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                graphTableSourceExecutor: GraphTableReadExecutors
                    .sourceExecutor,
                sparqlSourceExecutor: SPARQLReadExecutors.sourceExecutor(
                    functionRegistry: .empty
                ),
                entityRuntimes: [runtime.registration()]
            ),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        for index in 0..<edgeCount {
            try context.insert(
                GraphTableContractEdge(
                    id: "edge-\(index)",
                    source: "source",
                    label: "KNOWS",
                    target: "target-\(index)",
                    weight: Int64(index)
                )
            )
        }
        try await context.save()
        return (container, context)
    }

    private static func source(path: PathPattern) -> GraphTableSource {
        GraphTableSource(
            graphName: "graph_table_contract_index",
            matchPattern: MatchPattern(paths: [path]),
            columns: [
                GraphTableColumn(
                    expression: .column(
                        ColumnRef(table: "source", column: "id")
                    ),
                    alias: "source_id"
                ),
                GraphTableColumn(
                    expression: .column(
                        ColumnRef(table: "target", column: "id")
                    ),
                    alias: "target_id"
                ),
            ]
        )
    }

    private static func linearPath(
        left: NodePattern = NodePattern(variable: "source"),
        mode: PathMode? = nil
    ) -> PathPattern {
        PathPattern(
            elements: [
                .node(left),
                .edge(
                    EdgePattern(
                        variable: "edge",
                        labels: ["KNOWS"],
                        direction: .outgoing
                    )
                ),
                .node(NodePattern(variable: "target")),
            ],
            mode: mode
        )
    }
}
