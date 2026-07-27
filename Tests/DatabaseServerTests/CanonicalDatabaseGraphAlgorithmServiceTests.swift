import DatabaseKit
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import GraphIndex
import StorageKit
import Synchronization
import Testing

@Suite("Canonical graph algorithm service")
struct CanonicalDatabaseGraphAlgorithmServiceTests {
    private struct FixedSourceResolver: DatabaseGraphSourceResolving {
        let source: ResolvedDatabaseGraphSource

        func resolve(
            _ source: GraphAlgorithmOperation.Source
        ) async throws -> ResolvedDatabaseGraphSource {
            self.source
        }
    }

    private struct PropertyGraphAlgorithmContext {
        let engine: CountingEngine
        let container: DBContainer
        let service: CanonicalDatabaseGraphAlgorithmService
        let maintainer: GraphIndexMaintainer<CanonicalPropertyGraphEdge>
    }

    private final class CountingEngine: StorageEngine, Sendable {
        struct Configuration: Sendable {
            init() {}
        }

        typealias TransactionType = InMemoryTransaction

        private let underlying: InMemoryEngine
        private let transactionCount = Mutex(0)

        init() {
            self.underlying = InMemoryEngine()
        }

        init(configuration: Configuration) async throws {
            self.underlying = InMemoryEngine()
        }

        var createdTransactionCount: Int {
            transactionCount.withLock { $0 }
        }

        var directoryService: any DirectoryService {
            underlying.directoryService
        }

        func resetTransactionCount() {
            transactionCount.withLock { $0 = 0 }
        }

        func createTransaction() throws -> InMemoryTransaction {
            transactionCount.withLock { $0 += 1 }
            return try underlying.createTransaction()
        }

        func shutdown() {
            underlying.shutdown()
        }
    }

    @Test("path pages overlap at the boundary and preserve typed progress")
    func pathPagination() async throws {
        let graphContext = try await makePropertyGraphAlgorithmContext()
        let request = shortestPathRequest(pageLimit: 1)

        let first = try await execute(request, graphContext: graphContext)
        guard case .path(let firstPage) = first,
              let continuation = firstPage.progress.continuation else {
            Issue.record("Expected a first path page with continuation")
            return
        }
        #expect(
            try firstPage.materializedNodes(maximumCount: 2)
                == [.identifier("A"), .identifier("B")]
        )
        #expect(firstPage.progress.algorithmComplete)
        #expect(!firstPage.progress.resultPageComplete)

        let second = try await execute(
            shortestPathRequest(pageLimit: 1, continuation: continuation),
            graphContext: graphContext
        )
        guard case .path(let secondPage) = second else {
            Issue.record("Expected a second path page")
            return
        }
        #expect(
            try secondPage.materializedNodes(maximumCount: 2)
                == [.identifier("B"), .identifier("C")]
        )
        #expect(
            try secondPage.materializedEdgeLabels(maximumCount: 1)
                == [.identifier("link")]
        )
    }

    @Test("continuation detects a changed graph snapshot")
    func continuationDetectsSnapshotChange() async throws {
        let graphContext = try await makePropertyGraphAlgorithmContext()
        let first = try await execute(
            shortestPathRequest(pageLimit: 1),
            graphContext: graphContext
        )
        guard case .path(let page) = first,
              let continuation = page.progress.continuation else {
            Issue.record("Expected a continuation")
            return
        }

        try await insert(
            CanonicalPropertyGraphEdge(
                id: "direct",
                source: "A",
                label: "link",
                target: "D",
                weight: 10
            ),
            graphContext: graphContext
        )

        await #expect(throws: DatabaseGraphAlgorithmError.self) {
            try await execute(
                shortestPathRequest(pageLimit: 1, continuation: continuation),
                graphContext: graphContext
            )
        }
    }

    @Test("weighted path reads covering values through the graph index")
    func weightedPathReadsStoredWeight() async throws {
        let graphContext = try await makePropertyGraphAlgorithmContext()
        try await insert(
            CanonicalPropertyGraphEdge(
                id: "direct",
                source: "A",
                label: "link",
                target: "D",
                weight: 10
            ),
            graphContext: graphContext
        )
        let response = try await execute(
            GraphAlgorithmOperation.Request(
                source: Self.graphSource,
                invocation: .weightedShortestPath(
                    source: .identifier("A"),
                    target: .identifier("D"),
                    weightProperty: "weight",
                    maximumWeight: 100,
                    maximumNodes: 100
                ),
                page: GraphAlgorithmOperation.Page(limit: 10),
                budget: ExecutionBudget(maximumWorkUnits: 1_000)
            ),
            graphContext: graphContext
        )
        guard case .path(let path) = response else {
            Issue.record("Expected a weighted path")
            return
        }
        #expect(try path.materializedNodes(maximumCount: 4) == [
            GraphAlgorithmOperation.Term.identifier("A"),
            GraphAlgorithmOperation.Term.identifier("B"),
            GraphAlgorithmOperation.Term.identifier("C"),
            GraphAlgorithmOperation.Term.identifier("D"),
        ])
        #expect(try path.materializedWeights(maximumCount: 3) == [2, 1, 1])
        #expect(path.totalWeight == 4)
        #expect(path.progress == GraphAlgorithmOperation.Progress.complete)
    }

    @Test("work exhaustion is a typed incomplete result")
    func workExhaustionIsTyped() async throws {
        let graphContext = try await makePropertyGraphAlgorithmContext()
        let response = try await execute(
            shortestPathRequest(pageLimit: 10, maximumWorkUnits: 1),
            graphContext: graphContext
        )
        guard case .path(let path) = response else {
            Issue.record("Expected a path result")
            return
        }
        #expect(!path.found)
        #expect(!path.progress.algorithmComplete)
        #expect(path.progress.resultPageComplete)
        #expect(path.progress.limitReason == .maximumWorkUnits)
        #expect(path.progress.continuation == nil)
    }

    @Test("each unweighted and weighted operation uses one storage snapshot")
    func operationUsesOneStorageSnapshot() async throws {
        let graphContext = try await makePropertyGraphAlgorithmContext()

        graphContext.engine.resetTransactionCount()
        _ = try await execute(
            shortestPathRequest(pageLimit: 10),
            graphContext: graphContext
        )
        #expect(graphContext.engine.createdTransactionCount == 1)

        graphContext.engine.resetTransactionCount()
        _ = try await execute(
            GraphAlgorithmOperation.Request(
                source: Self.graphSource,
                invocation: .weightedShortestPath(
                    source: .identifier("A"),
                    target: .identifier("D"),
                    weightProperty: "weight",
                    maximumWeight: 100,
                    maximumNodes: 100
                ),
                page: GraphAlgorithmOperation.Page(limit: 10),
                budget: ExecutionBudget(maximumWorkUnits: 1_000)
            ),
            graphContext: graphContext
        )
        #expect(graphContext.engine.createdTransactionCount == 1)
    }

    @Test("ranking, community, cycle, component, and topological families execute")
    func remainingAlgorithmFamiliesExecute() async throws {
        let graphContext = try await makePropertyGraphAlgorithmContext()
        let budget = ExecutionBudget(maximumWorkUnits: 10_000)
        let page = GraphAlgorithmOperation.Page(limit: 100)

        let ranking = try await execute(
            GraphAlgorithmOperation.Request(
                source: Self.graphSource,
                invocation: .pageRank(
                    dampingFactor: 0.85,
                    maximumIterations: 100,
                    convergenceThreshold: 1e-8,
                    personalizedSource: nil
                ),
                page: page,
                budget: budget
            ),
            graphContext: graphContext
        )
        guard case .ranking(let rankingPage) = ranking else {
            Issue.record("Expected a ranking response")
            return
        }
        #expect(rankingPage.scoreCount == 4)
        #expect(rankingPage.progress.algorithmComplete)

        let communities = try await execute(
            GraphAlgorithmOperation.Request(
                source: Self.graphSource,
                invocation: .community(
                    maximumIterations: 100,
                    computeModularity: true,
                    minimumCommunitySize: 1,
                    seed: 7
                ),
                page: page,
                budget: budget
            ),
            graphContext: graphContext
        )
        guard case .communities(let communityPage) = communities else {
            Issue.record("Expected a community response")
            return
        }
        #expect(communityPage.assignmentCount == 4)
        #expect(communityPage.modularity != nil)

        let cycles = try await execute(
            GraphAlgorithmOperation.Request(
                source: Self.graphSource,
                invocation: .cycleDetection(maximumCycles: 10, maximumNodes: 100),
                page: page,
                budget: budget
            ),
            graphContext: graphContext
        )
        guard case .cycles(let cyclePage) = cycles else {
            Issue.record("Expected a cycle response")
            return
        }
        #expect(cyclePage.cycleCount == 0)
        #expect(cyclePage.progress.algorithmComplete)

        let components = try await execute(
            GraphAlgorithmOperation.Request(
                source: Self.graphSource,
                invocation: .stronglyConnectedComponents(
                    maximumComponents: 100,
                    maximumNodes: 100
                ),
                page: page,
                budget: budget
            ),
            graphContext: graphContext
        )
        guard case .components(let componentPage) = components else {
            Issue.record("Expected a component response")
            return
        }
        let materializedComponents = try componentPage.materializedComponents(
            maximumCount: 4
        )
        #expect(materializedComponents.count == 4)
        #expect(materializedComponents.allSatisfy { $0.termCount == 1 })

        let topological = try await execute(
            GraphAlgorithmOperation.Request(
                source: Self.graphSource,
                invocation: .topologicalSort(maximumNodes: 100),
                page: page,
                budget: budget
            ),
            graphContext: graphContext
        )
        guard case .topologicalOrder(let topologicalPage) = topological else {
            Issue.record("Expected a topological response")
            return
        }
        #expect(try topologicalPage.materializedOrder(maximumCount: 4) == [
            GraphAlgorithmOperation.Term.identifier("A"),
            GraphAlgorithmOperation.Term.identifier("B"),
            GraphAlgorithmOperation.Term.identifier("C"),
            GraphAlgorithmOperation.Term.identifier("D"),
        ])
        #expect(topologicalPage.progress.algorithmComplete)
    }

    private static let graphSource = GraphAlgorithmOperation.Source(
        index: "graph",
        edgeLabel: .identifier("link")
    )

    private func shortestPathRequest(
        pageLimit: UInt32,
        continuation: ByteString? = nil,
        maximumWorkUnits: UInt64 = 1_000
    ) -> GraphAlgorithmOperation.Request {
        GraphAlgorithmOperation.Request(
            source: Self.graphSource,
            invocation: .shortestPath(
                source: .identifier("A"),
                target: .identifier("D"),
                maximumDepth: 10,
                bidirectional: false,
                maximumNodes: 100
            ),
            page: GraphAlgorithmOperation.Page(
                limit: pageLimit,
                continuation: continuation
            ),
            budget: ExecutionBudget(
                maximumWorkUnits: maximumWorkUnits
            )
        )
    }

    private func makePropertyGraphAlgorithmContext() async throws -> PropertyGraphAlgorithmContext {
        let engine = CountingEngine()
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                    try CanonicalPropertyGraphEdge.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                persistableTypes: [
                    DatabaseEndpointEntity.self,
                    CanonicalPropertyGraphEdge.self,
                ]
            ),
            security: .disabled
        )
        let indexSubspace = Subspace(
            prefix: Tuple("canonical-graph-service").pack()
        )
        let kind = IndexKindMetadata(
            identifier: "graph",
            subspaceStructure: .hierarchical,
            fields: [
                IndexFieldMetadata(
                    identity: FieldIdentity(name: "source", number: 2)
                ),
                IndexFieldMetadata(
                    identity: FieldIdentity(name: "label", number: 3)
                ),
                IndexFieldMetadata(
                    identity: FieldIdentity(name: "target", number: 4)
                ),
            ],
            metadata: [
                "strategy": .string("tripleStore"),
                "hasEdgeField": .bool(true),
                "hasGraphField": .bool(false),
            ]
        )
        let metadata = try PropertyGraphIndexMetadata(canonical: kind)
        let index = Index(
            name: "graph",
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "source"),
                FieldKeyExpression(fieldName: "label"),
                FieldKeyExpression(fieldName: "target"),
            ]),
            itemTypes: Set([CanonicalPropertyGraphEdge.persistableType]),
            storedFieldNames: ["weight"]
        )
        let maintainer = try GraphIndexMaintainer<CanonicalPropertyGraphEdge>(
            index: index,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            metadata: metadata
        )
        let resolvedSource = ResolvedDatabaseGraphSource(
                entityName: CanonicalPropertyGraphEdge.persistableType,
            indexName: index.name,
            indexSubspace: indexSubspace,
            storedFieldNames: index.storedFieldNames,
            layout: .propertyGraph(
                ResolvedDatabaseGraphSource.PropertyGraphLayout(
                    strategy: .tripleStore,
                    scope: .all,
                    edgeLabel: "link"
                )
            )
        )
        let graphContext = PropertyGraphAlgorithmContext(
            engine: engine,
            container: container,
            service: CanonicalDatabaseGraphAlgorithmService(
                sourceResolver: FixedSourceResolver(source: resolvedSource)
            ),
            maintainer: maintainer
        )
        for edge in [
            CanonicalPropertyGraphEdge(
                id: "ab",
                source: "A",
                label: "link",
                target: "B",
                weight: 2
            ),
            CanonicalPropertyGraphEdge(
                id: "bc",
                source: "B",
                label: "link",
                target: "C",
                weight: 1
            ),
            CanonicalPropertyGraphEdge(
                id: "cd",
                source: "C",
                label: "link",
                target: "D",
                weight: 1
            ),
        ] {
            try await insert(edge, graphContext: graphContext)
        }
        return graphContext
    }

    private func insert(
        _ edge: CanonicalPropertyGraphEdge,
        graphContext: PropertyGraphAlgorithmContext
    ) async throws {
        try await graphContext.container.engine.withTransaction(
            configuration: .batch
        ) { transaction in
            try await graphContext.maintainer.updateIndex(
                oldItem: nil,
                newItem: edge,
                transaction: transaction
            )
        }
    }

    private func execute(
        _ request: GraphAlgorithmOperation.Request,
        graphContext: PropertyGraphAlgorithmContext
    ) async throws -> GraphAlgorithmOperation.Response {
        try await graphContext.service.execute(
            request,
            context: DatabaseOperationContext(
                container: graphContext.container,
                requestID: 1,
                metadata: OperationRequestMetadata(),
                requestPayload: try DatabaseWireEncoder().encodeRequestPayload(
                    DatabaseOperations.graphAlgorithm,
                    request: request
                )
            )
        )
    }
}
