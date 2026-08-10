import DatabaseKit
import TestSupport
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseTypes
import DatabaseWire
import GraphIndex
import StorageKit
import Testing

@Suite("Canonical RDF graph algorithm service")
struct CanonicalDatabaseRDFGraphAlgorithmServiceTests {
    private struct FixedSourceResolver: DatabaseGraphSourceResolving {
        let source: ResolvedDatabaseGraphSource

        func resolve(
            _ source: GraphAlgorithmOperation.Source,
            transaction: any TransactionAccess
        ) async throws -> ResolvedDatabaseGraphSource {
            self.source
        }
    }

    private struct RDFGraphAlgorithmContext {
        let container: DBContainer
        let databaseContext: DatabaseContext
        let service: CanonicalDatabaseGraphAlgorithmService
        let maintainer: RDFQuadIndexMaintainer<CanonicalRDFGraphStatement>
        let subspace: Subspace
    }

    private let predicate = RDFTerm.iri(.xsdString)
    private let namedGraph = RDFTerm.iri(.rdfLanguageString)

    @Test("RDF shortest and weighted paths execute from binary quad keys")
    func pathsDecodeCanonicalQuadKeys() async throws {
        let graphContext = try await makeRDFGraphAlgorithmContext()

        let shortest = try await execute(
            invocation: .shortestPath(
                source: .rdf(try RDFTerm.iri(validating: "urn:node:A")),
                target: .rdf(try RDFTerm.iri(validating: "urn:node:D")),
                maximumDepth: 10,
                bidirectional: true,
                maximumNodes: 100
            ),
            graphContext: graphContext
        )
        guard case .path(let shortestPath) = shortest else {
            Issue.record("Expected an RDF shortest-path response")
            return
        }
        let expectedPath: [GraphAlgorithmOperation.Term] = [
            .rdf(try RDFTerm.iri(validating: "urn:node:A")),
            .rdf(try RDFTerm.iri(validating: "urn:node:B")),
            .rdf(try RDFTerm.iri(validating: "urn:node:C")),
            .rdf(try RDFTerm.iri(validating: "urn:node:D")),
        ]
        #expect(
            try shortestPath.materializedNodes(maximumCount: 4)
                == expectedPath
        )

        let weighted = try await execute(
            invocation: .weightedShortestPath(
                source: .rdf(try RDFTerm.iri(validating: "urn:node:A")),
                target: .rdf(try RDFTerm.iri(validating: "urn:node:D")),
                weightProperty: "weight",
                maximumWeight: 100,
                maximumNodes: 100
            ),
            graphContext: graphContext
        )
        guard case .path(let weightedPath) = weighted else {
            Issue.record("Expected an RDF weighted-path response")
            return
        }
        #expect(
            try weightedPath.materializedNodes(maximumCount: 4)
                == expectedPath
        )
        #expect(
            try weightedPath.materializedWeights(maximumCount: 3)
                == [2, 1, 1]
        )
        #expect(weightedPath.totalWeight == 4)
    }

    @Test("RDF algorithm families preserve typed terms")
    func algorithmFamiliesPreserveTerms() async throws {
        let graphContext = try await makeRDFGraphAlgorithmContext()
        let expectedPath: [GraphAlgorithmOperation.Term] = [
            .rdf(try RDFTerm.iri(validating: "urn:node:A")),
            .rdf(try RDFTerm.iri(validating: "urn:node:B")),
            .rdf(try RDFTerm.iri(validating: "urn:node:C")),
            .rdf(try RDFTerm.iri(validating: "urn:node:D")),
        ]

        let ranking = try await execute(
            invocation: .pageRank(
                dampingFactor: 0.85,
                maximumIterations: 100,
                convergenceThreshold: 1e-8,
                personalizedSource: nil
            ),
            graphContext: graphContext
        )
        guard case .ranking(let rankingPage) = ranking else {
            Issue.record("Expected an RDF ranking response")
            return
        }
        let scores = try rankingPage.materializedScores(maximumCount: 4)
        #expect(scores.count == 4)
        #expect(scores.allSatisfy {
            if case .rdf = $0.vertex { return true }
            return false
        })

        let components = try await execute(
            invocation: .stronglyConnectedComponents(
                maximumComponents: 100,
                maximumNodes: 100
            ),
            graphContext: graphContext
        )
        guard case .components(let componentPage) = components else {
            Issue.record("Expected RDF components")
            return
        }
        #expect(componentPage.componentCount == 4)

        let topological = try await execute(
            invocation: .topologicalSort(maximumNodes: 100),
            graphContext: graphContext
        )
        guard case .topologicalOrder(let page) = topological else {
            Issue.record("Expected an RDF topological response")
            return
        }
        #expect(try page.materializedOrder(maximumCount: 4) == expectedPath)
    }

    @Test("Named and default RDF graphs remain isolated")
    func graphScopesRemainIsolated() async throws {
        let graphContext = try await makeRDFGraphAlgorithmContext()
        let source = try GraphIdentity.rdf(
            try RDFTerm.iri(validating: "urn:node:A")
        )
        let label = try GraphIdentity.rdf(predicate)
        let scanner = GraphEdgeScanner(
            indexSubspace: graphContext.subspace,
            strategy: .quadStore,
            graphTarget: .named(try .rdf(namedGraph))
        )

        let edges = try await graphContext.databaseContext.withBaseOperation {
            try await StorageTransactionExecutor(
                engine: graphContext.container.engine
            ).withTransaction(
                configuration: .default,
                clock: TestProcessMonotonicClock()
            ) { transaction in
                try await scanner.scanAllOutgoing(
                    from: source,
                    edgeLabel: label,
                    transaction: transaction
                )
            }
        }
        #expect(edges.count == 1)
        #expect(
            try edges[0].target.decodeRDFTerm()
                == (try RDFTerm.iri(validating: "urn:node:D"))
        )
        #expect(try edges[0].graph?.decodeRDFTerm() == namedGraph)
    }

    @Test("Malformed RDF key bytes fail deterministically")
    func malformedRDFBytesFail() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("malformed-rdf-graph").pack())
        let malformed = subspace.subspace(Int64(8)).pack(
            Tuple([
                RDFQuadIndexPhysicalLayout.defaultGraphDiscriminator,
                ByteString([0x01]),
                try RDFTermStorageFormat.encode(predicate),
                try RDFTermStorageFormat.encode(
                    try RDFTerm.iri(validating: "urn:node:B")
                ),
            ])
        )
        try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .default,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try transaction.setValue([], for: malformed)
        }
        let scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: .quadStore,
            graphTarget: .defaultGraph
        )

        do {
            try await StorageTransactionExecutor(engine: engine).withTransaction(
                configuration: .default,
                clock: TestProcessMonotonicClock()
            ) {
                transaction in
                var cursor = scanner.scanAllEdges(
                    edgeLabel: nil,
                    transaction: transaction
                ).makeCursor()
                while try await cursor.next() != nil {}
            }
            Issue.record("Expected malformed RDF bytes to fail")
        } catch let error as GraphIndexError {
            guard case .invalidRDFEncoding(let reason) = error else {
                Issue.record("Unexpected graph index error: \(error)")
                return
            }
            #expect(reason == .truncated)
        } catch {
            Issue.record("Unexpected error type: \(error)")
        }
    }

    private func makeRDFGraphAlgorithmContext() async throws -> RDFGraphAlgorithmContext {
        let engine = InMemoryEngine()
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseEndpointEntity.schemaEntity,
                    try CanonicalRDFGraphStatement.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseEndpointEntity.self), try DatabaseFrameworkRuntime.entity(CanonicalRDFGraphStatement.self)]
            ),
            security: .testingDisabled
        )
        let databaseContext = container.testBaseContext()
        let subspace = try await databaseContext.withBaseOperation {
            try databaseContext.requireOperationBaseLease().root
                .subspace("test-indexes")
                .subspace("canonical-rdf-graph-service")
        }
        guard let descriptor =
            try CanonicalRDFGraphStatement.indexDescriptors.first
        else {
            throw CanonicalRDFGraphAlgorithmSetupError
                .missingRDFDatasetIndex
        }
        let index = Index(
            name: "rdf-graph",
            kind: descriptor.kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object"),
                FieldKeyExpression(fieldName: "graph"),
            ]),
            itemTypes: Set([CanonicalRDFGraphStatement.persistableType]),
            storedFieldNames: ["weight"]
        )
        let maintainer = try RDFQuadIndexMaintainer<CanonicalRDFGraphStatement>(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            subjectField: "subject",
            predicateField: "predicate",
            objectField: "object",
            graphField: "graph"
        )
        let resolvedSource = ResolvedDatabaseGraphSource(
            entityName: CanonicalRDFGraphStatement.persistableType,
            indexName: index.name,
            indexSubspace: subspace,
            storedFieldNames: index.storedFieldNames,
            layout: .rdf(
                try ResolvedDatabaseGraphSource.RDFLayout(
                    graphTarget: .defaultGraph,
                    predicate: predicate
                )
            )
        )
        let graphContext = RDFGraphAlgorithmContext(
            container: container,
            databaseContext: databaseContext,
            service: CanonicalDatabaseGraphAlgorithmService(
                sourceResolver: FixedSourceResolver(source: resolvedSource)
            ),
            maintainer: maintainer,
            subspace: subspace
        )
        for statement in [
            CanonicalRDFGraphStatement(
                id: "ab",
                subject: try RDFTerm.iri(validating: "urn:node:A"),
                predicate: predicate,
                object: try RDFTerm.iri(validating: "urn:node:B"),
                graph: nil,
                weight: 2
            ),
            CanonicalRDFGraphStatement(
                id: "bc",
                subject: try RDFTerm.iri(validating: "urn:node:B"),
                predicate: predicate,
                object: try RDFTerm.iri(validating: "urn:node:C"),
                graph: nil,
                weight: 1
            ),
            CanonicalRDFGraphStatement(
                id: "cd",
                subject: try RDFTerm.iri(validating: "urn:node:C"),
                predicate: predicate,
                object: try RDFTerm.iri(validating: "urn:node:D"),
                graph: nil,
                weight: 1
            ),
            CanonicalRDFGraphStatement(
                id: "named-ad",
                subject: try RDFTerm.iri(validating: "urn:node:A"),
                predicate: predicate,
                object: try RDFTerm.iri(validating: "urn:node:D"),
                graph: namedGraph,
                weight: 0.1
            ),
        ] {
            try await databaseContext.withBaseOperation {
                try await StorageTransactionExecutor(engine: engine).withTransaction(
                    configuration: .batch,
                    clock: TestProcessMonotonicClock()
                ) { transaction in
                    try await maintainer.updateIndex(
                        oldItem: nil,
                        newItem: statement,
                        transaction: transaction
                    )
                }
            }
        }
        return graphContext
    }

    private func execute(
        invocation: GraphAlgorithmOperation.Invocation,
        graphContext: RDFGraphAlgorithmContext
    ) async throws -> GraphAlgorithmOperation.Response {
        let request = GraphAlgorithmOperation.Request(
            source: GraphAlgorithmOperation.Source(
                index: "rdf-graph",
                graph: .defaultGraph,
                edgeLabel: .rdf(predicate)
            ),
            invocation: invocation,
            page: GraphAlgorithmOperation.Page(limit: 100),
            budget: ExecutionBudget(maximumWorkUnits: 10_000)
        )
        return try await graphContext.databaseContext.withBaseOperation {
            try await graphContext.service.execute(
                request,
                context: DatabaseOperationContext(
                    container: graphContext.container,
                    target: .base(graphContext.databaseContext.baseID),
                    baseContext: graphContext.databaseContext,
                    composition: nil,
                    requirement: .canonical(for: .graphAlgorithm),
                    requestID: 1,
                    metadata: OperationRequestMetadata(),
                    authorization: TestBaseEnvironment.authorization,
                    requestPayload: try DatabaseWireEncoder().encodeRequestPayload(
                        DatabaseOperations.graphAlgorithm,
                        request: request
                    ),
                    wireLimits: .default
                )
            )
        }
    }
}

private enum CanonicalRDFGraphAlgorithmSetupError: Error {
    case missingRDFDatasetIndex
}
