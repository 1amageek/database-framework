import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseServer
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import StorageKit
import Testing

@Suite("Canonical RDF graph algorithm service")
struct CanonicalDatabaseRDFGraphAlgorithmServiceTests {
    private struct Statement: Persistable {
        typealias ID = String

        var id: String
        var subject: DatabaseRDFTerm
        var predicate: DatabaseRDFTerm
        var object: DatabaseRDFTerm
        var graph: DatabaseRDFTerm?
        var weight: Double

        static var persistableType: String { "CanonicalRDFGraphStatement" }
        static var allFields: [String] {
            ["id", "subject", "predicate", "object", "graph", "weight"]
        }
        static var fieldSchemas: [FieldSchema] {
            [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "subject", fieldNumber: 2, type: .rdfTerm),
                FieldSchema(name: "predicate", fieldNumber: 3, type: .rdfTerm),
                FieldSchema(name: "object", fieldNumber: 4, type: .rdfTerm),
                FieldSchema(
                    name: "graph",
                    fieldNumber: 5,
                    type: .rdfTerm,
                    isOptional: true
                ),
                FieldSchema(name: "weight", fieldNumber: 6, type: .double),
            ]
        }
        static var indexDescriptors: [IndexDescriptor] { [] }

        static func fieldNumber(for fieldName: String) -> Int? { nil }
        static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

        subscript(dynamicMember member: String) -> (any Sendable)? {
            switch member {
            case "id": return id
            case "subject": return subject
            case "predicate": return predicate
            case "object": return object
            case "graph": return graph
            case "weight": return weight
            default: return nil
            }
        }

        static func fieldName<Value>(
            for keyPath: KeyPath<Statement, Value>
        ) -> String {
            fieldName(for: keyPath as AnyKeyPath)
        }

        static func fieldName(
            for keyPath: PartialKeyPath<Statement>
        ) -> String {
            fieldName(for: keyPath as AnyKeyPath)
        }

        static func fieldName(for keyPath: AnyKeyPath) -> String {
            switch keyPath {
            case \Statement.id: return "id"
            case \Statement.subject: return "subject"
            case \Statement.predicate: return "predicate"
            case \Statement.object: return "object"
            case \Statement.graph: return "graph"
            case \Statement.weight: return "weight"
            default: return String(describing: keyPath)
            }
        }
    }

    private struct FixedSourceResolver: DatabaseGraphSourceResolving {
        let source: ResolvedDatabaseGraphSource

        func resolve(
            _ source: GraphAlgorithmOperation.Source
        ) async throws -> ResolvedDatabaseGraphSource {
            self.source
        }
    }

    private struct RDFGraphAlgorithmContext {
        let container: DBContainer
        let service: CanonicalDatabaseGraphAlgorithmService
        let maintainer: RDFQuadIndexMaintainer<Statement>
        let subspace: Subspace
    }

    private let predicate = DatabaseRDFTerm.iri("urn:relation:link")
    private let namedGraph = DatabaseRDFTerm.iri("urn:graph:named")

    @Test("RDF shortest and weighted paths execute from binary quad keys")
    func pathsDecodeCanonicalQuadKeys() async throws {
        let graphContext = try await makeRDFGraphAlgorithmContext()

        let shortest = try await execute(
            invocation: .shortestPath(
                source: .rdf(.iri("urn:node:A")),
                target: .rdf(.iri("urn:node:D")),
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
        #expect(shortestPath.nodes == [
            .rdf(.iri("urn:node:A")),
            .rdf(.iri("urn:node:B")),
            .rdf(.iri("urn:node:C")),
            .rdf(.iri("urn:node:D")),
        ])

        let weighted = try await execute(
            invocation: .weightedShortestPath(
                source: .rdf(.iri("urn:node:A")),
                target: .rdf(.iri("urn:node:D")),
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
        #expect(weightedPath.nodes == shortestPath.nodes)
        #expect(weightedPath.weights == [2, 1, 1])
        #expect(weightedPath.totalWeight == 4)
    }

    @Test("RDF algorithm families preserve typed terms")
    func algorithmFamiliesPreserveTerms() async throws {
        let graphContext = try await makeRDFGraphAlgorithmContext()

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
        #expect(rankingPage.scores.count == 4)
        #expect(rankingPage.scores.allSatisfy {
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
        #expect(componentPage.components.count == 4)

        let topological = try await execute(
            invocation: .topologicalSort(maximumNodes: 100),
            graphContext: graphContext
        )
        guard case .topologicalOrder(let page) = topological else {
            Issue.record("Expected an RDF topological response")
            return
        }
        #expect(page.order == [
            .rdf(.iri("urn:node:A")),
            .rdf(.iri("urn:node:B")),
            .rdf(.iri("urn:node:C")),
            .rdf(.iri("urn:node:D")),
        ])
    }

    @Test("Named and default RDF graphs remain isolated")
    func graphScopesRemainIsolated() async throws {
        let graphContext = try await makeRDFGraphAlgorithmContext()
        let source = try GraphIdentity.rdf(.iri("urn:node:A"))
        let label = try GraphIdentity.rdf(predicate)
        let scanner = GraphEdgeScanner(
            indexSubspace: graphContext.subspace,
            strategy: .quadStore,
            scope: .named(try .rdf(namedGraph))
        )

        let edges = try await graphContext.container.engine.withTransaction(
            configuration: .default
        ) { transaction in
            try await scanner.scanAllOutgoing(
                from: source,
                edgeLabel: label,
                transaction: transaction
            )
        }
        #expect(edges.count == 1)
        #expect(try edges[0].target.decodeRDFTerm() == .iri("urn:node:D"))
        #expect(try edges[0].graph?.decodeRDFTerm() == namedGraph)
    }

    @Test("Malformed RDF key bytes fail deterministically")
    func malformedRDFBytesFail() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("malformed-rdf-graph").pack())
        let malformed = subspace.subspace(Int64(8)).pack(
            Tuple([
                Bytes(
                    retaining: RDFQuadIndexPhysicalLayout
                        .defaultGraphDiscriminator
                ),
                Bytes([0x01]),
                Bytes(retaining: try DatabaseRDFTermCodec.encode(predicate)),
                Bytes(retaining: try DatabaseRDFTermCodec.encode(.iri("urn:node:B"))),
            ])
        )
        try await engine.withTransaction(configuration: .default) { transaction in
            try transaction.setValue([], for: malformed)
        }
        let scanner = GraphEdgeScanner(
            indexSubspace: subspace,
            strategy: .quadStore,
            scope: .defaultGraph
        )

        do {
            try await engine.withTransaction(configuration: .default) {
                transaction in
                for try await _ in scanner.scanAllEdges(
                    edgeLabel: nil,
                    transaction: transaction
                ) {}
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
            for: Schema(
                [DatabaseEndpointRecord.self, Statement.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        let subspace = Subspace(
            prefix: Tuple("canonical-rdf-graph-service").pack()
        )
        let kind = RDFQuadIndexKind<Statement>(
            subject: \Statement.subject,
            predicate: \Statement.predicate,
            object: \Statement.object,
            graph: \Statement.graph
        )
        let index = Index(
            name: "rdf-graph",
            kind: kind,
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "subject"),
                FieldKeyExpression(fieldName: "predicate"),
                FieldKeyExpression(fieldName: "object"),
                FieldKeyExpression(fieldName: "graph"),
            ]),
            itemTypes: [Statement.persistableType],
            storedFieldNames: ["weight"]
        )
        let maintainer = RDFQuadIndexMaintainer<Statement>(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            subjectField: "subject",
            predicateField: "predicate",
            objectField: "object",
            graphField: "graph"
        )
        let resolvedSource = ResolvedDatabaseGraphSource(
            entityName: Statement.persistableType,
            indexName: index.name,
            indexSubspace: subspace,
            storedFieldNames: index.storedFieldNames,
            layout: .rdf(
                try ResolvedDatabaseGraphSource.RDFLayout(
                    scope: .defaultGraph,
                    predicate: predicate
                )
            )
        )
        let graphContext = RDFGraphAlgorithmContext(
            container: container,
            service: CanonicalDatabaseGraphAlgorithmService(
                sourceResolver: FixedSourceResolver(source: resolvedSource)
            ),
            maintainer: maintainer,
            subspace: subspace
        )
        for statement in [
            Statement(
                id: "ab",
                subject: .iri("urn:node:A"),
                predicate: predicate,
                object: .iri("urn:node:B"),
                graph: nil,
                weight: 2
            ),
            Statement(
                id: "bc",
                subject: .iri("urn:node:B"),
                predicate: predicate,
                object: .iri("urn:node:C"),
                graph: nil,
                weight: 1
            ),
            Statement(
                id: "cd",
                subject: .iri("urn:node:C"),
                predicate: predicate,
                object: .iri("urn:node:D"),
                graph: nil,
                weight: 1
            ),
            Statement(
                id: "named-ad",
                subject: .iri("urn:node:A"),
                predicate: predicate,
                object: .iri("urn:node:D"),
                graph: namedGraph,
                weight: 0.1
            ),
        ] {
            try await engine.withTransaction(configuration: .batch) { transaction in
                try await maintainer.updateIndex(
                    oldItem: nil,
                    newItem: statement,
                    transaction: transaction
                )
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
            budget: DatabaseExecutionBudget(maximumWorkUnits: 10_000)
        )
        return try await graphContext.service.execute(
            request,
            context: DatabaseOperationContext(
                container: graphContext.container,
                requestID: 1,
                metadata: DatabaseRequestMetadata(),
                requestPayload: try DatabaseEnvelopeCodec.encode(request)
            )
        )
    }
}
