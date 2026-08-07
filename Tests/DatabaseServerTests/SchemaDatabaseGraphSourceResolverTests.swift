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

@Suite("Schema graph source resolver")
struct SchemaDatabaseGraphSourceResolverTests {
    @Test("property graph metadata is resolved without losing stored fields")
    func resolvesPropertyGraph() async throws {
        let container = try await makeContainer()
        let source = try await resolve(
            GraphAlgorithmOperation.Source(
                index: "source_graph",
                graph: .named(.identifier("calendar")),
                edgeLabel: .identifier("contains")
            ),
            container: container
        )

        guard case .propertyGraph(let layout) = source.layout else {
            Issue.record("Expected a property graph source")
            return
        }
        #expect(source.entityName == DatabaseGraphSourceEdge.persistableType)
        #expect(layout.strategy == .adjacency)
        #expect(layout.graphTarget == .named("calendar"))
        #expect(layout.edgeLabel == "contains")
        #expect(source.storedFieldNames == ["weight"])
        #expect(
            try source.encodeVertex(.identifier("event:1")) == "event:1"
        )
    }

    @Test("named property graph selection requires namespace metadata")
    func rejectsNamedPropertyGraphWithoutNamespace() async throws {
        let container = try await makeContainer()

        await #expect(
            throws: DatabaseGraphAlgorithmError.self
        ) {
            try await resolve(
                GraphAlgorithmOperation.Source(
                    index: "source_graph_default",
                    graph: .named(.identifier("calendar"))
                ),
                container: container
            )
        }
    }

    @Test("RDF terms retain typed identity and canonical binary storage")
    func resolvesRDFGraph() async throws {
        let container = try await makeContainer()
        guard let descriptor = try DatabaseSHACLStatement.indexDescriptors.first(
            where: {
                $0.kindIdentifier
                    == IndexDefinition.rdfDataset.identifier
            }
        ) else {
            Issue.record("Expected the RDF quad index descriptor")
            return
        }
        let source = try await resolve(
            GraphAlgorithmOperation.Source(
                index: descriptor.name,
                graph: .defaultGraph,
                edgeLabel: .rdf(try RDFTerm.iri(validating: "urn:predicate"))
            ),
            container: container
        )

        guard case .rdf(let layout) = source.layout else {
            Issue.record("Expected an RDF source")
            return
        }
        #expect(
            layout.graphTarget
                == ResolvedDatabaseGraphSource.RDFGraphTarget.defaultGraph
        )
        #expect(
            layout.predicate
                == (try RDFTerm.iri(validating: "urn:predicate"))
        )
        let blankNode = try RDFTerm.blankNode(identifier: "event")
        let encoded = try source.encodeVertex(.rdf(blankNode))
        #expect(try source.decodeVertex(encoded) == .rdf(blankNode))
    }

    @Test("graph representations reject terms from the other model")
    func rejectsMismatchedTerms() async throws {
        let container = try await makeContainer()

        await #expect(throws: DatabaseGraphAlgorithmError.self) {
            try await resolve(
                GraphAlgorithmOperation.Source(
                    index: "source_graph",
                    edgeLabel: .rdf(
                        try RDFTerm.iri(validating: "urn:predicate")
                    )
                ),
                container: container
            )
        }
    }

    @Test("default-graph RDF indexes reject named graph selection")
    func rejectsNamedGraphOutsideIndexCoverage() async throws {
        let container = try await makeContainer()

        await #expect(throws: DatabaseGraphAlgorithmError.self) {
            try await resolve(
                GraphAlgorithmOperation.Source(
                    index: "default_rdf",
                    graph: .named(
                        .rdf(try RDFTerm.iri(validating: "urn:calendar"))
                    )
                ),
                container: container
            )
        }
    }

    private func resolve(
        _ source: GraphAlgorithmOperation.Source,
        container: DBContainer
    ) async throws -> ResolvedDatabaseGraphSource {
        let resolver = SchemaDatabaseGraphSourceResolver(container: container)
        return try await StorageTransactionExecutor(
            engine: container.engine
        ).withTransaction(
            configuration: .readOnly,
            clock: TestProcessMonotonicClock()
        ) { transaction in
            try await resolver.resolve(
                source,
                transaction: transaction
            )
        }
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [
                    try DatabaseGraphSourceEdge.schemaEntity,
                    try DatabaseSHACLStatement.schemaEntity,
                    try DefaultGraphSourceStatement.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration.testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DatabaseGraphSourceEdge.self), try DatabaseFrameworkRuntime.entity(DatabaseSHACLStatement.self), try DatabaseFrameworkRuntime.entity(DefaultGraphSourceStatement.self)]
            ),
            security: .disabled
        )
    }
}
