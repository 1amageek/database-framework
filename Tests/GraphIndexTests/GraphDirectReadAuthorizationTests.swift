import DatabaseKit
import DatabaseTypes
import GraphIndex
import StorageKit
import TestHeartbeat
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Graph direct read authorization", .heartbeat)
struct GraphDirectReadAuthorizationTests {
    @Persistable(type: "SecuredDirectRDFStatement")
    struct RDFStatement: SecurityPolicy {
        #Directory<RDFStatement>("secured_direct_rdf_statements")

        var id: String = ""

        @Restricted(read: .roles(["graph-reader"]))
        var subject: RDFTerm = .iri(.xsdString)

        var predicate: RDFTerm = .iri(.xsdString)
        var object: RDFTerm = .iri(.xsdString)

        #Index(
            .graph(
                name: "secured_direct_rdf_index",
                definition: .rdf(
                    subject: \RDFStatement.subject,
                    predicate: \RDFStatement.predicate,
                    object: \RDFStatement.object,
                    graph: \RDFStatement.graph
                )
            )
        )

        var graph: RDFTerm? = nil

        static func permitsRead(
            of resource: borrowing RDFStatement,
            in context: borrowing AuthorizationContext
        ) -> Bool { true }

        static func permitsQuery(
            _ query: borrowing SecurityQuery,
            in context: borrowing AuthorizationContext
        ) -> Bool { true }

        static func permitsCreate(
            _ newResource: borrowing RDFStatement,
            in context: borrowing AuthorizationContext
        ) -> Bool { true }
    }

    @Persistable(type: "SecuredDirectPropertyEdge")
    struct PropertyEdge: SecurityPolicy {
        #Directory<PropertyEdge>("secured_direct_property_edges")

        var id: String = ""

        @Restricted(read: .roles(["graph-reader"]))
        var source: String = ""

        var label: String = ""
        var target: String = ""

        #Index(
            .graph(
                name: "secured_direct_property_index",
                definition: .property(
                    source: \PropertyEdge.source,
                    label: .field(\PropertyEdge.label),
                    target: \PropertyEdge.target,
                    graph: nil,
                    strategy: .tripleStore
                )
            )
        )

        static func permitsRead(
            of resource: borrowing PropertyEdge,
            in context: borrowing AuthorizationContext
        ) -> Bool { true }

        static func permitsQuery(
            _ query: borrowing SecurityQuery,
            in context: borrowing AuthorizationContext
        ) -> Bool { true }

        static func permitsCreate(
            _ newResource: borrowing PropertyEdge,
            in context: borrowing AuthorizationContext
        ) -> Bool { true }
    }

    @Test("Typed SPARQL entry points reject restricted index fields")
    func typedSPARQLEntryPointsRejectRestrictedFields() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        await #expect {
            _ = try await context.sparql(RDFStatement.self)
                .defaultIndex()
                .where("?subject", "?predicate", "?object")
                .execute()
        } throws: { error in
            Self.isReadDenial(
                error,
                entity: RDFStatement.persistableType,
                field: "subject"
            )
        }

        await #expect {
            _ = try await context.executeSPARQLPattern(
                .basic([
                    ExecutionTriple("?subject", "?predicate", "?object")
                ]),
                on: RDFStatement.self
            )
        } throws: { error in
            Self.isReadDenial(
                error,
                entity: RDFStatement.persistableType,
                field: "subject"
            )
        }
    }

    @Test("Named-graph SPARQL rejects restricted index fields")
    func namedGraphSPARQLRejectsRestrictedFields() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let graph = try RDFGraphName(
            iri: "https://database-framework.test/secured-graph"
        )

        await #expect {
            _ = try await context.sparql(namedGraph: graph)
                .where("?subject", "?predicate", "?object")
                .execute()
        } throws: { error in
            Self.isReadDenial(
                error,
                entity: RDFStatement.persistableType,
                field: "subject"
            )
        }
    }

    @Test("GRAPH_TABLE rejects restricted index fields")
    func graphTableRejectsRestrictedFields() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let source = GraphTableSource.match(
            graph: "SecuredGraph",
            from: NodePattern(variable: "source"),
            via: EdgePattern(labels: ["follows"], direction: .outgoing),
            to: NodePattern(variable: "target")
        )

        await #expect {
            _ = try await context.graphTable(PropertyEdge.self, source: source)
        } throws: { error in
            Self.isReadDenial(
                error,
                entity: PropertyEdge.persistableType,
                field: "source"
            )
        }
    }

    @Test("Shortest path rejects restricted index fields")
    func shortestPathRejectsRestrictedFields() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        await #expect {
            _ = try await context.shortestPath(PropertyEdge.self)
                .defaultIndex()
                .from("alice")
                .to("bob")
                .execute()
        } throws: { error in
            Self.isReadDenial(
                error,
                entity: PropertyEdge.persistableType,
                field: "source"
            )
        }
    }

    @Test("Property graph queries reject restricted index fields")
    func propertyGraphQueriesRejectRestrictedFields() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        await #expect {
            _ = try await context.graph(PropertyEdge.self)
                .defaultIndex()
                .execute()
        } throws: { error in
            Self.isReadDenial(
                error,
                entity: PropertyEdge.persistableType,
                field: "source"
            )
        }
    }

    @Test("Graph algorithms reject restricted index fields")
    func graphAlgorithmsRejectRestrictedFields() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        await #expect {
            _ = try await context.graphAlgorithm(PropertyEdge.self)
                .defaultIndex()
                .pageRank()
                .compute()
        } throws: { error in
            Self.isReadDenial(
                error,
                entity: PropertyEdge.persistableType,
                field: "source"
            )
        }
    }

    private func makeContainer() async throws -> DBContainer {
        let propertyProvider = GraphIndexMaintainerProvider()
        let rdfProvider = RDFQuadIndexMaintainerProvider()
        var rdfRuntime = try EntityRuntimeDefinition(RDFStatement.self)
        try rdfRuntime.register(rdfProvider)
        var propertyRuntime = try EntityRuntimeDefinition(PropertyEdge.self)
        try propertyRuntime.register(propertyProvider)

        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [
                    try RDFStatement.schemaEntity,
                    try PropertyEdge.schemaEntity,
                ],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "graph-direct-read-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: propertyProvider),
                    .init(describing: rdfProvider),
                ],
                graphTableSourceExecutor: GraphTableReadExecutors.sourceExecutor,
                sparqlSourceExecutor: SPARQLReadExecutors.sourceExecutor(
                    functionRegistry: .empty
                ),
                entityRuntimes: [
                    rdfRuntime.registration(),
                    propertyRuntime.registration(),
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(RDFStatement.self),
                    AuthorizationPolicyHandler(PropertyEdge.self),
                ]
            )
        )
        let context = container.testBaseContext()

        var defaultStatement = RDFStatement()
        defaultStatement.subject = try .iri(
            validating: "https://database-framework.test/alice"
        )
        defaultStatement.predicate = try .iri(
            validating: "https://database-framework.test/knows"
        )
        defaultStatement.object = try .iri(
            validating: "https://database-framework.test/bob"
        )
        try context.insert(defaultStatement)

        var namedStatement = defaultStatement
        namedStatement.id = "named-statement"
        namedStatement.graph = try .iri(
            validating: "https://database-framework.test/secured-graph"
        )
        try context.insert(namedStatement)

        var edge = PropertyEdge()
        edge.source = "alice"
        edge.label = "follows"
        edge.target = "bob"
        try context.insert(edge)
        try await context.save()

        return container
    }

    private static func isReadDenial(
        _ error: any Error,
        entity: String,
        field: String
    ) -> Bool {
        guard case .readNotAllowed(let type, let fields) = error
            as? FieldSecurityError
        else {
            return false
        }
        return type == entity && fields == [field]
    }
}
