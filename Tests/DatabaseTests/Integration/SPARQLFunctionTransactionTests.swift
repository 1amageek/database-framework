#if GraphIndexes
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@testable import Database
@testable import DatabaseEngine

@Persistable(type: "SPARQLTransactionUser")
private struct SPARQLTransactionUser {
    #Directory<SPARQLTransactionUser>("sparql_transaction_users")

    var id: String = ""
    var resource: RDFTerm = .iri(.xsdString)
    var name: String = ""
}

@Persistable(type: "SPARQLTransactionStatement")
private struct SPARQLTransactionStatement {
    #Directory<SPARQLTransactionStatement>("sparql_transaction_statements")
    #Index(
        .graph(
            name: "SPARQLTransactionStatement_rdf_quad_subject_predicate_object",
            definition: .rdf(
                subject: \SPARQLTransactionStatement.subject,
                predicate: \SPARQLTransactionStatement.predicate,
                object: \SPARQLTransactionStatement.object, graph: nil)))

    var id: String = ""
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
}

@Suite("SPARQL SQL transaction contract")
struct SPARQLFunctionTransactionTests {
    @Test("SPARQL transaction routing is limited to queries that require rewriting")
    func detectsOnlySPARQLFunctions() {
        let plainQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("SPARQLTransactionUser")),
            filter: .equal(
                .function(
                    FunctionCall(
                        name: "LOWER",
                        arguments: [.column(ColumnRef("name"))]
                    )
                ),
                .literal(.string("alice"))
            )
        )
        let graphQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("SPARQLTransactionUser")),
            filter: .inList(
                .column(ColumnRef("resource")),
                values: [
                    .function(
                        FunctionCall(
                            name: "SPARQL",
                            arguments: [
                                .column(ColumnRef("SPARQLTransactionStatement")),
                                .literal(.string("SELECT ?s WHERE { ?s ?p ?o }")),
                            ]
                        )
                    )
                ]
            )
        )
        let nestedGraphQuery = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .subquery(
                        SelectQuery(
                            projection: .items([
                                ProjectionItem(.column(ColumnRef("id"))),
                            ]),
                            source: .table(
                                TableRef("SPARQLTransactionUser")
                            ),
                            filter: graphQuery.filter
                        )
                    ),
                    alias: "nested_id"
                ),
            ]),
            source: .values([[]], columnNames: [])
        )
        let graphPatternQuery = SelectQuery(
            projection: .all,
            source: .graphPattern(
                .filter(
                    .values(variables: [], bindings: [[]]),
                    .inList(
                        .literal(.string("resource")),
                        values: [
                            .function(
                                FunctionCall(
                                    name: "SPARQL",
                                    arguments: [
                                        .column(
                                            ColumnRef(
                                                "SPARQLTransactionStatement"
                                            )
                                        ),
                                        .literal(
                                            .string(
                                                "SELECT ?s WHERE { ?s ?p ?o }"
                                            )
                                        ),
                                    ]
                                )
                            ),
                        ]
                    )
                )
            )
        )

        #expect(!SPARQLFunctionRewriter.containsSPARQLFunction(in: plainQuery))
        #expect(SPARQLFunctionRewriter.containsSPARQLFunction(in: graphQuery))
        #expect(
            SPARQLFunctionRewriter.containsSPARQLFunction(
                in: nestedGraphQuery
            )
        )
        #expect(
            SPARQLFunctionRewriter.containsSPARQLFunction(
                in: graphPatternQuery
            )
        )
    }

    @Test("SPARQL rewrite and parent SQL read share one transaction")
    func sharesOneTransaction() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()

        let userIRI = try RDFIRI("urn:user:alice")
        var user = SPARQLTransactionUser()
        user.id = userIRI.rawValue
        user.resource = .iri(userIRI)
        user.name = "Alice"

        var statement = SPARQLTransactionStatement()
        statement.id = "statement-1"
        statement.subject = .iri(userIRI)
        statement.predicate = try .iri(validating: "urn:predicate:active")
        statement.object = .literal(
            RDFLiteral(lexicalForm: "true", datatype: .xsdString)
        )

        try context.insert(user)
        try context.insert(statement)
        try await context.save()

        let transactionCountBeforeRead = scenario.engine.transactionCount
        let users = try await context.executeSQL(
            """
            SELECT * FROM SPARQLTransactionUser
            WHERE resource IN (
                SPARQL(
                    SPARQLTransactionStatement,
                    'SELECT ?s WHERE { ?s <urn:predicate:active> "true" }'
                )
            )
            """,
            as: SPARQLTransactionUser.self
        )

        #expect(users.map(\.id) == [user.id])
        #expect(
            scenario.engine.transactionCount - transactionCountBeforeRead == 1
        )
    }

    @Test("SPARQL RDF terms are not silently coerced to SQL strings")
    func preservesRDFTermIdentity() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()

        let userIRI = try RDFIRI("urn:user:alice")
        var user = SPARQLTransactionUser()
        user.id = userIRI.rawValue
        user.resource = .iri(userIRI)
        user.name = "Alice"

        var statement = SPARQLTransactionStatement()
        statement.id = "statement-1"
        statement.subject = .iri(userIRI)
        statement.predicate = try .iri(validating: "urn:predicate:active")
        statement.object = .literal(
            RDFLiteral(lexicalForm: "true", datatype: .xsdString)
        )

        try context.insert(user)
        try context.insert(statement)
        try await context.save()

        let users = try await context.executeSQL(
            """
            SELECT * FROM SPARQLTransactionUser
            WHERE id IN (
                SPARQL(
                    SPARQLTransactionStatement,
                    'SELECT ?s WHERE { ?s <urn:predicate:active> "true" }'
                )
            )
            """,
            as: SPARQLTransactionUser.self
        )

        #expect(users.isEmpty)
    }

    private func makeScenario() async throws -> (
        container: DBContainer,
        engine: SPARQLTransactionCountingEngine
    ) {
        let engine = SPARQLTransactionCountingEngine()
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try SPARQLTransactionUser.schemaEntity,
                    try SPARQLTransactionStatement.schemaEntity,
                ]
            ),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SPARQLTransactionUser.self
                    ),
                    try DatabaseFrameworkRuntime.entity(
                        SPARQLTransactionStatement.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
        return (container, engine)
    }
}

private final class SPARQLTransactionCountingEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {}

    typealias TransactionType = InMemoryTransaction

    private let engine = InMemoryEngine()
    private let transactionCountState = Mutex(0)

    init(configuration: Configuration = Configuration()) {
        _ = configuration
    }

    func createTransaction() throws -> InMemoryTransaction {
        let transaction = try engine.createTransaction()
        transactionCountState.withLock { $0 += 1 }
        return transaction
    }

    var namespaceResolver: any NamespaceResolver {
        engine.namespaceResolver
    }

    var namespaceCatalog: (any NamespaceCatalog)? {
        engine.namespaceCatalog
    }

    func requestShutdown() {
        engine.requestShutdown()
    }

    func waitUntilShutdown() async {
        await engine.waitUntilShutdown()
    }

    var transactionCount: Int {
        transactionCountState.withLock { $0 }
    }
}
#endif
