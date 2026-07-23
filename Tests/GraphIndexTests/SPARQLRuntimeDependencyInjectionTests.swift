import Core
import DatabaseRuntime
import DatabaseValue
import Graph
import QueryIR
import StorageKit
import TestHeartbeat
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("SPARQL runtime dependency injection", .serialized, .heartbeat)
struct SPARQLRuntimeDependencyInjectionTests {
    @Persistable
    struct Statement {
        #Directory<Statement>("sparql_runtime_dependency_injection")

        var id: String = ULID().ulidString
        var subject: DatabaseRDFTerm = .iri("did:example:subject")
        var predicate: DatabaseRDFTerm = .iri("did:example:predicate")
        var object: DatabaseRDFTerm = .iri("did:example:object")

        #Index(RDFQuadIndexKind<Statement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object
        ))
    }

    private struct IdentityFunction: SPARQLFunction {
        let identifier: DatabaseRDFIRI

        func evaluate(arguments: [FieldValue]) throws -> FieldValue {
            guard arguments.count == 1 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(
                    identifier.rawValue
                )
            }
            return arguments[0]
        }
    }

    @Test("The container-scoped registry reaches normal and transaction reads")
    func registryIsInjectedIntoEveryReadPath() async throws {
        let functionIdentifier = try DatabaseRDFIRI("did:example:identity")
        let functionRegistry = try SPARQLFunctionRegistry([
            IdentityFunction(identifier: functionIdentifier)
        ])
        let container = try await DBContainer(
            testing: Schema(
                [Statement.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .init(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                sparqlFunctionRegistry: functionRegistry
            ),
            security: .disabled
        )
        let context = container.newContext()
        context.insert(Statement())
        try await context.save()

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("subject"))),
                ProjectionItem(
                    .function(
                        FunctionCall(
                            name: functionIdentifier.rawValue,
                            arguments: [.variable(Variable("subject"))]
                        )
                    ),
                    alias: "identity"
                ),
            ]),
            source: .graphPattern(
                .basic([
                    TriplePattern(
                        subject: .variable("subject"),
                        predicate: .iri("did:example:predicate"),
                        object: .variable("object")
                    )
                ])
            )
        )

        let ordinary = try await context.query(query)
        let executor = try #require(
            container.runtimeConfiguration.logicalSourceExecutors
                .sparqlExecutor
        )
        let transactional = try await container.engine.withTransaction {
            transaction in
            try await executor.executeInTransaction(
                context: container.newContext(),
                selectQuery: query,
                options: ReadExecutionContext(),
                partitions: [],
                transaction: transaction
            )
        }

        for response in [ordinary, transactional] {
            #expect(response.rows.count == 1)
            #expect(
                response.rows[0].fields["subject"]
                    == .rdfTerm(.iri("did:example:subject"))
            )
            #expect(
                response.rows[0].fields["identity"]
                    == .rdfTerm(.iri("did:example:subject"))
            )
        }
    }
}
