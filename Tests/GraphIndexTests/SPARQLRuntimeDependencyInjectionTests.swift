#if DATABASE_RUNTIME_TEST_GRAPH_INDEXES
import DatabaseKit
import TestSupport
import DatabaseRuntime
import DatabaseTypes
import Foundation
import StorageKit
import TestHeartbeat
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine
@testable import GraphIndex

@Suite("SPARQL runtime dependency injection", .serialized, .heartbeat)
struct SPARQLRuntimeDependencyInjectionTests {
    @Persistable
    struct Statement {
        #Directory<Statement>("sparql_runtime_dependency_injection")

        var id: String = Foundation.UUID().uuidString
        var subject: RDFTerm = .iri(.xsdString)
        var predicate: RDFTerm = .iri(.xsdString)
        var object: RDFTerm = .iri(.xsdString)

        #Index(
            .graph(
                name: "Statement_rdf_quad_subject_predicate_object",
                definition: .rdf(
                    subject: \Statement.subject, predicate: \Statement.predicate,
                    object: \Statement.object,
                    graph: nil)))
    }

    private struct IdentityFunction: SPARQLFunction {
        let identifier: RDFIRI

        func evaluate(
            arguments: [FieldValue]
        ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
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
        let functionIdentifier = try RDFIRI("did:example:identity")
        let functionRegistry = try SPARQLFunctionRegistry([
            IdentityFunction(identifier: functionIdentifier)
        ])
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try Statement.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(Statement.self)],
                sparqlFunctionRegistry: functionRegistry
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let subject = try RDFTerm.iri(validating: "did:example:subject")
        let predicate = try RDFTerm.iri(
            validating: "did:example:predicate"
        )
        let object = try RDFTerm.iri(validating: "did:example:object")
        try context.insert(
            Statement(
                subject: subject,
                predicate: predicate,
                object: object
            )
        )
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
        let transactional = try await container.withTestBaseTransaction {
            transaction in
            try await executor.executeInTransaction(
                context: context,
                selectQuery: query,
                options: ReadExecutionContext(
                    monotonicClock: TestProcessMonotonicClock()
                ),
                partitions: FieldObject(),
                transaction: transaction
            )
        }

        let expectedSubject = FieldValue.rdfTerm(subject)
        #expect(ordinary.rows.count == 1)
        #expect(ordinary.rows[0].fields["subject"] == expectedSubject)
        #expect(ordinary.rows[0].fields["identity"] == expectedSubject)
        #expect(transactional.rows.count == 1)
        #expect(transactional.rows[0].fields["subject"] == expectedSubject)
        #expect(transactional.rows[0].fields["identity"] == expectedSubject)
    }
}
#endif
