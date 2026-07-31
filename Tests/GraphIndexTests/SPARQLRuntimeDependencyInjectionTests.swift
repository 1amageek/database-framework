import DatabaseKit
import TestSupport
import DatabaseRuntime
import DatabaseTypes
import Foundation
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

        var id: String = Foundation.UUID().uuidString
        var subject: RDFTerm = .iri(.xsdString)
        var predicate: RDFTerm = .iri(.xsdString)
        var object: RDFTerm = .iri(.xsdString)

        #Index(
            .rdfDataset,
            from: \Statement.subject,
            edge: \Statement.predicate,
            to: \Statement.object
        )
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
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(Statement.self)],
                sparqlFunctionRegistry: functionRegistry
            ),
            security: .disabled
        )
        let context = container.newContext()
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
        let transactional = try await container.engine.withTransaction {
            transaction in
            try await executor.executeInTransaction(
                context: container.newContext(),
                selectQuery: query,
                options: ReadExecutionContext(
                    monotonicClock: TestProcessMonotonicClock()
                ),
                partitions: FieldObject(),
                transaction: transaction
            )
        }

        let expectedSubject = FieldValue.rdfTerm(subject)
        for response in [ordinary, transactional] {
            #expect(response.rows.count == 1)
            #expect(
                response.rows[0].fields["subject"]
                    == expectedSubject
            )
            #expect(
                response.rows[0].fields["identity"]
                    == expectedSubject
            )
        }
    }
}
