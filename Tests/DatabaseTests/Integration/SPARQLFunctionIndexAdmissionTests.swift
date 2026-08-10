import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import Database
@testable import DatabaseEngine

@Persistable(type: "SPARQLFunctionAdmissionUser")
private struct SPARQLFunctionAdmissionUser {
    #Directory<SPARQLFunctionAdmissionUser>(
        "sparql_function_admission_users"
    )

    var id: String = ""
}

@Persistable(type: "SPARQLFunctionAdmissionStatement")
private struct SPARQLFunctionAdmissionStatement {
    #Directory<SPARQLFunctionAdmissionStatement>(
        "sparql_function_admission_statements"
    )
    #Index(
        .rdfDataset,
        from: \SPARQLFunctionAdmissionStatement.subject,
        edge: \SPARQLFunctionAdmissionStatement.predicate,
        to: \SPARQLFunctionAdmissionStatement.object,
        name: "SPARQLFunctionAdmissionStatement_rdf"
    )

    var id: String = ""
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
}

@Suite("SPARQL SQL function index admission")
struct SPARQLFunctionIndexAdmissionTests {
    @Test("SQL SPARQL reads an admitted index")
    func admittedIndexExecutes() async throws {
        let scenario = try await makeScenario()

        let rows = try await scenario.container.testBaseContext().executeSQL(
            Self.sql,
            as: SPARQLFunctionAdmissionUser.self
        )

        #expect(rows.isEmpty)
    }

    @Test("SQL SPARQL rejects a missing lifecycle state")
    func missingStateFails() async throws {
        let scenario = try await makeScenario()
        let descriptor = try #require(
            try SPARQLFunctionAdmissionStatement.indexDescriptors.first
        )
        let directory = try await scenario.container.testBaseDirectory(
            for: SPARQLFunctionAdmissionStatement.self
        )
        let stateKey = directory
            .subspace("state")
            .pack(Tuple(descriptor.name))
        try await scenario.engine.withTransaction { transaction in
            try transaction.clear(key: stateKey)
        }

        do {
            _ = try await scenario.container.testBaseContext().executeSQL(
                Self.sql,
                as: SPARQLFunctionAdmissionUser.self
            )
            Issue.record("Expected SQL SPARQL index admission to fail")
        } catch let error as IndexStateError {
            #expect(
                error == .missingPersistedState(index: descriptor.name)
            )
        }
    }

    private static let sql = """
        SELECT * FROM SPARQLFunctionAdmissionUser
        WHERE id IN (
            SPARQL(
                SPARQLFunctionAdmissionStatement,
                'SELECT ?s WHERE { ?s <urn:predicate:missing> "value" }'
            )
        )
        """

    private func makeScenario() async throws -> (
        container: DBContainer,
        engine: InMemoryEngine
    ) {
        let engine = InMemoryEngine()
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try SPARQLFunctionAdmissionUser.schemaEntity,
                    try SPARQLFunctionAdmissionStatement.schemaEntity,
                ]
            ),
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SPARQLFunctionAdmissionUser.self
                    ),
                    try DatabaseFrameworkRuntime.entity(
                        SPARQLFunctionAdmissionStatement.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
        return (container, engine)
    }
}
