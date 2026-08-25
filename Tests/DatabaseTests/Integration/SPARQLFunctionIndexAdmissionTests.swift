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
    var resource: RDFTerm = .iri(.xsdString)
}

@Persistable(type: "SPARQLFunctionAdmissionStatement")
private struct SPARQLFunctionAdmissionStatement {
    #Directory<SPARQLFunctionAdmissionStatement>(
        "sparql_function_admission_statements"
    )
    #Index(
        .graph(
            name: "SPARQLFunctionAdmissionStatement_rdf",
            definition: .rdf(
                subject: \SPARQLFunctionAdmissionStatement.subject,
                predicate: \SPARQLFunctionAdmissionStatement.predicate,
                object: \SPARQLFunctionAdmissionStatement.object, graph: nil)))

    var id: String = ""
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
}

@Persistable(type: "SPARQLFunctionAmbiguousStatement")
private struct SPARQLFunctionAmbiguousStatement {
    #Directory<SPARQLFunctionAmbiguousStatement>(
        "sparql_function_ambiguous_statements"
    )
    #Index(
        .graph(
            name: "SPARQLFunctionAmbiguousStatement_first",
            definition: .rdf(
                subject: \SPARQLFunctionAmbiguousStatement.subject,
                predicate: \SPARQLFunctionAmbiguousStatement.predicate,
                object: \SPARQLFunctionAmbiguousStatement.object,
                graph: nil
            )
        )
    )
    #Index(
        .graph(
            name: "SPARQLFunctionAmbiguousStatement_second",
            definition: .rdf(
                subject: \SPARQLFunctionAmbiguousStatement.subject,
                predicate: \SPARQLFunctionAmbiguousStatement.predicate,
                object: \SPARQLFunctionAmbiguousStatement.object,
                graph: nil
            )
        )
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
        defer { await scenario.container.shutdown() }
        let context = scenario.container.testBaseContext()
        let subject = try RDFTerm.iri(
            validating: "urn:database-framework:admitted-subject"
        )
        try context.insert(
            SPARQLFunctionAdmissionUser(
                id: "admitted-user",
                resource: subject
            )
        )
        try context.insert(
            SPARQLFunctionAdmissionStatement(
                id: "admitted-statement",
                subject: subject,
                predicate: try .iri(
                    validating: "urn:predicate:admitted"
                ),
                object: .literal(
                    RDFLiteral(
                        lexicalForm: "value",
                        datatype: .xsdString
                    )
                )
            )
        )
        try await context.save()

        let rows = try await context.executeSQL(
            Self.sql,
            as: SPARQLFunctionAdmissionUser.self
        )

        #expect(rows.map(\.id) == ["admitted-user"])
    }

    @Test("SQL SPARQL rejects a missing lifecycle state")
    func missingStateFails() async throws {
        let scenario = try await makeScenario()
        defer { await scenario.container.shutdown() }
        let descriptor = try #require(
            try SPARQLFunctionAdmissionStatement.indexDescriptors.first
        )
        let directory = try await scenario.container.testBaseDirectory(
            for: SPARQLFunctionAdmissionStatement.self
        )
        let lifecycleStore = IndexLifecycleStore(
            container: scenario.container,
            subspace: directory
        )
        let identity = try lifecycleStore.storageIdentity(for: descriptor.name)
        let stateKey = directory
            .subspace("state")
            .subspace(identity.name)
            .pack(
                Tuple(
                    identity.definitionFingerprint.bytes,
                    identity.layoutFingerprint
                )
            )
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

    @Test("SQL SPARQL reports a missing RDF dataset index exactly")
    func missingDatasetIndexFailsExactly() async throws {
        let scenario = try await makeScenario()
        defer { await scenario.container.shutdown() }
        let sql = """
            SELECT * FROM SPARQLFunctionAdmissionUser
            WHERE resource IN (
                SPARQL(
                    SPARQLFunctionAdmissionUser,
                    'SELECT ?s WHERE { ?s <urn:predicate:missing> "value" }'
                )
            )
            """

        do {
            _ = try await scenario.container.testBaseContext().executeSQL(
                sql,
                as: SPARQLFunctionAdmissionUser.self
            )
            Issue.record("Expected the missing RDF dataset index to fail")
        } catch let error as SPARQLFunctionError {
            guard case .graphIndexNotFound(let entityName) = error else {
                Issue.record("Expected graphIndexNotFound, got \(error)")
                return
            }
            #expect(
                entityName == SPARQLFunctionAdmissionUser.persistableType
            )
        }
    }

    @Test("SQL SPARQL reports ambiguous RDF dataset indexes exactly")
    func ambiguousDatasetIndexesFailExactly() async throws {
        let container = try await makeAmbiguousScenario()
        defer { await container.shutdown() }
        let sql = """
            SELECT * FROM SPARQLFunctionAdmissionUser
            WHERE resource IN (
                SPARQL(
                    SPARQLFunctionAmbiguousStatement,
                    'SELECT ?s WHERE { ?s <urn:predicate:any> "value" }'
                )
            )
            """

        do {
            _ = try await container.testBaseContext().executeSQL(
                sql,
                as: SPARQLFunctionAdmissionUser.self
            )
            Issue.record("Expected ambiguous RDF dataset indexes to fail")
        } catch let error as SPARQLFunctionError {
            #expect(
                error == .ambiguousGraphIndexes(
                    typeName: SPARQLFunctionAmbiguousStatement
                        .persistableType,
                    candidates: [
                        "SPARQLFunctionAmbiguousStatement:SPARQLFunctionAmbiguousStatement_first",
                        "SPARQLFunctionAmbiguousStatement:SPARQLFunctionAmbiguousStatement_second",
                    ]
                )
            )
        }
    }

    private static let sql = """
        SELECT * FROM SPARQLFunctionAdmissionUser
        WHERE resource IN (
            SPARQL(
                SPARQLFunctionAdmissionStatement,
                'SELECT ?s WHERE { ?s <urn:predicate:admitted> "value" }'
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
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
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

    private func makeAmbiguousScenario() async throws -> DBContainer {
        try await DBContainer.open(
            for: try Schema(
                entities: [
                    try SPARQLFunctionAdmissionUser.schemaEntity,
                    try SPARQLFunctionAmbiguousStatement.schemaEntity,
                ]
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-ambiguous-rdf-index-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SPARQLFunctionAdmissionUser.self
                    ),
                    try DatabaseFrameworkRuntime.entity(
                        SPARQLFunctionAmbiguousStatement.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
    }
}
