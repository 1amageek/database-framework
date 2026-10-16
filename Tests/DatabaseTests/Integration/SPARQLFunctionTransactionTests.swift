#if GraphIndexes
import DatabaseKit
import Foundation
import QueryAST
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
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
                        name: "lower",
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
                            name: "sparql",
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
                                    name: "sparql",
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
        let embedded = DatabaseKit.Expression.function(FunctionCall(name: "sparql", arguments: []))
        let scalarOperands: [DatabaseKit.Expression] = [
            .concatenate(.string("prefix"), embedded),
            .patternMatch(subject: embedded, pattern: .string("%"), escape: nil, kind: .like),
            .patternMatch(subject: .string("value"), pattern: embedded, escape: nil, kind: .like),
            .patternMatch(subject: .string("value"), pattern: .string("%"), escape: embedded, kind: .like),
        ]
        for expression in scalarOperands {
            let query = SelectQuery(projection: .items([ProjectionItem(expression)]),
                                    source: .values([[]], columnNames: []))
            #expect(SPARQLFunctionRewriter.containsSPARQLFunction(in: query))
        }
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
            SELECT * FROM "SPARQLTransactionUser"
            WHERE resource IN (
                SPARQL(
                    "SPARQLTransactionStatement",
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

    @Test("prepared SPARQL literals stay reserved through SQL execution")
    func retainsPreparedLiteralsThroughExecution() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let userIRI = try RDFIRI("urn:user:retained")

        var user = SPARQLTransactionUser()
        user.id = userIRI.rawValue
        user.resource = .iri(userIRI)
        user.name = "Retained"

        var statement = SPARQLTransactionStatement()
        statement.id = "retained-statement"
        statement.subject = .iri(userIRI)
        statement.predicate = try .iri(validating: "urn:predicate:active")
        statement.object = .literal(
            RDFLiteral(lexicalForm: "true", datatype: .xsdString)
        )

        try context.insert(user)
        try context.insert(statement)
        try await context.save()

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("SPARQLTransactionUser")),
            filter: .inList(
                .column(ColumnRef("resource")),
                values: [
                    .function(
                        FunctionCall(
                            name: "sparql",
                            arguments: [
                                .column(
                                    ColumnRef("SPARQLTransactionStatement")
                                ),
                                .literal(
                                    .string(
                                        "SELECT ?s WHERE { ?s <urn:predicate:active> \"true\" }"
                                    )
                                ),
                            ]
                        )
                    )
                ]
            )
        )
        let options = ReadExecutionOptions()
        let workMeter = DatabaseWorkMeter(
            budget: options.budget,
            monotonicClock: scenario.container.monotonicClock
        )
        let execution = ReadExecutionContext(
            options: options,
            monotonicClock: scenario.container.monotonicClock,
            workMeter: workMeter
        )

        let response = try await context.indexQueryContext.withSession(
            workMeter: workMeter
        ) {
            session in
            let sessionBaselineRows = workMeter.retainedIntermediateRows
            let sessionBaselineBytes = workMeter.retainedIntermediateBytes
            let foreignMeter = DatabaseWorkMeter(
                budget: options.budget,
                monotonicClock: scenario.container.monotonicClock
            )
            await #expect(
                throws: DatabaseReadSessionError.workMeterMismatch
            ) {
                _ = try await context
                    .prepareSQLSelectForCanonicalExecution(
                        query,
                        workMeter: foreignMeter,
                        session: session,
                        structuralLimits: .default
                    )
            }
            #expect(
                workMeter.retainedIntermediateRows == sessionBaselineRows
            )
            #expect(
                workMeter.retainedIntermediateBytes == sessionBaselineBytes
            )
            #expect(foreignMeter.retainedIntermediateRows == 0)
            #expect(foreignMeter.retainedIntermediateBytes == 0)

            let prepared = try await context
                .prepareSQLSelectForCanonicalExecution(
                    query,
                    workMeter: workMeter,
                    session: session,
                    structuralLimits: .default
            )
            #expect(
                workMeter.retainedIntermediateRows == sessionBaselineRows
            )
            #expect(
                workMeter.retainedIntermediateBytes > sessionBaselineBytes
            )
            let foreignExecution = ReadExecutionContext(
                options: options,
                monotonicClock: scenario.container.monotonicClock,
                workMeter: DatabaseWorkMeter(
                    budget: options.budget,
                    monotonicClock: scenario.container.monotonicClock
                )
            )
            await #expect(
                throws: DatabasePreparedSQLSelectError.workMeterMismatch
            ) {
                _ = try await prepared.execute(
                    in: session,
                    execution: foreignExecution
                )
            }
            #expect(
                workMeter.retainedIntermediateRows == sessionBaselineRows
            )
            #expect(
                workMeter.retainedIntermediateBytes > sessionBaselineBytes
            )
            let response = try await prepared.execute(
                in: session,
                execution: execution
            )
            withExtendedLifetime(prepared) {
                #expect(workMeter.retainedIntermediateRows == sessionBaselineRows)
                #expect(workMeter.retainedIntermediateBytes > sessionBaselineBytes)
            }
            return response
        }

        #expect(response.rows.count == 1)
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
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

        do {
            _ = try await context.executeSQL(
                """
                SELECT * FROM "SPARQLTransactionUser"
                WHERE id IN (
                    SPARQL(
                        "SPARQLTransactionStatement",
                        'SELECT ?s WHERE { ?s <urn:predicate:active> "true" }'
                    )
                )
                """,
                as: SPARQLTransactionUser.self
            )
            Issue.record("Expected an incompatible SQL string and RDF term comparison to fail")
        } catch let failure as SQLBindingSourceFailure {
            guard case .expression(.typeMismatch(operation: "common result type")) = failure.cause else {
                Issue.record("Expected an incompatible RDF membership type")
                return
            }
        }

        let users = try await context.executeSQL(
            """
            SELECT * FROM "SPARQLTransactionUser"
            WHERE resource IN (
                SPARQL(
                    "SPARQLTransactionStatement",
                    'SELECT ?s WHERE { ?s <urn:predicate:active> "true" }'
                )
            )
            """,
            as: SPARQLTransactionUser.self
        )
        #expect(users.map(\.id) == [user.id])
        #expect(users.first?.resource == .iri(userIRI))
    }

    @Test("Invalid surrounding SQL rejects before any graph range read")
    func invalidSQLPrecedesGraphReads() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        let sql = """
        SELECT absent FROM "SPARQLTransactionUser"
        WHERE resource IN (SPARQL("SPARQLTransactionStatement", 'SELECT ?s WHERE { ?s ?p ?o }'))
        """
        let before = scenario.engine.rangeReadCount
        do {
            _ = try await context.executeSQL(sql)
            Issue.record("Expected the invalid SQL target list to reject admission")
        } catch let failure as SQLBindingSourceFailure {
            guard case .expression(.missingColumn("absent")) = failure.cause else {
                Issue.record("Expected the exact unknown SQL column rejection")
                return
            }
            #expect(failure.position == 7)
        }
        #expect(scenario.engine.rangeReadCount == before)
        await scenario.container.shutdown()
    }

    @Test("Generated graph membership failures point to their SQL producer")
    func generatedMembershipFailureHasProducerSource() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        try await seedGraph(context)
        let sql = """
        SELECT id FROM "SPARQLTransactionUser"
        WHERE id IN (SPARQL("SPARQLTransactionStatement", 'SELECT ?s WHERE { ?s <urn:predicate:active> "true" }'))
        """
        let expected = sql.range(of: "SPARQL(").map { sql.utf8.distance(from: sql.utf8.startIndex, to: $0.lowerBound) }
        let before = scenario.engine.rangeReadCount
        do {
            _ = try await context.executeSQL(sql)
            Issue.record("Expected generated RDF terms to remain incompatible with SQL text")
        } catch let failure as SQLBindingSourceFailure {
            guard case .expression(.typeMismatch(operation: "common result type")) = failure.cause else {
                Issue.record("Expected the generated RDF membership type rejection")
                return
            }
            #expect(failure.position == expected)
        }
        #expect(scenario.engine.rangeReadCount > before)
        await scenario.container.shutdown()
    }

    @Test("SQL after a graph producer preserves the decisive CAST occurrence")
    func producerDoesNotShiftFollowingParameterSource() async throws {
        let scenario = try await makeScenario()
        let context = scenario.container.testBaseContext()
        try await seedGraph(context)
        let queryText = "SELECT ?s WHERE { ?s <urn:predicate:active> \"true\" }"
        let valid = try await context.executeSQL(
            "SELECT id FROM \"SPARQLTransactionUser\" WHERE resource IN (SPARQL(\"SPARQLTransactionStatement\", $1), $2)",
            parameters: [
                QueryParameter(position: 1, value: .primitive(.string(queryText))),
                QueryParameter(position: 2, value: .primitive(.null), type: .primitive(.rdfTerm))
            ]
        )
        #expect(valid.rows.count == 2)
        let invalid = "SELECT id FROM \"SPARQLTransactionUser\" WHERE resource IN (SPARQL(\"SPARQLTransactionStatement\", $1), $2::integer)"
        let before = scenario.engine.rangeReadCount
        let parsed = try SQLParser().parseWithSource(invalid)
        let filter = try #require(parsed.occurrences.child(of: parsed.root, field: .filter))
        let cast = try #require(parsed.occurrences.child(of: filter, field: .argument, index: 1))
        let parameter = try #require(parsed.occurrences.child(of: cast, field: .operand, index: 0))
        #expect(cast != parameter)
        let annotation = try #require(parsed.sourceMap[cast])
        do {
            _ = try await context.executeSQL(invalid, parameters: [
                QueryParameter(position: 1, value: .primitive(.string(queryText))),
                QueryParameter(position: 2, value: .primitive(.null))
            ])
            Issue.record("Expected the following CAST result to fail SQL admission")
        } catch let failure as SQLBindingSourceFailure {
            guard case .expression(.typeMismatch(operation: "common result type")) = failure.cause else {
                Issue.record("Expected the following CAST result's incompatible membership type")
                return
            }
            // Membership rejects the CAST result after its parameter binds successfully.
            #expect(failure.occurrence == cast)
            #expect(failure.position == (annotation.identifierRange?.start ?? annotation.range.start))
        }
        #expect(scenario.engine.rangeReadCount == before)
        await scenario.container.shutdown()
    }

    private func seedGraph(_ context: DatabaseContext) async throws {
        for ordinal in 1...2 {
            let iri = try RDFIRI("urn:user:source-\(ordinal)")
            var user = SPARQLTransactionUser()
            user.id = iri.rawValue
            user.resource = .iri(iri)
            user.name = "Source"
            var statement = SPARQLTransactionStatement()
            statement.id = "source-statement-\(ordinal)"
            statement.subject = .iri(iri)
            statement.predicate = try .iri(validating: "urn:predicate:active")
            statement.object = .literal(RDFLiteral(lexicalForm: "true", datatype: .xsdString))
            try context.insert(user)
            try context.insert(statement)
        }
        try await context.save()
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

    typealias TransactionType = SPARQLCountingTransaction

    private let engine = InMemoryEngine()
    private let transactionCountState = Mutex(0)
    private let rangeReadCountState = Mutex(0)

    init(configuration: Configuration = Configuration()) {
        _ = configuration
    }

    func createTransaction() throws -> SPARQLCountingTransaction {
        let transaction = try engine.createTransaction()
        transactionCountState.withLock { $0 += 1 }
        return SPARQLCountingTransaction(transaction: transaction) { [self] in
            rangeReadCountState.withLock { $0 += 1 }
        }
    }

    var transactionDomain: StorageTransactionDomain {
        engine.transactionDomain
    }

    var directoryAccess: any DirectoryAccess {
        engine.directoryAccess
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

    var rangeReadCount: Int {
        rangeReadCountState.withLock { $0 }
    }
}

private struct SPARQLCountingTransaction: Transaction {
    let transaction: InMemoryTransaction
    let rangeRead: @Sendable () -> Void

    init(transaction: InMemoryTransaction, rangeRead: @escaping @Sendable () -> Void) {
        self.transaction = transaction
        self.rangeRead = rangeRead
    }

    var transactionDomain: StorageTransactionDomain { transaction.transactionDomain }
    var storageFailure: StorageError? { transaction.storageFailure }
    var mutationByteLimit: Int? { transaction.mutationByteLimit }
    var capabilities: TransactionCapabilities { transaction.capabilities }
    var compaction: StorageCompactionAccess? { transaction.compaction }

    func configureMutationByteLimit(maximumBytes: Int?) throws { try transaction.configureMutationByteLimit(maximumBytes: maximumBytes) }
    func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? { try await transaction.getValue(for: key, snapshot: snapshot) }
    func getValue(for key: ByteString) async throws -> ByteString? { try await transaction.getValue(for: key) }
    func getValue(for key: ByteString, snapshot: Bool, maximumByteCount: Int) async throws -> ByteString? {
        try await transaction.getValue(for: key, snapshot: snapshot, maximumByteCount: maximumByteCount)
    }
    func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? { try await transaction.getKey(selector: selector, snapshot: snapshot) }
    func rangeCursor(from begin: KeySelector, to end: KeySelector, limit: Int, reverse: Bool, snapshot: Bool, streamingMode: StreamingMode) -> KeyValueCursor {
        rangeRead()
        return transaction.rangeCursor(from: begin, to: end, limit: limit, reverse: reverse, snapshot: snapshot, streamingMode: streamingMode)
    }
    func setValue(_ value: ByteString, for key: ByteString) throws { try transaction.setValue(value, for: key) }
    func clear(key: ByteString) throws { try transaction.clear(key: key) }
    func clearRange(beginKey: ByteString, endKey: ByteString) throws { try transaction.clearRange(beginKey: beginKey, endKey: endKey) }
    func atomicOp(key: ByteString, param: ByteString, mutationType: MutationType) throws { try transaction.atomicOp(key: key, param: param, mutationType: mutationType) }
    func setReadVersion(_ version: Int64) throws { try transaction.setReadVersion(version) }
    func getReadVersion() async throws -> Int64 { try await transaction.getReadVersion() }
    func setOption(forOption option: TransactionOption) throws { try transaction.setOption(forOption: option) }
    func setOption(to value: ByteString?, forOption option: TransactionOption) throws { try transaction.setOption(to: value, forOption: option) }
    func setOption(to value: Int, forOption option: TransactionOption) throws { try transaction.setOption(to: value, forOption: option) }
    func addConflictRange(beginKey: ByteString, endKey: ByteString, type: ConflictRangeType) throws { try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: type) }
    func getEstimatedRangeSizeBytes(beginKey: ByteString, endKey: ByteString) async throws -> Int { try await transaction.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey) }
    func getRangeSplitPoints(beginKey: ByteString, endKey: ByteString, chunkSize: Int) async throws -> [ByteString] { try await transaction.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize) }
    func requestVersionstamp() -> any PendingTransactionVersionstamp { transaction.requestVersionstamp() }
    func commit() async throws { try await transaction.commit() }
    func cancel() async throws { try await transaction.cancel() }
    func getCommittedVersion() throws -> Int64 { try transaction.getCommittedVersion() }
}
#endif
