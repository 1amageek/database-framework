import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S17 logical SQL authorization", .timeLimit(.minutes(1)))
struct S17SQLAuthorizationTests {
    @Test("An exact logical policy permits SQL DML while absent policies deny")
    func logicalAllowAndMissingDeny() async throws {
        let container = try await open(policy: { namespace, relation in
            namespace.utf8.elementsEqual("public".utf8) && relation.utf8.elementsEqual("allowed".utf8)
        })
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("CREATE TABLE allowed(value INTEGER); INSERT INTO allowed VALUES (7);", options: options())
        #expect(try await context.executeSQL("SELECT value FROM allowed").rows.map(\.values) == [[.primitive(.int32(7))]])
        _ = try await context.executeSQLBlock("CREATE TABLE denied(value INTEGER);", options: options())
        await #expect(throws: (any Error).self) { try await context.executeSQL("SELECT value FROM denied") }
        await #expect(throws: (any Error).self) {
            try await context.executeSQLBlock("INSERT INTO denied VALUES (8);", options: options())
        }
    }

    @Test("Quoted schema and table selection preserve case and exact UTF-8")
    func exactLogicalNames() async throws {
        let composed = "caf\u{e9}"
        let decomposed = "cafe\u{301}"
        let container = try await open(policy: { namespace, relation in
            namespace.utf8.elementsEqual("Exact".utf8) && relation.utf8.elementsEqual(composed.utf8)
        })
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("""
            CREATE SCHEMA "Exact";
            CREATE SCHEMA "exact";
            CREATE TABLE "Exact"."\(composed)"(value INTEGER);
            CREATE TABLE "Exact"."\(decomposed)"(value INTEGER);
            CREATE TABLE "exact"."\(composed)"(value INTEGER);
            INSERT INTO "Exact"."\(composed)" VALUES (3);
            """, options: options())
        #expect(try await context.executeSQL("SELECT value FROM \"Exact\".\"\(composed)\"").rows.count == 1)
        for sql in ["SELECT value FROM \"Exact\".\"\(decomposed)\"", "SELECT value FROM \"exact\".\"\(composed)\""] {
            await #expect(throws: (any Error).self) { try await context.executeSQL(sql) }
        }
    }

    @Test("Table and schema rename select policies configured only for their new names")
    func renamedLogicalNames() async throws {
        let container = try await open(policy: { namespace, relation in
            (namespace == "public" && relation == "renamed") || (namespace == "moved" && relation == "items")
        })
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("""
            CREATE TABLE original(value INTEGER);
            ALTER TABLE original RENAME TO renamed;
            INSERT INTO renamed VALUES (5);
            CREATE SCHEMA original_schema;
            CREATE TABLE original_schema.items(value INTEGER);
            ALTER SCHEMA original_schema RENAME TO moved;
            INSERT INTO moved.items VALUES (6);
            """, options: options())
        #expect(try await context.executeSQL("SELECT value FROM renamed").rows.map(\.values) == [[.primitive(.int32(5))]])
        #expect(try await context.executeSQL("SELECT value FROM moved.items").rows.map(\.values) == [[.primitive(.int32(6))]])
        let source = try #require(container.sqlCatalog.relations.first { $0.name == "renamed" })
        _ = try await context.executeSQLBlock("ALTER TABLE renamed RENAME TO forbidden;", options: options())
        #expect(container.sqlCatalog.relations.first { $0.name == "forbidden" }?.entityName == source.entityName)
        await #expect(throws: (any Error).self) { try await context.executeSQL("SELECT value FROM forbidden") }
        let catalog = container.sqlCatalog
        let renamed = try SQLRelation(id: source.id, namespaceID: source.namespaceID, name: "renamed",
            entityName: source.entityName, columns: source.columns)
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: catalog.namespaces,
            relations: catalog.relations.map { $0.id == source.id ? renamed : $0 }),
            expectedGeneration: container.schemaGeneration, idempotencyKey: "s17-direct-rename",
            authorization: context.authorization)
        #expect(try await context.executeSQL("SELECT value FROM renamed").rows.count == 1)
    }

    @Test("Savepoint and full rollback restore the prior policy generation")
    func rollbackRestoresPolicy() async throws {
        let container = try await open(policy: { _, relation in relation == "items" })
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("CREATE TABLE items(value INTEGER); INSERT INTO items VALUES (9);", options: options())
        _ = try await context.executeSQLBlock("""
            SAVEPOINT before_rename;
            ALTER TABLE items RENAME TO forbidden;
            ROLLBACK TO before_rename;
            SELECT value FROM items;
            """, options: options())
        #expect(try await context.executeSQL("SELECT value FROM items").rows.count == 1)
        await #expect(throws: (any Error).self) {
            try await context.executeSQLBlock("ALTER TABLE items RENAME TO forbidden; SELECT value FROM forbidden;", options: options())
        }
        #expect(container.sqlCatalog.relations.first?.name == "items")
        #expect(try await context.executeSQL("SELECT value FROM items").rows.count == 1)
    }

    @Test("Reopen and schema restoration reject initial physical and generic grants")
    func reopenRejectsPhysicalPolicyBypass() async throws {
        let engine = InMemoryEngine()
        defer { engine.requestShutdown(); await engine.waitUntilShutdown() }
        let first = try await open(engine: S17RetainedPolicyEngine(engine), policy: { _, _ in true })
        _ = try await first.testBaseContext().executeSQLBlock("CREATE TABLE items(value INTEGER); INSERT INTO items VALUES (4);", options: options())
        let schema = first.schema
        await first.shutdown()
        let runtime = try configuration(schema, policy: { _, _ in false }, physicalGrants: true)
        let reopened = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: S17RetainedPolicyEngine(engine)),
            runtimeConfiguration: runtime, security: .enabled())
        await #expect(throws: (any Error).self) { try await reopened.testBaseContext().executeSQL("SELECT value FROM items") }
        await reopened.shutdown()
        let restored = try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine: S17RetainedPolicyEngine(engine)),
            security: .enabled()) { schema in try configuration(schema, policy: { _, _ in false }, physicalGrants: true) }
        await #expect(throws: (any Error).self) { try await restored.testBaseContext().executeSQL("SELECT value FROM items") }
        await restored.shutdown()
        let allowed = try await DBContainer.openRestoringSchema(configuration: .testing(storageEngine: S17RetainedPolicyEngine(engine)),
            security: .enabled()) { schema in try configuration(schema, policy: { _, _ in true }) }
        defer { await allowed.shutdown() }
        #expect(try await allowed.testBaseContext().executeSQL("SELECT value FROM items").rows.map(\.values) == [[.primitive(.int32(4))]])
    }

    @Test("SQL policies coexist with compiled model authorization")
    func compiledPolicyCoexists() async throws {
        let entity = try S17PolicyModel.schemaEntity
        let schema = try Schema(entities: [entity])
        let runtime = try DatabaseFrameworkRuntime.configuration(executionIdentity: identity,
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(S17PolicyModel.self)],
            authorizationPolicies: [AuthorizationPolicyHandler(S17PolicyModel.self)],
            sqlAuthorizationPolicyFactory: { _, _, _ in nil })
        let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: runtime, security: .enabled())
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(S17PolicyModel(id: "kept", value: "compiled"))
        try await context.save()
        _ = try await context.executeSQLBlock("CREATE TABLE denied(value INTEGER);", options: options())
        #expect(try await context.executeSQL("SELECT value FROM \"S17PolicyModel\"").rows.map(\.values) == [[.primitive(.string("compiled"))]])
        await #expect(throws: (any Error).self) { try await context.executeSQL("SELECT value FROM denied") }
    }

    @Test("Materialized views select the visible logical policy for read and refresh")
    func materializedViewUsesVisibleName() async throws {
        let container = try await open(policy: { namespace, relation in
            namespace == "public" && ["source", "visible"].contains(relation)
        })
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        _ = try await context.executeSQLBlock("""
            CREATE TABLE source(value INTEGER);
            INSERT INTO source VALUES (2);
            CREATE MATERIALIZED VIEW visible AS SELECT value FROM source;
            INSERT INTO source VALUES (3);
            REFRESH MATERIALIZED VIEW visible;
            """, options: options())
        #expect(try await context.executeSQL("SELECT value FROM visible ORDER BY value").rows.map(\.values)
            == [[.primitive(.int32(2))], [.primitive(.int32(3))]])
        let view = try #require(container.sqlCatalog.materializedViews.first)
        let backing = try #require(container.sqlCatalog.relations.first { $0.id == view.backingRelationID })
        #expect(backing.name != "visible")
    }

    @Test("A mismatched physical handler fails with the typed configuration error")
    func mismatchedHandlerFails() async throws {
        let wrong = try Schema.Entity(name: "wrong", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)], directoryComponents: [.staticPath("wrong")])
        let schema = try Schema(entities: [])
        let runtime = try DatabaseFrameworkRuntime.configuration(executionIdentity: identity, schema: schema,
            sqlAuthorizationPolicyFactory: { _, _, _ in AuthorizationPolicyHandler(schemaDriven: wrong) { _, _ in true } })
        let container = try await DBContainer.open(for: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: runtime, security: .enabled())
        defer { await container.shutdown() }
        await #expect {
            try await container.testBaseContext().executeSQLBlock("CREATE TABLE items(value INTEGER);", options: options())
        } throws: { error in
            let cause = (error as? SQLExecutionFailure)?.underlyingError ?? error
            guard let error = cause as? DatabaseRuntimeConfigurationError,
                case .authorizationPolicyEntityMismatch(_, let actual) = error else { return false }
            return actual == "wrong"
        }
        #expect(container.sqlCatalog.relations.isEmpty)
    }

    private var identity: DatabaseExecutionRuntimeIdentity { .init(identifier: "s17-sql-policy", revision: 1) }
    private func options() throws -> SQLBlockExecutionOptions {
        SQLBlockExecutionOptions(checkpointLimits: try TransactionCheckpointLimits(maximumEntries: 1000, maximumDepth: 16,
            maximumRetainedBytes: 8_000_000, maximumWorkUnits: 1_000_000, maximumReadPageRows: 128))
    }
    private func open(engine: any StorageEngine = InMemoryEngine(),
        policy: @escaping @Sendable (String, String) -> Bool) async throws -> DBContainer {
        let schema = try Schema(entities: [])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: configuration(schema, policy: policy), security: .enabled())
    }
    private func configuration(_ schema: Schema, policy: @escaping @Sendable (String, String) -> Bool,
        physicalGrants: Bool = false) throws -> DatabaseRuntimeConfiguration {
        try DatabaseFrameworkRuntime.configuration(executionIdentity: identity, schema: schema,
            authorizationPolicies: physicalGrants ? schema.entities.map { entity in
                AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true }
            } : [], schemaDrivenAuthorizationPolicyFactory: { entity in
                AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true }
            }, sqlAuthorizationPolicyFactory: { entity, namespace, relation in
                guard policy(namespace.name, relation) else { return nil }
                return AuthorizationPolicyHandler(schemaDriven: entity) { _, _ in true }
            })
    }
}

@Persistable
private struct S17PolicyModel: SecurityPolicy {
    #Directory<S17PolicyModel>("s17-compiled-policy")
    var id: String = ""
    var value: String = ""
    static func permitsRead(of resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsQuery(_ query: borrowing SecurityQuery, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsCreate(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsUpdate(from resource: borrowing Self, to updated: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsDelete(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
}

/// Retains the fixture's storage lifecycle while each container closes independently.
private final class S17RetainedPolicyEngine: StorageEngine, Sendable {
    struct Configuration: Sendable {}
    typealias TransactionType = InMemoryTransaction
    private let shared: InMemoryEngine
    init(_ shared: InMemoryEngine) { self.shared = shared }
    init(configuration: Configuration) async throws { shared = InMemoryEngine() }
    var transactionDomain: StorageTransactionDomain { shared.transactionDomain }
    var directoryAccess: any DirectoryAccess { shared.directoryAccess }
    func createTransaction() throws -> InMemoryTransaction { try shared.createTransaction() }
    func requestShutdown() {}
    func waitUntilShutdown() async {}
}
