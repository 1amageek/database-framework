import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct S13SecuredGeneratedItem: SecurityPolicy {
    #Directory<S13SecuredGeneratedItem>("s13-generated-security")
    var id: String = ""
    var title: String = ""
    @Restricted(read: .roles(["secret"]), write: .roles(["secret"]))
    var secret: String = ""

    static func permitsRead(of resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsQuery(_ query: borrowing SecurityQuery, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsCreate(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsUpdate(from resource: borrowing Self, to updated: borrowing Self,
                              in context: borrowing AuthorizationContext) -> Bool { true }
    static func permitsDelete(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { true }
}

@Suite("S13 virtual generated reads", .timeLimit(.minutes(1)))
struct S13SQLGeneratedReadTests {
    @Test("Projection, filter, ordering and joins derive virtual values from current physical dependencies")
    func virtualValuesIgnoreStalePhysicalNames() async throws {
        let entity = try Schema.Entity(name: "GeneratedItem", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "quantity", fieldNumber: 2, type: .int64),
            FieldSchema(name: "doubled", fieldNumber: 3, type: .int64)],
            directoryComponents: [.staticPath("s13-generated-read")])
        let schema = try Schema(entities: [entity])
        let container = try await DBContainer.open(for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-generated-read", revision: 1), schema: schema),
            security: .testingDisabled)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await context.withTransaction(configuration: .batch) { transaction in
            for (id, quantity) in [("a", Int64(2)), ("b", Int64(5))] {
                let model = try PersistedModel(entity: entity.name, fields: [
                    PersistableField(number: 1, name: "id", value: .string(id)),
                    PersistableField(number: 2, name: "quantity", value: .int64(quantity)),
                    PersistableField(number: 3, name: "doubled", value: .int64(-999))])
                try await transaction.savePersistedModel(model, precondition: .notExists)
            }
        }
        let catalog = try SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [
            SQLRelation(id: "generated", namespaceID: "public", name: "items", entityName: entity.name,
                columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1),
                    SQLColumn(id: "quantity", name: "quantity", fieldNumber: 2),
                    SQLColumn(id: "doubled", name: "doubled", backing: .virtual(type: .primitive(.int64, isArray: false)),
                        policy: .generated(.multiply(.col("quantity"), .literal(.int(2)))))])])
        try await publish(catalog, in: container, key: "virtual")
        let result = try await context.executeSQL("SELECT doubled FROM items WHERE doubled > 5 ORDER BY doubled DESC")
        #expect(result.rows.map(\.values) == [[.primitive(.int64(10))]])
        let wildcard = try await context.executeSQL("SELECT * FROM items ORDER BY doubled")
        #expect(wildcard.rows.map(\.values) == [
            [.primitive(.string("a")), .primitive(.int64(2)), .primitive(.int64(4))],
            [.primitive(.string("b")), .primitive(.int64(5)), .primitive(.int64(10))]])
        let joined = try await context.executeSQL("SELECT a.doubled, b.doubled FROM items a JOIN items b ON a.doubled < b.doubled")
        #expect(joined.rows.map(\.values) == [[.primitive(.int64(4)), .primitive(.int64(10))]])
        let physical = try await context.query(SelectQuery(projection: [.column("doubled")],
            source: .table(TableRef(entity.name))))
        #expect(physical.rows.map(\.values) == [[.primitive(.int64(-999))], [.primitive(.int64(-999))]])
    }

    @Test("Virtual projection, filter, ordering, alias and whole-row paths retain physical field authority")
    func virtualDependenciesRequireAuthority() async throws {
        let entity = try S13SecuredGeneratedItem.schemaEntity
        let container = try await DBContainer.open(for: Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-generated-security", revision: 1),
                entityRuntimes: [EntityRuntimeDefinition(schemaDriven: entity).registration()],
                authorizationPolicies: [AuthorizationPolicyHandler(S13SecuredGeneratedItem.self)]), security: .enabled())
        defer { await container.shutdown() }
        let writer = container.testBaseContext(authorization: authorization(roles: ["secret"]))
        var item = S13SecuredGeneratedItem(id: "one", title: "visible")
        item.secret = "classified"
        try writer.insert(item)
        try await writer.save()
        let old = try #require(container.sqlCatalog.relations.first)
        let relation = try SQLRelation(id: old.id, namespaceID: old.namespaceID, name: "items", entityName: old.entityName,
            columns: old.columns + [SQLColumn(id: "derived", name: "derived",
                backing: .virtual(type: .primitive(.string, isArray: false)),
                policy: .generated(.concatenate(.col("secret"), .string("!"))))],
            primaryKeyColumnIDs: old.primaryKeyColumnIDs, primaryKeyConstraint: old.primaryKeyConstraint, constraints: old.constraints)
        try await publish(SQLCatalog(namespaces: container.sqlCatalog.namespaces, relations: [relation]),
            in: container, key: "security")
        let reader = container.testBaseContext(authorization: authorization(roles: []))
        #expect(try await reader.executeSQL("SELECT title FROM items").rows.map(\.values) == [[.primitive(.string("visible"))]])
        for sql in ["SELECT derived FROM items", "SELECT title FROM items WHERE derived = 'classified!'",
                    "SELECT title FROM items ORDER BY derived", "SELECT x.derived FROM items x", "SELECT items FROM items"] {
            await #expect(throws: FieldSecurityError.self) { try await reader.executeSQL(sql) }
        }
        #expect(try await writer.executeSQL("SELECT derived FROM items").rows.map(\.values)
            == [[.primitive(.string("classified!"))]])
    }

    private func authorization(roles: Set<String>) -> AuthorizationContext {
        .authenticated(Principal(identifier: "test-runner", roles: roles.union(["test-runner", "admin"])))
    }

    private func publish(_ catalog: SQLCatalog, in container: DBContainer, key: String) async throws {
        _ = try await container.publishSQLCatalog(catalog, expectedGeneration: container.schemaGeneration,
            idempotencyKey: key, authorization: TestBaseEnvironment.authorization)
    }
}
