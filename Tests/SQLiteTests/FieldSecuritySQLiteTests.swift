#if SQLITE
import Database
@_spi(Testing) import DatabaseEngine
import DatabaseRuntime
import SQLiteStorage
import StorageKit
import TestHeartbeat
import TestSupport
import Testing

@Persistable
private struct FieldSecuritySQLiteRecord: SecurityPolicy {
    #Directory<FieldSecuritySQLiteRecord>(
        "field-security",
        "records"
    )

    var id: String = "record"
    var title: String = ""

    @Restricted(read: .roles(["security"]), write: .roles(["security"]))
    var secret: String = ""

    static func permitsRead(
        of resource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }

    static func permitsCreate(
        _ newResource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }

    static func permitsUpdate(
        from resource: borrowing FieldSecuritySQLiteRecord,
        to newResource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }

    static func permitsDelete(
        _ resource: borrowing FieldSecuritySQLiteRecord,
        in context: borrowing AuthorizationContext
    ) -> Bool { context.isAuthenticated }
}

@Suite("SQLite field security execution", .serialized, .heartbeat)
struct FieldSecuritySQLiteTests {
    private let principalID = "field-security-principal"

    @Test("Canonical query authorizes every observed field before execution")
    func queryAuthorizesEveryObservedField() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let writer = context(in: container, roles: ["security"])
        var record = FieldSecuritySQLiteRecord(title: "Visible")
        record.secret = "classified"
        try writer.insert(record)
        try await writer.save()

        let reader = context(in: container)
        let publicResult = try await reader.query(
            SelectQuery(
                projection: .items([.column("title")]),
                source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType))
            )
        )
        #expect(publicResult.rows.map(\.fields) == [["title": .string("Visible")]])

        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(
                SelectQuery(
                    projection: .all,
                    source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType))
                )
            )
        }
        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(
                SelectQuery(
                    projection: .items([.column("title")]),
                    source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType)),
                    filter: .equal(
                        .column(ColumnRef(column: "secret")),
                        .literal(.string("classified"))
                    )
                )
            )
        }
        await #expect(throws: FieldSecurityError.self) {
            try await reader.query(
                SelectQuery(
                    projection: .items([.column("title")]),
                    source: .table(TableRef(FieldSecuritySQLiteRecord.persistableType)),
                    orderBy: [SortKey(.column(ColumnRef(column: "secret")))]
                )
            )
        }
        await #expect(throws: FieldSecurityError.self) {
            try await reader.model(
                for: record.id,
                as: FieldSecuritySQLiteRecord.self
            )
        }
    }

    @Test("Canonical mutation authorizes restricted field changes")
    func mutationAuthorizesRestrictedFieldChanges() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let writer = context(in: container, roles: ["security"])
        var original = FieldSecuritySQLiteRecord(title: "Original")
        original.secret = "classified"
        try writer.insert(original)
        try await writer.save()

        let employee = context(in: container)
        var renamed = original
        renamed.title = "Renamed"
        try employee.update(renamed)
        try await employee.save()

        var changedSecret = renamed
        changedSecret.secret = "changed"
        try employee.update(changedSecret)
        await #expect(throws: FieldSecurityError.self) {
            try await employee.save()
        }

        var unauthorizedCreate = FieldSecuritySQLiteRecord(title: "Other")
        unauthorizedCreate.id = "other"
        unauthorizedCreate.secret = "classified"
        let createContext = context(in: container)
        try createContext.insert(unauthorizedCreate)
        await #expect(throws: FieldSecurityError.self) {
            try await createContext.save()
        }
    }

    #if MultiBase
    @Test("A transaction binding cannot be reused by another session")
    func transactionBindingIsSessionBound() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }

        let writer = context(in: container, roles: ["security"])
        let employee = context(in: container)

        await #expect(throws: DatabaseGrantAuthorizationError.self) {
            try await writer.withTransaction { _ in
                _ = try await employee.query(
                    SelectQuery(
                        projection: .items([.column("title")]),
                        source: .table(
                            TableRef(FieldSecuritySQLiteRecord.persistableType)
                        )
                    )
                )
            }
        }
    }
    #endif

    private func makeContainer() async throws -> DBContainer {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        #if MultiBase
        let baseID = try Base.ID("field-security")
        let configuration = DBConfiguration(
            testingName: "field-security",
            storageTopology: try .testing(storageEngine: engine),
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            testingBaseID: baseID,
            testingPrincipal: Principal(
                identifier: principalID,
                roles: ["security"]
            )
        )
        #else
        let configuration = DBConfiguration(
            name: "field-security",
            storageEngine: engine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock()
        )
        #endif
        return try await DBContainer.open(
            for: try Schema(
                entities: [try FieldSecuritySQLiteRecord.schemaEntity],
                version: .init(1, 0, 0)
            ),
            configuration: configuration,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        FieldSecuritySQLiteRecord.self
                    )
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(FieldSecuritySQLiteRecord.self)
                ]
            ),
            security: .enabled()
        )
    }

    private func context(
        in container: DBContainer,
        roles: Set<String> = []
    ) -> DatabaseContext {
        let authorization = AuthorizationContext.authenticated(
            Principal(
                identifier: principalID,
                roles: roles.union(["admin"])
            )
        )
        #if MultiBase
        do {
            return container.session(authorization: authorization)
                .base(try Base.ID("field-security"))
                .newContext()
        } catch {
            preconditionFailure("The fixed test Base identity must be valid")
        }
        #else
        return container.newContext(authorization: authorization)
        #endif
    }
}
#endif
