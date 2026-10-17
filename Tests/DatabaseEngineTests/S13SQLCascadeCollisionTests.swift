import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S13 SQL cascade identity collisions", .timeLimit(.minutes(1)))
struct S13SQLCascadeCollisionTests {
    @Test("Identity-changing CASCADE rejects an occupied MATCH SIMPLE destination atomically")
    func cascadingIdentityCollisionRollsBackAllRows() async throws {
        let schema = try Schema(entities: [
            Schema.Entity(name: "Parent", identifierType: .string, fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "k", fieldNumber: 2, type: .string),
                FieldSchema(name: "a", fieldNumber: 3, type: .int64, queryType: .sql(.bigint))
            ], directoryComponents: [.staticPath("s13-collision-parents")]),
            Schema.Entity(name: "Child", identifierType: .string, fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "a", fieldNumber: 2, type: .int64, isOptional: true, queryType: .sql(.bigint)),
                FieldSchema(name: "payload", fieldNumber: 3, type: .string)
            ], directoryComponents: [.staticPath("s13-collision-children")])
        ], version: .init(1, 0, 0))
        let parent = try SQLRelation(id: "parents", namespaceID: "public", name: "parents", entityName: "Parent",
            columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1),
                SQLColumn(id: "k", name: "k", fieldNumber: 2), SQLColumn(id: "a", name: "a", fieldNumber: 3)],
            primaryKeyColumnIDs: ["k", "a"])
        let foreign = try SQLForeignKey(columnIDs: ["id", "a"], referencedRelationID: "parents",
            match: .simple, onUpdate: .cascade)
        let child = try SQLRelation(id: "children", namespaceID: "public", name: "children", entityName: "Child",
            columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1),
                SQLColumn(id: "a", name: "a", fieldNumber: 2), SQLColumn(id: "payload", name: "payload", fieldNumber: 3)],
            primaryKeyColumnIDs: ["id"], constraints: [SQLConstraint(
                attributes: SQLConstraintAttributes(id: "child_fk", name: "child_fk"), kind: .foreignKey(foreign))])
        let container = try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s13-collision", revision: 1), schema: schema),
            security: .testingDisabled)
        defer { await container.shutdown() }
        _ = try await container.publishSQLCatalog(SQLCatalog(
            namespaces: [SQLNamespace(id: "public", name: "public")], relations: [parent, child]),
            expectedGeneration: container.schemaGeneration, idempotencyKey: "s13-collision-initial",
            authorization: TestBaseEnvironment.authorization)

        try await execute(.insert(InsertQuery(target: TableRef("parents"), columns: ["id", "k", "a"],
            source: .values([[.string("p"), .string("old"), .int(1)]]))), container)
        try await execute(.insert(InsertQuery(target: TableRef("children"), columns: ["id", "a", "payload"],
            source: .values([[.string("old"), .int(1), .string("A")],
                [.string("new"), .literal(.null), .string("B")]]))), container)

        let originalParent = try #require(try await fields("p", entity: "Parent", container: container))
        let originalSource = try #require(try await fields("old", entity: "Child", container: container))
        let originalDestination = try #require(try await fields("new", entity: "Child", container: container))
        #expect(originalParent["k"] == .string("old"))
        #expect(originalParent["a"] == .int64(1))
        #expect(originalSource["a"] == .int64(1))
        #expect(originalSource["payload"] == .string("A"))
        #expect(originalDestination["a"] == .null)
        #expect(originalDestination["payload"] == .string("B"))

        let destination = try EntityReference(entity: "Child", id: .string("new"))
        await #expect(throws: DatabaseContextError.preconditionFailed(identity: destination,
            precondition: .notExists, reason: "row already exists")) {
            try await execute(.update(UpdateQuery(target: TableRef("parents"),
                assignments: [Assignment(column: "k", value: .string("new"))],
                filter: .equal(.col("id"), .string("p")))), container)
        }

        #expect(try await fields("p", entity: "Parent", container: container) == originalParent)
        #expect(try await fields("old", entity: "Child", container: container) == originalSource)
        #expect(try await fields("new", entity: "Child", container: container) == originalDestination)
    }

    private func execute(_ statement: QueryStatement, _ container: DBContainer) async throws {
        let executor = DatabaseEntityStatementMutationExecutor(container: container,
            limits: try DatabaseEntityMutationLimits(maximumChanges: 64, maximumPreconditions: 64))
        let work = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1_000, maximumWorkUnits: 100_000,
            maximumIntermediateRows: 1_000, maximumIntermediateBytes: 8_000_000), monotonicClock: SystemStorageClock())
        defer {
            #expect(work.retainedIntermediateRows == 0)
            #expect(work.retainedIntermediateBytes == 0)
        }
        _ = try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
            try await executor.executeSQL(statement, searchPath: .default, transaction: transaction, workMeter: work)
        }
    }

    private func fields(_ id: String, entity: String, container: DBContainer) async throws -> FieldObject? {
        try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
            guard let model = try await transaction.loadPersistedModel(entity: entity, id: Tuple(id), partition: nil) else { return nil }
            return try DatabaseEntityProjection.fieldObject(for: model)
        }
    }
}
