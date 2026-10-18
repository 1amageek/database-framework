import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S14 transaction SQL mutation authority", .timeLimit(.minutes(1)))
struct S14SQLTransactionMutationTests {
    @Test("Relocation requires UPDATE while CREATE and DELETE remain denied")
    func relocationUsesUpdateAuthority() async throws {
        let fixture = try await open(enforced: true)
        defer { await fixture.container.shutdown() }
        let old = try model("old")
        let new = try model("new")
        try await seed([old], fixture.container)
        let writer = AuthorizationContext.authenticated(Principal(identifier: "update-only"))
        #if MultiBase
        try await fixture.container.grantTestBaseAccess(to: .principal("update-only"), access: .write)
        #endif
        let outcomes = try await apply([DatabaseSQLMutationChange(kind: .update, oldModel: old, newModel: new)],
            fixture.container, authorization: writer)
        #expect(outcomes.count == 1)
        #expect(outcomes[0].effect.kind == .update)
        #expect(outcomes[0].oldModel == old)
        #expect(outcomes[0].newModel == new)
        #expect(try await fetch("old", fixture.container) == nil)
        #expect(try await fetch("new", fixture.container) == new)
        await #expect(throws: SecurityError.self) {
            _ = try await apply([DatabaseSQLMutationChange(kind: .insert, oldModel: nil, newModel: model("blocked"))],
                fixture.container, authorization: writer)
        }
        await #expect(throws: SecurityError.self) {
            _ = try await apply([DatabaseSQLMutationChange(kind: .delete, oldModel: new, newModel: nil)],
                fixture.container, authorization: writer)
        }
    }

    @Test("Destination collision and later stale source roll the whole action batch back")
    func collisionAndLateFailureRollback() async throws {
        let fixture = try await open()
        defer { await fixture.container.shutdown() }
        let old = try model("old", value: 1)
        let occupied = try model("occupied", value: 2)
        try await seed([old, occupied], fixture.container)
        await #expect(throws: DatabaseContextError.self) {
            _ = try await apply([DatabaseSQLMutationChange(kind: .update, oldModel: old,
                newModel: model("occupied", value: 3))], fixture.container)
        }
        #expect(try await fetch("old", fixture.container) == old)
        #expect(try await fetch("occupied", fixture.container) == occupied)
        await #expect(throws: DatabaseContextError.self) {
            _ = try await apply([
                DatabaseSQLMutationChange(kind: .update, oldModel: old, newModel: model("moved", value: 3)),
                DatabaseSQLMutationChange(kind: .update, oldModel: model("occupied", value: 999),
                    newModel: model("late", value: 4))], fixture.container)
        }
        #expect(try await fetch("old", fixture.container) == old)
        #expect(try await fetch("moved", fixture.container) == nil)
        #expect(try await fetch("occupied", fixture.container) == occupied)
    }

    @Test("Dynamic placement relocation clears the old partition and capture obeys its row bound")
    func placementAndTargetSnapshot() async throws {
        let fixture = try await open(dynamic: true)
        defer { await fixture.container.shutdown() }
        let old = try model("same", value: 1)
        let new = try model("same", value: 2)
        try await seed([old], fixture.container)
        _ = try await apply([DatabaseSQLMutationChange(kind: .update, oldModel: old, newModel: new)], fixture.container)
        #expect(try await fetch("same", fixture.container, partition: 1) == nil)
        #expect(try await fetch("same", fixture.container, partition: 2) == new)
        try await seed([model("second", value: 3)], fixture.container)
        let meter = meter()
        let rows = try await fixture.container.testBaseContext().withTransaction { transaction in
            try await transaction.scanSQLMutationModels(relation: fixture.relations[0], fields: ["value"],
                maximumRows: 2, workMeter: meter)
        }
        #expect(rows.count == 2)
        await #expect(throws: DatabaseEntityMutationError.self) {
            try await fixture.container.testBaseContext().withTransaction { transaction in
                _ = try await transaction.scanSQLMutationModels(relation: fixture.relations[0], fields: [],
                    maximumRows: 1, workMeter: meter)
            }
        }
    }

    @Test("A relocated referenced identity dispatches UPDATE and never DELETE")
    func relocationCascadesUpdateOnce() async throws {
        let fixture = try await open(withChild: true)
        defer { await fixture.container.shutdown() }
        let parent = try model("old")
        let child = try model("child", entity: "Child", reference: "old")
        try await seed([parent, child], fixture.container)
        _ = try await apply([DatabaseSQLMutationChange(kind: .update, oldModel: parent,
            newModel: model("new"))], fixture.container)
        #expect(try await fetch("child", fixture.container, entity: "Child")?.value(forFieldNamed: "ref") == .string("new"))
        #expect(try await fetch("old", fixture.container) == nil)
        #expect(try await fetch("new", fixture.container) != nil)
    }

    @Test("TRUNCATE RESTRICT rejects an empty external referencing table")
    func truncateEmptyReferenceRestrict() async throws {
        let fixture = try await open(withChild: true)
        defer { await fixture.container.shutdown() }
        try await seed([model("parent")], fixture.container)
        await #expect(throws: SQLConstraintError.self) {
            _ = try await truncate([fixture.relations[0]], cascade: false, fixture.container)
        }
        #expect(try await fetch("parent", fixture.container) != nil)
    }

    @Test("TRUNCATE CASCADE clears the closed table set without ON DELETE RESTRICT")
    func truncateCascadeClosedSet() async throws {
        let fixture = try await open(withChild: true)
        defer { await fixture.container.shutdown() }
        try await seed([model("parent"), model("child", entity: "Child", reference: "parent")], fixture.container)
        let outcome = try await truncate([fixture.relations[0]], cascade: true, fixture.container)
        #expect(outcome.affectedRows == 2)
        #expect(outcome.relationIDs == ["items", "children"])
        #expect(try await fetch("parent", fixture.container) == nil)
        #expect(try await fetch("child", fixture.container, entity: "Child") == nil)
    }

    @Test("Checkpoint rollback restores TRUNCATE rows and its journal")
    func truncateCheckpointRollback() async throws {
        let fixture = try await open()
        defer { await fixture.container.shutdown() }
        try await seed([model("one"), model("two")], fixture.container)
        let work = meter()
        let limits = try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 8,
            maximumRetainedBytes: 4_000_000, maximumWorkUnits: 100_000, maximumReadPageRows: 128)
        try await fixture.container.testBaseContext().withCheckpointTransaction(checkpointLimits: limits, workMeter: work) { transaction in
            let checkpoint = try await transaction.createCheckpoint()
            let removed = try await transaction.withEntityMutationOperation(workMeter: work) { operation in
                try await transaction.truncateSQLRelations(fixture.relations, cascade: false,
                    within: operation, workMeter: work)
            }
            #expect(removed.affectedRows == 2)
            try await transaction.rollback(to: checkpoint)
            #expect(try await transaction.persistedMutationEffectCount() == 0)
        }
        #expect(try await fetch("one", fixture.container) != nil)
        #expect(try await fetch("two", fixture.container) != nil)
        #expect(work.retainedIntermediateBytes == 0)
        #expect(work.retainedIntermediateRows == 0)
    }

    @Test("Public TRUNCATE CONTINUE preserves owned and unowned sequence cursors")
    func truncateContinueIdentity() async throws {
        let fixture = try await open(identity: true)
        defer { await fixture.container.shutdown() }
        try await seed([model("one", value: 10)], fixture.container)
        let first = try await block([call("nextval", "owned"), call("nextval", "unowned")], fixture.container)
        #expect(try sequenceValue(first, at: 0) == 10)
        #expect(try sequenceValue(first, at: 1) == 100)
        let result = try await block([
            .statement(.truncate(TruncateQuery(tables: [TableRef("items")]))),
            call("nextval", "owned"), call("nextval", "unowned")], fixture.container)
        #expect(try sequenceValue(result, at: 1) == 11)
        #expect(try sequenceValue(result, at: 2) == 101)
        #expect(try await fetch("one", fixture.container) == nil)
    }

    @Test("Public TRUNCATE RESTART rollback restores the old epoch and rows but retains CURRVAL")
    func truncateRestartRollbackHistory() async throws {
        let fixture = try await open(identity: true)
        defer { await fixture.container.shutdown() }
        try await seed([model("one", value: 10)], fixture.container)
        _ = try await block([call("nextval", "owned"), call("nextval", "unowned")], fixture.container)
        let result = try await block([.savepoint("before"),
            .statement(.truncate(TruncateQuery(tables: [TableRef("items")], identity: .restart))),
            call("nextval", "owned"), .rollbackTo("before"),
            call("currval", "owned"), call("nextval", "owned"), call("nextval", "unowned")], fixture.container)
        #expect(try sequenceValue(result, at: 2) == 10)
        #expect(try sequenceValue(result, at: 4) == 10)
        #expect(try sequenceValue(result, at: 5) == 11)
        #expect(try sequenceValue(result, at: 6) == 101)
        #expect(try await fetch("one", fixture.container) != nil)
        let committed = try await block([
            .statement(.truncate(TruncateQuery(tables: [TableRef("items")], identity: .restart))),
            call("nextval", "owned"), call("nextval", "unowned")], fixture.container)
        #expect(try sequenceValue(committed, at: 1) == 10)
        #expect(try sequenceValue(committed, at: 2) == 102)
    }

    @Test("Explicit DML target partitions isolate DELETE UPDATE MERGE and reject misplaced inserts")
    func explicitTargetPartitionSelection() async throws {
        let fixture = try await open(dynamic: true, indexed: true)
        defer { await fixture.container.shutdown() }
        let selected = try model("selected", value: 1, reference: "selected")
        let unselected = try model("unselected", value: 2, reference: "untouched")
        try await seed([selected, unselected], fixture.container)
        func target(_ partition: Int64) throws -> TableRef {
            try TableRef("items", partitions: FieldObject([("value", .int64(partition))]))
        }
        func verifyUnselected() async throws {
            #expect(try await fetch("unselected", fixture.container, partition: 2) == unselected)
            let indexed = try await fixture.container.testBaseContext().query(SelectQuery(projection: [.column("id")],
                source: .table(TableRef("Item", partitions: FieldObject([("value", .int64(2))]))),
                accessPath: .index(IndexScanSource(indexName: "s14_partition_ref", indexType: .ordered)),
                filter: .equal(.col("ref"), .string("untouched"))))
            #expect(indexed.rows.map(\.values) == [[.primitive(.string("unselected"))]])
        }
        _ = try await block([.statement(.insert(InsertQuery(target: TableRef("items"),
            columns: ["id", "value", "ref"], source: .values([[.string("derived"), .int(4), .string("derived")]]))))], fixture.container)
        #expect(try await fetch("derived", fixture.container, partition: 4)?.value(forFieldNamed: "ref") == .string("derived"))
        _ = try await block([.statement(.update(UpdateQuery(target: TableRef("items"), assignments: [
            Assignment(column: "ref", value: .string("global-update"))], filter: .equal(.col("id"), .string("derived")))))], fixture.container)
        #expect(try await fetch("derived", fixture.container, partition: 4)?.value(forFieldNamed: "ref") == .string("global-update"))
        try await verifyUnselected()
        _ = try await block([.statement(.merge(MergeQuery(target: TableRef("items"), source: .values([[.int(1)]]),
            condition: .equal(.col("id"), .string("derived")), clauses: [SQLMergeClause(match: .matched,
                action: .update([Assignment(column: "ref", value: .string("global-merge"))]))])))], fixture.container)
        #expect(try await fetch("derived", fixture.container, partition: 4)?.value(forFieldNamed: "ref") == .string("global-merge"))
        try await verifyUnselected()
        _ = try await block([.statement(.delete(DeleteQuery(target: TableRef("items"),
            filter: .equal(.col("id"), .string("derived")))))], fixture.container)
        #expect(try await fetch("derived", fixture.container, partition: 4) == nil)
        try await verifyUnselected()
        _ = try await block([.statement(.delete(DeleteQuery(target: target(1))))], fixture.container)
        #expect(try await fetch("selected", fixture.container, partition: 1) == nil)
        try await verifyUnselected()
        try await seed([selected], fixture.container)
        _ = try await block([.statement(.update(UpdateQuery(target: target(1), assignments: [
            Assignment(column: "value", value: .int(3)), Assignment(column: "ref", value: .string("updated"))])))], fixture.container)
        #expect(try await fetch("selected", fixture.container, partition: 1) == nil)
        #expect(try await fetch("selected", fixture.container, partition: 3)?.value(forFieldNamed: "ref") == .string("updated"))
        try await verifyUnselected()
        _ = try await block([.statement(.merge(MergeQuery(target: target(3), source: .values([[.int(1)]]),
            condition: .literal(.bool(true)), clauses: [SQLMergeClause(match: .matched, action: .delete)])))], fixture.container)
        #expect(try await fetch("selected", fixture.container, partition: 3) == nil)
        try await verifyUnselected()
        await #expect(throws: DatabaseEntityMutationError.self) {
            try await block([.statement(.insert(InsertQuery(target: target(1), columns: ["id", "value", "ref"],
                source: .values([[.string("misplaced"), .int(2), .string("invalid")]]))))], fixture.container)
        }
        await #expect(throws: DatabaseEntityMutationError.self) {
            try await block([.statement(.merge(MergeQuery(target: target(1), source: .values([[.int(1)]]),
                condition: .literal(.bool(false)), clauses: [SQLMergeClause(match: .notMatchedByTarget,
                    action: .insert(columns: ["id", "value", "ref"],
                        values: [.string("misplaced"), .int(2), .string("invalid")], overriding: nil))])))], fixture.container)
        }
        #expect(try await fetch("misplaced", fixture.container, partition: 1) == nil)
        #expect(try await fetch("misplaced", fixture.container, partition: 2) == nil)
        try await verifyUnselected()
    }

    private func call(_ name: String, _ sequence: String) -> SQLBlockStep {
        .statement(.select(SelectQuery(projection: [.expression(.function(FunctionCall(name: name,
            arguments: [.string(sequence)])))], source: .values([[]]))))
    }

    private func block(_ steps: [SQLBlockStep], _ container: DBContainer) async throws -> SQLBlockResult {
        try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: steps),
            options: SQLBlockExecutionOptions(checkpointLimits: TransactionCheckpointLimits(
                maximumEntries: 10_000, maximumDepth: 8, maximumRetainedBytes: 4_000_000,
                maximumWorkUnits: 100_000, maximumReadPageRows: 128)))
    }

    private func sequenceValue(_ result: SQLBlockResult, at index: Int) throws -> Int64 {
        guard case .rows(let rows) = result.results[index],
              case .primitive(.int64(let value)) = rows.rows.first?.values.first else {
            throw SQLSequenceExecutionError.invalidPersistedState
        }
        return value
    }

    private struct Fixture: Sendable {
        let container: DBContainer
        let relations: [SQLRelation]
    }

    private func open(dynamic: Bool = false, enforced: Bool = false, withChild: Bool = false, identity: Bool = false, indexed: Bool = false) async throws -> Fixture {
        let fields = [FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "value", fieldNumber: 2, type: .int64),
            FieldSchema(name: "ref", fieldNumber: 3, type: .string, isOptional: true)]
        let components: [DirectoryPathComponent] = dynamic
            ? [.staticPath("s14-items"), .dynamicField(fieldName: "value")]
            : [.staticPath("s14-items")]
        let indexes = indexed ? try [IndexDescriptor(entityName: "Item", declaration: .ordered(name: "s14_partition_ref",
            keys: [.ascending(FieldIdentity(name: "ref", number: 3))]), fieldSchemas: fields)] : []
        var entities = [try Schema.Entity(name: "Item", identifierType: .string, fields: fields,
            directoryComponents: components, indexes: indexes)]
        if withChild {
            entities.append(try Schema.Entity(name: "Child", identifierType: .string, fields: fields,
                directoryComponents: [.staticPath("s14-children")]))
        }
        let schema = try Schema(entities: entities)
        let policies = entities.map { entity in
            AuthorizationPolicyHandler(schemaDriven: entity) { operation, authorization in
                authorization.principal?.identifier != "update-only" || operation == .update || operation == .get || operation == .list
            }
        }
        let container = try await DBContainer.open(testing: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s14-transaction-tests", revision: 1),
                schema: schema, authorizationPolicies: policies),
            security: enforced ? .enabled() : .testingDisabled)
        let valueColumn = identity
            ? try SQLColumn(id: "value", name: "value", backing: .field(number: 2),
                policy: .identity(SQLIdentityColumn(sequenceID: "owned", generation: .byDefault)))
            : try SQLColumn(id: "value", name: "value", fieldNumber: 2)
        let columns = try [SQLColumn(id: "id", name: "id", fieldNumber: 1), valueColumn,
            SQLColumn(id: "ref", name: "ref", fieldNumber: 3)]
        var relations = [try SQLRelation(id: "items", namespaceID: "public", name: "items",
            entityName: "Item", columns: columns, primaryKeyColumnIDs: ["id"])]
        if withChild {
            let foreign = try SQLForeignKey(columnIDs: ["ref"], referencedRelationID: "items",
                onUpdate: .cascade, onDelete: .restrict)
            relations.append(try SQLRelation(id: "children", namespaceID: "public", name: "children", entityName: "Child",
                columns: columns, constraints: [SQLConstraint(attributes: SQLConstraintAttributes(id: "fk", name: "fk"), kind: .foreignKey(foreign))]))
        }
        let sequences = identity ? try [
            SQLSequence(id: "owned", namespaceID: "public", name: "owned", start: 10, increment: 1,
                minimum: 1, maximum: 1_000, ownedBy: SQLSequenceOwner(relationID: "items", columnID: "value")),
            SQLSequence(id: "unowned", namespaceID: "public", name: "unowned", start: 100, increment: 1,
                minimum: 1, maximum: 1_000)] : []
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: relations, sequences: sequences),
            expectedGeneration: container.schemaGeneration, idempotencyKey: "s14-transaction", authorization: TestBaseEnvironment.authorization)
        return Fixture(container: container, relations: relations)
    }

    private func model(_ id: String, value: Int64 = 0, entity: String = "Item", reference: String? = nil) throws -> PersistedModel {
        try PersistedModel(entity: entity, fields: [
            PersistableField(number: 1, name: "id", value: .string(id)),
            PersistableField(number: 2, name: "value", value: .int64(value)),
            PersistableField(number: 3, name: "ref", value: reference.map(FieldValue.string) ?? .null)])
    }

    private func seed(_ models: [PersistedModel], _ container: DBContainer) async throws {
        try await container.testBaseContext().withTransaction { transaction in
            for model in models { try await transaction.savePersistedModel(model, precondition: .notExists) }
        }
    }

    private func apply(_ changes: [DatabaseSQLMutationChange], _ container: DBContainer,
        authorization: AuthorizationContext = TestBaseEnvironment.authorization
    ) async throws -> DatabaseSharedRetainedArray<DatabaseSQLMutationOutcome> {
        let work = meter()
        var builder = try DatabaseRetainedArrayBuilder<DatabaseSQLMutationChange>(workMeter: work,
            stage: .mutationPlanning, layout: DatabaseRetainedArrayLayout.forElement(DatabaseSQLMutationChange.self))
        for change in changes {
            var footprint = DatabaseIntermediateFootprint()
            if let old = change.oldModel { footprint = try footprint.adding(DatabaseEntityMutationFootprintMeter.footprint(of: old, workMeter: work)) }
            if let new = change.newModel { footprint = try footprint.adding(DatabaseEntityMutationFootprintMeter.footprint(of: new, workMeter: work)) }
            try builder.append(footprint: footprint, make: { change })
        }
        let retained = try builder.finish().moveToSharedOwnership(at: .mutationPlanning)
        let executor = DatabaseEntityMutationExecutor(container: container,
            limits: try DatabaseEntityMutationLimits(maximumChanges: 64, maximumPreconditions: 64))
        return try await container.testBaseContext(authorization: authorization).withTransaction { transaction in
            try await executor.executeSQLChanges(retained, workMeter: work, transaction: transaction)
        }
    }

    private func fetch(_ id: String, _ container: DBContainer, entity: String = "Item", partition: Int64? = nil) async throws -> PersistedModel? {
        let path: AnyDirectoryPath?
        if let partition {
            path = try AnyDirectoryPath(entity: #require(container.schema.entity(named: entity)),
                partitions: FieldObject([("value", .int64(partition))]))
        } else { path = nil }
        return try await container.testBaseContext().withTransaction { transaction in
            try await transaction.loadPersistedModel(entity: entity, id: Tuple(id), partition: path)
        }
    }

    private func truncate(_ relations: [SQLRelation], cascade: Bool, _ container: DBContainer) async throws -> DatabaseSQLTruncateOutcome {
        let work = meter()
        return try await container.testBaseContext().withTransaction { transaction in
            try await transaction.withEntityMutationOperation(workMeter: work) { operation in
                try await transaction.truncateSQLRelations(relations, cascade: cascade, within: operation, workMeter: work)
            }
        }
    }

    private func meter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1_000, maximumWorkUnits: 100_000,
            maximumIntermediateRows: 1_000, maximumIntermediateBytes: 8_000_000), monotonicClock: SystemStorageClock())
    }
}
