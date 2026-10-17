import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import TestSupport
import Testing
import Synchronization
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S13 SQL constraint execution", .timeLimit(.minutes(1)))
struct S13SQLConstraintExecutionTests {
    @Test("Arbitrary composite primary keys use SQL columns rather than entity identity")
    func compositePrimaryKey() async throws {
        let relation = try relation(primary: ["a", "b"])
        let container = try await makeContainer([relation])
        defer { await container.shutdown() }
        _ = try await execute(insert("one", a: .int(1), b: .int(2)), container)
        _ = try await execute(insert("two", a: .int(1), b: .int(3)), container)
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "items_pkey")) {
            _ = try await execute(insert("different-identity", a: .int(1), b: .int(2)), container)
        }
        #expect(try await fields("different-identity", container) == nil)
        await #expect(throws: SQLConstraintError.self) { _ = try await execute(insert("null", a: .literal(.null), b: .int(4)), container) }
    }

    @Test("No primary key permits repeated logical values with distinct physical identities")
    func noPrimaryKey() async throws {
        let container = try await makeContainer([relation(primary: nil)])
        defer { await container.shutdown() }
        _ = try await execute(insert("one", a: .int(1), b: .int(2)), container)
        _ = try await execute(insert("two", a: .int(1), b: .int(2)), container)
        #expect(try await fields("one", container) != nil)
        #expect(try await fields("two", container) != nil)
    }

    @Test("UNIQUE NULLS DISTINCT and NOT DISTINCT have different NULL behavior")
    func nullUniqueness() async throws {
        for nulls: SQLNullUniqueness in [.distinct, .notDistinct] {
            let unique = try constraint("u", kind: .unique(columnIDs: ["a", "b"], nulls: nulls))
            let container = try await makeContainer([relation(primary: nil, constraints: [unique])])
            defer { await container.shutdown() }
            _ = try await execute(insert("one", a: .literal(.null), b: .int(2)), container)
            if nulls == .distinct {
                _ = try await execute(insert("two", a: .literal(.null), b: .int(2)), container)
            } else {
                await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "u")) {
                    _ = try await execute(insert("two", a: .literal(.null), b: .int(2)), container)
                }
            }
        }
    }

    @Test("CHECK accepts UNKNOWN and rejects FALSE; NOT NULL rejects NULL")
    func checkAndNotNull() async throws {
        let check = try constraint("positive", kind: .check(.greaterThan(.col("a"), .int(0))))
        let container = try await makeContainer([relation(primary: nil, constraints: [check])])
        defer { await container.shutdown() }
        _ = try await execute(insert("unknown", a: .literal(.null), b: .int(1)), container)
        _ = try await execute(insert("positive", a: .int(1), b: .int(1)), container)
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "positive")) {
            _ = try await execute(insert("negative", a: .int(-1), b: .int(1)), container)
        }
        let nonNull = try await makeContainer([relation(primary: nil, constraints: [constraint("required", kind: .notNull(columnID: "a"))])])
        defer { await nonNull.shutdown() }
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "required")) {
            _ = try await execute(insert("null", a: .literal(.null), b: .int(1)), nonNull)
        }
    }

    @Test("Nondeferrable UNIQUE rejects a row swap; deferrable immediate checks the statement result")
    func rowVersusStatementTiming() async throws {
        for timing: SQLConstraintTiming in [.immediate, .deferrable(initiallyDeferred: false)] {
            let unique = try constraint("u", timing: timing, kind: .unique(columnIDs: ["a"], nulls: .distinct))
            let container = try await makeContainer([relation(primary: nil, constraints: [unique])])
            defer { await container.shutdown() }
            _ = try await execute(insert("one", a: .int(1), b: .int(0)), container)
            _ = try await execute(insert("two", a: .int(2), b: .int(0)), container)
            let swap = QueryStatement.update(UpdateQuery(target: TableRef("items"),
                assignments: [Assignment(column: "a", value: .subtract(.int(3), .col("a")))]))
            if timing == .immediate {
                await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "u")) { _ = try await execute(swap, container) }
                #expect(try await fields("one", container)?["a"] == .int64(1))
            } else {
                _ = try await execute(swap, container)
                #expect(try await fields("one", container)?["a"] == .int64(2))
            }
        }
    }

    @Test("Deferred violation fails commit and SET IMMEDIATE checks retroactively")
    func deferredCommitAndRetroactiveCheck() async throws {
        let unique = try constraint("u", timing: .deferrable(initiallyDeferred: true), kind: .unique(columnIDs: ["a"], nulls: .distinct))
        let container = try await makeContainer([relation(primary: nil, constraints: [unique])])
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let executor = try executor(container)
        let work = meter()
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "u")) {
            try await context.withTransaction(configuration: .batch) { transaction in
                _ = try await executor.executeSQL(insert("one", a: .int(1), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
                _ = try await executor.executeSQL(insert("two", a: .int(1), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
            }
        }
        #expect(try await fields("one", container) == nil)
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "u")) {
            try await context.withTransaction(configuration: .batch) { transaction in
                _ = try await executor.executeSQL(insert("one", a: .int(1), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
                _ = try await executor.executeSQL(insert("two", a: .int(1), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
                try await transaction.setSQLConstraintsDeferred(false, names: ["u"])
            }
        }
    }

    @Test("Checkpoint rollback restores deferred mode and removes pending violating rows")
    func checkpointRestoresModeAndRows() async throws {
        let unique = try constraint("u", timing: .deferrable(initiallyDeferred: false), kind: .unique(columnIDs: ["a"], nulls: .distinct))
        let container = try await makeContainer([relation(primary: nil, constraints: [unique])])
        defer { await container.shutdown() }
        let executor = try executor(container)
        let work = meter()
        let limits = try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 8, maximumRetainedBytes: 1_000_000, maximumWorkUnits: 100_000, maximumReadPageRows: 128)
        try await container.testBaseContext().withCheckpointTransaction(checkpointLimits: limits, workMeter: work) { transaction in
            _ = try await executor.executeSQL(insert("one", a: .int(1), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
            let checkpoint = try await transaction.createCheckpoint()
            try await transaction.setSQLConstraintsDeferred(true, names: ["u"])
            _ = try await executor.executeSQL(insert("abandoned", a: .int(1), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
            try await transaction.rollback(to: checkpoint)
            await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "u")) {
                _ = try await executor.executeSQL(insert("must-fail", a: .int(1), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
            }
            try await transaction.rollback(to: checkpoint)
            _ = try await executor.executeSQL(insert("survives", a: .int(2), b: .int(0)), searchPath: .default, transaction: transaction, workMeter: work)
        }
        #expect(try await fields("abandoned", container) == nil)
        #expect(try await fields("survives", container) != nil)
        #expect(work.retainedIntermediateBytes == 0)
        #expect(work.retainedIntermediateRows == 0)
    }

    @Test("Defaults are SQL expressions and stored generated columns recompute on UPDATE")
    func defaultAndGenerated() async throws {
        let columns = try [SQLColumn(id: "id", name: "id", fieldNumber: 1),
            SQLColumn(id: "a", name: "a", backing: .field(number: 2), policy: .input(defaultExpression: .int(7))),
            SQLColumn(id: "b", name: "b", fieldNumber: 3),
            SQLColumn(id: "g", name: "computed", backing: .field(number: 4), policy: .generated(.add(.col("a"), .int(1))))]
        let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item", columns: columns)
        let container = try await makeContainer([relation])
        defer { await container.shutdown() }
        _ = try await execute(.insert(InsertQuery(target: TableRef("items"), columns: ["id"], source: .values([[.string("one")]]))), container)
        #expect(try await fields("one", container)?["a"] == .int64(7))
        #expect(try await fields("one", container)?["g"] == .int64(8))
        _ = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "a", value: .int(9))])), container)
        #expect(try await fields("one", container)?["g"] == .int64(10))
        await #expect(throws: SQLConstraintError.generatedAssignment(relation: "items", column: "computed")) {
            _ = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "computed", value: .int(99))])), container)
        }
        _ = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "computed", value: .defaultValue)])), container)
    }

    @Test("Raw canonical model writes reject inconsistent stored generated values")
    func rawGeneratedInvariant() async throws {
        let columns = try [SQLColumn(id: "id", name: "id", fieldNumber: 1), SQLColumn(id: "a", name: "a", fieldNumber: 2),
            SQLColumn(id: "g", name: "g", backing: .field(number: 4), policy: .generated(.add(.col("a"), .int(1))))]
        let container = try await makeContainer([SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item", columns: columns)])
        defer { await container.shutdown() }
        let runtime = try #require(container.acquirePublishedSchemaLease().runtimeConfiguration.entityRuntimes.registration(named: "Item"))
        let bad = try runtime.persistedModel(from: FieldObject([("id", .string("bad")), ("a", .int64(4)), ("g", .int64(0))]))
        await #expect(throws: SQLConstraintError.self) {
            try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
                try await transaction.savePersistedModel(bad, precondition: .none)
            }
        }
        #expect(try await fields("bad", container) == nil)
    }

    @Test("Generated policies reject row-external and generated dependencies before scanning")
    func generatedAdmission() throws {
        let schema = try schema()
        let ordinary = try SQLColumn(id: "a", name: "a", fieldNumber: 2)
        for expression: Expression in [.currentValue(kind: .timestamp, precision: nil), .function(FunctionCall(name: "current_schema", arguments: [])), .col("generated")] {
            let generated = try SQLColumn(id: "g", name: "generated", backing: .field(number: 4), policy: .generated(expression))
            let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item", columns: [ordinary, generated])
            #expect(throws: (any Error).self) { try SQLConstraintCompiler.validate(catalog: SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [relation]), physicalSchema: schema) }
        }
    }

    @Test("Foreign key MATCH SIMPLE and FULL distinguish partial NULL keys")
    func foreignKeyMatching() async throws {
        for match: SQLForeignKeyMatch in [.simple, .full] {
            let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents", match: match)
            let child = try relation(primary: nil, constraints: [constraint("fk", kind: .foreignKey(foreign))])
            let container = try await makeContainer([parentRelation(), child])
            defer { await container.shutdown() }
            _ = try await execute(parentInsert("p", a: 1, b: 2), container)
            _ = try await execute(insert("valid", a: .int(1), b: .int(2)), container)
            _ = try await execute(insert("null", a: .literal(.null), b: .literal(.null)), container)
            if match == .simple { _ = try await execute(insert("partial", a: .int(1), b: .literal(.null)), container) }
            else {
                await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "fk")) {
                    _ = try await execute(insert("partial", a: .int(1), b: .literal(.null)), container)
                }
            }
            await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "fk")) {
                _ = try await execute(insert("missing", a: .int(9), b: .int(9)), container)
            }
        }
    }

    @Test("Foreign-key UPDATE CASCADE and subset DELETE SET NULL use the same transaction")
    func foreignKeyActions() async throws {
        let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents",
            onUpdate: .cascade, onDelete: .setNull, onDeleteColumnIDs: ["b"])
        let container = try await makeContainer([parentRelation(), relation(primary: nil, constraints: [constraint("fk", kind: .foreignKey(foreign))])])
        defer { await container.shutdown() }
        _ = try await execute(parentInsert("p", a: 1, b: 2), container)
        _ = try await execute(insert("child", a: .int(1), b: .int(2)), container)
        _ = try await execute(.update(UpdateQuery(target: TableRef("parents"), assignments: [Assignment(column: "a", value: .int(3))])), container)
        #expect(try await fields("child", container)?["a"] == .int64(3))
        #expect(try await fields("child", container)?["b"] == .int64(2))
        _ = try await execute(.delete(DeleteQuery(target: TableRef("parents"))), container)
        #expect(try await fields("child", container)?["a"] == .int64(3))
        #expect(try await fields("child", container)?["b"] == .null)
    }

    @Test("Foreign-key DELETE CASCADE removes actual children; RESTRICT and NO ACTION roll back")
    func foreignKeyDeletion() async throws {
        for action: SQLReferentialAction in [.cascade, .restrict, .noAction] {
            let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents", onDelete: action)
            let container = try await makeContainer([parentRelation(), relation(primary: nil, constraints: [constraint("fk", kind: .foreignKey(foreign))])])
            defer { await container.shutdown() }
            _ = try await execute(parentInsert("p", a: 1, b: 2), container)
            _ = try await execute(insert("child", a: .int(1), b: .int(2)), container)
            let deletion = QueryStatement.delete(DeleteQuery(target: TableRef("parents")))
            if action == .cascade {
                _ = try await execute(deletion, container)
                #expect(try await fields("child", container) == nil)
            } else {
                await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "fk")) { _ = try await execute(deletion, container) }
                #expect(try await fields("child", container) != nil)
                #expect(try await fields("p", container, entity: "Parent") != nil)
            }
        }
    }

    @Test("Adding CHECK to existing invalid data fails publication and leaves the generation unchanged")
    func existingDataAdmission() async throws {
        let container = try await makeContainer([relation(primary: nil)])
        defer { await container.shutdown() }
        _ = try await execute(insert("bad", a: .int(-1), b: .int(0)), container)
        let generation = container.schemaGeneration
        let candidate = try SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")],
            relations: [relation(primary: nil, constraints: [constraint("positive", kind: .check(.greaterThan(.col("a"), .int(0))))])])
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "positive")) {
            _ = try await container.publishSQLCatalog(candidate, expectedGeneration: generation,
                idempotencyKey: "invalid-existing", authorization: TestBaseEnvironment.authorization)
        }
        #expect(container.schemaGeneration == generation)
        #expect(container.sqlCatalog.relations[0].constraints.isEmpty)
        #expect(try await fields("bad", container)?["a"] == .int64(-1))
    }

    @Test("Integrity sees hidden keys and foreign rows without granting the caller read access")
    func hiddenRowsAndCascadeAuthority() async throws {
        let schema = try schema()
        let handlers = schema.entities.map { entity in
            AuthorizationPolicyHandler(schemaDriven: entity) { operation, authorization in
                let identifier = authorization.principal?.identifier
                if identifier != "limited" && identifier != "fk-writer" { return true }
                if entity.name == "Parent" { return identifier == "limited" }
                return operation == .create
            }
        }
        let container = try await DBContainer.open(testing: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s13-hidden", revision: 1),
                schema: schema, authorizationPolicies: handlers), security: .enabled())
        defer { await container.shutdown() }
        let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents", onDelete: .cascade)
        let child = try relation(primary: nil, constraints: [constraint("fk", kind: .foreignKey(foreign)), constraint("u", kind: .unique(columnIDs: ["a", "b"], nulls: .distinct))])
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [parentRelation(), child]),
            expectedGeneration: container.schemaGeneration, idempotencyKey: "hidden", authorization: TestBaseEnvironment.authorization)
        let limited = AuthorizationContext.authenticated(Principal(identifier: "limited"))
        let writer = AuthorizationContext.authenticated(Principal(identifier: "fk-writer"))
        #if MultiBase
        try await container.grantTestBaseAccess(to: .principal("limited"), access: .write)
        try await container.grantTestBaseAccess(to: .principal("fk-writer"), access: .write)
        #endif
        _ = try await execute(parentInsert("p", a: 1, b: 2), container)
        _ = try await execute(insert("hidden", a: .int(1), b: .int(2)), container)
        _ = try await execute(parentInsert("hidden-parent", a: 8, b: 9), container)
        _ = try await execute(insert("writer-child", a: .int(8), b: .int(9)), container, authorization: writer)
        await #expect(throws: SecurityError.self) { _ = try await fields("hidden-parent", container, entity: "Parent", authorization: writer) }
        await #expect(throws: SecurityError.self) { _ = try await fields("hidden", container, authorization: limited) }
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "u")) {
            _ = try await execute(insert("duplicate", a: .int(1), b: .int(2)), container, authorization: limited)
        }
        _ = try await execute(.delete(DeleteQuery(target: TableRef("parents"))), container, authorization: limited)
        #expect(try await fields("hidden", container) == nil)
        // Permission remains denied for surviving hidden rows after the action.
        _ = try await execute(parentInsert("other", a: 3, b: 4), container)
        _ = try await execute(insert("still-hidden", a: .int(3), b: .int(4)), container)
        await #expect(throws: SecurityError.self) { _ = try await fields("still-hidden", container, authorization: limited) }
    }

    @Test("Virtual generated DEFAULT targets and current-row sources stay logical")
    func virtualPolicyTargets() async throws {
        let columns = try [SQLColumn(id: "id", name: "key", fieldNumber: 1),
            SQLColumn(id: "a", name: "input", fieldNumber: 2),
            SQLColumn(id: "g", name: "stored", backing: .field(number: 4), policy: .generated(.add(.col("input"), .int(1)))),
            SQLColumn(id: "v", name: "virtual", backing: .virtual(type: .sql(.bigint)), policy: .generated(.multiply(.col("input"), .int(2))))]
        let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item", columns: columns)
        let container = try await makeContainer([relation])
        defer { await container.shutdown() }
        _ = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .values([[.string("one"), .int(3), .defaultValue, .defaultValue]]))), container)
        #expect(try await fields("one", container)?["g"] == .int64(4))
        _ = try await execute(.update(UpdateQuery(target: TableRef(table: "items", alias: "t"), assignments: [Assignment(column: "input", value: .col("virtual"))])), container)
        #expect(try await fields("one", container)?["a"] == .int64(6))
        #expect(try await fields("one", container)?["g"] == .int64(7))
        _ = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "virtual", value: .defaultValue)])), container)
        await #expect(throws: SQLConstraintError.generatedAssignment(relation: "items", column: "virtual")) {
            _ = try await execute(.insert(InsertQuery(target: TableRef("items"), columns: ["key", "input", "virtual"], source: .values([[.string("bad"), .int(1), .int(2)]]))), container)
        }
        await #expect(throws: (any Error).self) {
            _ = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "b", value: .int(99))])), container)
        }
    }

    @Test("Foreign-key numeric widths use canonical numeric equality")
    func foreignKeyNumericWidths() async throws {
        let base = try schema()
        let parent = try #require(base.entity(named: "Parent"))
        let item = try Schema.Entity(name: "Item", identifierType: .string, fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "a", fieldNumber: 2, type: .int32, isOptional: true, queryType: .sql(.integer)),
            FieldSchema(name: "b", fieldNumber: 3, type: .int64, isOptional: true, queryType: .sql(.bigint))], directoryComponents: [.staticPath("s13-items")])
        let schema = try Schema(entities: [item, parent], version: .init(1, 0, 0))
        let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents")
        let container = try await makeContainer([parentRelation(), relation(primary: nil, constraints: [constraint("fk", kind: .foreignKey(foreign))])], physicalSchema: schema)
        defer { await container.shutdown() }
        _ = try await execute(parentInsert("p", a: 7, b: 8), container)
        _ = try await execute(insert("child", a: .int(7), b: .int(8)), container)
        #expect(try await fields("child", container)?["a"] == .int32(7))
    }

    @Test("SQL uniqueness enumerates dynamic directories within the admitted data root")
    func dynamicDirectoryIntegrity() async throws {
        let base = try schema()
        let item = try #require(base.entity(named: "Item"))
        let fields = item.fields.map { field in
            field.name == "b" ? FieldSchema(name: "b", fieldNumber: 3, type: .int64, queryType: .sql(.bigint)) : field
        }
        let dynamic = try Schema.Entity(name: item.name, identifierType: item.identifierType, fields: fields,
            directoryComponents: [.staticPath("s13-dynamic"), .dynamicField(fieldName: "b")])
        let schema = try Schema(entities: [dynamic], version: .init(1, 0, 0))
        let unique = try constraint("u", kind: .unique(columnIDs: ["a"], nulls: .distinct))
        let container = try await makeContainer([relation(primary: nil, constraints: [unique])], physicalSchema: schema)
        defer { await container.shutdown() }
        func partitionInsert(_ id: String, value: Int64, partition: Int64) throws -> QueryStatement {
            try .insert(InsertQuery(target: TableRef(table: "items", partitions: FieldObject([("b", .int64(partition))])),
                columns: ["id", "a", "b"], source: .values([[.string(id), .int(value), .int(partition)]])))
        }
        _ = try await execute(partitionInsert("one", value: 7, partition: 1), container)
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "u")) {
            _ = try await execute(partitionInsert("two", value: 7, partition: 2), container)
        }
        _ = try await execute(partitionInsert("valid", value: 8, partition: 2), container)
    }

    @Test("SET DEFAULT evaluates declared SQL defaults and rolls back if the new key is absent")
    func foreignKeyDefaultAction() async throws {
        for replacement: Int64 in [9, 8] {
            let columns = try [SQLColumn(id: "id", name: "id", fieldNumber: 1),
                SQLColumn(id: "a", name: "a", backing: .field(number: 2), policy: .input(defaultExpression: .int(replacement))),
                SQLColumn(id: "b", name: "b", backing: .field(number: 3), policy: .input(defaultExpression: .int(replacement)))]
            let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents", onDelete: .setDefault)
            let child = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item", columns: columns,
                constraints: [constraint("fk", kind: .foreignKey(foreign))])
            let container = try await makeContainer([parentRelation(), child])
            defer { await container.shutdown() }
            _ = try await execute(parentInsert("p", a: 1, b: 2), container)
            _ = try await execute(parentInsert("default-target", a: 9, b: 9), container)
            _ = try await execute(insert("child", a: .int(1), b: .int(2)), container)
            let deletion = QueryStatement.delete(DeleteQuery(target: TableRef("parents"), filter: .equal(.col("id"), .string("p"))))
            if replacement == 9 {
                _ = try await execute(deletion, container)
                #expect(try await fields("child", container)?["a"] == .int64(9))
                #expect(try await fields("child", container)?["b"] == .int64(9))
            } else {
                await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "fk")) { _ = try await execute(deletion, container) }
                #expect(try await fields("p", container, entity: "Parent") != nil)
                #expect(try await fields("child", container)?["a"] == .int64(1))
            }
        }
    }

    @Test("Deferred foreign keys permit repair before commit and reject a surviving orphan")
    func deferredForeignKey() async throws {
        let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents")
        let child = try relation(primary: nil, constraints: [constraint("fk", timing: .deferrable(initiallyDeferred: true), kind: .foreignKey(foreign))])
        let container = try await makeContainer([parentRelation(), child])
        defer { await container.shutdown() }
        let executor = try executor(container)
        let work = meter()
        try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
            _ = try await executor.executeSQL(insert("child", a: .int(1), b: .int(2)), searchPath: .default, transaction: transaction, workMeter: work)
            _ = try await executor.executeSQL(parentInsert("p", a: 1, b: 2), searchPath: .default, transaction: transaction, workMeter: work)
        }
        #expect(try await fields("child", container) != nil)
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "fk")) {
            try await container.testBaseContext().withTransaction(configuration: .batch) { transaction in
                _ = try await executor.executeSQL(insert("orphan", a: .int(8), b: .int(9)), searchPath: .default, transaction: transaction, workMeter: work)
            }
        }
        #expect(try await fields("orphan", container) == nil)
        #expect(work.retainedIntermediateBytes == 0)
    }

    @Test("R-B1 successful volatile CHECK rows are not evaluated at later boundaries")
    func checkRunsOnlyForTheChangedRow() async throws {
        let live = S13PolicyClock(start: 10, increment: 100)
        let check = try constraint("volatile", kind: .check(.or(.equal(.col("a"), .int(1)),
            .lessThan(.function(FunctionCall(name: "clock_timestamp", arguments: [])),
                .literal(.timestamp(Timestamp(secondsSinceUnixEpoch: 20)))))))
        let container = try await makeContainer([relation(primary: nil, constraints: [check])])
        defer { await container.shutdown() }
        let temporal = SQLTemporalEnvironment(transactionInstant: Timestamp(secondsSinceUnixEpoch: 0),
            statementInstant: Timestamp(secondsSinceUnixEpoch: 5), wallClock: live)
        _ = try await execute(insert("historical", a: .int(0), b: .int(0)), container, temporalEnvironment: temporal)
        #expect(live.sampleCount == 1)
        _ = try await execute(insert("new", a: .int(1), b: .int(0)), container, temporalEnvironment: temporal)
        _ = try await execute(.update(UpdateQuery(target: TableRef("items"),
            assignments: [Assignment(column: "b", value: .int(99))], filter: .equal(.col("id"), .string("new")))),
            container, temporalEnvironment: temporal)
        #expect(live.sampleCount == 1)
        #expect(try await fields("historical", container)?["a"] == .int64(0))
        #expect(try await fields("new", container)?["b"] == .int64(99))
    }

    @Test("R-B2 identity relocation propagates UPDATE through grandchildren")
    func cascadingIdentityRelocationPreservesUpdateMeaning() async throws {
        for deleteAction: SQLReferentialAction in [.cascade, .restrict] {
            let schema = try Schema(entities: [
                Schema.Entity(name: "Parent", identifierType: .string, fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string), FieldSchema(name: "k", fieldNumber: 2, type: .string)], directoryComponents: [.staticPath("s13-relocation-parent")]),
                Schema.Entity(name: "Child", identifierType: .string, fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)], directoryComponents: [.staticPath("s13-relocation-child")]),
                Schema.Entity(name: "Grandchild", identifierType: .string, fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string), FieldSchema(name: "c", fieldNumber: 2, type: .string)], directoryComponents: [.staticPath("s13-relocation-grandchild")])
            ], version: .init(1, 0, 0))
            let parent = try SQLRelation(id: "parents", namespaceID: "public", name: "parents", entityName: "Parent",
                columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1), SQLColumn(id: "k", name: "k", fieldNumber: 2)], primaryKeyColumnIDs: ["k"])
            let childFK = try SQLForeignKey(columnIDs: ["id"], referencedRelationID: "parents", onUpdate: .cascade)
            let child = try SQLRelation(id: "children", namespaceID: "public", name: "children", entityName: "Child",
                columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1)], primaryKeyColumnIDs: ["id"], constraints: [constraint("child_fk", kind: .foreignKey(childFK))])
            let grandFK = try SQLForeignKey(columnIDs: ["c"], referencedRelationID: "children", onUpdate: .cascade, onDelete: deleteAction)
            let grand = try SQLRelation(id: "grandchildren", namespaceID: "public", name: "grandchildren", entityName: "Grandchild",
                columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1), SQLColumn(id: "c", name: "c", fieldNumber: 2)], constraints: [constraint("grand_fk", kind: .foreignKey(grandFK))])
            let container = try await makeContainer([parent, child, grand], physicalSchema: schema)
            defer { await container.shutdown() }
            _ = try await execute(.insert(InsertQuery(target: TableRef("parents"), columns: ["id", "k"], source: .values([[.string("p"), .string("old")]]))), container)
            _ = try await execute(.insert(InsertQuery(target: TableRef("children"), source: .values([[.string("old")]]))), container)
            _ = try await execute(.insert(InsertQuery(target: TableRef("grandchildren"), source: .values([[.string("g"), .string("old")]]))), container)
            _ = try await execute(.update(UpdateQuery(target: TableRef("parents"), assignments: [Assignment(column: "k", value: .string("new"))])), container)
            #expect(try await fields("old", container, entity: "Child") == nil)
            #expect(try await fields("new", container, entity: "Child") != nil)
            #expect(try await fields("g", container, entity: "Grandchild")?["c"] == .string("new"))
        }
    }

    @Test("R-B3 sequence staging preserves deferred repair and validates newly added CHECK")
    func catalogStagingPreservesUnchangedDeferredObligations() async throws {
        let foreign = try SQLForeignKey(columnIDs: ["a", "b"], referencedRelationID: "parents")
        let deferred = try constraint("fk", timing: .deferrable(initiallyDeferred: true), kind: .foreignKey(foreign))
        let child = try relation(primary: nil, constraints: [deferred])
        let container = try await makeContainer([parentRelation(), child])
        defer { await container.shutdown() }
        let sequence = try SQLSequence(id: "unrelated", namespaceID: "public", name: "unrelated", start: 1, increment: 1, minimum: 1, maximum: 100)
        let checkpoints = try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 8,
            maximumRetainedBytes: 4_000_000, maximumWorkUnits: 100_000, maximumReadPageRows: 128)
        _ = try await SQLBlockExecutor(context: container.testBaseContext()).execute(SQLBlock(steps: [
            .statement(insert("child", a: .int(1), b: .int(2))), .sequence(.create(sequence)),
            .statement(parentInsert("p", a: 1, b: 2))
        ]), options: SQLBlockExecutionOptions(checkpointLimits: checkpoints))
        #expect(container.sqlCatalog.sequences.contains { $0.id == "unrelated" })
        #expect(try await fields("child", container) != nil)
        let invalid = try relation(primary: nil, constraints: [deferred, constraint("new_check", kind: .check(.lessThan(.col("a"), .int(0))))])
        let catalog = try SQLCatalog(namespaces: container.sqlCatalog.namespaces, relations: [parentRelation(), invalid], sequences: container.sqlCatalog.sequences)
        let generation = container.schemaGeneration
        let work = meter()
        await #expect(throws: SQLConstraintError.violation(relation: "items", constraint: "new_check")) {
            try await container.testBaseContext().withCheckpointTransaction(checkpointLimits: checkpoints, workMeter: work) { transaction in
                _ = try await transaction.stageSQLCatalog(catalog, expectedGeneration: generation, idempotencyKey: "new-invalid-check")
            }
        }
        #expect(container.schemaGeneration == generation)
        #expect(container.sqlCatalog.relations.first { $0.id == "items" }?.constraints == [deferred])
        #expect(work.retainedIntermediateBytes == 0)
    }

    @Test("R-B4 CHECK and referential defaults use the admitted principal")
    func contextualCheckAndReferentialDefault() async throws {
        let schema = try Schema(entities: [
            Schema.Entity(name: "Parent", identifierType: .string, fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)], directoryComponents: [.staticPath("s13-session-parent")]),
            Schema.Entity(name: "Child", identifierType: .string, fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string), FieldSchema(name: "ref", fieldNumber: 2, type: .string)], directoryComponents: [.staticPath("s13-session-child")])
        ], version: .init(1, 0, 0))
        let user = Expression.function(FunctionCall(name: "current_user", arguments: []))
        let parent = try SQLRelation(id: "parents", namespaceID: "public", name: "parents", entityName: "Parent", columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1)], primaryKeyColumnIDs: ["id"])
        let foreign = try SQLForeignKey(columnIDs: ["ref"], referencedRelationID: "parents", onDelete: .setDefault)
        let child = try SQLRelation(id: "children", namespaceID: "public", name: "children", entityName: "Child", columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1),
            SQLColumn(id: "ref", name: "ref", backing: .field(number: 2), policy: .input(defaultExpression: user))],
            constraints: [constraint("session_check", kind: .check(.equal(user, .string("alice")))), constraint("fk", kind: .foreignKey(foreign))])
        let container = try await makeContainer([parent, child], physicalSchema: schema)
        defer { await container.shutdown() }
        let alice = AuthorizationContext.authenticated(Principal(identifier: "alice"))
        #if MultiBase
        try await container.grantTestBaseAccess(to: .principal("alice"), access: .write)
        #endif
        for id in ["old", "alice"] { _ = try await execute(.insert(InsertQuery(target: TableRef("parents"), source: .values([[.string(id)]]))), container) }
        _ = try await execute(.insert(InsertQuery(target: TableRef("children"), source: .values([[.string("child"), .string("old")]]))), container, authorization: alice)
        _ = try await execute(.delete(DeleteQuery(target: TableRef("parents"), filter: .equal(.col("id"), .string("old")))), container, authorization: alice)
        #expect(try await fields("child", container, entity: "Child")?["ref"] == .string("alice"))
        // The raw model path also derives the real attempt principal.
        let runtime = try #require(container.runtimeConfiguration.entityRuntimes.registration(named: "Child"))
        let raw = try runtime.persistedModel(from: FieldObject([("id", .string("raw")), ("ref", .string("alice"))]))
        try await container.testBaseContext(authorization: alice).withTransaction { transaction in try await transaction.savePersistedModel(raw, precondition: .none) }
        #expect(try await fields("raw", container, entity: "Child") != nil)
    }

    @Test("R-B5 defaults and CHECK share the original explicit statement timestamp")
    func rowPoliciesRetainStatementTime() async throws {
        let temporalType = QueryColumnType.sql(.timestamp(withTimeZone: true))
        let schema = try Schema(entities: [Schema.Entity(name: "Item", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string), FieldSchema(name: "ts", fieldNumber: 2, type: .timestamp, queryType: temporalType)
        ], directoryComponents: [.staticPath("s13-statement-time")])], version: .init(1, 0, 0))
        let timestamp = Expression.function(FunctionCall(name: "statement_timestamp", arguments: []))
        let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item", columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1),
            SQLColumn(id: "ts", name: "ts", backing: .field(number: 2), policy: .input(defaultExpression: timestamp))],
            constraints: [constraint("same_statement", kind: .check(.equal(.col("ts"), timestamp)))])
        let container = try await makeContainer([relation], physicalSchema: schema)
        defer { await container.shutdown() }
        let live = S13PolicyClock(start: 100, increment: 1)
        let instant = Timestamp(secondsSinceUnixEpoch: 42)
        let temporal = SQLTemporalEnvironment(transactionInstant: Timestamp(secondsSinceUnixEpoch: 40), statementInstant: instant, wallClock: live)
        _ = try await execute(.insert(InsertQuery(target: TableRef("items"), columns: ["id"], source: .values([[.string("one")]]))), container, temporalEnvironment: temporal)
        #expect(try await fields("one", container)?["ts"] == .timestamp(instant))
        #expect(live.sampleCount == 0)
    }

    private func parentRelation() throws -> SQLRelation {
        try SQLRelation(id: "parents", namespaceID: "public", name: "parents", entityName: "Parent",
            columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1), SQLColumn(id: "a", name: "a", fieldNumber: 2), SQLColumn(id: "b", name: "b", fieldNumber: 3)],
            primaryKeyColumnIDs: ["a", "b"])
    }

    private func parentInsert(_ id: String, a: Int64, b: Int64) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("parents"), columns: ["id", "a", "b"], source: .values([[.string(id), .int(a), .int(b)]])))
    }

    private func relation(primary: [String]?, constraints: [SQLConstraint] = []) throws -> SQLRelation {
        try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item",
            columns: [SQLColumn(id: "id", name: "id", fieldNumber: 1), SQLColumn(id: "a", name: "a", fieldNumber: 2), SQLColumn(id: "b", name: "b", fieldNumber: 3)],
            primaryKeyColumnIDs: primary, constraints: constraints)
    }

    private func constraint(_ name: String, timing: SQLConstraintTiming = .immediate, kind: SQLConstraintKind) throws -> SQLConstraint {
        try SQLConstraint(attributes: SQLConstraintAttributes(id: name, name: name, timing: timing), kind: kind)
    }

    private func schema() throws -> Schema {
        let fields: [FieldSchema] = [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "a", fieldNumber: 2, type: .int64, isOptional: true, queryType: .sql(.bigint)),
            FieldSchema(name: "b", fieldNumber: 3, type: .int64, isOptional: true, queryType: .sql(.bigint)),
            FieldSchema(name: "g", fieldNumber: 4, type: .int64, isOptional: true, queryType: .sql(.bigint))
        ]
        return try Schema(entities: [Schema.Entity(name: "Item", identifierType: .string, fields: fields, directoryComponents: [.staticPath("s13-items")]),
            Schema.Entity(name: "Parent", identifierType: .string, fields: fields, directoryComponents: [.staticPath("s13-parents")])], version: .init(1, 0, 0))
    }

    private func makeContainer(_ relations: [SQLRelation], physicalSchema: Schema? = nil) async throws -> DBContainer {
        let schema = try physicalSchema ?? schema()
        let container = try await DBContainer.open(testing: schema, configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s13-constraints", revision: 1), schema: schema), security: .testingDisabled)
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: relations),
            expectedGeneration: container.schemaGeneration, idempotencyKey: "s13-initial", authorization: TestBaseEnvironment.authorization)
        return container
    }

    private func insert(_ id: String, a: Expression, b: Expression) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("items"), columns: ["id", "a", "b"], source: .values([[.string(id), a, b]])))
    }

    private func executor(_ container: DBContainer) throws -> DatabaseEntityStatementMutationExecutor {
        DatabaseEntityStatementMutationExecutor(container: container, limits: try DatabaseEntityMutationLimits(maximumChanges: 64, maximumPreconditions: 64))
    }

    private func execute(_ statement: QueryStatement, _ container: DBContainer,
        authorization: AuthorizationContext = TestBaseEnvironment.authorization,
        temporalEnvironment: SQLTemporalEnvironment? = nil) async throws -> [EntityMutationEffect] {
        let executor = try executor(container)
        let work = meter()
        let result = try await container.testBaseContext(authorization: authorization).withTransaction(configuration: .batch) { transaction in
            try await executor.executeSQL(statement, searchPath: .default, transaction: transaction, workMeter: work,
                temporalEnvironment: temporalEnvironment)
        }
        #expect(work.retainedIntermediateRows == 0)
        #expect(work.retainedIntermediateBytes == 0)
        return result
    }

    private func fields(_ id: String, _ container: DBContainer, entity: String = "Item",
        authorization: AuthorizationContext = TestBaseEnvironment.authorization) async throws -> FieldObject? {
        try await container.testBaseContext(authorization: authorization).withTransaction(configuration: .batch) { transaction in
            guard let model = try await transaction.loadPersistedModel(entity: entity, id: Tuple(id), partition: nil) else { return nil }
            return try DatabaseEntityProjection.fieldObject(for: model)
        }
    }

    private func meter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1_000, maximumWorkUnits: 100_000,
            maximumIntermediateRows: 1_000, maximumIntermediateBytes: 8_000_000), monotonicClock: SystemStorageClock())
    }
}


private final class S13PolicyClock: WallClock, Sendable {
    private struct State: Sendable {
        var next: Int64
        var samples: Int = 0
    }
    private let state: Mutex<State>
    private let increment: Int64

    init(start: Int64, increment: Int64) {
        state = Mutex(State(next: start))
        self.increment = increment
    }

    var now: Timestamp {
        state.withLock { value in
            let instant = Timestamp(secondsSinceUnixEpoch: value.next)
            value.next += increment
            value.samples += 1
            return instant
        }
    }

    var sampleCount: Int { state.withLock { $0.samples } }
}
