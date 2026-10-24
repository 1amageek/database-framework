import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S14 SQL mutation execution", .timeLimit(.minutes(1)))
struct S14SQLMutationExecutionTests {
    @Test("INSERT SELECT retains positional conversion and empty-source metadata")
    func insertSelect() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let select = SelectQuery(projection: [.expression(.string("one")), .expression(.int(3)), .expression(.int(4))], source: .values([[]]))
        let inserted = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .select(select), returning: [.wildcard()])), container)
        #expect(inserted.count == 1)
        #expect(inserted.rows.first?.values == [.primitive(.string("one")), .primitive(.int64(3)), .primitive(.int64(4))])
        let empty = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .select(select.replacing(filter: .bool(false))), returning: [.column("a")])), container)
        #expect(empty.count == 0 && empty.rows.isEmpty && empty.columns.count == 1)
    }

    @Test("Omitted INSERT columns and DEFAULT obtain SQL policy values")
    func defaultsAndOmission() async throws {
        let container = try await makeContainer(defaultA: .int(7), defaultB: .int(9))
        defer { await container.shutdown() }
        let result = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .values([[.string("one")]]), returning: [.wildcard()])), container)
        #expect(result.rows.first?.values == [.primitive(.string("one")), .primitive(.int64(7)), .primitive(.int64(9))])
        let explicit = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .values([[.string("two"), .defaultValue, .int(2)]]), returning: [.column("a")])), container)
        #expect(explicit.rows.first?.values == [.primitive(.int64(7))])
    }

    @Test("Hidden dynamic IDs allow duplicate visible SQL rows and remain invisible")
    func hiddenIdentity() async throws {
        let container = try await makeContainer(hidden: true)
        defer { await container.shutdown() }
        let result = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .values([[.int(1), .int(2)], [.int(1), .int(2)]]), returning: [.wildcard()])), container)
        #expect(result.count == 2 && result.columns.map(\.name) == ["a", "b"])
        #expect(result.rows[0].values == result.rows[1].values)
        let deleted = try await execute(.delete(DeleteQuery(target: TableRef("items"))), container)
        #expect(deleted.count == 2)
    }

    @Test("UPDATE RHS expressions all observe the old target")
    func simultaneousAssignments() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let result = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "a", value: .col("b")), Assignment(column: "b", value: .col("a"))], returning: [.column("a"), .column("b")])), container)
        #expect(result.rows.first?.values == [.primitive(.int64(2)), .primitive(.int64(1))])
    }

    @Test("UPDATE tuple constructor and DEFAULT preserve one old-row evaluation")
    func tupleAndDefault() async throws {
        let container = try await makeContainer(defaultA: .int(8))
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let result = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(columns: ["a", "b"], value: .row([.defaultValue, .col("a")]))], returning: [.column("a"), .column("b")])), container)
        #expect(result.rows.first?.values == [.primitive(.int64(8)), .primitive(.int64(1))])
    }

    @Test("Tuple subquery cardinality failure leaves the target unchanged")
    func tupleSubqueryCardinality() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let query = SelectQuery(projection: [.expression(.int(7)), .expression(.int(8))], source: .values([[.int(1)], [.int(2)]]))
        await #expect(throws: (any Error).self) {
            try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(columns: ["a", "b"], value: .subquery(query))])), container)
        }
        let values = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "a", value: .col("a"))], returning: [.column("a")])), container)
        #expect(values.rows.first?.values == [.primitive(.int64(1))])
    }

    @Test("UPDATE FROM duplicate candidates update and count a target once")
    func updateFromMultiplicity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let result = try await execute(.update(UpdateQuery(target: TableRef(table: "items", alias: "t"), assignments: [Assignment(column: "a", value: col("s", "x"))], from: source([3, 4]), returning: [.column("a"), .expression(col("s", "x"))])), container)
        #expect(result.count == 1 && result.rows.count == 1)
        #expect([QueryValue.primitive(.int64(3)), .primitive(.int64(4))].contains(result.rows[0].values[0]))
        #expect(result.rows[0].values[0] == result.rows[0].values[1])
    }

    @Test("DELETE USING duplicate candidates retain one old row and one count")
    func deleteUsingMultiplicity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let result = try await execute(.delete(DeleteQuery(target: TableRef("items"), using: source([3, 4]), returning: [.column("a"), .expression(col("s", "x"))])), container)
        #expect(result.count == 1 && result.rows.count == 1)
        #expect(result.rows[0].values[0] == .primitive(.int64(1)))
        #expect(try await execute(.delete(DeleteQuery(target: TableRef("items"))), container).count == 0)
    }

    @Test("RETURNING keeps duplicate labels, transitions, and no-op UPDATE count")
    func returningTransitionsAndLabels() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let result = try await execute(.update(UpdateQuery(target: TableRef("items"), assignments: [Assignment(column: "a", value: .col("a"))], returning: [.expression(col("old", "a"), alias: "x"), .expression(col("new", "a"), alias: "x")])), container)
        #expect(result.count == 1 && result.columns.map(\.name) == ["x", "x"])
        #expect(result.rows.first?.values == [.primitive(.int64(1)), .primitive(.int64(1))])
    }

    @Test("RETURNING empty metadata binds before source enumeration")
    func returningEmptyMetadata() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await execute(.delete(DeleteQuery(target: TableRef("items"), returning: [.expression(.col("a"), alias: "x"), .expression(.col("b"), alias: "x")])), container)
        #expect(result.count == 0 && result.rows.isEmpty && result.columns.map(\.name) == ["x", "x"])
    }

    @Test("Composite ON CONFLICT inference ignores key order and uses EXCLUDED")
    func compositeArbiter() async throws {
        let container = try await makeContainer(unique: .distinct)
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let result = try await execute(conflictInsert("two", a: .int(1), b: .int(2), action: .doUpdate(assignments: [Assignment(column: "b", value: .add(col("excluded", "b"), .int(3)))], where: nil), target: .columns(["b", "a"], where: nil)), container)
        #expect(result.count == 1)
        #expect(result.rows.first?.values == [.primitive(.string("one")), .primitive(.int64(1)), .primitive(.int64(5))])
    }

    @Test("Named conflict constraint resolves logical UNIQUE rather than physical identity")
    func namedArbiter() async throws {
        let container = try await makeContainer(unique: .distinct)
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let result = try await execute(conflictInsert("different-id", a: .int(1), b: .int(2), action: .doNothing, target: .constraint("u")), container)
        #expect(result.count == 0)
        await #expect(throws: SQLMutationError.invalidConflictTarget) {
            try await execute(conflictInsert("bad", a: .int(4), b: .int(5), action: .doNothing, target: .constraint("missing")), container)
        }
    }

    @Test("Conflict NULL policy preserves DISTINCT and NOT DISTINCT")
    func conflictNullPolicy() async throws {
        for nulls: SQLNullUniqueness in [.distinct, .notDistinct] {
            let container = try await makeContainer(unique: nulls)
            defer { await container.shutdown() }
            _ = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .values([[.string("one"), .literal(.null), .int(2)]]))), container)
            let result = try await execute(conflictInsert("two", a: .literal(.null), b: .int(2), action: .doNothing), container)
            #expect(result.count == (nulls == .distinct ? 1 : 0))
        }
    }

    @Test("Filtered conflict duplicates follow effective-update cardinality")
    func filteredConflictDuplicates() async throws {
        let container = try await makeContainer(unique: .distinct)
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let filtered = InsertQuery(target: TableRef("items"), source: .values([[.string("a"), .int(1), .int(2)], [.string("b"), .int(1), .int(2)]]), onConflict: .doUpdate(assignments: [Assignment(column: "a", value: col("items", "a"))], where: .bool(false)), conflictTarget: .columns(["a", "b"], where: nil))
        #expect(try await execute(.insert(filtered), container).count == 0)
        let effective = InsertQuery(target: TableRef("items"), source: filtered.source, onConflict: .doUpdate(assignments: [Assignment(column: "a", value: col("items", "a"))], where: .bool(true)), conflictTarget: filtered.conflictTarget)
        await #expect(throws: SQLMutationError.cardinalityViolation) { try await execute(.insert(effective), container) }
    }

    @Test("Newly inserted duplicate conflict fails before its false action filter")
    func insertedThenFilteredDuplicate() async throws {
        let container = try await makeContainer(unique: .distinct)
        defer { await container.shutdown() }
        let query = InsertQuery(target: TableRef("items"), source: .values([[.string("a"), .int(1), .int(2)], [.string("b"), .int(1), .int(2)]]), onConflict: .doUpdate(assignments: [Assignment(column: "a", value: col("items", "a"))], where: .bool(false)), conflictTarget: .columns(["a", "b"], where: nil))
        await #expect(throws: SQLMutationError.cardinalityViolation) { try await execute(.insert(query), container) }
        #expect(try await execute(.delete(DeleteQuery(target: TableRef("items"))), container).count == 0)
    }

    @Test("MERGE first true clause and DO NOTHING determine effective count")
    func mergeClausePriority() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let query = MergeQuery(target: TableRef("items"), source: source([1, 3]), condition: .bool(true), clauses: [SQLMergeClause(match: .matched, condition: .equal(col("s", "x"), .int(1)), action: .doNothing), SQLMergeClause(match: .matched, action: .update([Assignment(column: "a", value: col("s", "x"))]))], returning: [.expression(.function(FunctionCall(name: "merge_action", arguments: []))), .column("a")])
        let result = try await execute(.merge(query), container)
        #expect(result.count == 1)
        #expect(result.rows.first?.values == [.primitive(.string("UPDATE")), .primitive(.int64(3))])
    }

    @Test("MERGE repeated effective modification is a cardinality failure")
    func mergeRepeatedTarget() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 2), container)
        let query = MergeQuery(target: TableRef("items"), source: source([3, 4]), condition: .bool(true), clauses: [SQLMergeClause(match: .matched, action: .delete)])
        await #expect(throws: SQLMutationError.cardinalityViolation) { try await execute(.merge(query), container) }
        #expect(try await execute(.delete(DeleteQuery(target: TableRef("items"))), container).count == 1)
    }

    @Test("Generic SPI rejects new SQL-only statement kinds before effects")
    func genericSPIRejectsSQLOnlyForms() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let executor = DatabaseEntityStatementMutationExecutor(container: container, limits: try DatabaseEntityMutationLimits(maximumChanges: 32, maximumPreconditions: 4))
        let statement = QueryStatement.merge(MergeQuery(target: TableRef("items"), source: source([1]), condition: .bool(false), clauses: [SQLMergeClause(match: .notMatchedByTarget, action: .insert(columns: ["id", "a", "b"], values: [.string("never"), .int(1), .int(2)], overriding: nil))]))
        await #expect(throws: DatabaseEntityStatementMutationError.self) {
            try await container.testBaseContext().withTransaction { transaction in
                try await executor.execute(statement, transaction: transaction, workMeter: DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock))
            }
        }
        #expect(try await execute(.delete(DeleteQuery(target: TableRef("items"))), container).count == 0)
    }

    @Test("DEFAULT VALUES evaluates defaults for one hidden-identity row")
    func defaultValues() async throws {
        let container = try await makeContainer(hidden: true, defaultA: .int(5), defaultB: .int(6))
        defer { await container.shutdown() }
        let result = try await execute(.insert(InsertQuery(target: TableRef("items"), source: .defaultValues, returning: [.wildcard()])), container)
        #expect(result.count == 1 && result.rows.first?.values == [.primitive(.int64(5)), .primitive(.int64(6))])
    }

    @Test("Hidden identity replay is stable and rejects divergent event order")
    func hiddenIdentityReplay() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let request = SQLMutationRequest(workMeter: meter)
        let field = FieldSchema(name: "id", fieldNumber: 1, type: .string)
        let first = try await request.allocateHiddenIdentity(relationID: "a", column: field, statement: 0)
        await request.beginAttempt()
        #expect(try await request.allocateHiddenIdentity(relationID: "a", column: field, statement: 0) == first)
        await request.beginAttempt()
        await #expect(throws: SQLMutationError.identityReplayDiverged) {
            try await request.allocateHiddenIdentity(relationID: "b", column: field, statement: 0)
        }
    }

    @Test("MERGE executes all three match classes with one mixed RETURNING schema")
    func mergeAllMatchClasses() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        _ = try await execute(insert("one", 1, 10), container)
        _ = try await execute(insert("two", 2, 20), container)
        let query = MergeQuery(target: TableRef("items"), source: source([1, 3]),
            condition: .equal(.col("a"), col("s", "x")), clauses: [
                SQLMergeClause(match: .matched, action: .update([Assignment(column: "b", value: .int(99))])),
                SQLMergeClause(match: .notMatchedByTarget, action: .insert(columns: ["id", "a", "b"], values: [.cast(col("s", "x"), targetType: .text), col("s", "x"), .int(30)], overriding: nil)),
                SQLMergeClause(match: .notMatchedBySource, action: .delete)
            ], returning: [.expression(.function(FunctionCall(name: "merge_action", arguments: [])), alias: "action"), .wildcard()])
        let result = try await execute(.merge(query), container)
        #expect(result.count == 3 && result.columns.count == 5)
        #expect(result.columns[4].isNullable)
        let deleted = try #require(result.rows.first { $0.values[0] == .primitive(.string("DELETE")) })
        #expect(deleted.values[4] == .primitive(.null))
        #expect(Set(result.rows.map { $0.values[0] }) == Set([QueryValue.primitive(.string("INSERT")), .primitive(.string("UPDATE")), .primitive(.string("DELETE"))]))
        #expect(try await execute(.delete(DeleteQuery(target: TableRef("items"))), container).count == 2)
    }

    @Test("Late conversion failure rolls back every prepared INSERT")
    func lateConversionAtomicity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let query = InsertQuery(target: TableRef("items"), source: .values([[.string("one"), .int(1), .int(2)], [.string("bad"), .string("not-an-integer"), .int(3)]]))
        await #expect(throws: (any Error).self) { try await execute(.insert(query), container) }
        #expect(try await execute(.delete(DeleteQuery(target: TableRef("items"))), container).count == 0)
    }

    private struct Output {
        let count: UInt64
        let columns: [QueryColumn]
        let rows: [ExecutionQueryRow]
    }
    private func execute(_ statement: QueryStatement, _ container: DBContainer) async throws -> Output {
        let work = DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1_024), monotonicClock: container.monotonicClock)
        let request = SQLMutationRequest(workMeter: work)
        let executor = DatabaseEntityStatementMutationExecutor(container: container, limits: try DatabaseEntityMutationLimits(maximumChanges: 128, maximumPreconditions: 16))
        let outcome = try await SQLMutationRuntime.$request.withValue(request) {
            try await container.testBaseContext().withTransaction { transaction in
                await request.beginAttempt()
                return try await executor.executeSQL(statement, searchPath: .default, transaction: transaction, workMeter: work)
            }
        }
        return Output(count: outcome.affectedRows, columns: outcome.returning?.rows.columns ?? [],
            rows: outcome.returning.map { $0.rows.materializePage($0.visibleRange) } ?? [])
    }
    private func insert(_ id: String, _ a: Int64, _ b: Int64) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("items"), source: .values([[.string(id), .int(a), .int(b)]])))
    }
    private func conflictInsert(_ id: String, a: Expression, b: Expression, action: OnConflictAction,
        target: SQLConflictTarget? = .columns(["a", "b"], where: nil)) -> QueryStatement {
        .insert(InsertQuery(target: TableRef("items"), source: .values([[.string(id), a, b]]), onConflict: action,
            returning: [.wildcard()], conflictTarget: target))
    }
    private func col(_ table: String, _ name: String) -> Expression { .column(ColumnRef(table: table, column: name)) }
    private func source(_ values: [Int64]) -> DataSource { .aliased(.values(values.map { [.int($0)] }), name: "s", columns: ["x"]) }

    private func makeContainer(hidden: Bool = false, unique: SQLNullUniqueness? = nil,
        defaultA: Expression? = nil, defaultB: Expression? = nil) async throws -> DBContainer {
        let fields = [FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "a", fieldNumber: 2, type: .int64, isOptional: true, queryType: .sql(.bigint)),
            FieldSchema(name: "b", fieldNumber: 3, type: .int64, isOptional: true, queryType: .sql(.bigint))]
        let schema = try Schema(entities: [Schema.Entity(name: "Item", identifierType: .string, fields: fields, directoryComponents: [.staticPath("s14-items")])], version: .init(1, 0, 0))
        let columns: [SQLColumn] = try (hidden ? [] : [SQLColumn(id: "id", name: "id", fieldNumber: 1)]) + [
            SQLColumn(id: "a", name: "a", backing: .field(number: 2), policy: .input(defaultExpression: defaultA)),
            SQLColumn(id: "b", name: "b", backing: .field(number: 3), policy: .input(defaultExpression: defaultB))]
        let constraints = try unique.map { [try SQLConstraint(attributes: SQLConstraintAttributes(id: "u", name: "u"), kind: .unique(columnIDs: ["a", "b"], nulls: $0))] } ?? []
        let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: "Item", columns: columns, constraints: constraints)
        let container = try await DBContainer.open(testing: schema, configuration: .testing(storageEngine: InMemoryEngine()), runtimeConfiguration: DatabaseFrameworkRuntime.configuration(executionIdentity: .init(identifier: "s14-mutations", revision: 1), schema: schema), security: .testingDisabled)
        _ = try await container.publishSQLCatalog(SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")], relations: [relation]), expectedGeneration: container.schemaGeneration, idempotencyKey: "s14-initial", authorization: TestBaseEnvironment.authorization)
        return container
    }
}
