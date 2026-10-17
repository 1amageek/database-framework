#if SQLITE
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(Testing) @_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S10 public recursive SQL", .timeLimit(.minutes(2)))
struct S10RecursiveQueryExecutionTests {
    @Persistable
    struct RecursiveItem: SecurityPolicy {
        #Directory<RecursiveItem>("s10_recursive_items")
        var id: String
        var n: Int32
        @Restricted(read: .roles(["secret"]), write: .roles(["secret"]))
        var secret: String = ""

        static func permitsRead(of resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { context.isAuthenticated }
        static func permitsQuery(_ query: borrowing SecurityQuery, in context: borrowing AuthorizationContext) -> Bool {
            context.principal?.roles.contains("reader") == true
        }
        static func permitsCreate(_ resource: borrowing Self, in context: borrowing AuthorizationContext) -> Bool { context.isAuthenticated }
    }

    @Test("Seed identity, current frontiers and forward sibling scopes match PostgreSQL")
    func frontiersAndForwardScopes() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let distinct = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1),(1) UNION SELECT n+1 FROM t WHERE n<3) SELECT n FROM t ORDER BY n")
        #expect(distinct.columns.map(\.type) == [.sql(.integer)])
        #expect(distinct.rows.map(\.values) == integers([1, 2, 3]))
        let all = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1),(1) UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT n,count(*) FROM t GROUP BY n ORDER BY n")
        #expect(all.rows.map(\.values) == (1...3).map { [.primitive(.int32(Int32($0))), .primitive(.int64(2))] })
        let null = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(NULL::integer),(NULL::integer) UNION SELECT n FROM t) SELECT n FROM t")
        #expect(null.rows.map(\.values) == [[.primitive(.null)]])
        let forward = try await context.executeSQL("WITH RECURSIVE a AS (SELECT * FROM b), b(n) AS (VALUES(7)) SELECT * FROM a")
        #expect(forward.rows.map(\.values) == integers([7]))
        let shadow = try await context.executeSQL("WITH RECURSIVE b(n) AS (VALUES(9)), a AS (WITH b(n) AS (VALUES(7)) SELECT n FROM b) SELECT * FROM a")
        #expect(shadow.rows.map(\.values) == integers([7]))
    }

    @Test("SEARCH publishes sortable rich descriptors with anonymous fields")
    func searchMetadataAndValues() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        for mode in ["DEPTH", "BREADTH"] {
            let response = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<3) SEARCH \(mode) FIRST BY n SET ord SELECT n,ord FROM t ORDER BY ord")
            #expect(response.columns.map(\.name) == ["n", "ord"])
            #expect(response.rows.map { $0.values[0] } == integers([1, 2, 3]).map { $0[0] })
            for (index, row) in response.rows.enumerated() {
                if mode == "DEPTH" {
                    try assertPath(row.values[1], keys: (1...Int32(index + 1)).map { .primitive(.int32($0)) })
                    guard case .sql(.array(.composite(let descriptor))) = response.columns[1].type else {
                        Issue.record("Expected SEARCH DEPTH record array metadata"); continue
                    }
                    #expect(descriptor.columns.map(\.name) == ["f1"])
                    #expect(descriptor.columns.map(\.type) == [.sql(.integer)])
                } else {
                    guard case .composite(let record) = row.values[1], case .sql(.composite(let descriptor)) = response.columns[1].type else {
                        Issue.record("Expected SEARCH BREADTH record metadata"); continue
                    }
                    #expect(descriptor.columns.map(\.name) == ["f1", "f2"])
                    #expect(descriptor.columns.map(\.type) == [.sql(.bigint), .sql(.integer)])
                    #expect(Array(record.fields) == [.primitive(.int64(Int64(index))), .primitive(.int32(Int32(index + 1)))])
                }
            }
        }
        let identity = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION SELECT 3-n FROM t) SEARCH DEPTH FIRST BY n SET ord SELECT n,ord FROM t LIMIT 5")
        #expect(identity.rows.map { $0.values[0] } == integers([1, 2, 1, 2, 1]).map { $0[0] })
        try #require(identity.rows.count == 5)
        try assertPath(identity.rows[4].values[1], keys: integers([1, 2, 1, 2, 1]).map { $0[0] })
        let expanded = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<2) SEARCH BREADTH FIRST BY n SET ord SELECT (ord).* FROM t ORDER BY ord")
        #expect(expanded.columns.map(\.name) == ["f1", "f2"])
        #expect(expanded.columns.map(\.type) == [.sql(.bigint), .sql(.integer)])
        #expect(expanded.rows.map(\.values) == [[.primitive(.int64(0)), .primitive(.int32(1))], [.primitive(.int64(1)), .primitive(.int32(2))]])
    }

    @Test("CYCLE emits the repeated key, uses NULL-safe paths and selected marks")
    func cyclePathsAndMarks() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let cycle = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n%2+1 FROM t) SEARCH BREADTH FIRST BY n SET ord CYCLE n SET cyclic TO 'Y' DEFAULT 'N' USING path SELECT n,cyclic,path FROM t ORDER BY ord")
        #expect(cycle.columns.map(\.name) == ["n", "cyclic", "path"])
        #expect(cycle.columns[1].type == .sql(.text))
        #expect(cycle.rows.map { $0.values[1] } == ["N", "N", "Y"].map { .primitive(.string($0)) })
        for index in cycle.rows.indices { try assertPath(cycle.rows[index].values[2], keys: integers(Array([1, 2, 1].prefix(index + 1))).map { $0[0] }) }
        let null = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(NULL::integer) UNION ALL SELECT n FROM t) CYCLE n SET cyclic USING path SELECT n,cyclic,path FROM t")
        try #require(null.rows.count == 2)
        #expect(null.rows.map { $0.values[1] } == [.primitive(.bool(false)), .primitive(.bool(true))])
        try assertPath(null.rows[1].values[2], keys: [.primitive(.null), .primitive(.null)])
        for constants in ["TO true DEFAULT true", "TO NULL DEFAULT NULL"] {
            let stopped = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<3) CYCLE n SET cyclic \(constants) USING path SELECT * FROM t")
            #expect(stopped.rows.count == 1)
        }
        let generated = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE NOT cyclic) CYCLE n SET cyclic USING path SELECT n,cyclic FROM t LIMIT 2")
        #expect(generated.rows.map(\.values) == [[.primitive(.int32(1)), .primitive(.bool(false))], [.primitive(.int32(2)), .primitive(.bool(false))]])
        let joined = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT s.n+v.step FROM t s JOIN (VALUES(1)) v(step) ON NOT s.cyclic WHERE (s.ord).f1<2) SEARCH BREADTH FIRST BY n SET ord CYCLE n SET cyclic USING path SELECT n FROM t ORDER BY ord")
        #expect(joined.rows.map(\.values) == integers([1, 2, 3]))
    }

    @Test("LIMIT and EXISTS stop infinite recursion while consumers replay one volatile prefix")
    func recursiveDemandAndReplay() async throws {
        let clock = S10RecursiveClock()
        let (container, _) = try await makeContainer(clock: clock); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let limited = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t) SELECT n FROM t LIMIT 5")
        #expect(limited.rows.map(\.values) == integers([1, 2, 3, 4, 5]))
        let exists = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t) SELECT EXISTS(SELECT 1 FROM t)")
        #expect(exists.rows.map(\.values) == [[.primitive(.bool(true))]])
        var before = clock.count
        _ = try await context.executeSQL("SELECT 1")
        let overhead = clock.count - before
        before = clock.count
        let unused = try await context.executeSQL("WITH RECURSIVE t(n) AS (SELECT 1/0 UNION ALL SELECT n+1 FROM t) SELECT 7")
        #expect(unused.rows.map(\.values) == integers([7]))
        #expect(clock.count - before == overhead)
        before = clock.count
        let replay = try await context.executeSQL("WITH RECURSIVE t(n,v) AS (SELECT 1,clock_timestamp() UNION ALL SELECT n+1,clock_timestamp() FROM t WHERE n<3) SELECT (SELECT count(*) FROM t),(SELECT count(*) FROM t)")
        #expect(replay.rows.map(\.values) == [[.primitive(.int64(3)), .primitive(.int64(3))]])
        #expect(clock.count - before == overhead + 3)
        before = clock.count
        let prelude = try await context.executeSQL("WITH RECURSIVE t(n,v) AS (WITH x AS MATERIALIZED (SELECT clock_timestamp() v) SELECT 1,v FROM x UNION ALL SELECT t.n+1,x.v FROM t CROSS JOIN x WHERE t.n<3) SELECT n,v FROM t ORDER BY n")
        #expect(prelude.rows.map { $0.values[0] } == integers([1, 2, 3]).map { $0[0] })
        try #require(prelude.rows.count == 3)
        let preludeValue = prelude.rows[0].values[1]
        #expect(!preludeValue.isNull)
        #expect(prelude.rows.allSatisfy { $0.values[1] == preludeValue })
        #expect(clock.count - before == overhead + 1)
    }

    @Test("Invalid and unused recursive definitions fail before physical reads")
    func bindingRejectsBeforeReads() async throws {
        let (container, control) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let invalid = [
            "WITH t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t) SELECT n FROM \"RecursiveItem\"",
            "WITH RECURSIVE a(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM b), b(n) AS (SELECT n FROM a) SELECT n FROM \"RecursiveItem\"",
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT a.n+b.n FROM t a,t b) SELECT 1",
            "WITH RECURSIVE t(n) AS (SELECT n FROM t UNION ALL SELECT n+1 FROM t) SELECT 1",
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT (SELECT n FROM t)) SELECT 1",
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT t.n FROM (VALUES(1)) v(n) LEFT JOIN t ON true) SELECT 1",
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT sum(n)::integer FROM t GROUP BY n) SELECT 1",
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n::bigint+1 FROM t) SELECT 1",
            "WITH RECURSIVE t(n) AS (VALUES(1::bigint) UNION ALL SELECT n::integer+1 FROM t) SELECT 1",
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n,n FROM t) SELECT 1",
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t) CYCLE missing SET cyclic USING path SELECT 1"
        ]
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        try await context.withReadSnapshot(workMeter: meter) { _ in
            let before = control.dataReadOperationCount
            for sql in invalid {
                let readingSQL = sql.hasSuffix("SELECT 1") ? String(sql.dropLast(8)) + "SELECT n FROM \"RecursiveItem\"" : sql
                do { _ = try await context.executeSQL(readingSQL); Issue.record("Expected recursive binding failure: \(sql)") }
                catch is SQLBindingSourceFailure { }
                catch is SQLParser.ParseError { }
            }
            await #expect(throws: QueryStructuralValidationError.invalidCTESearchClause) {
                _ = try await context.executeSQL("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t) SEARCH DEPTH FIRST BY n,n SET ord SELECT n FROM \"RecursiveItem\"")
            }
            #expect(control.dataReadOperationCount == before)
        }
    }

    @Test("Logical self tables shadow physical names while real fields remain authorized")
    func recursiveAuthority() async throws {
        let (container, _) = try await makeContainer(security: .enabled()); defer { await container.shutdown() }
        let writer = container.testBaseContext(authorization: .authenticated(Principal(identifier: "test-runner", roles: ["admin", "reader", "secret"])))
        try writer.insert(RecursiveItem(id: "one", n: 99, secret: "classified"))
        try writer.insert(RecursiveItem(id: "seed", n: 1, secret: "classified"))
        try await writer.save()
        let reader = container.testBaseContext(authorization: .authenticated(Principal(identifier: "test-runner", roles: ["admin", "reader"])))
        let denied = container.testBaseContext(authorization: .authenticated(Principal(identifier: "test-runner", roles: ["admin"])))
        let logical = try await denied.executeSQL("WITH RECURSIVE \"RecursiveItem\"(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM \"RecursiveItem\" WHERE n<2) SELECT n FROM \"RecursiveItem\"")
        #expect(logical.rows.map(\.values) == integers([1, 2]))
        let physical = try await reader.executeSQL("WITH RECURSIVE t(n) AS (SELECT n FROM \"RecursiveItem\" UNION ALL SELECT n+1 FROM t WHERE n<2) SELECT n FROM t ORDER BY n")
        #expect(physical.columns.map(\.type) == [.primitive(.int32)])
        #expect(physical.rows.map(\.values) == integers([1, 2, 99]))
        await #expect(throws: FieldSecurityError.readNotAllowed(type: RecursiveItem.persistableType, fields: ["secret"])) {
            _ = try await reader.executeSQL("WITH RECURSIVE t(n) AS (SELECT secret FROM \"RecursiveItem\" UNION ALL SELECT n FROM t WHERE false) SELECT * FROM t")
        }
        await #expect(throws: SecurityError.self) {
            _ = try await denied.executeSQL("WITH RECURSIVE t(n) AS (SELECT n FROM \"RecursiveItem\" UNION ALL SELECT n+1 FROM t WHERE n<2) SELECT n FROM t")
        }
    }

    @Test("Recursive output composes with grouping and selected windows")
    func recursiveAnalytics() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let response = try await container.testBaseContext().executeSQL("WITH RECURSIVE t(n) AS (VALUES(1),(1) UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT n,count(*),sum(count(*)) OVER(ORDER BY n) FROM t GROUP BY n ORDER BY n")
        #expect(response.columns.map(\.type) == [.sql(.integer), .sql(.bigint), .sql(.decimal(precision: nil, scale: nil))])
        let expected: [[QueryValue]] = try (1...3).map { n in
            [.primitive(.int32(Int32(n))), .primitive(.int64(2)), .numeric(try QueryNumericValue(sqlLexicalForm: String(n * 2)[...], maximumUTF8Count: 2))]
        }
        #expect(response.rows.map(\.values) == expected)
    }

    @Test("Recursive growth fails boundedly, releases claims and leaves immutable public values")
    func recursiveResourcesAndRelease() async throws {
        let (container, _) = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let finite = try SQLParser().parseSelect("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<4) CYCLE n SET cyclic USING path SELECT n,path FROM t")
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let retained = try await context.queryRetained(finite, execution: execution(container, meter))
        #expect(meter.retainedIntermediateRows > 0 && meter.retainedIntermediateBytes > 0)
        let response = try retained.promoteToPublicResponse()
        #expect(meter.retainedIntermediateRows == 0 && meter.retainedIntermediateBytes == 0)
        let infinite = try SQLParser().parseSelect("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t) SELECT n FROM t")
        for budget in [ExecutionBudget(maximumWorkUnits: 20_000), ExecutionBudget(maximumIntermediateRows: 20), ExecutionBudget(maximumIntermediateBytes: 262_144)] {
            let limited = DatabaseWorkMeter(budget: budget, monotonicClock: container.monotonicClock)
            await #expect(throws: DatabaseWorkLimitError.self) { _ = try await context.queryRetained(infinite, execution: execution(container, limited)) }
            #expect(limited.retainedIntermediateRows == 0 && limited.retainedIntermediateBytes == 0)
        }
        let cancelled = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let completion = S10RecursiveCompletion()
        let task = Task {
            defer { completion.finish() }
            _ = try await context.queryRetained(infinite, execution: execution(container, cancelled))
        }
        while cancelled.consumedWorkUnits < 2_000 && !completion.isFinished { await Task.yield() }
        task.cancel()
        do { _ = try await task.value; Issue.record("Expected recursive cancellation") } catch is CancellationError { }
        #expect(cancelled.retainedIntermediateRows == 0 && cancelled.retainedIntermediateBytes == 0)
        let followup = try await context.queryRetained(try SQLParser().parseSelect("SELECT 7"), execution: execution(container, cancelled))
        let followupResponse = try followup.promoteToPublicResponse()
        #expect(followupResponse.rows.map(\.values) == integers([7]))
        #expect(cancelled.retainedIntermediateRows == 0 && cancelled.retainedIntermediateBytes == 0)
        await container.shutdown()
        #expect(response.rows.count == 4)
        try assertPath(response.rows[3].values[1], keys: integers([1, 2, 3, 4]).map { $0[0] })
    }

    private func integers(_ values: [Int32]) -> [[QueryValue]] { values.map { [.primitive(.int32($0))] } }

    private func assertPath(_ value: QueryValue, keys: [QueryValue]) throws {
        guard case .array(let path) = value, case .composite(let descriptor) = path.elementType else {
            Issue.record("Expected generated record array"); return
        }
        #expect(path.dimensions.map(\.lowerBound) == [1])
        #expect(path.dimensions.map(\.length) == [Int32(keys.count)])
        #expect(descriptor.columns.map(\.name) == ["f1"])
        #expect(descriptor.columns.map(\.type) == [.sql(.integer)])
        for (element, key) in zip(path.elements, keys) {
            guard case .composite(let record) = element else { Issue.record("Expected generated record"); continue }
            #expect(record.type == descriptor)
            #expect(Array(record.fields) == [key])
        }
    }

    private func execution(_ container: DBContainer, _ meter: DatabaseWorkMeter) -> ReadExecutionContext {
        ReadExecutionContext(options: ReadExecutionOptions(budget: meter.budget), monotonicClock: container.monotonicClock, workMeter: meter)
    }

    private func makeContainer(clock: any WallClock = FixedTestWallClock(), security: SecurityConfiguration = .testingDisabled) async throws -> (DBContainer, StorageTransactionControl) {
        let storage = ControlledStorageEngine(base: try SQLiteStorageEngine(configuration: .inMemory))
        let configuration: DBConfiguration
        #if MultiBase
        configuration = DBConfiguration(testingName: "sql-s10-recursive", storageTopology: try .testing(storageEngine: storage), monotonicClock: TestProcessMonotonicClock(), wallClock: clock, testingBaseID: try TestBaseEnvironment.id(), testingPrincipal: Principal(identifier: "test-runner", roles: ["admin"]))
        #else
        configuration = try DBConfiguration(storageEngine: storage, monotonicClock: TestProcessMonotonicClock(), wallClock: clock)
        #endif
        let container = try await DBContainer.open(for: Schema(entities: [try RecursiveItem.schemaEntity]), configuration: configuration,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "s10-recursive", revision: 1),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(RecursiveItem.self)], authorizationPolicies: [AuthorizationPolicyHandler(RecursiveItem.self)]), security: security)
        return (container, storage.control)
    }
}

private final class S10RecursiveClock: WallClock, Sendable {
    private let state = Mutex<Int64>(0)
    var count: Int64 { state.withLock { $0 } }
    var now: Timestamp { state.withLock { $0 += 1; return Timestamp(secondsSinceUnixEpoch: 1_704_067_200 + $0) } }
}

private final class S10RecursiveCompletion: Sendable {
    private let state = Mutex(false)
    var isFinished: Bool { state.withLock { $0 } }
    func finish() { state.withLock { $0 = true } }
}
#endif
