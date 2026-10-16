import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@testable import Database
@_spi(Testing) @_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S08 selected relational SQL", .timeLimit(.minutes(2)))
struct S08RelationalExecutionTests {
    @Test("S08-E01 mixed wildcards and partial aliases preserve empty descriptors")
    func projectionAndEmptyMetadata() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let response = try await context.executeSQL("SELECT *, v.*, 9 a, NULL::text b FROM (VALUES (1,NULL::text)) v(a,b)")
        try #require(response.rows.count == 1)
        #expect(response.columns.map(\.name) == ["a", "b", "a", "b", "a", "b"])
        #expect(response.rows[0].values == [.primitive(.int32(1)), .primitive(.null), .primitive(.int32(1)), .primitive(.null), .primitive(.int32(9)), .primitive(.null)])
        let empty = try await context.executeSQL("SELECT * FROM (SELECT 1 AS a, NULL::text AS b) v(x) WHERE FALSE")
        #expect(empty.rows.isEmpty)
        #expect(empty.columns.map(\.name) == ["x", "b"])
        #expect(empty.columns.map(\.type) == [.sql(.integer), .sql(.text)])
        await #expect(throws: (any Error).self) { _ = try await context.executeSQL("SELECT * FROM (VALUES (1)) v(a,b)") }
        let expanded = try await context.executeSQL("VALUES ((ROW(1,2)).*), (3,4)")
        #expect(expanded.rows.map(\.values) == [[.primitive(.int32(1)), .primitive(.int32(2))],
            [.primitive(.int32(3)), .primitive(.int32(4))]])
        let direct = SelectQuery(projection: [.wildcard()], source: .values([
            [.fieldExpansion(.row([.cast(.literal(.int(1)), targetType: .integer),
                .cast(.literal(.int(2)), targetType: .integer)]))],
            [.cast(.literal(.int(3)), targetType: .integer), .cast(.literal(.int(4)), targetType: .integer)]
        ]))
        let directExpanded = try await context.query(direct)
        #expect(directExpanded.rows.map(\.values) == expanded.rows.map(\.values))
        let contextual = try await context.executeSQL("VALUES ((ROW(NULL,'a')).*), (1,'b')")
        #expect(contextual.columns.map(\.type) == [.sql(.integer), .sql(.text)])
        #expect(contextual.rows.map(\.values) == [[.primitive(.null), .primitive(.string("a"))],
            [.primitive(.int32(1)), .primitive(.string("b"))]])
        await #expect(throws: (any Error).self) { _ = try await context.executeSQL("VALUES ((ROW(1,2)).*), (3)") }
    }

    @Test("S08-E01 NATURAL order and FULL USING retain qualified source presence")
    func naturalAndFullJoin() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let natural = try await context.executeSQL("SELECT * FROM (VALUES(1,2,3)) l(z,a,lval) NATURAL JOIN (VALUES(2,1,4)) r(a,z,rval)")
        #expect(natural.columns.map(\.name) == ["z", "a", "lval", "rval"])
        try #require(natural.rows.count == 1)
        #expect(natural.rows[0].values == [.primitive(.int32(1)), .primitive(.int32(2)), .primitive(.int32(3)), .primitive(.int32(4))])
        let full = try await context.executeSQL("SELECT *,l.*,r.* FROM (VALUES(1::smallint,'l1'),(2,'l2')) l(k,lval) FULL JOIN (VALUES(2::bigint,'r2'),(3,'r3')) r(k,rval) USING(k) ORDER BY coalesce(l.k,r.k)")
        #expect(full.columns.map(\.name) == ["k", "lval", "rval", "k", "lval", "k", "rval"])
        #expect(full.columns[0].type == .sql(.bigint))
        try #require(full.rows.count == 3)
        #expect(full.rows[0].values[5].isNull)
        #expect(full.rows[2].values[3].isNull)
        let temporal = try await context.executeSQL("SELECT k FROM (VALUES(DATE '2020-01-01')) l(k) FULL JOIN (VALUES(TIMESTAMP '2020-01-02 00:00:00')) r(k) USING(k) ORDER BY k")
        #expect(temporal.columns.map(\.type) == [.sql(.timestamp(withTimeZone: false))])
        #expect(temporal.rows.count == 2)
        let arrayKey = try await context.executeSQL("SELECT k FROM (VALUES(ARRAY[1::smallint])) l(k) JOIN (VALUES(ARRAY[1::bigint])) r(k) USING(k)")
        #expect(arrayKey.columns.map(\.type) == [.sql(.array(.bigint))])
        try #require(arrayKey.rows.count == 1)
        guard case .array(let key) = arrayKey.rows[0].values[0] else { Issue.record("Expected coerced USING array"); return }
        #expect(Array(key.elements) == [.primitive(.int64(1))])
    }

    @Test("S08-E02 present all-NULL composite differs from absent outer row")
    func wholeRowPresenceAndJSON() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let response = try await container.testBaseContext().executeSQL("SELECT l.key,row_to_json(r),to_json(r),r IS NULL,r IS NOT NULL,r IS DISTINCT FROM NULL,row_to_json(ROW(r.*)) FROM (VALUES(1),(2)) l(key) LEFT JOIN (SELECT NULL::integer a,NULL::text b) r ON l.key=1 ORDER BY l.key")
        try #require(response.rows.count == 2)
        #expect(try jsonText(response.rows[0].values[1]) == "{\"a\":null,\"b\":null}")
        #expect(response.rows[1].values[1].isNull && response.rows[1].values[2].isNull)
        #expect(response.rows[0].values[3] == .primitive(.bool(true)))
        #expect(response.rows[1].values[3] == .primitive(.bool(true)))
        #expect(response.rows[0].values[5] == .primitive(.bool(true)))
        #expect(response.rows[1].values[5] == .primitive(.bool(false)))
        for row in response.rows { #expect(try jsonText(row.values[6]) == "{\"f1\":null,\"f2\":null}") }
    }

    @Test("S08-E02 constructor comparison and IN use selected common types")
    func constructorAndSubqueryComparison() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let response = try await container.testBaseContext().executeSQL("SELECT ROW(1,NULL)=ROW(1,NULL),ROW(1,NULL)<ROW(2,0),ROW(1,NULL)<ROW(1,0),2 IN (SELECT 1.5::numeric),ROW(1,2)=(SELECT 1,2),ROW(1,NULL) IS NOT NULL")
        try #require(response.rows.count == 1)
        #expect(response.rows[0].values == [.primitive(.null), .primitive(.bool(true)), .primitive(.null), .primitive(.bool(false)), .primitive(.bool(true)), .primitive(.bool(false))])
    }

    @Test("S08-E03 ARRAY subquery retains scalar NULLs and inner array lower bounds")
    func arraySubqueryShapes() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let response = try await context.executeSQL("SELECT ARRAY(SELECT i FROM (VALUES(1),(NULL::integer),(2)) v(i) ORDER BY i NULLS FIRST),ARRAY(SELECT 1 WHERE FALSE),ARRAY(SELECT a FROM (VALUES('[0:1]={1,2}'::integer[]),('[0:1]={3,4}'::integer[])) v(a))")
        try #require(response.rows.count == 1)
        guard case .array(let scalar) = response.rows[0].values[0], case .array(let empty) = response.rows[0].values[1], case .array(let nested) = response.rows[0].values[2] else { Issue.record("Expected selected array values"); return }
        #expect(Array(scalar.elements) == [.primitive(.null), .primitive(.int32(1)), .primitive(.int32(2))])
        #expect(empty.elements.isEmpty && empty.dimensions.isEmpty)
        #expect(nested.dimensions.map(\.lowerBound) == [1, 0])
        #expect(nested.dimensions.map(\.length) == [2, 2])
        for sql in ["SELECT ARRAY(SELECT 1,2)", "SELECT ARRAY(SELECT NULL::integer[])", "SELECT ARRAY(SELECT '{}'::integer[])", "SELECT ARRAY(SELECT a FROM (VALUES('[0:1]={1,2}'::integer[]),('[1:2]={3,4}'::integer[])) v(a))"] {
            await #expect(throws: (any Error).self) { _ = try await context.executeSQL(sql) }
        }
    }

    @Test("S08-E04 binary set ALL multiplicities and local pagination remain positional")
    func setBags() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let intersect = try await context.executeSQL("SELECT x FROM (VALUES(1),(1),(2),(NULL::integer)) v(x) INTERSECT ALL SELECT x FROM (VALUES(1),(1),(1),(NULL::integer)) v(x) ORDER BY x NULLS LAST")
        #expect(intersect.rows.map { $0.values[0] } == [.primitive(.int32(1)), .primitive(.int32(1)), .primitive(.null)])
        let except = try await context.executeSQL("SELECT x FROM (VALUES(1),(1),(1),(2)) v(x) EXCEPT ALL SELECT x FROM (VALUES(1),(2)) v(x) ORDER BY x")
        #expect(except.rows.map { $0.values[0] } == [.primitive(.int32(1)), .primitive(.int32(1))])
        let precedence = try await context.executeSQL("SELECT 1 x UNION SELECT 2 INTERSECT SELECT 2 ORDER BY x DESC")
        #expect(precedence.rows.map { $0.values[0] } == [.primitive(.int32(2)), .primitive(.int32(1))])
        for value in ["1", "NULL::integer"] {
            let excluded = try await context.executeSQL("SELECT x FROM (VALUES(\(value)),(\(value))) v(x) EXCEPT SELECT \(value)")
            #expect(excluded.rows.isEmpty)
            let remaining = try await context.executeSQL("SELECT x FROM (VALUES(\(value)),(\(value))) v(x) EXCEPT ALL SELECT \(value)")
            #expect(remaining.rows.count == 1)
        }
        let json = try await context.executeSQL("SELECT '{}'::json UNION ALL SELECT '{}'::json")
        #expect(json.rows.count == 2)
        for row in json.rows { #expect(try jsonText(row.values[0]) == "{}") }
        await #expect(throws: (any Error).self) { _ = try await context.executeSQL("SELECT '{}'::json UNION SELECT '{}'::json") }
        let arrays = try await context.executeSQL("SELECT ARRAY[1::smallint] UNION ALL SELECT ARRAY[2::bigint]")
        #expect(arrays.columns.map(\.type) == [.sql(.array(.bigint))])
        var arrayCells: [[QueryValue]] = []
        for row in arrays.rows {
            guard case .array(let value) = row.values[0] else { Issue.record("Expected common bigint array"); return }
            arrayCells.append(Array(value.elements))
        }
        #expect(arrayCells == [[.primitive(.int64(1))], [.primitive(.int64(2))]])
        let temporal = try await context.executeSQL("SELECT DATE '2020-01-01' UNION ALL SELECT TIMESTAMP '2020-01-02 00:00:00'")
        #expect(temporal.columns.map(\.type) == [.sql(.timestamp(withTimeZone: false))])
        #expect(temporal.rows.count == 2)
        let records = try await context.executeSQL("SELECT ROW(1::smallint) UNION ALL SELECT ROW(2::bigint)")
        try #require(records.rows.count == 2)
        for (index, row) in records.rows.enumerated() {
            guard case .composite(let record) = row.values[0] else { Issue.record("Expected common composite"); return }
            #expect(record.type.columns.map(\.type) == [.sql(.bigint)])
            #expect(Array(record.fields) == [.primitive(.int64(Int64(index + 1)))])
        }
    }

    @Test("S08-E04 DISTINCT ON and final peer groups use only selected SQL keys")
    func distinctOnAndTies() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let distinct = try await context.executeSQL("SELECT DISTINCT ON(a,b) a,b,c FROM (VALUES(1,1,2),(1,1,1),(1,2,3)) v(a,b,c) ORDER BY b,a,c")
        #expect(distinct.rows.map { $0.values[2] } == [.primitive(.int32(1)), .primitive(.int32(3))])
        let ties = try await context.executeSQL("SELECT a,b FROM (VALUES(1,'a'),(1,'b'),(2,'c'),(2,'d'),(3,'e')) v(a,b) ORDER BY a OFFSET 1 ROW FETCH NEXT 2 ROWS WITH TIES")
        #expect(ties.rows.map { $0.values[0] } == [.primitive(.int32(1)), .primitive(.int32(2)), .primitive(.int32(2))])
        let nulls = try await context.executeSQL("SELECT x FROM (VALUES(NULL::integer),(2),(1)) v(x) ORDER BY x")
        #expect(nulls.rows.map { $0.values[0] } == [.primitive(.int32(1)), .primitive(.int32(2)), .primitive(.null)])
        for sql in ["SELECT DISTINCT ON(a) a FROM (VALUES(1),(1)) v(a) ORDER BY v.a",
                    "SELECT DISTINCT ON(v.a) a FROM (VALUES(1),(1)) v(a) ORDER BY a"] {
            let qualified = try await context.executeSQL(sql)
            #expect(qualified.rows.map(\.values) == [[.primitive(.int32(1))]])
        }
        await #expect(throws: (any Error).self) {
            _ = try await context.executeSQL("SELECT DISTINCT ON(a) a,b FROM (VALUES(1,2)) v(a,b) ORDER BY b,a")
        }
    }

    @Test("S08-E05 selected pagination parameters and explicit NULL CAST retain meaning")
    func expressionPagination() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let response = try await context.executeSQL("SELECT x FROM (VALUES(1),(2),(3),(4)) v(x) ORDER BY x OFFSET $1 LIMIT $2", parameters: [QueryParameter(position: 1, value: .primitive(.int64(1))), QueryParameter(position: 2, value: .primitive(.int64(2)))])
        try #require(response.rows.count == 2)
        #expect(response.rows.map { $0.values[0] } == [.primitive(.int32(2)), .primitive(.int32(3))])
        let unlimited = try await context.executeSQL("SELECT x FROM (VALUES(1),(2)) v(x) ORDER BY x FETCH FIRST (NULL::bigint) ROWS WITH TIES")
        #expect(unlimited.rows.count == 2)
        for sql in ["SELECT 1 LIMIT -1", "SELECT x FROM (VALUES(1)) v(x) LIMIT x", "SELECT 1 ORDER BY 1 FETCH FIRST NULL ROWS WITH TIES", "SELECT 1 FETCH FIRST 1 ROW WITH TIES"] {
            await #expect(throws: (any Error).self) { _ = try await context.executeSQL(sql) }
        }
    }

    @Test("S08-E06 materialized CTE demand grows one prefix without volatile reruns")
    func materializedCTEDemand() async throws {
        let clock = S08CountingClock()
        let container = try await makeContainer(clock: clock); defer { await container.shutdown() }
        let context = container.testBaseContext()
        var before = clock.count
        _ = try await context.executeSQL("SELECT 1")
        let overhead = clock.count - before
        before = clock.count
        let limited = try await context.executeSQL("WITH c AS MATERIALIZED (SELECT clock_timestamp() t FROM (VALUES(1),(2),(3)) v(x)) SELECT t FROM c LIMIT 1")
        #expect(limited.rows.count == 1)
        #expect(clock.count - before == overhead + 1)
        before = clock.count
        _ = try await context.executeSQL("WITH c AS MATERIALIZED (SELECT clock_timestamp() t FROM (VALUES(3),(2),(1)) v(x) ORDER BY x) SELECT t FROM c LIMIT 1")
        #expect(clock.count - before == overhead + 1)
        before = clock.count
        _ = try await context.executeSQL("WITH c AS MATERIALIZED (SELECT clock_timestamp() t FROM (VALUES(1),(2),(3)) v(x)) SELECT (SELECT t FROM c LIMIT 1), (SELECT count(*) FROM c)")
        #expect(clock.count - before == overhead + 3)
        before = clock.count
        _ = try await context.executeSQL("WITH c AS NOT MATERIALIZED (SELECT clock_timestamp() t FROM (VALUES(1),(2),(3)) v(x)) SELECT count(*) FROM c l CROSS JOIN c r")
        #expect(clock.count - before == overhead + 3)
        before = clock.count
        _ = try await context.executeSQL("WITH c AS (SELECT clock_timestamp()) SELECT 1")
        #expect(clock.count - before == overhead)
    }

    @Test("S08-E04 an ORDER alias reuses its target while separate occurrences remain separate")
    func orderedProjectionOccurrence() async throws {
        let clock = S08CountingClock()
        let container = try await makeContainer(clock: clock); defer { await container.shutdown() }
        let context = container.testBaseContext()
        var before = clock.count
        _ = try await context.executeSQL("SELECT 1")
        let overhead = clock.count - before
        before = clock.count
        let response = try await context.executeSQL("SELECT clock_timestamp() x,clock_timestamp() y FROM (VALUES(1),(2),(3)) v(n) ORDER BY x")
        try #require(response.rows.count == 3)
        #expect(clock.count - before == overhead + 6)
        #expect(response.rows.allSatisfy { $0.values[0] != $0.values[1] })
        for (sql, failingExpression) in [
            ("SELECT * FROM (VALUES(1,1),(2,7/0)) v(a,b)", "7/0"),
            ("WITH c AS (SELECT 1 x) SELECT 1 FROM c ORDER BY 11/(x-1)", "11/(x-1)"),
            ("WITH c AS (SELECT 1 x) SELECT DISTINCT ON(13/(x-1)) 1 FROM c", "13/(x-1)")
        ] {
            do {
                _ = try await context.executeSQL(sql)
                Issue.record("Expected the selected relational expression to fail")
            } catch let failure as SQLBindingSourceFailure {
                guard case .expression(.divisionByZero) = failure.cause else {
                    Issue.record("Expected the original division failure"); continue
                }
                let start = try #require(sql.range(of: failingExpression)?.lowerBound)
                #expect(failure.position == sql.utf8.distance(from: sql.utf8.startIndex, to: start))
                #expect(failure.occurrence != nil)
            }
        }
    }

    @Test("EXISTS skips unneeded targets and stops after its first selected row")
    func existenceDemandSkipsProjection() async throws {
        let container = try await makeContainer(); defer { await container.shutdown() }
        let response = try await container.testBaseContext().executeSQL("SELECT EXISTS(SELECT 1/0 FROM (VALUES(1),(2)) v(n)),EXISTS(SELECT 1/0 LIMIT 0),EXISTS(SELECT 1 FROM (VALUES(1),(2)) v(n) OFFSET 2)")
        try #require(response.rows.count == 1)
        #expect(response.rows[0].values == [.primitive(.bool(true)), .primitive(.bool(false)), .primitive(.bool(false))])
    }

    private func jsonText(_ value: QueryValue) throws -> String {
        guard case .json(let json) = value else { throw S08TestFailure.expectedJSON }
        return String(decoding: json.bytes, as: UTF8.self)
    }

    private func makeContainer(clock: S08CountingClock? = nil) async throws -> DBContainer {
        let engine = InMemoryEngine()
        let configuration: DBConfiguration
        if let clock {
            #if MultiBase
            let domainID = try DatabaseStorageDomain.ID("sql-s08")
            let placementID = try Base.Placement.ID("sql-s08")
            let topology = try DatabaseStorageTopology(controlDomainID: domainID,
                domains: [try DatabaseStorageDomain(id: domainID,
                    rootPath: testingDatabaseRootPath(databaseIdentifier: "sql-s08"), storageEngine: engine)],
                placements: [DatabaseStoragePlacement(id: placementID, domainID: domainID)], defaultPlacementID: placementID)
            configuration = DBConfiguration(testingName: "sql-s08", storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(), wallClock: clock,
                testingBaseID: try TestBaseEnvironment.id(), testingPrincipal: Principal(identifier: "test-runner", roles: ["test-runner", "admin"]))
            #else
            configuration = try DBConfiguration(storageEngine: engine, monotonicClock: TestProcessMonotonicClock(), wallClock: clock)
            #endif
        } else { configuration = try .testing(storageEngine: engine) }
        return try await DBContainer.open(testing: Schema(entities: []), configuration: configuration,
            runtimeConfiguration: DatabaseRuntimeConfiguration(executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "sql-s08-tests", revision: 1)), security: .testingDisabled)
    }
}

private enum S08TestFailure: Error { case expectedJSON }

private final class S08CountingClock: WallClock, Sendable {
    private let state = Mutex<Int64>(0)
    var count: Int64 { state.withLock { $0 } }
    var now: Timestamp {
        state.withLock { count in
            count += 1
            return Timestamp(secondsSinceUnixEpoch: 1_704_067_200 + count)
        }
    }
}
