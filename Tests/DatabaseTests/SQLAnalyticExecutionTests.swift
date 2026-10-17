import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import Database
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL grouping and analytics")
struct SQLAnalyticExecutionTests {
    @Persistable
    struct AnalyticItem {
        #Directory<AnalyticItem>("sql_analytic_items")
        var id: String
        var category: String
        var value: Int32
    }

    private struct Fixture {
        let id: String
        let sql: String
        let names: [String]
        let types: [QueryColumnType]
        let rows: [[QueryValue]]
    }

    @Test("Public grouping sets match PostgreSQL 18.6", .timeLimit(.minutes(2)))
    func groupingReferenceCases() async throws {
        try await referenceCases(prefix: "grouping.")
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let response = try await container.testBaseContext().executeSQL("""
            SELECT a,(SELECT a),a+1,COALESCE(a,0),GROUPING(a),COUNT(*)
            FROM (VALUES(1),(2)) t(a) GROUP BY ROLLUP(a) ORDER BY 5,1
            """)
        #expect(response.rows.map(\.values) == [
            [.primitive(.int32(1)), .primitive(.int32(1)), .primitive(.int32(2)), .primitive(.int32(1)), .primitive(.int32(0)), .primitive(.int64(1))],
            [.primitive(.int32(2)), .primitive(.int32(2)), .primitive(.int32(3)), .primitive(.int32(2)), .primitive(.int32(0)), .primitive(.int64(1))],
            [.primitive(.null), .primitive(.null), .primitive(.null), .primitive(.int32(0)), .primitive(.int32(1)), .primitive(.int64(2))]
        ])
    }

    @Test("Public aggregates and ordered sets match PostgreSQL 18.6", .timeLimit(.minutes(2)))
    func aggregateReferenceCases() async throws {
        for prefix in ["aggregate.", "stats.", "ordered."] { try await referenceCases(prefix: prefix) }
    }

    @Test("Public window frames and navigation match PostgreSQL 18.6", .timeLimit(.minutes(2)))
    func windowReferenceCases() async throws { try await referenceCases(prefix: "window.") }

    @Test("F11 boolean bit string and JSON families execute through SQL", .timeLimit(.minutes(2)))
    func ordinaryFamilies() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let response = try await container.testBaseContext().executeSQL("""
            SELECT bool_and(b), bool_or(b), every(b), bit_and(n), bit_or(n), bit_xor(n),
                string_agg(s,d ORDER BY n), array_agg(n ORDER BY n DESC),
                json_array_length(json_agg(n)), jsonb_array_length(jsonb_agg(n)),
                json_object_agg(k,n ORDER BY n)->>'x', jsonb_object_agg(k,n ORDER BY n)->>'x'
            FROM (VALUES (TRUE,3,'a','/','x'), (NULL,1,NULL,'-','y'), (FALSE,2,'c',NULL,'x')) v(b,n,s,d,k)
            """)
        #expect(response.rows.count == 1)
        #expect(response.rows[0].values == [
            .primitive(.bool(false)), .primitive(.bool(true)), .primitive(.bool(false)),
            .primitive(.int32(0)), .primitive(.int32(3)), .primitive(.int32(0)),
            .primitive(.string("c/a")),
            try Self.array(element: .integer, dimensions: [QueryArrayDimension(lowerBound: 1, length: 3)],
                values: [.primitive(.int32(3)), .primitive(.int32(2)), .primitive(.int32(1))]),
            .primitive(.int32(3)), .primitive(.int32(3)), .primitive(.string("3")), .primitive(.string("3"))
        ])
        #expect(response.columns.map(\.type) == [.sql(.boolean), .sql(.boolean), .sql(.boolean),
            .sql(.integer), .sql(.integer), .sql(.integer), .sql(.text), .sql(.array(.integer)),
            .sql(.integer), .sql(.integer), .sql(.text), .sql(.text)])
    }

    @Test("Registered primary identity proves grouping dependency without derived inference", .timeLimit(.minutes(2)))
    func registeredIdentityDependency() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(AnalyticItem(id: "a", category: "first", value: 3))
        try context.insert(AnalyticItem(id: "b", category: "second", value: 4))
        try await context.save()
        let response = try await context.executeSQL("SELECT a.id,a.category,a.value,count(*) FROM \"AnalyticItem\" a GROUP BY a.id ORDER BY a.id")
        #expect(response.rows.map(\.values) == [
            [.primitive(.string("a")), .primitive(.string("first")), .primitive(.int32(3)), .primitive(.int64(1))],
            [.primitive(.string("b")), .primitive(.string("second")), .primitive(.int32(4)), .primitive(.int64(1))]
        ])
        for sql in [
            "SELECT category,count(*) FROM \"AnalyticItem\" GROUP BY value",
            "SELECT category,count(*) FROM (SELECT * FROM \"AnalyticItem\") a GROUP BY id",
            "SELECT category,count(*) FROM \"AnalyticItem\" GROUP BY GROUPING SETS((id),())"
        ] {
            await #expect(throws: SQLBindingSourceFailure.self) { _ = try await context.executeSQL(sql) }
        }
    }

    @Test("Analytic phases and malformed frames fail on empty sources", .timeLimit(.minutes(2)))
    func phaseAndFrameFailures() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for sql in [
            "SELECT x FROM (SELECT 1 x WHERE FALSE) v WHERE row_number() OVER() > 1",
            "SELECT sum(sum(x)) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT percentile_cont(x) WITHIN GROUP(ORDER BY y) FROM (VALUES(0.25,1),(0.75,3)) v(x,y)",
            "SELECT sum(row_number() OVER()) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT row_number() OVER(ORDER BY row_number() OVER()) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT sum(x) OVER(ORDER BY x,x RANGE 1 PRECEDING) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT sum(x) OVER(GROUPS 1 PRECEDING) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT sum(x) OVER(w) FROM (SELECT 1 x WHERE FALSE) v WINDOW w AS(ORDER BY x ROWS UNBOUNDED PRECEDING)",
            "SELECT array_agg(DISTINCT x ORDER BY y) FROM (SELECT 1 x,2 y WHERE FALSE) v"
        ] {
            await #expect(throws: SQLBindingSourceFailure.self) { _ = try await context.executeSQL(sql) }
        }
        for sql in [
            "SELECT sum(x) OVER(ROWS -1 PRECEDING) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT sum(x) OVER(ROWS NULL PRECEDING) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT sum(x) OVER(ORDER BY x GROUPS -1 PRECEDING) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT sum(x) OVER(ORDER BY x RANGE NULL PRECEDING) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT percentile_cont(-0.1) WITHIN GROUP(ORDER BY x) FROM (SELECT 1::double precision x WHERE FALSE) v"
        ] {
            do { _ = try await context.executeSQL(sql); Issue.record("Expected a typed analytic failure: \(sql)") }
            catch is CanonicalReadError {}
            catch is DatabaseExpressionEvaluationError {}
            catch is SQLBindingSourceFailure {}
        }
        let recovery = try await context.executeSQL("SELECT count(*) FROM (VALUES(1),(2)) v(x)")
        #expect(recovery.rows.first?.values == [.primitive(.int64(2))])
        let groupedFraction = try await context.executeSQL("""
            SELECT percentile_cont(f) WITHIN GROUP(ORDER BY y)
            FROM (VALUES(0.5,1),(0.5,3)) v(f,y) GROUP BY f
            """)
        #expect(groupedFraction.rows.first?.values == [.primitive(.float64(2))])
    }

    @Test("Empty windows preserve PostgreSQL initialization and nullable offsets", .timeLimit(.minutes(2)))
    func emptyAndNullableWindowOffsets() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for sql in [
            "SELECT sum(x) OVER(ORDER BY x RANGE -1 PRECEDING) FROM (SELECT 1 x WHERE FALSE) v",
            "SELECT ntile(0) OVER(),nth_value(x,0) OVER() FROM (SELECT 1 x WHERE FALSE) v"
        ] { #expect(try await context.executeSQL(sql).rows.isEmpty) }
        let response = try await context.executeSQL("SELECT ntile(NULL) OVER(),nth_value(x,NULL) OVER() FROM (VALUES(1),(2)) v(x)")
        #expect(response.rows.map(\.values) == [[.primitive(.null), .primitive(.null)], [.primitive(.null), .primitive(.null)]])
    }

    @Test("Rich aggregate values keep their declarations and typed overflow", .timeLimit(.minutes(2)))
    func richAggregateResults() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let response = try await context.executeSQL("""
            SELECT sum(i) = '3 months 6 days 9 seconds'::interval,
                avg(i) = '1 month 18 days 4.5 seconds'::interval,
                sum('infinity'::interval)::text, avg('infinity'::interval)::text,
                string_agg(b,'\\x00'::bytea ORDER BY n) = '\\x010002'::bytea
            FROM (VALUES('1 month 2 days 3 seconds'::interval,'\\x01'::bytea,1),
                ('2 months 4 days 6 seconds'::interval,'\\x02'::bytea,2)) v(i,b,n)
            """)
        #expect(response.rows[0].values == [.primitive(.bool(true)), .primitive(.bool(true)),
            .primitive(.string("infinity")), .primitive(.string("infinity")), .primitive(.bool(true))])
    }

    @Test("Analytic result owners release claims on promotion and quota failure", .timeLimit(.minutes(2)))
    func analyticOwnershipAndQuota() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let query = SelectQuery(projection: [ProjectionItem(.window(WindowCall(
            function: .aggregate(SQLAggregateCall(function: .sum, arguments: [.column(ColumnRef("column1"))])),
            reference: .specification(WindowSpecification(orderBy: [SortKey(.column(ColumnRef("column1")))])))))],
            source: .values((1...3).map { [.literal(.queryValue(.primitive(.int32(Int32($0))), type: .sql(.integer)))] }))
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateRows: 1_000,
            maximumIntermediateBytes: 4 * 1_024 * 1_024), monotonicClock: TestProcessMonotonicClock())
        let retained = try await container.testBaseContext().queryRetained(query,
            execution: ReadExecutionContext(options: ReadExecutionOptions(budget: meter.budget),
                monotonicClock: container.monotonicClock, workMeter: meter))
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)
        let response = try retained.promoteToPublicResponse()
        #expect(response.rows.map(\.values) == [[.primitive(.int64(1))], [.primitive(.int64(3))], [.primitive(.int64(6))]])
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)

        let limited = DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateRows: 2,
            maximumIntermediateBytes: 4 * 1_024 * 1_024), monotonicClock: TestProcessMonotonicClock())
        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await container.testBaseContext().queryRetained(query,
                execution: ReadExecutionContext(options: ReadExecutionOptions(budget: limited.budget),
                    monotonicClock: container.monotonicClock, workMeter: limited))
        }
        #expect(limited.retainedIntermediateRows == 0)
        #expect(limited.retainedIntermediateBytes == 0)
        #expect(try await container.testBaseContext().query(query).rows.count == 3)
    }

    private func referenceCases(prefix: String) async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for fixture in try Self.fixtures() where fixture.id.hasPrefix(prefix) {
            do {
                let response = try await context.executeSQL(fixture.sql)
                #expect(response.columns.map(\.name) == fixture.names, "\(fixture.id) names")
                #expect(response.columns.map(\.type) == fixture.types, "\(fixture.id) types")
                #expect(response.rows.count == fixture.rows.count, "\(fixture.id) count")
                for (actual, expected) in zip(response.rows, fixture.rows) {
                    #expect(actual.values.count == expected.count, "\(fixture.id) arity")
                    for (value, witness) in zip(actual.values, expected) {
                        if case .primitive(.float64(let a)) = value, case .primitive(.float64(let b)) = witness {
                            #expect(abs(a-b) <= max(1e-13, abs(b)*1e-12), "\(fixture.id) floating accuracy")
                        } else { #expect(value == witness, "\(fixture.id) value") }
                    }
                }
            } catch { Issue.record(error, "PostgreSQL 18.6 reference case \(fixture.id)") }
        }
    }

    private static func numeric(_ text: String) throws -> QueryValue {
        .numeric(try QueryNumericValue(sqlLexicalForm: text[...], maximumUTF8Count: UInt64(text.utf8.count)))
    }

    private static func array(element: DataType, dimensions: [QueryArrayDimension], values: [QueryValue]) throws -> QueryValue {
        .array(try QueryArrayValue(elementType: element, dimensions: dimensions, elements: values,
            limits: QueryValueLimits(maximumRetainedBytes: 1_024 * 1_024, maximumTotalNodes: 10_000,
                maximumNestingDepth: 64, maximumCollectionElements: 10_000)))
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [try AnalyticItem.schemaEntity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "sql-analytic-execution", revision: 1),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(AnalyticItem.self)]),
            security: .testingDisabled)
    }

    // PostgreSQL 18.6 reference values retained in the S09 isolated-oracle evidence.
    private static func fixtures() throws -> [Fixture] {
        [
            Fixture(id: "grouping.compound-rollup", sql: "SELECT a,b,c,count(*),grouping(a,b,c) bits FROM (VALUES (1,10,100),(1,20,200),(2,10,300),(NULL,10,400)) v(a,b,c) GROUP BY ROLLUP((a,b),c) ORDER BY bits,a,b,c",
                names: ["a", "b", "c", "count", "bits"], types: [.sql(.integer), .sql(.integer), .sql(.integer), .sql(.bigint), .sql(.integer)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int32(100)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.int32(200)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(2)), .primitive(.int32(10)), .primitive(.int32(300)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.null), .primitive(.int32(10)), .primitive(.int32(400)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.int32(2)), .primitive(.int32(10)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.null), .primitive(.int32(10)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.null), .primitive(.null), .primitive(.null), .primitive(.int64(4)), .primitive(.int32(7))]
                ]),
            Fixture(id: "grouping.compound-cube", sql: "SELECT a,b,c,count(*),grouping(a,b,c) bits FROM (VALUES (1,10,100),(1,20,200),(2,10,300),(NULL,10,400)) v(a,b,c) GROUP BY CUBE((a,b),c) ORDER BY bits,a,b,c",
                names: ["a", "b", "c", "count", "bits"], types: [.sql(.integer), .sql(.integer), .sql(.integer), .sql(.bigint), .sql(.integer)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int32(100)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.int32(200)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(2)), .primitive(.int32(10)), .primitive(.int32(300)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.null), .primitive(.int32(10)), .primitive(.int32(400)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.int32(2)), .primitive(.int32(10)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.null), .primitive(.int32(10)), .primitive(.null), .primitive(.int64(1)), .primitive(.int32(1))],
                    [.primitive(.null), .primitive(.null), .primitive(.int32(100)), .primitive(.int64(1)), .primitive(.int32(6))],
                    [.primitive(.null), .primitive(.null), .primitive(.int32(200)), .primitive(.int64(1)), .primitive(.int32(6))],
                    [.primitive(.null), .primitive(.null), .primitive(.int32(300)), .primitive(.int64(1)), .primitive(.int32(6))],
                    [.primitive(.null), .primitive(.null), .primitive(.int32(400)), .primitive(.int64(1)), .primitive(.int32(6))],
                    [.primitive(.null), .primitive(.null), .primitive(.null), .primitive(.int64(4)), .primitive(.int32(7))]
                ]),
            Fixture(id: "grouping.duplicates", sql: "SELECT a,count(*),grouping(a) bits FROM (VALUES(1),(2)) v(a) GROUP BY GROUPING SETS((a),(a),(),()) ORDER BY bits,a",
                names: ["a", "count", "bits"], types: [.sql(.integer), .sql(.bigint), .sql(.integer)], rows: [
                    [.primitive(.int32(1)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(1)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(2)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.int32(2)), .primitive(.int64(1)), .primitive(.int32(0))],
                    [.primitive(.null), .primitive(.int64(2)), .primitive(.int32(1))],
                    [.primitive(.null), .primitive(.int64(2)), .primitive(.int32(1))]
                ]),
            Fixture(id: "grouping.empty", sql: "SELECT a,count(*),sum(a),grouping(a) bits FROM (SELECT 1 a WHERE false) v GROUP BY GROUPING SETS((a),(),()) ORDER BY bits,a",
                names: ["a", "count", "sum", "bits"], types: [.sql(.integer), .sql(.bigint), .sql(.bigint), .sql(.integer)], rows: [
                    [.primitive(.null), .primitive(.int64(0)), .primitive(.null), .primitive(.int32(1))],
                    [.primitive(.null), .primitive(.int64(0)), .primitive(.null), .primitive(.int32(1))]
                ]),
            Fixture(id: "grouping.bit-order", sql: "SELECT a,b,grouping(a,b) ab,grouping(b,a) ba FROM (VALUES(1,2)) v(a,b) GROUP BY GROUPING SETS((a),(b),()) ORDER BY ab",
                names: ["a", "b", "ab", "ba"], types: [.sql(.integer), .sql(.integer), .sql(.integer), .sql(.integer)], rows: [
                    [.primitive(.int32(1)), .primitive(.null), .primitive(.int32(1)), .primitive(.int32(2))],
                    [.primitive(.null), .primitive(.int32(2)), .primitive(.int32(2)), .primitive(.int32(1))],
                    [.primitive(.null), .primitive(.null), .primitive(.int32(3)), .primitive(.int32(3))]
                ]),
            Fixture(id: "aggregate.filter-distinct-order", sql: "SELECT array_agg(DISTINCT x ORDER BY x DESC) FILTER (WHERE keep),string_agg(DISTINCT x::text,',' ORDER BY x::text) FILTER (WHERE keep),count(DISTINCT x) FILTER (WHERE keep) FROM (VALUES(2,true),(1,true),(2,true),(NULL,true),(3,false)) v(x,keep)",
                names: ["array_agg", "string_agg", "count"], types: [.sql(.array(.integer)), .sql(.text), .sql(.bigint)], rows: [
                    [try array(element: .integer, dimensions: [try QueryArrayDimension(lowerBound: 1, length: 3)], values: [.primitive(.null), .primitive(.int32(2)), .primitive(.int32(1))]), .primitive(.string("1,2")), .primitive(.int64(2))]
                ]),
            Fixture(id: "aggregate.filter-before-input", sql: "SELECT sum(10/x) FILTER(WHERE x<>0),array_agg(x ORDER BY x DESC NULLS FIRST) FROM (VALUES(0),(2),(NULL)) v(x)",
                names: ["sum", "array_agg"], types: [.sql(.bigint), .sql(.array(.integer))], rows: [
                    [.primitive(.int64(5)), try array(element: .integer, dimensions: [try QueryArrayDimension(lowerBound: 1, length: 3)], values: [.primitive(.null), .primitive(.int32(2)), .primitive(.int32(0))])]
                ]),
            Fixture(id: "stats.numeric", sql: "SELECT var_pop(x),var_samp(x),variance(x),stddev_pop(x),stddev_samp(x),stddev(x) FROM (VALUES(1::numeric),(2),(3),(NULL)) v(x)",
                names: ["var_pop", "var_samp", "variance", "stddev_pop", "stddev_samp", "stddev"], types: [.sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil))], rows: [
                    [try numeric("0.66666666666666666667"), try numeric("1.00000000000000000000"), try numeric("1.00000000000000000000"), try numeric("0.81649658092772603273"), try numeric("1.00000000000000000000"), try numeric("1.00000000000000000000")]
                ]),
            Fixture(id: "stats.empty", sql: "SELECT var_pop(x),var_samp(x),variance(x),stddev_pop(x),stddev_samp(x),stddev(x) FROM (SELECT 1::numeric x WHERE false) v",
                names: ["var_pop", "var_samp", "variance", "stddev_pop", "stddev_samp", "stddev"], types: [.sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil))], rows: [
                    [.primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null)]
                ]),
            Fixture(id: "stats.singleton", sql: "SELECT var_pop(x),var_samp(x),stddev_pop(x),stddev_samp(x) FROM (VALUES(7::smallint)) v(x)",
                names: ["var_pop", "var_samp", "stddev_pop", "stddev_samp"], types: [.sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil)), .sql(.decimal(precision: nil, scale: nil))], rows: [
                    [try numeric("0"), .primitive(.null), try numeric("0"), .primitive(.null)]
                ]),
            Fixture(id: "stats.pairwise-null", sql: "SELECT corr(y,x),covar_pop(y,x),covar_samp(y,x),regr_avgx(y,x),regr_avgy(y,x),regr_count(y,x),regr_intercept(y,x),regr_r2(y,x),regr_slope(y,x),regr_sxx(y,x),regr_sxy(y,x),regr_syy(y,x) FROM (VALUES(1::double precision,3::double precision),(2,5),(3,7),(NULL,9),(9,NULL),(NULL,NULL)) v(x,y)",
                names: ["corr", "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy"], types: [.sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.bigint), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision)], rows: [
                    [.primitive(.float64(1.0)), .primitive(.float64(1.3333333333333333)), .primitive(.float64(2.0)), .primitive(.float64(2.0)), .primitive(.float64(5.0)), .primitive(.int64(3)), .primitive(.float64(1.0)), .primitive(.float64(1.0)), .primitive(.float64(2.0)), .primitive(.float64(2.0)), .primitive(.float64(4.0)), .primitive(.float64(8.0))]
                ]),
            Fixture(id: "stats.constant-x", sql: "SELECT corr(y,x),covar_pop(y,x),covar_samp(y,x),regr_avgx(y,x),regr_avgy(y,x),regr_count(y,x),regr_intercept(y,x),regr_r2(y,x),regr_slope(y,x),regr_sxx(y,x),regr_sxy(y,x),regr_syy(y,x) FROM (VALUES(2::double precision,3::double precision),(2,4)) v(x,y)",
                names: ["corr", "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy"], types: [.sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.bigint), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision)], rows: [
                    [.primitive(.null), .primitive(.float64(0.0)), .primitive(.float64(0.0)), .primitive(.float64(2.0)), .primitive(.float64(3.5)), .primitive(.int64(2)), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.float64(0.0)), .primitive(.float64(0.0)), .primitive(.float64(0.5))]
                ]),
            Fixture(id: "stats.constant-y", sql: "SELECT corr(y,x),covar_pop(y,x),covar_samp(y,x),regr_avgx(y,x),regr_avgy(y,x),regr_count(y,x),regr_intercept(y,x),regr_r2(y,x),regr_slope(y,x),regr_sxx(y,x),regr_sxy(y,x),regr_syy(y,x) FROM (VALUES(1::double precision,3::double precision),(2,3)) v(x,y)",
                names: ["corr", "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy"], types: [.sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.bigint), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision)], rows: [
                    [.primitive(.null), .primitive(.float64(0.0)), .primitive(.float64(0.0)), .primitive(.float64(1.5)), .primitive(.float64(3.0)), .primitive(.int64(2)), .primitive(.float64(3.0)), .primitive(.float64(1.0)), .primitive(.float64(0.0)), .primitive(.float64(0.5)), .primitive(.float64(0.0)), .primitive(.float64(0.0))]
                ]),
            Fixture(id: "stats.reg-singleton", sql: "SELECT corr(y,x),covar_pop(y,x),covar_samp(y,x),regr_avgx(y,x),regr_avgy(y,x),regr_count(y,x),regr_intercept(y,x),regr_r2(y,x),regr_slope(y,x),regr_sxx(y,x),regr_sxy(y,x),regr_syy(y,x) FROM (VALUES(2::double precision,3::double precision)) v(x,y)",
                names: ["corr", "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy"], types: [.sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.bigint), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision)], rows: [
                    [.primitive(.null), .primitive(.float64(0.0)), .primitive(.null), .primitive(.float64(2.0)), .primitive(.float64(3.0)), .primitive(.int64(1)), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.float64(0.0)), .primitive(.float64(0.0)), .primitive(.float64(0.0))]
                ]),
            Fixture(id: "stats.reg-empty", sql: "SELECT corr(y,x),covar_pop(y,x),covar_samp(y,x),regr_avgx(y,x),regr_avgy(y,x),regr_count(y,x),regr_intercept(y,x),regr_r2(y,x),regr_slope(y,x),regr_sxx(y,x),regr_sxy(y,x),regr_syy(y,x) FROM (SELECT 1::double precision x,2::double precision y WHERE false) v",
                names: ["corr", "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy"], types: [.sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.bigint), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision)], rows: [
                    [.primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.int64(0)), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null), .primitive(.null)]
                ]),
            Fixture(id: "ordered.scalar", sql: "SELECT percentile_cont(0.25) WITHIN GROUP(ORDER BY x),percentile_disc(0.25) WITHIN GROUP(ORDER BY x),percentile_cont(0) WITHIN GROUP(ORDER BY x),percentile_disc(1) WITHIN GROUP(ORDER BY x) FROM (VALUES(10::double precision),(20),(40),(NULL)) v(x)",
                names: ["percentile_cont", "percentile_disc", "percentile_cont", "percentile_disc"], types: [.sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision), .sql(.doublePrecision)], rows: [
                    [.primitive(.float64(15.0)), .primitive(.float64(10.0)), .primitive(.float64(10.0)), .primitive(.float64(40.0))]
                ]),
            Fixture(id: "ordered.fraction-array", sql: "SELECT percentile_cont('[0:4]={0,0.25,NULL,0.75,1}'::double precision[]) WITHIN GROUP(ORDER BY x),percentile_disc(ARRAY[[0,0.5],[NULL,1]]::double precision[]) WITHIN GROUP(ORDER BY x) FROM (VALUES(10::double precision),(20),(40),(NULL)) v(x)",
                names: ["percentile_cont", "percentile_disc"], types: [.sql(.array(.doublePrecision)), .sql(.array(.doublePrecision))], rows: [
                    [try array(element: .doublePrecision, dimensions: [try QueryArrayDimension(lowerBound: 0, length: 5)], values: [.primitive(.float64(10.0)), .primitive(.float64(15.0)), .primitive(.null), .primitive(.float64(30.0)), .primitive(.float64(40.0))]), try array(element: .doublePrecision, dimensions: [try QueryArrayDimension(lowerBound: 1, length: 2), try QueryArrayDimension(lowerBound: 1, length: 2)], values: [.primitive(.float64(10.0)), .primitive(.float64(20.0)), .primitive(.null), .primitive(.float64(40.0))])]
                ]),
            Fixture(id: "ordered.empty-array", sql: "SELECT percentile_cont(ARRAY[]::double precision[]) WITHIN GROUP(ORDER BY x),percentile_disc(NULL::double precision[]) WITHIN GROUP(ORDER BY x) FROM (VALUES(10::double precision),(20),(40),(NULL)) v(x)",
                names: ["percentile_cont", "percentile_disc"], types: [.sql(.array(.doublePrecision)), .sql(.array(.doublePrecision))], rows: [
                    [try array(element: .doublePrecision, dimensions: [], values: []), .primitive(.null)]
                ]),
            Fixture(id: "ordered.empty-input", sql: "SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY x),percentile_disc(ARRAY[0,1]::double precision[]) WITHIN GROUP(ORDER BY x) FROM (SELECT 1::double precision x WHERE false) v",
                names: ["percentile_cont", "percentile_disc"], types: [.sql(.doublePrecision), .sql(.array(.doublePrecision))], rows: [
                    [.primitive(.null), .primitive(.null)]
                ]),
            Fixture(id: "ordered.mode-tie-direction", sql: "SELECT mode() WITHIN GROUP(ORDER BY x),mode() WITHIN GROUP(ORDER BY x DESC) FROM (VALUES(1),(2),(2),(1),(NULL)) v(x)",
                names: ["mode", "mode"], types: [.sql(.integer), .sql(.integer)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(2))]
                ]),
            Fixture(id: "ordered.mode-empty", sql: "SELECT mode() WITHIN GROUP(ORDER BY x) FROM (SELECT 1 x WHERE false) v",
                names: ["mode"], types: [.sql(.integer)], rows: [
                    [.primitive(.null)]
                ]),
            Fixture(id: "window.default-peer", sql: "SELECT k,x,sum(x) OVER(ORDER BY k),last_value(x) OVER(ORDER BY k),rank() OVER(ORDER BY k),dense_rank() OVER(ORDER BY k) FROM (VALUES(1,10),(1,20),(2,30),(3,40),(3,50)) v(k,x) ORDER BY k,x",
                names: ["k", "x", "sum", "last_value", "rank", "dense_rank"], types: [.sql(.integer), .sql(.integer), .sql(.bigint), .sql(.integer), .sql(.bigint), .sql(.bigint)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int64(30)), .primitive(.int32(20)), .primitive(.int64(1)), .primitive(.int64(1))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.int64(30)), .primitive(.int32(20)), .primitive(.int64(1)), .primitive(.int64(1))],
                    [.primitive(.int32(2)), .primitive(.int32(30)), .primitive(.int64(60)), .primitive(.int32(30)), .primitive(.int64(3)), .primitive(.int64(2))],
                    [.primitive(.int32(3)), .primitive(.int32(40)), .primitive(.int64(150)), .primitive(.int32(50)), .primitive(.int64(4)), .primitive(.int64(3))],
                    [.primitive(.int32(3)), .primitive(.int32(50)), .primitive(.int64(150)), .primitive(.int32(50)), .primitive(.int64(4)), .primitive(.int64(3))]
                ]),
            Fixture(id: "window.frame-kinds", sql: "SELECT k,x,sum(x) OVER(ORDER BY k,x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) r,sum(x) OVER(ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) v,sum(x) OVER(ORDER BY k GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) g FROM (VALUES(1,10),(1,20),(2,30),(3,40),(3,50)) v(k,x) ORDER BY k,x",
                names: ["k", "x", "r", "v", "g"], types: [.sql(.integer), .sql(.integer), .sql(.bigint), .sql(.bigint), .sql(.bigint)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int64(30)), .primitive(.int64(30)), .primitive(.int64(30))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.int64(60)), .primitive(.int64(30)), .primitive(.int64(30))],
                    [.primitive(.int32(2)), .primitive(.int32(30)), .primitive(.int64(90)), .primitive(.int64(60)), .primitive(.int64(60))],
                    [.primitive(.int32(3)), .primitive(.int32(40)), .primitive(.int64(120)), .primitive(.int64(120)), .primitive(.int64(120))],
                    [.primitive(.int32(3)), .primitive(.int32(50)), .primitive(.int64(90)), .primitive(.int64(120)), .primitive(.int64(120))]
                ]),
            Fixture(id: "window.exclusions", sql: "SELECT k,x,sum(x) OVER(ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) c,sum(x) OVER(ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) g,sum(x) OVER(ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) t,sum(x) OVER(ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) n FROM (VALUES(1,10),(1,20),(2,30),(3,40),(3,50)) v(k,x) ORDER BY k,x",
                names: ["k", "x", "c", "g", "t", "n"], types: [.sql(.integer), .sql(.integer), .sql(.bigint), .sql(.bigint), .sql(.bigint), .sql(.bigint)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int64(20)), .primitive(.null), .primitive(.int64(10)), .primitive(.int64(30))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.int64(10)), .primitive(.null), .primitive(.int64(20)), .primitive(.int64(30))],
                    [.primitive(.int32(2)), .primitive(.int32(30)), .primitive(.int64(30)), .primitive(.int64(30)), .primitive(.int64(60)), .primitive(.int64(60))],
                    [.primitive(.int32(3)), .primitive(.int32(40)), .primitive(.int64(110)), .primitive(.int64(60)), .primitive(.int64(100)), .primitive(.int64(150))],
                    [.primitive(.int32(3)), .primitive(.int32(50)), .primitive(.int64(100)), .primitive(.int64(60)), .primitive(.int64(110)), .primitive(.int64(150))]
                ]),
            Fixture(id: "window.desc-null", sql: "SELECT k,x,sum(x) OVER(ORDER BY k DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES(1,10),(2,20),(3,30),(NULL,40),(NULL,50)) v(k,x) ORDER BY k DESC NULLS LAST,x",
                names: ["k", "x", "sum"], types: [.sql(.integer), .sql(.integer), .sql(.bigint)], rows: [
                    [.primitive(.int32(3)), .primitive(.int32(30)), .primitive(.int64(30))],
                    [.primitive(.int32(2)), .primitive(.int32(20)), .primitive(.int64(50))],
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int64(30))],
                    [.primitive(.null), .primitive(.int32(40)), .primitive(.int64(90))],
                    [.primitive(.null), .primitive(.int32(50)), .primitive(.int64(90))]
                ]),
            Fixture(id: "window.empty-frame", sql: "SELECT k,x,count(*) OVER(ORDER BY k,x ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING),sum(x) OVER(ORDER BY k,x ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) FROM (VALUES(1,10),(1,20),(2,30),(3,40),(3,50)) v(k,x) ORDER BY k,x",
                names: ["k", "x", "count", "sum"], types: [.sql(.integer), .sql(.integer), .sql(.bigint), .sql(.bigint)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int64(2)), .primitive(.int64(70))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.int64(2)), .primitive(.int64(90))],
                    [.primitive(.int32(2)), .primitive(.int32(30)), .primitive(.int64(1)), .primitive(.int64(50))],
                    [.primitive(.int32(3)), .primitive(.int32(40)), .primitive(.int64(0)), .primitive(.null)],
                    [.primitive(.int32(3)), .primitive(.int32(50)), .primitive(.int64(0)), .primitive(.null)]
                ]),
            Fixture(id: "window.named-copy", sql: "SELECT k,x,sum(x) OVER w,sum(x) OVER(w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (VALUES(1,10),(1,20),(2,30),(3,40),(3,50)) v(k,x) WINDOW w AS(ORDER BY k) ORDER BY k,x",
                names: ["k", "x", "sum", "sum"], types: [.sql(.integer), .sql(.integer), .sql(.bigint), .sql(.bigint)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.int64(30)), .primitive(.int64(10))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.int64(30)), .primitive(.int64(30))],
                    [.primitive(.int32(2)), .primitive(.int32(30)), .primitive(.int64(60)), .primitive(.int64(60))],
                    [.primitive(.int32(3)), .primitive(.int32(40)), .primitive(.int64(150)), .primitive(.int64(100))],
                    [.primitive(.int32(3)), .primitive(.int32(50)), .primitive(.int64(150)), .primitive(.int64(150))]
                ]),
            Fixture(id: "window.lag-lead-null-negative", sql: "SELECT k,x,lag(x,NULL,99) OVER(ORDER BY k,x),lead(x,NULL,99) OVER(ORDER BY k,x),lag(x,-1,99) OVER(ORDER BY k,x),lead(x,-1,99) OVER(ORDER BY k,x) FROM (VALUES(1,10),(1,20),(2,30),(3,40),(3,50)) v(k,x) ORDER BY k,x",
                names: ["k", "x", "lag", "lead", "lag", "lead"], types: [.sql(.integer), .sql(.integer), .sql(.integer), .sql(.integer), .sql(.integer), .sql(.integer)], rows: [
                    [.primitive(.int32(1)), .primitive(.int32(10)), .primitive(.null), .primitive(.null), .primitive(.int32(20)), .primitive(.int32(99))],
                    [.primitive(.int32(1)), .primitive(.int32(20)), .primitive(.null), .primitive(.null), .primitive(.int32(30)), .primitive(.int32(10))],
                    [.primitive(.int32(2)), .primitive(.int32(30)), .primitive(.null), .primitive(.null), .primitive(.int32(40)), .primitive(.int32(20))],
                    [.primitive(.int32(3)), .primitive(.int32(40)), .primitive(.null), .primitive(.null), .primitive(.int32(50)), .primitive(.int32(30))],
                    [.primitive(.int32(3)), .primitive(.int32(50)), .primitive(.null), .primitive(.null), .primitive(.int32(99)), .primitive(.int32(40))]
                ])
        ]
    }
}
