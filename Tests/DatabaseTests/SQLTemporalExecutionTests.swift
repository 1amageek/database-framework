import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import Database
@testable import DatabaseEngine

@Suite("Selected SQL temporal execution")
struct SQLTemporalExecutionTests {
    @Persistable
    struct TemporalItem {
        #Directory<TemporalItem>("selected_temporal_items")
        var id: String
    }

    @Test("Lexical precision, 24 hour endpoint and offset time survive selected execution", .timeLimit(.minutes(2)))
    func lexicalPrecision() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT '24:00:00'::time::text, '12:34:56.1234565'::time::text,
                '12:34:56.123456'::time(3)::text, '12:00:00+02'::timetz::text,
                '2000-01-01 00:00:00.500000'::timestamp(0)::text,
                '1999-12-31 23:59:59.500000'::timestamp(0)::text,
                '2000-01-01 00:00:00+02'::timestamptz::text
            """)
        #expect(result.rows[0].values == strings(["24:00:00", "12:34:56.123456", "12:34:56.123", "12:00:00+02", "2000-01-01 00:00:01", "1999-12-31 23:59:59", "1999-12-31 22:00:00+00"]))
    }

    @Test("Calendar arithmetic preserves independent interval components and wraps time", .timeLimit(.minutes(2)))
    func temporalArithmetic() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT ('2024-01-31'::date + '1 month'::interval)::date::text,
                ('2024-03-01'::date - '2024-02-28'::date),
                ('2024-01-01'::date + '12:30:00'::time)::text,
                ('23:00:00'::time + '2 hours'::interval)::text,
                ('2024-01-02 00:00:00'::timestamp - '2024-01-01 00:00:00'::timestamp) = '24 hours'::interval,
                '1 month'::interval = '30 days'::interval,
                (MAKE_INTERVAL(months => 1, days => -30, secs => 2) * 2) = '4 seconds'::interval
            """)
        #expect(result.rows[0].values == [.primitive(.string("2024-02-29")), .primitive(.int32(2)),
            .primitive(.string("2024-01-01 12:30:00")), .primitive(.string("01:00:00")),
            .primitive(.bool(true)), .primitive(.bool(true)), .primitive(.bool(true))])
    }

    @Test("F07 syntax and overloads execute exact result families", .timeLimit(.minutes(2)))
    func temporalFunctions() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT EXTRACT(YEAR FROM '2024-02-29'::date), DATE_PART('year', '2024-02-29'::date),
                DATE_TRUNC('month', '2024-02-29 12:34:56'::timestamp)::text,
                DATE_BIN('15 minutes'::interval, '2001-01-01 00:23:00'::timestamp, '2001-01-01'::timestamp)::text,
                AGE('2001-03-01'::timestamp, '2000-03-01'::timestamp) = '1 year'::interval,
                MAKE_DATE(2024,2,29)::text, MAKE_TIME(12,34,56)::text,
                MAKE_TIMESTAMP(2024,2,29,12,34,56)::text,
                MAKE_TIMESTAMPTZ(2024,2,29,12,34,56,'UTC')::text,
                TO_TIMESTAMP(0)::text
            """)
        let year = try QueryNumericValue(sqlLexicalForm: "2024", maximumUTF8Count: 4)
        #expect(result.rows[0].values == [.numeric(year), .primitive(.float64(2024)),
            .primitive(.string("2024-02-01 00:00:00")), .primitive(.string("2001-01-01 00:15:00")),
            .primitive(.bool(true)), .primitive(.string("2024-02-29")), .primitive(.string("12:34:56")),
            .primitive(.string("2024-02-29 12:34:56")), .primitive(.string("2024-02-29 12:34:56+00")),
            .primitive(.string("1970-01-01 00:00:00+00"))])
        #expect(result.columns[0].type == .sql(.decimal(precision: nil, scale: nil)))
        #expect(result.columns[1].type == .sql(.doublePrecision))
    }

    @Test("AT TIME ZONE and temporal formatting use explicit deterministic UTC authority", .timeLimit(.minutes(2)))
    func zoneAndFormatting() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT ('2000-01-01 12:00:00'::timestamp AT TIME ZONE INTERVAL '+02:00')::text,
                ('2000-01-01 12:00:00+00'::timestamptz AT TIME ZONE 'UTC')::text,
                TO_CHAR('2024-02-29'::date, 'YYYY-MM-DD'), TO_DATE('2024-02-29', 'YYYY-MM-DD')::text,
                TO_TIMESTAMP('2024-02-29 12:34:56', 'YYYY-MM-DD HH24:MI:SS')::text,
                CONCAT('2024-02-29'::date, '/', '24:00'::time),
                TO_CHAR('infinity'::timestamp,'YYYY'), TO_CHAR('2024-01-01'::date,'')
            """)
        #expect(result.rows[0].values == [.primitive(.string("2000-01-01 10:00:00+00")),
            .primitive(.string("2000-01-01 12:00:00")), .primitive(.string("2024-02-29")),
            .primitive(.string("2024-02-29")), .primitive(.string("2024-02-29 12:34:56+00")),
            .primitive(.string("2024-02-29/24:00:00")), .primitive(.null), .primitive(.null)])
    }

    @Test("Temporal conditionals, quantified comparisons and infinity use typed equality", .timeLimit(.minutes(2)))
    func conditionalIdentity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT CASE '1 month'::interval WHEN '30 days'::interval THEN 1 ELSE 0 END,
                NULLIF('1 month'::interval,'30 days'::interval),
                '12:00+02'::timetz < '10:00+00'::timetz,
                '12:00+02'::timetz = '10:00+00'::timetz,
                'infinity'::date > '9999-12-31'::date,
                '-infinity'::interval < '-100000 years'::interval,
                'infinity'::date::timestamptz::text,
                GREATEST('-infinity'::timestamp,'2000-01-01'::timestamp)::text,
                '1 month'::interval IN ('30 days'::interval)
            """)
        #expect(result.rows[0].values == [.primitive(.int32(1)), .primitive(.null), .primitive(.bool(true)),
            .primitive(.bool(false)), .primitive(.bool(true)), .primitive(.bool(true)),
            .primitive(.string("infinity")), .primitive(.string("2000-01-01 00:00:00")), .primitive(.bool(true))])
        let oneMonth = try QueryIntervalValue(period: CalendarPeriod(months: 1, days: 0), time: TimeSpan(seconds: 0))
        let limits = QueryValueLimits(maximumRetainedBytes: 4096, maximumTotalNodes: 20, maximumNestingDepth: 4, maximumCollectionElements: 10)
        let array = try QueryArrayValue(elementType: .interval(), dimensions: [QueryArrayDimension(lowerBound: 1, length: 1)], elements: [.interval(oneMonth)], limits: limits)
        let quantified = try await container.testBaseContext().executeSQL("SELECT '30 days'::interval = ANY($1)", parameters: [QueryParameter(position: 1, value: .array(array), type: .sql(.array(.interval())))])
        #expect(quantified.rows[0].values == [.primitive(.bool(true))])
    }

    @Test("GROUP DISTINCT ORDER extrema and hash joins share interval identity", .timeLimit(.minutes(2)))
    func relationalIdentity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let month = try QueryIntervalValue(period: CalendarPeriod(months: 1, days: 0), time: TimeSpan(seconds: 0))
        let days = try QueryIntervalValue(period: CalendarPeriod(months: 0, days: 30), time: TimeSpan(seconds: 0))
        let day = try QueryIntervalValue(period: CalendarPeriod(months: 0, days: 1), time: TimeSpan(seconds: 0))
        let source = DataSource.aliased(.values([[.literal(.queryValue(.interval(month), type: .sql(.interval())))], [.literal(.queryValue(.interval(days), type: .sql(.interval())))], [.literal(.queryValue(.interval(day), type: .sql(.interval())))]]), name: "values", columns: ["i"])
        let distinct = try await context.query(SelectQuery(projection: [ProjectionItem(.col("i"))],
            source: source,
            orderBy: [SortKey(.col("i"))],
            quantifier: .distinct))
        #expect(distinct.rows.count == 2 && distinct.rows[0].values == [.interval(day)])
        let grouped = try await context.query(SelectQuery(projection: [ProjectionItem(.col("i")), ProjectionItem(.aggregate(.count(nil, distinct: false)))],
            source: source,
            groupBy: [.expression(.col("i"))],
            orderBy: [SortKey(.col("i"))]))
        #expect(grouped.rows.count == 2)
        #expect(grouped.rows[0].values[1] == .primitive(.int64(1)))
        #expect(grouped.rows[1].values[1] == .primitive(.int64(2)))
        let aggregate = try await context.query(SelectQuery(projection: [ProjectionItem(.aggregate(.count(.col("i"), distinct: true))), ProjectionItem(.aggregate(.min(.col("i")))), ProjectionItem(.aggregate(.max(.col("i"))))],
            source: source))
        #expect(aggregate.rows[0].values == [.primitive(.int64(2)), .interval(day), .interval(month)])
        let joined = try await context.query(SelectQuery(projection: [ProjectionItem(.col("a", "i"))],
            source: .join(JoinClause(type: .inner, left: .aliased(.subquery(SelectQuery(projection: [.wildcard()],
            source: source)), name: "a"), right: .aliased(.subquery(SelectQuery(projection: [.wildcard()],
            source: source)), name: "b"), condition: .on(.equal(.col("a", "i"), .col("b", "i")))))))
        #expect(joined.rows.count == 5)
        let local = try QueryTimeValue(microsecondsSinceMidnight: 43_200_000_000)
        let utc = try QueryTimeValue(microsecondsSinceMidnight: 36_000_000_000)
        let east = QueryTimeWithTimeZoneValue(time: local, utcOffsetSeconds: 7200)
        let zero = QueryTimeWithTimeZoneValue(time: utc, utcOffsetSeconds: 0)
        let offsetSource = DataSource.aliased(.values([[.literal(.queryValue(.timeWithTimeZone(zero), type: .sql(.time(withTimeZone: true))))], [.literal(.queryValue(.timeWithTimeZone(east), type: .sql(.time(withTimeZone: true))))], [.literal(.queryValue(.timeWithTimeZone(east), type: .sql(.time(withTimeZone: true))))]]), name: "values", columns: ["i"])
        let offsetDistinct = try await context.query(SelectQuery(projection: [ProjectionItem(.col("i"))],
            source: offsetSource,
            orderBy: [SortKey(.col("i"))],
            quantifier: .distinct))
        #expect(offsetDistinct.rows.map(\.values) == [[.timeWithTimeZone(east)], [.timeWithTimeZone(zero)]])
        let infiniteSource = DataSource.aliased(.values([[.literal(.queryValue(.dateInfinity(.positive), type: .sql(.date)))], [.literal(.queryValue(.dateInfinity(.negative), type: .sql(.date)))], [.literal(.queryValue(.dateInfinity(.positive), type: .sql(.date)))]]), name: "values", columns: ["i"])
        let infinite = try await context.query(SelectQuery(projection: [ProjectionItem(.aggregate(.count(.col("i"), distinct: true))), ProjectionItem(.aggregate(.min(.col("i")))), ProjectionItem(.aggregate(.max(.col("i"))))],
            source: infiniteSource))
        #expect(infinite.rows[0].values == [.primitive(.int64(2)), .dateInfinity(.negative), .dateInfinity(.positive)])
    }

    @Test("Strict NULL skips runtime validation and typed failures retain their source occurrence", .timeLimit(.minutes(2)))
    func failureAndRecovery() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQL("SELECT DATE_TRUNC('invalid', NULL::timestamp), TO_DATE(NULL::text,'invalid'), NULL::date + NULL::integer")
        #expect(result.rows[0].values == [.primitive(.null), .primitive(.null), .primitive(.null)])
        for sql in ["SELECT '2023-02-29'::date", "SELECT '25:00'::time", "SELECT DATE_BIN('1 month'::interval,'2001-01-01'::timestamp,'2000-01-01'::timestamp)", "SELECT EXTRACT(EPOCH FROM 'infinity'::timestamp)", "SELECT '2000-01-01'::timestamp AT TIME ZONE 'Missing/Zone'"] {
            do { _ = try await context.executeSQL(sql); Issue.record("Expected typed temporal failure") }
            catch let failure as SQLBindingSourceFailure {
                guard case .expression = failure.cause else { throw failure }
                #expect(failure.occurrence != nil)
            }
        }
        let recovered = try await context.executeSQL("SELECT '2024-02-29'::date::text")
        #expect(recovered.rows[0].values == strings(["2024-02-29"]))
    }

    private func strings(_ values: [String]) -> [QueryValue] { values.map { .primitive(.string($0)) } }
    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [try TemporalItem.schemaEntity]), configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "selected-temporal-tests", revision: 1), entityRuntimes: [try DatabaseFrameworkRuntime.entity(TemporalItem.self)]), security: .testingDisabled)
    }
}
