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

@Suite("S10 public FROM functions", .timeLimit(.minutes(2)))
struct S10TableFunctionExecutionTests {
    @Persistable
    struct FunctionItem {
        #Directory<FunctionItem>("s10_function_items")
        var id: String
        var n: Int32
    }

    @Test("Integer and numeric series preserve overloads, inclusive ends and NULL strictness")
    func seriesOverloadsAndBoundaries() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let cases: [(String, DataType, [[QueryValue]])] = [
            ("generate_series(1,5,2)", .integer, integers([1, 3, 5])),
            ("generate_series(3,1,-1)", .integer, integers([3, 2, 1])),
            ("generate_series(3,1)", .integer, []),
            ("generate_series(NULL::integer,3,0)", .integer, []),
            ("generate_series(2147483647,2147483647,1)", .integer, integers([Int32.max])),
            ("generate_series(1::bigint,3::bigint)", .bigint, (1...3).map { [.primitive(.int64(Int64($0)))] }),
            ("generate_series(9223372036854775807::bigint,9223372036854775807::bigint,1::bigint)", .bigint, [[.primitive(.int64(Int64.max))]]),
            ("generate_series(1.1::numeric,3.8::numeric,1.3::numeric)", .decimal(precision: nil, scale: nil), try ["1.1", "2.4", "3.7"].map { [try numeric($0)] }),
            ("generate_series(1::numeric,3::numeric)", .decimal(precision: nil, scale: nil), try ["1", "2", "3"].map { [try numeric($0)] }),
            ("generate_series(99999999999999999999999999999999999999::numeric,99999999999999999999999999999999999999::numeric,1::numeric)", .decimal(precision: nil, scale: nil), [[try numeric("99999999999999999999999999999999999999")]])
        ]
        for (function, type, rows) in cases {
            let response = try await context.executeSQL("SELECT * FROM \(function)")
            #expect(response.columns.map(\.name) == ["generate_series"], "\(function) names")
            #expect(response.columns.map(\.type) == [.sql(type)], "\(function) type")
            #expect(response.rows.map(\.values) == rows, "\(function) rows")
        }
        for sql in ["SELECT * FROM generate_series(1,3,0)", "SELECT * FROM generate_series(1::numeric,3::numeric,0::numeric)"] {
            try await expectFunctionFailure(sql, cause: .invalidFunctionArgument(function: "generate_series", reason: "step size cannot equal zero"), in: context)
        }
    }

    @Test("Temporal series use calendar months, admitted DST rules and bounded infinity demand")
    func temporalSeries() async throws {
        let provider = try SQLTimeZoneDatabase(definitions: [.init(name: "America/New_York", tzif: newYorkRules())],
            provenance: ByteString(utf8: "S10 synthetic EST5EDT,M3.2.0,M11.1.0"),
            limits: .init(maximumInputBytes: 1_024, maximumRetainedBytes: 4_096, maximumTransitions: 0))
        let temporal = try SQLTemporalConfiguration(rulesProvider: provider, limits: .init(maximumAbbreviations: 0, maximumRetainedBytes: 0))
        let (container, _) = try await makeContainer(temporal: temporal); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let months = try await context.executeSQL("SELECT n::text FROM generate_series('2024-01-30'::timestamp,'2024-04-30'::timestamp,'1 month'::interval) s(n)")
        #expect(months.rows.map(\.values) == strings(["2024-01-30 00:00:00", "2024-02-29 00:00:00", "2024-03-29 00:00:00", "2024-04-29 00:00:00"]))
        let dst = try await context.executeSQL("SELECT n::text FROM generate_series('2024-03-09 17:00+00'::timestamptz,'2024-03-12 16:00+00'::timestamptz,'1 day'::interval,'America/New_York') s(n)")
        #expect(dst.rows.map(\.values) == strings(["2024-03-09 17:00:00+00", "2024-03-10 16:00:00+00", "2024-03-11 16:00:00+00", "2024-03-12 16:00:00+00"]))
        let infinite = try await context.executeSQL("SELECT * FROM generate_series('infinity'::timestamp,'infinity'::timestamp,'1 day'::interval) LIMIT 2")
        #expect(infinite.columns.map(\.type) == [.sql(.timestamp(withTimeZone: false))])
        #expect(infinite.rows.map(\.values) == [[.timestampInfinity(.positive, withTimeZone: false)], [.timestampInfinity(.positive, withTimeZone: false)]])
    }

    @Test("UNNEST zips rows and expands closed composites while subscripts retain lower bounds")
    func arraySourcesAndCompositeDescriptors() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let zipped = try await context.executeSQL("SELECT * FROM unnest('[0:1]={4,5}'::integer[],ARRAY['a']) WITH ORDINALITY")
        #expect(zipped.columns.map(\.name) == ["unnest", "unnest", "ordinality"])
        #expect(zipped.columns.map(\.type) == [.sql(.integer), .sql(.text), .sql(.bigint)])
        #expect(!zipped.columns[2].isNullable)
        #expect(zipped.rows.map(\.values) == [[.primitive(.int32(4)), .primitive(.string("a")), .primitive(.int64(1))], [.primitive(.int32(5)), .primitive(.null), .primitive(.int64(2))]])
        let flat = try await context.executeSQL("SELECT * FROM unnest(ARRAY[[1,2],[3,4]])")
        #expect(flat.rows.map(\.values) == integers([1, 2, 3, 4]))
        let subscripts = try await context.executeSQL("SELECT * FROM generate_subscripts('[0:2]={4,5,6}'::integer[],1,true) WITH ORDINALITY")
        #expect(subscripts.rows.map(\.values) == [[.primitive(.int32(2)), .primitive(.int64(1))], [.primitive(.int32(1)), .primitive(.int64(2))], [.primitive(.int32(0)), .primitive(.int64(3))]])
        // The closed descriptor supplies field expansion without a column-definition list.
        let composite = try await context.executeSQL("SELECT * FROM unnest(ARRAY[ROW(1,'a'),ROW(2,'b')]) WITH ORDINALITY u(n,s,o)")
        #expect(composite.columns.map(\.name) == ["n", "s", "o"])
        #expect(composite.columns.map(\.type) == [.sql(.integer), .sql(.text), .sql(.bigint)])
        #expect(composite.rows.map(\.values) == [[.primitive(.int32(1)), .primitive(.string("a")), .primitive(.int64(1))], [.primitive(.int32(2)), .primitive(.string("b")), .primitive(.int64(2))]])
        let descriptor = try QueryCompositeType(columns: [QueryColumn(number: 1, name: "a", type: .sql(.integer), isNullable: true), QueryColumn(number: 2, name: "b", type: .sql(.text), isNullable: true)], limits: valueLimits)
        let array = try QueryArrayValue(elementType: .composite(descriptor), dimensions: [QueryArrayDimension(lowerBound: -2, length: 1)], elements: [.primitive(.null)], limits: valueLimits)
        let nullComposite = try await context.executeSQL("SELECT * FROM unnest($1) WITH ORDINALITY", parameters: [QueryParameter(position: 1, value: .array(array), type: .sql(.array(.composite(descriptor))))])
        #expect(nullComposite.columns.map(\.name) == ["a", "b", "ordinality"])
        #expect(nullComposite.rows.map(\.values) == [[.primitive(.null), .primitive(.null), .primitive(.int64(1))]])
    }

    @Test("JSON row cursors preserve lexical duplicates, JSONB order and JSON null")
    func jsonSources() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let lexical = try await context.executeSQL("SELECT * FROM json_each_text('{\"b\":null,\"a\":1,\"a\":2}'::json) WITH ORDINALITY")
        #expect(lexical.columns.map(\.name) == ["key", "value", "ordinality"])
        #expect(lexical.rows.map(\.values) == [[.primitive(.string("b")), .primitive(.null), .primitive(.int64(1))], [.primitive(.string("a")), .primitive(.string("1")), .primitive(.int64(2))], [.primitive(.string("a")), .primitive(.string("2")), .primitive(.int64(3))]])
        let canonical = try await context.executeSQL("SELECT * FROM jsonb_each_text('{\"b\":null,\"a\":1,\"a\":2}'::jsonb) WITH ORDINALITY")
        #expect(canonical.rows.map(\.values) == [[.primitive(.string("a")), .primitive(.string("2")), .primitive(.int64(1))], [.primitive(.string("b")), .primitive(.null), .primitive(.int64(2))]])
        for family in ["json", "jsonb"] {
            let elements = try await context.executeSQL("SELECT * FROM \(family)_array_elements_text('[1,null,\"x\"]'::\(family)) WITH ORDINALITY")
            #expect(elements.columns.map(\.type) == [.sql(.text), .sql(.bigint)])
            #expect(elements.rows.map(\.values) == [[.primitive(.string("1")), .primitive(.int64(1))], [.primitive(.null), .primitive(.int64(2))], [.primitive(.string("x")), .primitive(.int64(3))]])
            let null = try await context.executeSQL("SELECT value,value IS NULL FROM \(family)_array_elements('[null]'::\(family))")
            try #require(null.rows.count == 1)
            #expect(!null.rows[0].values[0].isNull)
            #expect(null.rows[0].values[1] == .primitive(.bool(false)))
            let keys = try await context.executeSQL("SELECT * FROM \(family)_object_keys('{\"x\":1}'::\(family))")
            #expect(keys.rows.map(\.values) == strings(["x"]))
            let each = try await context.executeSQL("SELECT key,value IS NULL FROM \(family)_each('{\"x\":null}'::\(family))")
            #expect(each.rows.map(\.values) == [[.primitive(.string("x")), .primitive(.bool(false))]])
        }
        try await expectFunctionFailure("SELECT * FROM json_each('[]'::json)", cause: .collection(.jsonObjectRootRequired(function: "json_each")), in: context)
    }

    @Test("Pattern sources distinguish captures and empty splits while preserving typed failures")
    func patternSources() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let matches = try await context.executeSQL("SELECT * FROM regexp_matches('a1 b22','([a-z])([0-9]+)','g') WITH ORDINALITY")
        let first = try textArray([.primitive(.string("a")), .primitive(.string("1"))])
        let second = try textArray([.primitive(.string("b")), .primitive(.string("22"))])
        #expect(matches.columns.map(\.type) == [.sql(.array(.text)), .sql(.bigint)])
        #expect(matches.rows.map(\.values) == [[first, .primitive(.int64(1))], [second, .primitive(.int64(2))]])
        let single = try await context.executeSQL("SELECT * FROM regexp_matches('a1 b22','([a-z])([0-9]+)')")
        #expect(single.rows.map(\.values) == [[first]])
        let captures = try await context.executeSQL("SELECT * FROM regexp_matches('b','(a)?(b*)')")
        let captureValue = try textArray([.primitive(.null), .primitive(.string("b"))])
        #expect(captures.rows.map(\.values) == [[captureValue]])
        let split = try await context.executeSQL("SELECT * FROM regexp_split_to_table('a,,b,',',') WITH ORDINALITY")
        #expect(split.rows.map(\.values) == ["a", "", "b", ""].enumerated().map { [.primitive(.string($0.element)), .primitive(.int64(Int64($0.offset + 1)))] })
        let zeroWidth = try await context.executeSQL("SELECT * FROM regexp_split_to_table('ab','')")
        #expect(zeroWidth.rows.map(\.values) == strings(["a", "b"]))
        let null = try await context.executeSQL("SELECT * FROM regexp_matches(NULL::text,'(')")
        #expect(null.rows.isEmpty)
        #expect(null.columns.map(\.type) == [.sql(.array(.text))])
        try await expectFunctionFailure("SELECT * FROM regexp_matches('a','(')", cause: .invalidPattern(offset: 1, reason: "unterminated group"), in: context)
        try await expectFunctionFailure("SELECT * FROM regexp_matches('a','a','z')", cause: .invalidFunctionArgument(function: "regexp_matches", reason: "flag z at 0"), in: context)
    }

    @Test("Aliased implicit and explicit lateral functions reset ordinality and preserve LEFT absence")
    func lateralAliasesAndOrdinality() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        for join in [",", "CROSS JOIN", "CROSS JOIN LATERAL"] {
            let response = try await context.executeSQL("SELECT v.n,u.x,u.o FROM (VALUES(2),(3)) v(n) \(join) unnest(ARRAY[v.n,v.n+1]) WITH ORDINALITY u(x,o) ORDER BY v.n,u.o")
            #expect(response.rows.map(\.values) == [[.primitive(.int32(2)), .primitive(.int32(2)), .primitive(.int64(1))], [.primitive(.int32(2)), .primitive(.int32(3)), .primitive(.int64(2))], [.primitive(.int32(3)), .primitive(.int32(3)), .primitive(.int64(1))], [.primitive(.int32(3)), .primitive(.int32(4)), .primitive(.int64(2))]])
        }
        let series = try await context.executeSQL("SELECT v.n,s.x FROM (VALUES(2),(3)) v(n) CROSS JOIN generate_series(1,v.n) s(x) ORDER BY v.n,s.x")
        #expect(series.rows.map(\.values) == [[.primitive(.int32(2)), .primitive(.int32(1))], [.primitive(.int32(2)), .primitive(.int32(2))], [.primitive(.int32(3)), .primitive(.int32(1))], [.primitive(.int32(3)), .primitive(.int32(2))], [.primitive(.int32(3)), .primitive(.int32(3))]])
        let left = try await context.executeSQL("SELECT v.n,u.x,u.o FROM (VALUES(0),(1)) v(n) LEFT JOIN unnest(CASE WHEN v.n=0 THEN ARRAY[]::integer[] ELSE ARRAY[7] END) WITH ORDINALITY u(x,o) ON true ORDER BY v.n")
        #expect(left.rows.map(\.values) == [[.primitive(.int32(0)), .primitive(.null), .primitive(.null)], [.primitive(.int32(1)), .primitive(.int32(7)), .primitive(.int64(1))]])
        #expect(left.columns[1].isNullable && left.columns[2].isNullable)
        let empty = try await context.executeSQL("SELECT * FROM unnest(NULL::integer[]) WITH ORDINALITY u(x)")
        #expect(empty.rows.isEmpty)
        #expect(empty.columns.map(\.name) == ["x", "ordinality"])
        #expect(empty.columns.map(\.type) == [.sql(.integer), .sql(.bigint)])
        #expect(!empty.columns[1].isNullable)
    }

    @Test("Scalar FROM emits one NULL row and malformed source signatures reject before reads")
    func scalarSourcesAndBindingFailures() async throws {
        let (container, control) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let scalar = try await context.executeSQL("SELECT * FROM abs(-2) WITH ORDINALITY")
        #expect(scalar.rows.map(\.values) == [[.primitive(.int32(2)), .primitive(.int64(1))]])
        let qualified = try await context.executeSQL("SELECT generate_series.generate_series FROM generate_series(1,2)")
        #expect(qualified.rows.map(\.values) == integers([1, 2]))
        let null = try await context.executeSQL("SELECT * FROM abs(NULL::integer) WITH ORDINALITY")
        #expect(null.rows.map(\.values) == [[.primitive(.null), .primitive(.int64(1))]])
        #expect(null.columns.map(\.type) == [.sql(.integer), .sql(.bigint)])
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        try await context.withReadSnapshot(workMeter: meter) { _ in
            let before = control.dataReadOperationCount
            for source in ["generate_series(NULL,NULL)", "generate_series('1','3')", "generate_series('2024-01-01'::timestamp,'2024-01-02'::timestamp)", "unnest(NULL::integer[]) u(x,y)", "unnest(ARRAY[missing])"] {
                await #expect(throws: SQLBindingSourceFailure.self) {
                    _ = try await context.executeSQL("SELECT * FROM \"FunctionItem\",\(source)")
                }
            }
            await #expect(throws: SQLBindingSourceFailure.self) {
                _ = try await context.executeSQL("SELECT * FROM (VALUES(1)) v(n) RIGHT JOIN unnest(ARRAY[v.n]) u(x) ON true")
            }
            #expect(control.dataReadOperationCount == before)
        }
    }

    @Test("Function rows compose with analytics and release resources on growth failure and cancellation")
    func functionAnalyticsAndResources() async throws {
        let (container, _) = try await makeContainer(); defer { await container.shutdown() }
        let context = container.testBaseContext()
        let analytic = try await context.executeSQL("SELECT n%2 k,count(*),row_number() OVER(ORDER BY n%2) FROM generate_series(1,4) s(n) GROUP BY n%2 ORDER BY k")
        #expect(analytic.rows.map(\.values) == [[.primitive(.int32(0)), .primitive(.int64(2)), .primitive(.int64(1))], [.primitive(.int32(1)), .primitive(.int64(2)), .primitive(.int64(2))]])
        let query = try SQLParser().parseSelect("SELECT * FROM generate_series(1,2147483647)")
        for budget in [ExecutionBudget(maximumWorkUnits: 10_000), ExecutionBudget(maximumIntermediateRows: 12), ExecutionBudget(maximumIntermediateBytes: 262_144)] {
            let meter = DatabaseWorkMeter(budget: budget, monotonicClock: container.monotonicClock)
            await #expect(throws: DatabaseWorkLimitError.self) { _ = try await context.queryRetained(query, execution: execution(container, meter)) }
            #expect(meter.retainedIntermediateRows == 0 && meter.retainedIntermediateBytes == 0)
        }
        let names = (0..<16).map { "a\($0)" }
        let outerValues = Array(repeating: "ARRAY[1]", count: names.count).joined(separator: ",")
        let arguments = names.map { "v.\($0)" }.joined(separator: ",")
        let correlated = try SQLParser().parseSelect("SELECT u.* FROM (VALUES(\(outerValues))) v(\(names.joined(separator: ","))) CROSS JOIN unnest(\(arguments)) u")
        let probe = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let admitted = try await context.queryRetained(correlated, execution: execution(container, probe))
        let admittedResponse = try admitted.promoteToPublicResponse()
        #expect(admittedResponse.rows.map(\.values) == [Array(repeating: .primitive(.int32(1)), count: names.count)])
        #expect(probe.retainedIntermediateRows == 0 && probe.retainedIntermediateBytes == 0)
        let quota = DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: probe.peakIntermediateBytes - 1), monotonicClock: container.monotonicClock)
        do {
            _ = try await context.queryRetained(correlated, execution: execution(container, quota))
            Issue.record("Expected correlated multi-argument function byte admission to fail")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes = error else { throw error }
        }
        #expect(quota.retainedIntermediateRows == 0 && quota.retainedIntermediateBytes == 0)
        let recovered = try await context.queryRetained(try SQLParser().parseSelect("SELECT 7"), execution: execution(container, quota))
        let recoveredResponse = try recovered.promoteToPublicResponse()
        #expect(recoveredResponse.rows.map(\.values) == integers([7]))
        #expect(quota.retainedIntermediateRows == 0 && quota.retainedIntermediateBytes == 0)
        let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let completion = S10FunctionCompletion()
        let task = Task {
            defer { completion.finish() }
            _ = try await context.queryRetained(query, execution: execution(container, meter))
        }
        while meter.consumedWorkUnits < 2_000 && !completion.isFinished { await Task.yield() }
        task.cancel()
        do { _ = try await task.value; Issue.record("Expected function cancellation") } catch is CancellationError { }
        #expect(meter.retainedIntermediateRows == 0 && meter.retainedIntermediateBytes == 0)
        let finite = try SQLParser().parseSelect("SELECT * FROM regexp_matches('a1 b22','([a-z])([0-9]+)','g') WITH ORDINALITY LIMIT 1")
        let retained = try await context.queryRetained(finite, execution: execution(container, meter))
        #expect(meter.retainedIntermediateBytes > 0)
        let response = try retained.promoteToPublicResponse()
        #expect(meter.retainedIntermediateRows == 0 && meter.retainedIntermediateBytes == 0)
        let exists = try await context.executeSQL("SELECT EXISTS(SELECT 1 FROM generate_series(1,2147483647))")
        #expect(exists.rows.map(\.values) == [[.primitive(.bool(true))]])
        await container.shutdown()
        let expected = try textArray([.primitive(.string("a")), .primitive(.string("1"))])
        #expect(response.rows.map(\.values) == [[expected, .primitive(.int64(1))]])
    }

    private var valueLimits: QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: 65_536, maximumTotalNodes: 1_024, maximumNestingDepth: 32, maximumCollectionElements: 1_024)
    }
    private func integers(_ values: [Int32]) -> [[QueryValue]] { values.map { [.primitive(.int32($0))] } }
    private func strings(_ values: [String]) -> [[QueryValue]] { values.map { [.primitive(.string($0))] } }
    private func numeric(_ text: String) throws -> QueryValue { .numeric(try QueryNumericValue(sqlLexicalForm: text[...], maximumUTF8Count: UInt64(text.utf8.count))) }
    private func textArray(_ values: [QueryValue]) throws -> QueryValue {
        .array(try QueryArrayValue(elementType: .text, dimensions: [QueryArrayDimension(lowerBound: 1, length: Int32(values.count))], elements: values, limits: valueLimits))
    }
    private func execution(_ container: DBContainer, _ meter: DatabaseWorkMeter) -> ReadExecutionContext {
        ReadExecutionContext(options: ReadExecutionOptions(budget: meter.budget), monotonicClock: container.monotonicClock, workMeter: meter)
    }
    private func expectFunctionFailure(_ sql: String, cause: DatabaseExpressionEvaluationError, in context: DatabaseContext) async throws {
        do {
            _ = try await context.executeSQL(sql)
            Issue.record("Expected source-addressed table function failure")
        } catch let failure as SQLBindingSourceFailure {
            guard case .expression(let actual) = failure.cause else { throw failure }
            #expect(actual == cause)
            #expect(failure.position == 14)
            #expect(failure.occurrence != nil)
        }
    }
    private func makeContainer(temporal: SQLTemporalConfiguration = .utc) async throws -> (DBContainer, StorageTransactionControl) {
        let storage = ControlledStorageEngine(base: try SQLiteStorageEngine(configuration: .inMemory))
        let configuration: DBConfiguration
        #if MultiBase
        configuration = DBConfiguration(testingName: "sql-s10-functions", storageTopology: try .testing(storageEngine: storage), monotonicClock: TestProcessMonotonicClock(), wallClock: FixedTestWallClock(), sqlTemporal: temporal, testingBaseID: try TestBaseEnvironment.id(), testingPrincipal: Principal(identifier: "test-runner", roles: ["admin"]))
        #else
        configuration = try DBConfiguration(storageEngine: storage, monotonicClock: TestProcessMonotonicClock(), wallClock: FixedTestWallClock(), sqlTemporal: temporal)
        #endif
        let container = try await DBContainer.open(for: Schema(entities: [try FunctionItem.schemaEntity]), configuration: configuration,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "s10-table-functions", revision: 1), entityRuntimes: [try DatabaseFrameworkRuntime.entity(FunctionItem.self)]), security: .testingDisabled)
        return (container, storage.control)
    }

    // Same TZif construction as SQLTimeZoneRulesTests.synthetic; only the admitted US rule is needed.
    private func newYorkRules() -> ByteString {
        var bytes: [UInt8] = []
        func u32(_ value: UInt32) { for shift in stride(from: 24, through: 0, by: -8) { bytes.append(UInt8(truncatingIfNeeded: value >> shift)) } }
        func header(types: UInt32, characters: UInt32) {
            bytes += [84, 90, 105, 102, 50] + [UInt8](repeating: 0, count: 15)
            for value: UInt32 in [0, 0, 0, 0, types, characters] { u32(value) }
        }
        header(types: 1, characters: 4)
        u32(0); bytes += [0, 0, 85, 84, 67, 0]
        header(types: 2, characters: 8)
        u32(UInt32(bitPattern: -18_000)); bytes += [0, 0]
        u32(UInt32(bitPattern: -14_400)); bytes += [1, 4]
        bytes += [69, 83, 84, 0, 69, 68, 84, 0, 10]
        bytes += "EST5EDT,M3.2.0,M11.1.0".utf8
        bytes.append(10)
        return ByteString(bytes)
    }
}

private final class S10FunctionCompletion: Sendable {
    private let state = Mutex(false)
    var isFinished: Bool { state.withLock { $0 } }
    func finish() { state.withLock { $0 = true } }
}
#endif
