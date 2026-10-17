import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import Database
@testable import DatabaseEngine

@Suite("Selected SQL collection execution")
struct SQLCollectionExecutionTests {
    @Persistable
    struct CollectionItem {
        #Directory<CollectionItem>("selected_collection_items")
        var id: String
        var numbers: [Int32] = []
    }

    private let valueLimits = QueryValueLimits(maximumRetainedBytes: 65536,
        maximumTotalNodes: 2048, maximumNestingDepth: 32, maximumCollectionElements: 1024)

    @Test("Constructors select common types while indexing and slices preserve shape", .timeLimit(.minutes(2)))
    func constructorsAndSlices() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT ARRAY[1,2.50,NULL] AS common, ARRAY[NULL,NULL] AS unknowns,
                ARRAY[[1,2],[3,4]] AS matrix,
                ('[-2:0][5:6]={{1,2},{3,4},{5,6}}'::integer[])[-1][6] AS cell,
                ('[-2:0][5:6]={{1,2},{3,4},{5,6}}'::integer[])[-1:0][6:9] AS clipped,
                ('[-2:0][5:6]={{1,2},{3,4},{5,6}}'::integer[])[0:0][6] AS promoted,
                ('[-2:0][5:6]={{1,2},{3,4},{5,6}}'::integer[])[7][6] AS absent
            """)
        #expect(result.rows.count == 1)
        #expect(result.columns.map(\.name) == ["common", "unknowns", "matrix", "cell", "clipped", "promoted", "absent"])
        #expect(result.columns.map(\.type) == [.sql(.array(.decimal(precision: nil, scale: nil))), .sql(.array(.text)),
            .sql(.array(.integer)), .sql(.integer), .sql(.array(.integer)), .sql(.array(.integer)), .sql(.integer)])
        #expect(result.columns[6].isNullable)
        #expect(result.rows[0].values == [
            try array(.decimal(precision: nil, scale: nil), [numeric("1"), numeric("2.50"), .primitive(.null)]),
            try array(.text, [.primitive(.null), .primitive(.null)]),
            try array(.integer, ints([1, 2, 3, 4]), dimensions: [(1, 2), (1, 2)]),
            .primitive(.int32(4)),
            try array(.integer, ints([4, 6]), dimensions: [(1, 2), (1, 1)]),
            try array(.integer, ints([5, 6]), dimensions: [(1, 1), (1, 2)]), .primitive(.null),
        ])
    }

    @Test("F08 uses actual subscripts NULL-safe searches and selected temporal cell conversion", .timeLimit(.minutes(2)))
    func arrayFunctionsAndTemporalCells() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQL("""
            SELECT array_position('[-2:0]={1,NULL,1}'::integer[],NULL),
                array_positions('[-2:0]={1,NULL,1}'::integer[],1),
                array_remove('[-2:0]={1,NULL,1}'::integer[],NULL),
                array_replace('[-2:0]={1,NULL,1}'::integer[],NULL,9),
                array_to_string(ARRAY[1,NULL,2],',','nil'), string_to_array('a,,b',',',''),
                array_ndims('{}'::integer[]), cardinality('{}'::integer[]),
                array_to_string('{2024-02-29,infinity}'::date[],'|'),
                array_to_string('{"0001-02-29 BC"}'::date[],'|')
            """)
        #expect(result.rows[0].values == [.primitive(.int32(-1)),
            try array(.integer, ints([-2, 0])),
            try array(.integer, ints([1, 1]), dimensions: [(-2, 2)]),
            try array(.integer, ints([1, 9, 1]), dimensions: [(-2, 3)]),
            .primitive(.string("1,nil,2")), try array(.text, [.primitive(.string("a")), .primitive(.null), .primitive(.string("b"))]),
            .primitive(.null), .primitive(.int32(0)), .primitive(.string("2024-02-29|infinity")),
            .primitive(.string("0001-02-29 BC"))])
        #expect(result.columns[0].isNullable)
        #expect(result.columns[1].type == .sql(.array(.integer)))

        try context.insert(CollectionItem(id: "native", numbers: [3, 5]))
        try await context.save()
        let storedCases: [(String, [QueryColumnType], [QueryValue])] = [
            ("SELECT numbers[1], numbers[4] FROM \"CollectionItem\"",
             [.sql(.integer), .sql(.integer)], [.primitive(.int32(3)), .primitive(.null)]),
            ("SELECT jsonb_build_array(VARIADIC numbers) FROM \"CollectionItem\"",
             [.sql(.jsonb)], [try jsonb("[3,5]")]),
        ]
        for (sql, types, values) in storedCases {
            do {
                let stored = try await context.executeSQL(sql)
                #expect(stored.columns.map(\.type) == types)
                #expect(stored.rows.map(\.values) == [values])
            } catch {
                Issue.record(error)
            }
        }
    }

    @Test("Lexical JSON extraction retains bytes while JSONB and path NULL retain distinct meaning", .timeLimit(.minutes(2)))
    func lexicalJSONAndPaths() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT '{"a":[1],"a": [ 3, {"x":4} ],"n":null}'::json -> 'a',
                '{"a":[1],"a": [ 3, {"x":4} ],"n":null}'::json #> ARRAY['a','1'],
                '{"n":null}'::json -> 'n', '{"n":null}'::json ->> 'n',
                json_typeof('null'::json), jsonb_typeof(NULL::jsonb),
                '{"a":[1],"a":[3,{"x":4}]}'::jsonb -> 'a',
                '{"a":{"b":2}}'::jsonb #> ARRAY[['a','b']],
                '{"a":1}'::jsonb #> ARRAY['a',NULL]
            """)
        #expect(result.rows[0].values == [try json("[ 3, {\"x\":4} ]"), try json("{\"x\":4}"),
            try json("null"), .primitive(.null), .primitive(.string("null")), .primitive(.null),
            try jsonb("[3,{\"x\":4}]"), try jsonb("2"), .primitive(.null)])
        #expect(result.columns[0].type == .sql(.json))
        #expect(result.columns[3].type == .sql(.text))
        #expect(result.columns[6].type == .sql(.jsonb))
    }

    @Test("JSONB containment concatenation and mutation compose through selected public operators", .timeLimit(.minutes(2)))
    func jsonbMutation() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT '{"a":1}'::jsonb @> '{"a":1.0}'::jsonb,
                '[1,2]'::jsonb || '3'::jsonb,
                '{"a":1}'::jsonb || '{"a":2,"b":3}'::jsonb,
                '[1,2,3]'::jsonb - -1, '{"a":1,"b":2}'::jsonb - 'a',
                '{"a":[1,2]}'::jsonb #- ARRAY['a','0'],
                jsonb_set('{"a":[1,2]}'::jsonb,ARRAY['a','1'],'9'::jsonb),
                jsonb_insert('{"a":[1,2]}'::jsonb,ARRAY['a','1'],'9'::jsonb,FALSE),
                jsonb_strip_nulls('{"a":null,"b":[null,1]}'::jsonb,TRUE),
                jsonb_set('{"a":1}'::jsonb,ARRAY['missing','child'],'2'::jsonb)
            """)
        #expect(result.rows[0].values == [.primitive(.bool(true)), try jsonb("[1,2,3]"),
            try jsonb("{\"a\":2,\"b\":3}"), try jsonb("[1,2]"), try jsonb("{\"b\":2}"),
            try jsonb("{\"a\":[2]}"), try jsonb("{\"a\":[1,9]}"), try jsonb("{\"a\":[1,9,2]}"),
            try jsonb("{\"b\":[1]}"), try jsonb("{\"a\":1}")])
    }

    @Test("Registered JSON builders and path calls preserve explicit VARIADIC and SQL NULL", .timeLimit(.minutes(2)))
    func jsonBuildersAndVariadic() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT pg_catalog.jsonb_build_array(1,NULL,TRUE),
                jsonb_build_array(VARIADIC ARRAY[1,2]), jsonb_build_array(ARRAY[1,2]),
                jsonb_build_array(VARIADIC NULL::integer[]), jsonb_build_array(VARIADIC ARRAY[]::text[]),
                jsonb_build_object(VARIADIC ARRAY['a','first','a','last']),
                json_build_object('a',1,'a',2) ->> 'a',
                jsonb_extract_path('{"a":{"b":7}}'::jsonb,VARIADIC ARRAY['a','b']),
                jsonb_extract_path('{"a":1}'::jsonb,VARIADIC NULL::text[])
            """)
        #expect(result.rows[0].values == [try jsonb("[1,null,true]"), try jsonb("[1,2]"), try jsonb("[[1,2]]"),
            .primitive(.null), try jsonb("[]"), try jsonb("{\"a\":\"last\"}"),
            .primitive(.string("2")), try jsonb("7"), .primitive(.null)])
        #expect(result.columns[0].type == .sql(.jsonb))
        #expect(result.columns[6].type == .sql(.text))
    }

    @Test("Relational identity shares array temporal equality and JSONB numeric equality", .timeLimit(.minutes(2)))
    func relationalIdentity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let month = try QueryIntervalValue(period: CalendarPeriod(months: 1, days: 0), time: TimeSpan(seconds: 0))
        let days = try QueryIntervalValue(period: CalendarPeriod(months: 0, days: 30), time: TimeSpan(seconds: 0))
        let day = try QueryIntervalValue(period: CalendarPeriod(months: 0, days: 1), time: TimeSpan(seconds: 0))
        let monthArray = try array(.interval(), [.interval(month)])
        let daysArray = try array(.interval(), [.interval(days)])
        let dayArray = try array(.interval(), [.interval(day)])
        let firstJSON = try jsonb("{\"n\":1.00}")
        let equalJSON = try jsonb("{\"n\":1.0}")
        let secondJSON = try jsonb("{\"n\":2}")
        let fixtures: [(DataType, [QueryValue], [[QueryValue]], [Int64])] = [
            (.array(.interval()), [monthArray, daysArray, dayArray], [[dayArray], [monthArray, daysArray]], [1, 2]),
            (.jsonb, [firstJSON, equalJSON, secondJSON], [[firstJSON, equalJSON], [secondJSON]], [2, 1]),
        ]
        for (type, values, ordered, counts) in fixtures {
            let source = DataSource.aliased(.values((values.map { [.queryValue($0, type: .sql(type))] } as [[DatabaseKit.Literal]]).map { $0.map { DatabaseKit.Expression.literal($0) } }), name: "input_2", columns: ["value"])
            let distinct = try await context.query(SelectQuery(projection: [ProjectionItem(.col("value"))],
            source: source,
            orderBy: [SortKey(.col("value"))],
            quantifier: .distinct))
            #expect(distinct.columns[0].type == .sql(type))
            #expect(distinct.rows.count == ordered.count)
            for (row, representatives) in zip(distinct.rows, ordered) {
                #expect(representatives.contains { row.values == [$0] })
            }
            let grouped = try await context.query(SelectQuery(projection: [ProjectionItem(.col("value")),
                ProjectionItem(.aggregate(.count(nil, distinct: false)))], source: source,
                groupBy: [.expression(.col("value"))], orderBy: [SortKey(.col("value"))]))
            #expect(grouped.rows.map { $0.values[1] } == counts.map { .primitive(.int64($0)) })
            let counted = try await context.query(SelectQuery(projection: [
                ProjectionItem(.aggregate(.count(.col("value"), distinct: true)))], source: source))
            #expect(counted.rows[0].values == [.primitive(.int64(2))])
            let one = DataSource.aliased(.values(([[.queryValue(values[0], type: .sql(type))]] as [[DatabaseKit.Literal]]).map { $0.map { DatabaseKit.Expression.literal($0) } }), name: "input_1", columns: ["value"])
            let union = try await context.query(SelectQuery(projection: [.wildcard()], source: .subquery(SelectQuery(projection: [.wildcard()], source: .setOperation(operator: .union, quantifier: .distinct, left: SelectQuery(projection: [.wildcard()], source: source), right: SelectQuery(projection: [.wildcard()], source: one))))))
            let intersect = try await context.query(SelectQuery(projection: [.wildcard()], source: .subquery(SelectQuery(projection: [.wildcard()], source: .setOperation(operator: .intersect, quantifier: .distinct, left: SelectQuery(projection: [.wildcard()], source: source), right: SelectQuery(projection: [.wildcard()], source: one))))))
            let except = try await context.query(SelectQuery(projection: [.wildcard()], source: .subquery(SelectQuery(projection: [.wildcard()], source: .setOperation(operator: .except, quantifier: .distinct, left: SelectQuery(projection: [.wildcard()], source: source), right: SelectQuery(projection: [.wildcard()], source: one))))))
            #expect(union.rows.count == 2 && intersect.rows.count == 1 && except.rows.count == 1)
            let joined = try await context.query(SelectQuery(projection: [ProjectionItem(.col("a", "value"))],
                source: .join(JoinClause(type: .inner,
                    left: .aliased(.subquery(SelectQuery(projection: [.wildcard()], source: source)), name: "a"),
                    right: .aliased(.subquery(SelectQuery(projection: [.wildcard()], source: source)), name: "b"),
                    condition: .on(.equal(.col("a", "value"), .col("b", "value")))))))
            #expect(joined.rows.count == 5)
        }
    }

    @Test("Lazy branches strict NULL failures and recovery retain the actual selected path", .timeLimit(.minutes(2)))
    func lazyEvaluationAndRecovery() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQL("""
            SELECT CASE WHEN TRUE THEN ARRAY[DATE '2024-02-29'] ELSE '{not a date}'::date[] END,
                COALESCE('{"ok":true}'::jsonb,jsonb_build_object(NULL,1)),
                NULL::jsonb -> 'a', array_position(NULL::integer[],1,NULL)
            """)
        let date = try CivilDate(year: 2024, month: 2, day: 29)
        #expect(result.rows[0].values == [try array(.date, [.primitive(.date(date))]),
            try jsonb("{\"ok\":true}"), .primitive(.null), .primitive(.null)])
        let failures: [(String, SQLCollectionError)] = [
            ("SELECT json_array_length('{}'::json)", .jsonArrayRootRequired(function: "json_array_length")),
            ("SELECT jsonb_insert('{\"a\":1}'::jsonb,ARRAY['a'],'2'::jsonb)", .existingJSONInsertKey(key: "a")),
            ("SELECT array_position(ARRAY[1],1,NULL)", .nullArraySubscript(function: "array_position")),
        ]
        for (sql, expected) in failures {
            do { _ = try await context.executeSQL(sql); Issue.record("Expected selected collection failure") }
            catch let failure as SQLBindingSourceFailure {
                guard case .expression(.collection(let cause)) = failure.cause else { throw failure }
                #expect(cause == expected)
                #expect(failure.occurrence != nil && failure.position != nil)
            }
        }
        let recovered = try await context.executeSQL("SELECT (ARRAY[3,4])[2], '{\"a\":7}'::jsonb ->> 'a'")
        #expect(recovered.rows[0].values == [.primitive(.int32(4)), .primitive(.string("7"))])
    }

    @Test("Typed AST and canonical SQL execute equal ordinary rich arrays in the same environment", .timeLimit(.minutes(2)))
    func canonicalRoundtrip() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let date = try CivilDate(year: 0, month: 2, day: 29)
        let time = try CivilTime(hour: 1, minute: 2, second: 3, nanoseconds: 123_000_000)
        let uuid = try #require(UUID(canonicalString: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
        let fixtures: [(name: String, type: DataType, value: QueryValue)] = [
            ("dates", .array(.date), try array(.date, [.primitive(.date(date)), .dateInfinity(.positive)], dimensions: [(-2, 2)])),
            ("local_values", .array(.timestamp(withTimeZone: false)), try array(.timestamp(withTimeZone: false),
                [.primitive(.dateTime(CivilDateTime(date: date, time: time)))], dimensions: [(-2, 1)])),
            ("utc_values", .array(.timestamp(withTimeZone: true)), try array(.timestamp(withTimeZone: true),
                [.primitive(.timestamp(Timestamp(secondsSinceUnixEpoch: -62_167_219_200)))])),
            ("uuids", .array(.uuid), try array(.uuid, [.primitive(.uuid(uuid))])),
            ("bytes", .array(.blob), try array(.blob, [.primitive(.bytes([0, 255, 92]))])),
            ("lexical", .json, try json(" {\"x\":1, \"x\":2} ")),
            ("canonical", .jsonb, try jsonb("{\"n\":1.200}")),
        ]
        let query = SelectQuery(projection: fixtures.map { fixture in
            ProjectionItem(.literal(.queryValue(fixture.value, type: .sql(fixture.type))), alias: fixture.name)
        }, source: .values(([[]] as [[DatabaseKit.Literal]]).map { $0.map { DatabaseKit.Expression.literal($0) } }))
        let direct = try await context.query(query)
        let text = try SQLSerializer().serialize(query)
        let emitted = try await context.executeSQL(text)
        #expect(direct.columns.map(\.name) == emitted.columns.map(\.name))
        #expect(direct.columns.map(\.type) == emitted.columns.map(\.type))
        #expect(direct.columns.map(\.isNullable) == emitted.columns.map(\.isNullable))
        #expect(direct.rows.map(\.values) == emitted.rows.map(\.values))
    }

    @Test("Composite JSON and ARRAY subqueries compose while table functions remain S10", .timeLimit(.minutes(2)))
    func stageGates() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for sql in ["SELECT row_to_json(ROW(1,'x'))", "SELECT to_json(ROW(1,'x'))"] {
            let response = try await context.executeSQL(sql)
            guard case .json(let value) = response.rows[0].values[0] else {
                Issue.record("Expected composite JSON output"); continue
            }
            #expect(String(decoding: value.bytes, as: UTF8.self) == "{\"f1\":1,\"f2\":\"x\"}")
        }
        let array = try await context.executeSQL("SELECT ARRAY(SELECT 1)")
        guard case .array(let value) = array.rows[0].values[0] else {
            Issue.record("Expected ARRAY subquery output"); return
        }
        #expect(Array(value.elements) == [.primitive(.int32(1))])
        do { _ = try await context.executeSQL("SELECT * FROM unnest(ARRAY[1,2])"); Issue.record("Expected table function gate") }
        catch SQLParser.ParseError.unsupportedFeature(let reason) { #expect(reason.contains("S10")) }
    }

    @Test("Binding checks collection types and hidden branches before empty source execution", .timeLimit(.minutes(2)))
    func bindingBeforeRows() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for expression in ["ARRAY[1,missing]", "ARRAY[1,TRUE]", "(ARRAY[1])[TRUE]", "'{}'::json = '{}'::json"] {
            do {
                _ = try await context.executeSQL("SELECT \(expression) FROM \"CollectionItem\" WHERE FALSE")
                Issue.record("Expected collection binding to reject before reading empty source")
            } catch let failure as SQLBindingSourceFailure {
                guard case .expression = failure.cause else { throw failure }
                #expect(failure.occurrence != nil && failure.position != nil)
            }
        }
        let empty = try await context.executeSQL("SELECT ARRAY[1,NULL] AS typed FROM \"CollectionItem\" WHERE FALSE")
        #expect(empty.rows.isEmpty)
        #expect(empty.columns.count == 1 && empty.columns[0].type == .sql(.array(.integer)))
        #expect(!empty.columns[0].isNullable)
    }

    private func ints(_ values: [Int32]) -> [QueryValue] { values.map { .primitive(.int32($0)) } }

    private func numeric(_ text: String) throws -> QueryValue {
        .numeric(try QueryNumericValue(sqlLexicalForm: text[...], maximumUTF8Count: 256))
    }

    private func array(_ type: DataType, _ values: [QueryValue], dimensions: [(Int32, Int32)]? = nil) throws -> QueryValue {
        let shape: [QueryArrayDimension]
        if let dimensions { shape = try dimensions.map { try QueryArrayDimension(lowerBound: $0.0, length: $0.1) } }
        else { shape = values.isEmpty ? [] : [try QueryArrayDimension(lowerBound: 1, length: Int32(values.count))] }
        return .array(try QueryArrayValue(elementType: type, dimensions: shape, elements: values, limits: valueLimits))
    }

    private func json(_ text: String) throws -> QueryValue {
        .json(try QueryJSONValue(bytes: ByteString(utf8: text), limits: valueLimits))
    }

    private func jsonb(_ text: String) throws -> QueryValue {
        .jsonb(try QueryJSONBValue(json: QueryJSONValue(bytes: ByteString(utf8: text), limits: valueLimits), limits: valueLimits))
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [try CollectionItem.schemaEntity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "selected-collection-tests", revision: 1),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(CollectionItem.self)]), security: .testingDisabled)
    }
}
