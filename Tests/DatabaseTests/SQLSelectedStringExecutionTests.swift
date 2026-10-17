import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import Database
@testable import DatabaseEngine

@Suite("S05 selected string, pattern and binary SQL")
struct SQLSelectedStringExecutionTests {
    @Persistable
    struct StringItem {
        #Directory<StringItem>("selected_string_items")
        var id: String
        var value: String
    }

    @Test("F04 special syntax and scalar signatures execute Unicode codepoint semantics", .timeLimit(.minutes(2)))
    func stringFunctionsUseSelectedCodepoints() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT LENGTH('é😀'), CHAR_LENGTH('é😀'), CHARACTER_LENGTH('é😀'),
                OCTET_LENGTH('é😀'), BIT_LENGTH('é😀'), SUBSTRING('abcdef' FROM 2 FOR 3),
                SUBSTRING('abcdef' FOR 2), SUBSTR('abcdef', -1, 4),
                POSITION('cd' IN 'abcdef'), TRIM(BOTH 'xy' FROM 'xyabcxy'),
                LTRIM('  a '), RTRIM(' a  '), REPLACE('abab', 'ab', 'x'),
                TRANSLATE('abcabc', 'abc', 'xy'), SPLIT_PART('a,b,c', ',', -1),
                LEFT('abcde', -2), RIGHT('abcde', -2), LPAD('x', 4, 'ab'),
                RPAD('x', 4, 'ab'), REPEAT('ab', 2), REVERSE('é😀'), ASCII('😀'), CHR(128512),
                LOWER('ÄAZ'), UPPER('äaz'), 'a' || 'b'
            """)
        #expect(result.rows[0].values == [
            .primitive(.int32(3)), .primitive(.int32(3)), .primitive(.int32(3)),
            .primitive(.int32(7)), .primitive(.int32(56)), .primitive(.string("bcd")),
            .primitive(.string("ab")), .primitive(.string("ab")), .primitive(.int32(3)),
            .primitive(.string("abc")), .primitive(.string("a ")), .primitive(.string(" a")),
            .primitive(.string("xx")), .primitive(.string("xyxy")), .primitive(.string("c")),
            .primitive(.string("abc")), .primitive(.string("cde")), .primitive(.string("abax")),
            .primitive(.string("xaba")), .primitive(.string("abab")), .primitive(.string("😀́e")),
            .primitive(.int32(128512)), .primitive(.string("😀")), .primitive(.string("Äaz")),
            .primitive(.string("äAZ")), .primitive(.string("ab"))])
        #expect(result.columns.prefix(5).allSatisfy { $0.type == .sql(.integer) })
    }

    @Test("CAST and CONCAT retain declared lexical scale and character widths", .timeLimit(.minutes(2)))
    func scalarCastsRetainLexicalMetadata() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let result = try await context.executeSQL("""
            SELECT CAST('1.2300'::text AS numeric), CAST('1.2350'::text AS numeric(4,2)),
                CAST(NULL::text AS numeric(4,2)), CAST('😀xy' AS char(2)), CAST('😀xy' AS varchar(2)),
                CAST('a' AS char(3)), LENGTH(CAST('a' AS char(3))), OCTET_LENGTH(CAST('a' AS char(3))),
                CONCAT(NULL, true, 1.2300, CAST('a' AS char(3))), CONCAT(NULL, NULL),
                CONCAT_WS(',', NULL, 2, false), CONCAT_WS(NULL, 1), CAST(true AS text)
            """)
        #expect(result.rows[0].values == [
            .numeric(try QueryNumericValue(sqlLexicalForm: "1.2300", maximumUTF8Count: 6)),
            .numeric(try QueryNumericValue(sqlLexicalForm: "1.24", maximumUTF8Count: 4)),
            .primitive(.null), .primitive(.string("😀x")), .primitive(.string("😀x")),
            .primitive(.string("a  ")), .primitive(.int32(1)), .primitive(.int32(3)),
            .primitive(.string("t1.2300a  ")), .primitive(.string("")), .primitive(.string("2,f")),
            .primitive(.null), .primitive(.string("true"))])
        #expect(result.columns[0].type == .sql(.decimal(precision: nil, scale: nil)))
        #expect(result.columns[1].type == .sql(.decimal(precision: 4, scale: 2)))
        #expect(result.columns[3].type == .sql(.char(length: 2)))
        #expect(!result.columns[9].isNullable)
        let parameter = QueryParameter(position: 1, value: .primitive(.string("0.001200e+2")), type: .sql(.text))
        let parameterResult = try await context.executeSQL("SELECT $1::numeric", parameters: [parameter])
        #expect(parameterResult.rows[0].values == [.numeric(try QueryNumericValue(sqlLexicalForm: "0.1200", maximumUTF8Count: 6))])
        let timestamp = Timestamp(secondsSinceUnixEpoch: 123)
        let temporal = try await context.executeSQL("SELECT $1::TIMESTAMPTZ", parameters: [
            QueryParameter(position: 1, value: .primitive(.timestamp(timestamp)),
                type: .sql(.timestamp(withTimeZone: true, precision: 6)))])
        #expect(temporal.rows[0].values == [.primitive(.timestamp(timestamp))])
        #expect(temporal.columns[0].type == .sql(.timestamp(withTimeZone: true, precision: nil)))
    }

    @Test("F06 binary and UUID operations execute immutable byte semantics", .timeLimit(.minutes(2)))
    func binaryAndUUIDFunctions() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL(#"""
            SELECT ENCODE(DECODE('00ff80', 'hex'), 'hex'), ENCODE(DECODE('YWJj', 'base64'), 'escape'),
                GET_BYTE(DECODE('00ff80', 'hex'), 1), GET_BIT(DECODE('01', 'hex'), 0),
                ENCODE(SET_BYTE(DECODE('00ff', 'hex'), 0, 257), 'hex'),
                ENCODE(SET_BIT(DECODE('00', 'hex'), 7, 1), 'hex'),
                LENGTH(DECODE('00ff80', 'hex')), BIT_LENGTH(DECODE('00ff80', 'hex')),
                ENCODE(SUBSTRING(DECODE('01020304', 'hex') FROM 2 FOR 2), 'hex'),
                ENCODE(DECODE('01', 'hex') || DECODE('02', 'hex'), 'hex'),
                CAST('{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}' AS uuid)::text
            """#)
        #expect(result.rows[0].values == [.primitive(.string("00ff80")), .primitive(.string("abc")),
            .primitive(.int32(255)), .primitive(.int32(1)), .primitive(.string("01ff")),
            .primitive(.string("80")), .primitive(.int32(3)), .primitive(.int32(24)),
            .primitive(.string("0203")), .primitive(.string("0102")),
            .primitive(.string("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))])
    }

    @Test("F05 dynamic patterns, escapes, captures and scalar arrays execute selected signatures", .timeLimit(.minutes(2)))
    func dynamicPatternsAndScalarArrays() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(StringItem(id: "one", value: "abAB"))
        try await context.save()
        let result = try await context.executeSQL(#"""
            SELECT value LIKE :pattern, value ILIKE :pattern, value SIMILAR TO '(ab|AB)%',
                value ~ 'ab|AB', value ~* '^ABAB$', 'a_b' LIKE 'a!_b' ESCAPE '!',
                'a\b' LIKE 'a\b' ESCAPE '', NULL::text LIKE '[' ESCAPE NULL,
                REGEXP_LIKE(value, '^ABAB$', 'i'), REGEXP_MATCH('ab', '(a)(z)?'),
                REGEXP_SPLIT_TO_ARRAY('a,,b', ','), REGEXP_REPLACE('aba', 'a', 'x', 'g'),
                SUBSTRING('abc123def' FROM '[0-9]+')
            FROM "StringItem"
            """#, parameters: [QueryParameter(position: 1, name: "pattern", value: .primitive(.string("ab%")), type: .sql(.text))])
        let values = result.rows[0].values
        #expect(Array(values.prefix(9)) == [.primitive(.bool(true)), .primitive(.bool(true)),
            .primitive(.bool(true)), .primitive(.bool(true)), .primitive(.bool(true)),
            .primitive(.bool(true)), .primitive(.bool(true)), .primitive(.null), .primitive(.bool(true))])
        guard case .array(let matched) = values[9], case .array(let split) = values[10] else {
            Issue.record("Expected typed regex text arrays"); return
        }
        #expect(matched.elementType == .text)
        #expect(matched.elements.elementsEqual([QueryValue.primitive(.string("a")), .primitive(.null)]))
        #expect(split.elements.elementsEqual([QueryValue.primitive(.string("a")), .primitive(.string("")), .primitive(.string("b"))]))
        #expect(values[11] == .primitive(.string("xbx")))
        #expect(values[12] == .primitive(.string("123")))
        #expect(result.columns[9].type == .sql(.array(.text)))
    }

    @Test("CHAR equality reaches conditional, quantified and extrema operators", .timeLimit(.minutes(2)))
    func characterComparisonOperators() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let result = try await container.testBaseContext().executeSQL("""
            SELECT 'a'::char(1) = 'a'::char(4), 'a'::char(1) IS NOT DISTINCT FROM 'a'::char(4),
                'a'::char(1) BETWEEN 'a'::char(2) AND 'a'::char(3),
                'a'::char(1) IN ('a'::char(3)), NULLIF('a'::char(1), 'a'::char(3)),
                CASE 'a'::char(1) WHEN 'a'::char(3) THEN 1 ELSE 0 END,
                GREATEST('a'::char(1), 'a'::char(3)) = 'a'::char(2),
                'a '::text = 'a'::text, 'é'::text = 'é'::text,
                'a'::char(3) LIKE 'a', 'a'::char(3) ILIKE 'A', 'a'::char(3) SIMILAR TO 'a',
                'a'::char(3) ~ '^a$', 'a'::char(3) ~* '^A$', 'a'::char(3) LIKE 'a  ',
                REGEXP_LIKE('a'::char(3), '^a$'), SUBSTRING('a'::char(3), '^a$'),
                CASE WHEN TRUE THEN 'a'::char(3) ELSE 'b'::char(1) END,
                COALESCE(NULL::char(1), 'a'::char(3)), GREATEST('a'::char(3), 'a'::char(1)),
                CASE 'a'::char(3) WHEN 'a'::text THEN 1 ELSE 0 END,
                CAST(COALESCE(CAST('abc' AS CHAR(3)), CAST('abc' AS CHAR(4))) AS CHAR)
            """)
        #expect(result.rows[0].values == [.primitive(.bool(true)), .primitive(.bool(true)),
            .primitive(.bool(true)), .primitive(.bool(true)), .primitive(.null), .primitive(.int32(1)),
            .primitive(.bool(true)), .primitive(.bool(false)), .primitive(.bool(false)),
            .primitive(.bool(false)), .primitive(.bool(false)), .primitive(.bool(false)),
            .primitive(.bool(false)), .primitive(.bool(false)), .primitive(.bool(true)),
            .primitive(.bool(true)), .primitive(.string("a")),
            .primitive(.string("a  ")), .primitive(.string("a  ")), .primitive(.string("a  ")), .primitive(.int32(1)),
            .primitive(.string("a"))])
        let limits = QueryValueLimits(maximumRetainedBytes: 4096, maximumTotalNodes: 20,
            maximumNestingDepth: 4, maximumCollectionElements: 10)
        let array = try QueryArrayValue(elementType: .char(length: 4),
            dimensions: [QueryArrayDimension(lowerBound: 1, length: 1)], elements: [.primitive(.string("a   "))], limits: limits)
        let quantified = try await container.testBaseContext().executeSQL("SELECT 'a'::char(1) = ANY($1)",
            parameters: [QueryParameter(position: 1, value: .array(array), type: .sql(.array(.char(length: 4))))])
        #expect(quantified.rows[0].values == [.primitive(.bool(true))])
        let textQuantified = try await container.testBaseContext().executeSQL("SELECT 'a'::text = ANY($1)",
            parameters: [QueryParameter(position: 1, value: .array(array), type: .sql(.array(.char(length: 4))))])
        #expect(textQuantified.rows[0].values == [.primitive(.bool(true))])
    }

    @Test("CHAR grouping, DISTINCT, hash join, ordering and aggregates share identity", .timeLimit(.minutes(2)))
    func characterRelationalIdentity() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let source = DataSource.aliased(.values([[.literal(.queryValue(.primitive(.string("a")), type: .sql(.char(length: 1))))], [.literal(.queryValue(.primitive(.string("a   ")), type: .sql(.char(length: 4))))], [.literal(.queryValue(.primitive(.string("b ")), type: .sql(.char(length: 2))))]]), name: "values", columns: ["c"])
        let distinct = try await context.query(SelectQuery(projection: [ProjectionItem(.col("c"))],
            source: source,
            orderBy: [SortKey(.col("c"))],
            quantifier: .distinct))
        #expect(distinct.rows.count == 2)
        let count = Expression.aggregate(.count(nil, distinct: false))
        let groups = try await context.query(SelectQuery(projection: [ProjectionItem(count)],
            source: source,
            groupBy: [.expression(.col("c"))],
            orderBy: [SortKey(count, direction: .descending)]))
        #expect(groups.rows.map(\.values) == [[.primitive(.int64(2))], [.primitive(.int64(1))]])
        let aggregate = try await context.query(SelectQuery(projection: [
            ProjectionItem(.aggregate(.count(.col("c"), distinct: true))), ProjectionItem(.aggregate(.min(.col("c")))),
            ProjectionItem(.aggregate(.max(.col("c"))))],
            source: source))
        #expect(aggregate.rows[0].values[0] == .primitive(.int64(2)))
        #expect(aggregate.rows[0].values[1] == .primitive(.string("a")))
        #expect(aggregate.rows[0].values[2] == .primitive(.string("b ")))
        let joined = try await context.executeSQL("""
            SELECT lhs.c FROM (SELECT 'a'::char(1) AS c) AS lhs
                JOIN (SELECT 'a'::char(4) AS c) AS rhs ON lhs.c = rhs.c
            """)
        #expect(joined.rows.count == 1)
    }

    @Test("Invalid selected signatures and deferred row functions fail on empty sources", .timeLimit(.minutes(2)))
    func invalidSignaturesFailBeforeRead() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for sql in ["SELECT lower(NULL::integer)", "SELECT decode(NULL::integer, 'hex')",
                    "SELECT regexp_like(NULL::integer, '[')", "SELECT regexp_matches(value, 'a')",
                    "SELECT regexp_split_to_table(value, ',')"] {
            await #expect(throws: SQLBindingSourceFailure.self) {
                _ = try await context.executeSQL(sql + " FROM \"StringItem\"")
            }
        }
        let concat = try await context.executeSQL("SELECT concat(NULL::json) FROM \"StringItem\"")
        #expect(concat.rows.isEmpty)
        #expect(concat.columns.map(\.type) == [.sql(.text)])
        #expect(!concat.columns[0].isNullable)
        let empty = try await context.executeSQL("SELECT regexp_match(value, '(a)'), length(value), value::char(2) FROM \"StringItem\"")
        #expect(empty.rows.isEmpty)
        #expect(empty.columns.map(\.type) == [.sql(.array(.text)), .sql(.integer), .sql(.char(length: 2))])
    }

    @Test("Scalar failures remain typed and NULL suppresses pattern compilation", .timeLimit(.minutes(2)))
    func failuresAndStrictNulls() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let nulls = try await context.executeSQL("SELECT regexp_like(NULL::text, '['), substring(NULL::text, '['), repeat(NULL::text, -1)")
        #expect(nulls.rows[0].values == [.primitive(.null), .primitive(.null), .primitive(.null)])
        let failures = [
            ("SELECT split_part('a', ',', 0)", "function"), ("SELECT substring('a', 1, -1)", "function"),
            ("SELECT chr(0)", "function"), ("SELECT decode('xz', 'hex')", "function"),
            ("SELECT get_byte(decode('00','hex'), 1)", "function"),
            ("SELECT 'a' LIKE 'a' ESCAPE 'xy'", "function"), ("SELECT regexp_like('a', '[')", "pattern"),
            ("SELECT regexp_like('aa', '(a)\\1')", "unsupported"), ("SELECT '999.99'::text::numeric(3,2)", "overflow")]
        for (sql, category) in failures {
            do {
                _ = try await context.executeSQL(sql)
                Issue.record("Expected a typed scalar failure for \(sql)")
            } catch let failure as SQLBindingSourceFailure {
                guard case .expression(let cause) = failure.cause else {
                    Issue.record("Expected the original expression failure"); continue
                }
                let actual: String
                switch cause {
                case .invalidFunctionArgument: actual = "function"
                case .invalidPattern: actual = "pattern"
                case .unsupportedPattern: actual = "unsupported"
                case .numericOverflow: actual = "overflow"
                default: actual = "unexpected"
                }
                #expect(actual == category)
                #expect(failure.occurrence != nil)
            }
        }
    }

    private func makeContainer() async throws -> DBContainer {
        try await DBContainer.open(for: Schema(entities: [try StringItem.schemaEntity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(identifier: "selected-string-tests", revision: 1),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(StringItem.self)]), security: .testingDisabled)
    }
}
