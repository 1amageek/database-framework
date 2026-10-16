import DatabaseKit
import QueryAST
import Testing

@Suite("SQL exact constants and constructor grammar")
struct SQLParserConstructorTests {
    private func expressions(_ sql: String) throws -> [Expression] {
        let query = try SQLParser().parseSelect(sql)
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }) else {
            Issue.record("Expected projection items")
            return []
        }
        return try projectionExpressions(items)
    }

    @Test("Direct signs select PostgreSQL signed integer widths before normalization")
    func signedIntegerWidths() throws {
        #expect(try expressions("SELECT 2147483647, -2147483648, +2147483648, -9223372036854775808") == [
            .literal(.queryValue(.primitive(.int32(Int32.max)), type: .sql(.integer))),
            .literal(.queryValue(.primitive(.int32(Int32.min)), type: .sql(.integer))),
            .literal(.queryValue(.primitive(.int64(2_147_483_648)), type: .sql(.bigint))),
            .literal(.queryValue(.primitive(.int64(Int64.min)), type: .sql(.bigint))),
        ])
    }

    @Test("Decimal points and exponents always select exact numeric with dscale")
    func exactNumericScale() throws {
        let values = try expressions("SELECT .5000, 12., 1.2300e2, 0.00e-2, 9223372036854775808")
        let expected: [(Int128, Int32, Int32)] = [(5, 1, 4), (12, 0, 0), (123, 0, 2), (0, 0, 4), (9_223_372_036_854_775_808, 0, 0)]
        #expect(values.count == expected.count)
        for (expression, expected) in zip(values, expected) {
            guard case .literal(.queryValue(.numeric(let numeric), let type)) = expression else {
                Issue.record("Expected an exact SQL numeric literal")
                continue
            }
            #expect(numeric.value == .init(coefficient: expected.0, scale: expected.1))
            #expect(numeric.displayScale == expected.2)
            #expect(type == .sql(.decimal(precision: nil, scale: nil)))
        }
    }

    @Test("Radix integers and legal separators use the same signed classifier")
    func radixAndSeparators() throws {
        #expect(try expressions("SELECT 0x_7f_ff, 0o_7_7, 0b10_10, 1_000, - 0x8000_0000") == [
            .literal(.queryValue(.primitive(.int32(32767)), type: .sql(.integer))),
            .literal(.queryValue(.primitive(.int32(63)), type: .sql(.integer))),
            .literal(.queryValue(.primitive(.int32(10)), type: .sql(.integer))),
            .literal(.queryValue(.primitive(.int32(1000)), type: .sql(.integer))),
            .literal(.queryValue(.primitive(.int32(Int32.min)), type: .sql(.integer))),
        ])
    }

    @Test("Malformed numeric tokens and enormous exponent work fail explicitly")
    func malformedNumericTokens() {
        for token in ["1_", "1__0", "1_.2", "1._2", "1e", "1e+", "1e_2", "0x", "0b2", "0o8", "0x__1", "1suffix", "1e99999999999999999999999999999999999999999"] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT " + token) }
        }
    }

    @Test("Typed prefixes, CAST and postfix :: share one intrinsic normalizer")
    func equivalentTypedConstants() throws {
        let values = try expressions("SELECT NUMERIC '12.5000', CAST('12.5000' AS NUMERIC), '12.5000'::NUMERIC")
        #expect(values.count == 3)
        #expect(values[0] == values[1])
        #expect(values[1] == values[2])
        guard case .literal(.queryValue(.numeric(let numeric), _)) = values[0] else {
            Issue.record("Expected normalized NUMERIC")
            return
        }
        #expect(numeric.displayScale == 4)
        #expect(try expressions("SELECT BYTEA '\\x00ff'") == [.literal(.queryValue(.primitive(.bytes([0, 255])), type: .sql(.blob)))])
    }

    @Test("Temporal lexical input preserves typmods without host timezone conversion")
    func temporalTypedInput() throws {
        #expect(try expressions("SELECT DATE '2026-10-15', TIME(3) WITH TIME ZONE '10:20:30+09', TIMESTAMP(6) WITHOUT TIME ZONE '2026-10-15 10:20:30'") == [
            .cast(.literal(.sqlUnknownString("2026-10-15")), targetType: .date),
            .cast(.literal(.sqlUnknownString("10:20:30+09")), targetType: .time(withTimeZone: true, precision: 3)),
            .cast(.literal(.sqlUnknownString("2026-10-15 10:20:30")), targetType: .timestamp(withTimeZone: false, precision: 6)),
        ])
    }

    @Test("NULL casts retain exact type aliases, precision, signed scale and array nesting")
    func typedNullAliasesAndModifiers() throws {
        let types: [DataType] = [
            .smallint, .integer, .bigint, .real, .doublePrecision, .blob,
            .varchar(length: 20), .decimal(precision: 2, scale: 3), .decimal(precision: 38, scale: -1000),
            .time(withTimeZone: true, precision: 0), .timestamp(withTimeZone: true, precision: 6),
            .array(.integer),
        ]
        let sql = "SELECT NULL::int2, NULL::int4, NULL::int8, NULL::float4, NULL::float8, NULL::bytea, NULL::character varying(20), NULL::numeric(2,3), NULL::decimal(38,-1000), NULL::timetz(0), NULL::timestamptz(6), NULL::int[3][]"
        #expect(try expressions(sql) == types.map { .literal(.queryValue(.primitive(.null), type: .sql($0))) })
    }

    @Test("Invalid intrinsic typmods and malformed type suffixes fail")
    func invalidTypeModifiers() {
        for type in ["numeric(0)", "numeric(39)", "numeric(2,1001)", "numeric(2,-1001)", "varchar(0)", "time(7)", "timestamp(-1)", "int[0]", "time WITH ZONE", "double", "int["] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT NULL::" + type) }
        }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT INTEGER[] '{1}'") }
        let failures: [(String, String, Int)] = [
            ("SELECT NULL::TIME(7)", "Temporal precision must be in 0...6", 18),
            ("SELECT NULL::TIMESTAMP(7)", "Temporal precision must be in 0...6", 23),
            ("SELECT NULL::NUMERIC(39,0)", "NUMERIC precision must be in 1...38", 21),
            ("SELECT NULL::NUMERIC(2,1001)", "NUMERIC scale must be in -1000...1000", 23),
            ("SELECT NULL::NUMERIC(2,-1001)", "NUMERIC scale must be in -1000...1000", 24),
            ("SELECT 'é', NULL::TIME(7)", "Temporal precision must be in 0...6", 24),
            ("SELECT 'é', NULL::TIMESTAMP(7)", "Temporal precision must be in 0...6", 29),
            ("SELECT 'é', NULL::NUMERIC(39,0)", "NUMERIC precision must be in 1...38", 27),
            ("SELECT 'é', NULL::NUMERIC(2,1001)", "NUMERIC scale must be in -1000...1000", 29),
            ("SELECT 'é', NULL::NUMERIC(2,-1001)", "NUMERIC scale must be in -1000...1000", 30),
        ]
        for (sql, message, position) in failures {
            #expect(throws: SQLParser.ParseError.invalidSyntax(message: message, position: position)) {
                _ = try SQLParser().parseSelect(sql)
            }
        }
    }

    @Test("ARRAY and ROW preserve nested children, NULLs and parameter identities")
    func constructors() throws {
        #expect(try expressions("SELECT ARRAY[[$1, NULL], [:Named, $1]], ROW(:Named, NULL)").count == 2)
        let values = try expressions("SELECT ARRAY[?, NULL], ROW(:Named, NULL)")
        #expect(values == [
            .array([.parameter(.position(1)), .literal(.null)]),
            .row([.parameter(.name("Named")), .literal(.null)]),
        ])
        #expect(try expressions("SELECT ARRAY[], ROW()") == [.array([]), .row([])])
        #expect(try expressions("SELECT COALESCE(:x, NULL), NULLIF(:x, :y)") == [
            .coalesce([.parameter(.name("x")), .literal(.null)]),
            .nullIf(.parameter(.name("x")), .parameter(.name("y"))),
        ])
        #expect(try expressions("SELECT \"COALESCE\"(:x)") == [.function(FunctionCall(name: "COALESCE", arguments: [.parameter(.name("x"))]))])
        for name in ["LOWER", "lower", "LoWeR", "COALESCE", "NULLIF"] {
            let call = Expression.function(FunctionCall(name: name, arguments: [.parameter(.name("x")), .parameter(.name("y"))], distinct: true))
            let sql = try SQLSerializer().serialize(call)
            #expect(try expressions("SELECT " + sql) == [call])
        }
        #expect(try expressions("SELECT \"F\"(DISTINCT :x, :y)") == [.function(FunctionCall(name: "F", arguments: [.parameter(.name("x")), .parameter(.name("y"))], distinct: true))])
        for sql in ["SELECT \"F\"(DISTINCT)", "SELECT COALESCE(DISTINCT :x, :y)", "SELECT NULLIF(DISTINCT :x, :y)"] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
        for sql in ["SELECT COALESCE()", "SELECT NULLIF()", "SELECT NULLIF(:x)", "SELECT NULLIF(:x, :y, :z)"] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT ARRAY[1,]") }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT ROW(1,)") }
    }

    @Test("Intrinsic malformed typed constants cannot fall back to untyped strings")
    func malformedTypedConstants() {
        for sql in ["SELECT NUMERIC 'bad'", "SELECT JSON '{bad}'", "SELECT JSONB '{bad}'", "SELECT UUID 'bad'", "SELECT BYTEA '\\xzz'", "SELECT CAST('{1,}' AS INTEGER[])", "SELECT INTERVAL 'bad mons 2 days 3 microseconds'"] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
    }

    @Test("Qualified names preserve components and reject incomplete syntax")
    func strictQualification() throws {
        #expect(try expressions("SELECT \"a.b\", \"a\".\"b\"") == [.col("a.b"), .column(ColumnRef(table: "a", column: "b"))])
        #expect(try SQLParser().parseSelect("SELECT MiXeD.* FROM items AS MiXeD").projection == [.wildcard(qualifier: "mixed")])
        for qualifier in ["t", "MiXeD", "a.b", "select"] {
            let query = SelectQuery(projection: [.wildcard(qualifier: qualifier)], source: .aliased(.table(TableRef(table: "items")), name: qualifier))
            let sql = try SQLSerializer().serialize(query)
            #expect(try SQLParser().parseSelect(sql) == query)
        }
        #expect(try SQLParser().parseSelect("SELECT t.*, :x FROM items AS t").projection == [.wildcard(qualifier: "t"), ProjectionItem(.parameter(.name("x")))])
        let qualified = try expressions("SELECT a.f(1), \"a.b\"(1), NULL::a.b, NULL::\"a.b\"")
        let integer = Expression.literal(.queryValue(.primitive(.int32(1)), type: .sql(.integer)))
        #expect(qualified == [
            .function(FunctionCall(name: "f", arguments: [integer], schema: "a")),
            .function(FunctionCall(name: "a.b", arguments: [integer])),
            .cast(.null, targetType: .custom("b", schema: "a")),
            .cast(.null, targetType: .custom("a.b")),
        ])
        for expression in qualified {
            #expect(try expressions("SELECT " + SQLSerializer().serialize(expression)) == [expression])
        }
        for sql in ["SELECT a.", "SELECT 1 FROM schema."] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
    }

    @Test("Projection AS, NULLS and CTE column lists reject missing members")
    func strictClauseOperands() {
        for sql in ["SELECT 1 AS", "SELECT 1 AS FROM t", "SELECT id ORDER BY id NULLS", "SELECT id ORDER BY id NULLS ASC", "WITH q(a,) AS (SELECT 1) SELECT * FROM q", "SELECT id WHERE", "SELECT id HAVING"] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
    }

    @Test("Ordinary joins require conditions and CROSS/NATURAL cannot acquire them")
    func strictJoinConditions() throws {
        _ = try SQLParser().parseSelect("SELECT * FROM a LEFT OUTER JOIN b USING (id)")
        _ = try SQLParser().parseSelect("SELECT * FROM a CROSS JOIN LATERAL (SELECT 1) AS b")
        for sql in ["SELECT * FROM a JOIN b", "SELECT * FROM a LEFT JOIN b", "SELECT * FROM a JOIN b USING (id,)", "SELECT * FROM a JOIN b ON", "SELECT * FROM a CROSS JOIN b ON TRUE", "SELECT * FROM a NATURAL JOIN b USING (id)", "SELECT * FROM a CROSS JOIN LATERAL (SELECT 1) AS b ON TRUE", "SELECT * FROM a JOIN LATERAL (SELECT 1) AS b"] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
    }

    @Test("General statement dispatch preserves WITH hints, LATERAL and joined groups")
    func withAndGroupedJoins() throws {
        let statement = try SQLParser().parse("WITH q AS NOT MATERIALIZED (SELECT 1) SELECT * FROM (a JOIN LATERAL (SELECT 1) AS b ON TRUE)")
        guard case .select(let query) = statement else { Issue.record("Expected SELECT"); return }
        #expect(query.subqueries?.first?.materialized == .notMaterialized)
        guard case .join(let join) = query.source else { Issue.record("Expected joined source"); return }
        #expect(join.type == .lateral)
        #expect(join.condition == .on(.literal(.bool(true))))
        let graph = try SQLParser().parseSelect("SELECT $1 FROM GRAPH_TABLE(\"g\", MATCH ((\"a\":\"A\":\"B\") | (\"b\")){1,2} COLUMNS ($1 AS \"value\")) AS \"t\"")
        guard case .aliased(.graphTable(let graphSource), "t", nil) = graph.source,
              case .quantified(let inner, let quantifier) = graphSource.matchPattern.paths[0].elements[0],
              case .alternation(let alternatives) = inner.elements[0] else {
            Issue.record("Expected quantified PGQ alternatives")
            return
        }
        #expect(quantifier == .range(min: 1, max: 2))
        #expect(alternatives.count == 2)
        #expect(alternatives[0].elements == [.node(NodePattern(variable: "a", labels: ["A", "B"]))])
        _ = try SQLParser().parseSelect("SELECT $1 FROM GRAPH_TABLE(g, MATCH ((a))? COLUMNS ($1 AS v))")
    }
}
