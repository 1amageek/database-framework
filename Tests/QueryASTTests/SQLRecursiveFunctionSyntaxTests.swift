import Testing
import DatabaseKit
@testable import QueryAST

@Suite("SQL recursive and function source syntax", .timeLimit(.minutes(1)))
struct SQLRecursiveFunctionSyntaxTests {
    private func roundTrip(_ text: String) throws -> SelectQuery {
        let query = try SQLParser().parseSelect(text)
        let sql = try SQLSerializer().serialize(query)
        #expect(try SQLParser().parseSelect(sql) == query)
        #expect(try SQLParser().parseSelectWithSource(text).query == query)
        return query
    }

    @Test("Recursive visibility, forward siblings and SEARCH/CYCLE metadata survive text")
    func recursiveMetadata() throws {
        let query = try roundTrip("WITH RECURSIVE a(n) AS (SELECT n FROM b), b(n) AS NOT MATERIALIZED (VALUES(1) UNION ALL SELECT n+1 FROM b WHERE n<3) SEARCH BREADTH FIRST BY n SET seq CYCLE n SET cyclic TO 'Y' DEFAULT 'N' USING path SELECT * FROM a")
        #expect(query.withRecursive)
        let cte = try #require(query.subqueries?.last)
        #expect(cte.search == CTESearchClause(order: .breadthFirst, columns: ["n"], sequenceColumn: "seq"))
        #expect(cte.cycle == CTECycleClause(columns: ["n"], markColumn: "cyclic", pathColumn: "path",
            markValue: .sqlUnknownString("Y"), defaultValue: .sqlUnknownString("N")))
        #expect(cte.materialized == .notMaterialized)
        _ = try roundTrip("WITH RECURSIVE t(\"Key\") AS (VALUES(1) UNION ALL SELECT \"Key\"+1 FROM t) SEARCH DEPTH FIRST BY \"Key\" SET \"Order\" CYCLE \"Key\" SET \"Cycle\" USING \"Path\" SELECT * FROM t LIMIT 1")
        #expect(!query.replacing(withRecursive: false).withRecursive)
    }

    @Test("CYCLE constants retain exact typed negative and numeric values without expression grammar")
    func cycleConstants() throws {
        for pair in ["2 DEFAULT 0", "NULL DEFAULT NULL", "TRUE DEFAULT FALSE",
                     "BIGINT '-1' DEFAULT BIGINT '0'", "NUMERIC '1' DEFAULT NUMERIC '0'",
                     "DATE '2020-01-01' DEFAULT DATE '2020-01-02'", "TEXT 'Y' DEFAULT TEXT 'N'"] {
            _ = try roundTrip("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n FROM t) CYCLE n SET c TO " + pair + " USING p SELECT * FROM t")
        }
        for pair in ["DATE 'today' DEFAULT DATE '2020-01-01'", "DATE 'Jan 1 2020' DEFAULT DATE '2020-01-01'",
                     "DATE '2020-02-30' DEFAULT DATE '2020-01-01'", "TIMESTAMP '2020-01-01 00:00:00' DEFAULT TIMESTAMP '2020-01-02 00:00:00'"] {
            #expect(throws: SQLParser.ParseError.self) {
                try SQLParser().parseSelect("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n FROM t) CYCLE n SET c TO " + pair + " USING p SELECT * FROM t")
            }
        }
        let body = SelectQuery(projection: [.wildcard()], source: .values([[]]))
        let time = try QueryTimeValue(microsecondsSinceMidnight: 0)
        for literal: Literal in [.queryValue(.time(time), type: .sql(.time(withTimeZone: false))),
                                 .queryValue(.timestampInfinity(.positive, withTimeZone: false), type: .sql(.timestamp(withTimeZone: false))),
                                 .queryValue(.dateInfinity(.positive), type: .sql(.date))] {
            let cte = NamedSubquery(name: "t", query: body,
                cycle: CTECycleClause(columns: ["n"], markColumn: "c", pathColumn: "p", markValue: literal, defaultValue: literal))
            let query = body.replacing(subqueries: [cte]).replacing(withRecursive: true)
            #expect(throws: SQLSerializationError.unsupportedLiteral("CYCLE constant has no exact SQL constant spelling")) {
                try SQLSerializer().serialize(query)
            }
        }
    }

    @Test("Function source aliases, exact names and ordinality survive canonical text")
    func functionSources() throws {
        for call in ["unnest(ARRAY[1,2])", "pg_catalog.generate_series(1,3)",
                     "\"Catalog\".\"Fn\"(VARIADIC :values)", "regexp_matches(:text,:pattern,'g')",
                     "jsonb_each(JSONB '{\"x\":1}')", "abs(NULL)"] {
            let query = try roundTrip("SELECT * FROM " + call + " WITH ORDINALITY AS f(value, position)")
            guard case .aliased(.tableFunction(let source), "f", .some(let aliases)) = query.source else {
                Issue.record("Expected aliased function source"); continue
            }
            #expect(source.withOrdinality)
            #expect(aliases == ["value", "position"])
        }
    }

    @Test("Explicit function LATERAL normalizes while lateral subqueries remain distinct")
    func lateralFunctions() throws {
        #expect(try roundTrip("SELECT * FROM LATERAL unnest(:values)") == roundTrip("SELECT * FROM unnest(:values)"))
        for join in [",", "CROSS JOIN", "JOIN", "LEFT JOIN"] {
            let condition = join == "JOIN" || join == "LEFT JOIN" ? " ON TRUE" : ""
            let implicit = try roundTrip("SELECT * FROM t " + join + " unnest(t.values) f" + condition)
            let explicit = try roundTrip("SELECT * FROM t " + join + " LATERAL unnest(t.values) f" + condition)
            #expect(implicit == explicit)
        }
        let query = try roundTrip("SELECT * FROM t CROSS JOIN LATERAL (SELECT t.id) s")
        guard case .join(let join) = query.source else { Issue.record("Expected join"); return }
        #expect(join.type == .lateral)
    }

    @Test("Repeated function arguments and CYCLE constants retain separate exact source edges")
    func occurrenceSources() throws {
        let text = "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n FROM t) CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT * FROM unnest(:名前,:名前) WITH ORDINALITY"
        let parsed = try SQLParser().parseSelectWithSource(text)
        let cte = try #require(parsed.occurrences.child(of: parsed.root, field: .cte, index: 0))
        #expect(parsed.sourceMap[cte] != nil)
        let mark = try #require(parsed.occurrences.child(of: parsed.root, field: .cteCycleMark, index: 0))
        let fallback = try #require(parsed.occurrences.child(of: parsed.root, field: .cteCycleDefault, index: 0))
        let source = try #require(parsed.occurrences.child(of: parsed.root, field: .source))
        let first = try #require(parsed.occurrences.child(of: source, field: .argument, index: 0))
        let second = try #require(parsed.occurrences.child(of: source, field: .argument, index: 1))
        #expect(first != second)
        for (id, expected) in [(mark, "'Y'"), (fallback, "'N'"), (first, ":名前"), (second, ":名前")] {
            let range = try #require(parsed.sourceMap[id]).range
            #expect(String(decoding: Array(text.utf8)[range.start..<range.end], as: UTF8.self) == expected)
        }
        #expect(parsed.sourceMap[first]?.range.start != parsed.sourceMap[second]?.range.start)
    }

    @Test("Fluent builder retains complete CTE declarations and recursive visibility")
    func fluentCarriage() throws {
        let query = try roundTrip("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n FROM t) SEARCH DEPTH FIRST BY n SET seq CYCLE n SET c USING p SELECT * FROM t")
        let cte = try #require(query.subqueries?.first)
        let built = SQLQueryBuilder(from: "t").with(cte).withRecursive().buildAST()
        #expect(built == query)
        #expect(try SQLParser().parseSelect(SQLSerializer().serialize(built)) == built)
    }

    @Test("Malformed and excluded recursive/function syntax fails without accepted prefixes")
    func malformedSyntax() throws {
        for suffix in ["SEARCH FIRST BY n SET s", "SEARCH DEPTH BY n SET s", "SEARCH DEPTH FIRST BY SET s",
                       "CYCLE n SET c TO -1 DEFAULT 0 USING p", "CYCLE n SET c TO :value DEFAULT 0 USING p",
                       "CYCLE n SET c TO 2+1 DEFAULT 0 USING p", "CYCLE n SET c TO CAST(1 AS INTEGER) DEFAULT 0 USING p",
                       "CYCLE n SET c TO 1 USING p", "CYCLE n SET c DEFAULT 0 USING p", "CYCLE n SET c USING"] {
            #expect(throws: (any Error).self) {
                try SQLParser().parseSelect("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n FROM t) " + suffix + " SELECT * FROM t")
            }
        }
        for source in ["unnest(:x) WITH", "unnest(:x) WITH ORDINALITY WITH ORDINALITY",
                       "unnest(:x) AS f(x INTEGER)", "ROWS FROM(unnest(:x))", "LATERAL t"] {
            #expect(throws: (any Error).self) { try SQLParser().parseSelect("SELECT * FROM " + source) }
        }
    }

    @Test("Function arguments and generated metadata obey shared source and structural budgets")
    func structuralLimits() throws {
        let text = "SELECT * FROM unnest(:x)"
        let parser = SQLParser(structuralLimits: QueryStructuralLimits(maximumRetainedBytes: UInt64(text.utf8.count)))
        _ = try parser.parseSelect(text)
        #expect(throws: QueryStructuralValidationError.self) {
            try SQLParser(structuralLimits: QueryStructuralLimits(maximumRetainedBytes: UInt64(text.utf8.count - 1))).parseSelect(text)
        }
        #expect(throws: QueryStructuralValidationError.self) {
            try SQLParser(structuralLimits: QueryStructuralLimits(maximumCollectionElements: 1)).parseSelect("SELECT * FROM unnest(:a,:b)")
        }
    }
}
