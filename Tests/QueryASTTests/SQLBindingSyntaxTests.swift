import DatabaseKit
import DatabaseTypes
import QueryAST
import Testing

@Suite("SQL binding syntax fidelity")
struct SQLBindingSyntaxTests {
    private func expressions(_ sql: String) throws -> [Expression] {
        let query = try SQLParser().parseSelect(sql)
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }) else {
            Issue.record("Expected projection items")
            return []
        }
        return try projectionExpressions(items)
    }

    @Test("Qualified functions retain exact namespace and terminal identifier")
    func qualifiedFunctions() throws {
        #expect(try expressions("SELECT pg_catalog.LOWER('A'), \"pg_catalog.lower\"('A'), pg_catalog.\"LOWER\"('A')") == [
            .function(FunctionCall(name: "lower", arguments: [.literal(.sqlUnknownString("A"))], schema: "pg_catalog")),
            .function(FunctionCall(name: "pg_catalog.lower", arguments: [.literal(.sqlUnknownString("A"))])),
            .function(FunctionCall(name: "LOWER", arguments: [.literal(.sqlUnknownString("A"))], schema: "pg_catalog")),
        ])
        let qualified = Expression.function(FunctionCall(name: "lower", arguments: [.literal(.sqlUnknownString("A"))], schema: "pg_catalog"))
        let dotted = Expression.function(FunctionCall(name: "pg_catalog.lower", arguments: []))
        #expect(try SQLSerializer().serialize(qualified) == "\"pg_catalog\".\"lower\"('A')")
        #expect(try expressions("SELECT " + SQLSerializer().serialize(dotted)) == [dotted])
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT a.b.c()") }
    }

    @Test("Qualified custom types do not split exact dotted identifiers")
    func qualifiedCustomTypes() throws {
        let values = try expressions("SELECT NULL::pg_catalog.int4, NULL::\"pg_catalog.int4\", NULL::\"Mixed\".\"Type.Name\"[]")
        #expect(values == [
            .cast(.null, targetType: .custom("int4", schema: "pg_catalog")),
            .cast(.null, targetType: .custom("pg_catalog.int4")),
            .cast(.null, targetType: .array(.custom("Type.Name", schema: "Mixed"))),
        ])
        for value in values {
            #expect(try expressions("SELECT " + SQLSerializer().serialize(value)) == [value])
        }
    }

    @Test("Unknown SQL strings and typed text retain distinct SQL type evidence")
    func unknownAndTypedText() throws {
        #expect(try expressions("SELECT '7', TEXT '7'") == [
            .literal(.sqlUnknownString("7")),
            .literal(.queryValue(.primitive(.string("7")), type: .sql(.text))),
        ])
        let serializer = SQLSerializer()
        #expect(try serializer.serialize(Literal.sqlUnknownString("7")) == "'7'")
        #expect(try serializer.serialize(Literal.string("7")) == "CAST('7' AS TEXT)")
        #expect(try serializer.serialize(Literal.queryValue(.primitive(.string("7")))) == "CAST('7' AS TEXT)")
        #expect(try expressions("SELECT " + serializer.serialize(Literal.string("7"))) == [
            .literal(.queryValue(.primitive(.string("7")), type: .sql(.text))),
        ])
    }

    @Test("Nested NULL casts retain incoming types for binding rejection")
    func typedNullCastChains() throws {
        #expect(try expressions("SELECT NULL::boolean::integer, CAST(CAST(NULL AS INTEGER) AS DATE)") == [
            .cast(.literal(.queryValue(.primitive(.null), type: .sql(.boolean))), targetType: .integer),
            .cast(.literal(.queryValue(.primitive(.null), type: .sql(.integer))), targetType: .date),
        ])
        #expect(try expressions("SELECT NULL::integer") == [.literal(.queryValue(.primitive(.null), type: .sql(.integer)))])
    }

    @Test("Query output positions retain bare integer syntax without changing constants")
    func outputPositionSyntax() throws {
        let query = try SQLParser().parseSelect(
            "SELECT 1 GROUP BY 1, 0, -1, (2), +3, -(-4), 2147483648 ORDER BY 1, 0, -1, (2), +3, -(-4), 2147483648"
        )
        let expected: [Expression] = [1, 0, -1, 2, 3, 4, 2147483648].map { .literal(.int($0)) }
        #expect(query.groupBy == expected.map(GroupingElement.expression))
        #expect(query.orderBy?.map(\.expression) == expected)
        #expect(query.projection == [
            ProjectionItem(.literal(.queryValue(.primitive(.int32(1)), type: .sql(.integer))))
        ])

        let constants = try SQLParser().parseSelect(
            "SELECT 1 GROUP BY CAST(1 AS INTEGER), 1::BIGINT, INTEGER '1', 1 + 1 ORDER BY CAST(1 AS INTEGER), 1::BIGINT, INTEGER '1', 1 + 1"
        )
        let integer = Expression.literal(.queryValue(.primitive(.int32(1)), type: .sql(.integer)))
        let expectedConstants: [Expression] = [
            .cast(integer, targetType: .integer),
            .cast(integer, targetType: .bigint),
            .cast(.literal(.sqlUnknownString("1")), targetType: .integer),
            .add(integer, integer),
        ]
        #expect(constants.groupBy == expectedConstants.map(GroupingElement.expression))
        #expect(constants.orderBy?.map(\.expression) == expectedConstants)
        let nested = try SQLParser().parseSelect("SELECT ARRAY_AGG(1 ORDER BY 1) ORDER BY (SELECT CAST(1 AS INTEGER))")
        #expect(nested.projection == [ProjectionItem(.aggregate(.sql(SQLAggregateCall(function: .arrayAgg, arguments: [integer], orderBy: [SortKey(integer)]))))])
        guard case .subquery(let inner) = try #require(nested.orderBy?.first?.expression) else {
            Issue.record("Expected nested SELECT")
            return
        }
        #expect(inner.projection == [ProjectionItem(integer)])
    }

    @Test("Query output positions and explicit constants stabilize across SQL round trips")
    func outputPositionRoundTrips() throws {
        let parser = SQLParser()
        let serializer = SQLSerializer()
        for sql in [
            "SELECT 7 AS value GROUP BY (1), -1 ORDER BY +1 DESC NULLS FIRST, 0",
            "SELECT 7 AS value GROUP BY CAST(1 AS INTEGER), 1::BIGINT ORDER BY CAST(1 AS INTEGER), INTEGER '1'",
            "SELECT 7 AS value GROUP BY CAST(CAST(1 AS INTEGER) AS BIGINT) ORDER BY CAST(1 + 1 AS INTEGER)",
            "SELECT 7 AS value ORDER BY ABS(CAST(1 AS BIGINT)), ARRAY_AGG(1 ORDER BY 1), (SELECT CAST(1 AS INTEGER))",
        ] {
            let original = try parser.parseSelect(sql)
            let emitted = try serializer.serialize(original)
            let reparsed = try parser.parseSelect(emitted)
            #expect(reparsed.groupBy == original.groupBy)
            #expect(reparsed.orderBy == original.orderBy)
            #expect(try serializer.serialize(reparsed) == emitted)
        }
        for literal in [
            Literal.queryValue(.primitive(.int32(1))),
            Literal.queryValue(.primitive(.int64(2147483648))),
        ] {
            let original = try parser.parseSelect("SELECT 7")
            let constantQuery = SelectQuery(
                projection: original.projection, source: original.source,
                orderBy: [SortKey(.literal(literal))]
            )
            let emitted = try serializer.serialize(constantQuery)
            let reparsed = try parser.parseSelect(emitted)
            guard case .cast = try #require(reparsed.orderBy?.first?.expression) else {
                Issue.record("Expected explicit constant CAST")
                continue
            }
            #expect(try serializer.serialize(reparsed) == emitted)
        }
    }
}
