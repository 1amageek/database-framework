import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("SQL builder query literals", .heartbeat)
struct SQLQueryBuilderLiteralTests {
    @Test("Primitive builder literals delegate to the canonical width-preserving serializer")
    func primitiveQueryValuesDelegate() throws {
        let builder = SQLQueryBuilder(from: "items").select(
            ProjectionItem(.literal(.queryValue(.primitive(.int64(42)))), alias: "answer")
        )
        #expect(try builder.toSQL() == "SELECT CAST(42 AS BIGINT) AS \"answer\" FROM \"items\"")
        #expect(try builder.toSQL() == SQLSerializer().serialize(builder.buildAST()))
    }

    @Test("Rich query literals serialize through projections, filters, sources and CTEs")
    func richQueryValuesAreSupported() throws {
        let limits = QueryValueLimits(maximumRetainedBytes: 4096, maximumTotalNodes: 32,
                                      maximumNestingDepth: 8, maximumCollectionElements: 32)
        let values: [QueryValue] = [
            .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 3)),
            .json(try QueryJSONValue(bytes: ByteString(utf8: "null"), limits: limits)),
            .jsonb(try QueryJSONBValue(root: .null, limits: limits)),
            .array(try QueryArrayValue(elementType: .integer, dimensions: [], elements: [], limits: limits)),
            .interval(try QueryIntervalValue(period: CalendarPeriod(months: 2, days: 3), time: TimeSpan(seconds: 4)))
        ]
        for value in values {
            let literal = Literal.queryValue(value)
            let rich = SQLQueryBuilder(from: "items").select(ProjectionItem(.literal(literal), alias: "value"))
            let nested = SQLQueryBuilder(from: "items").where(.isNotNull(.literal(literal)))
            let source = SQLQueryBuilder(source: .aliased(.subquery(rich.buildAST()), name: "nested"))
            let cte = SQLQueryBuilder(from: "nested").with("nested", as: rich.buildAST())
            for builder in [rich, nested, source, cte] {
                let text = try builder.toSQL()
                let parsed = try SQLParser().parseSelect(text)
                #expect(try SQLParser().parseSelect(SQLSerializer().serialize(parsed)) == parsed)
            }
        }
    }
}
