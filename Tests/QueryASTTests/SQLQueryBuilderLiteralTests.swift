import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("SQL builder query literals", .heartbeat)
struct SQLQueryBuilderLiteralTests {
    @Test("Primitive query literals preserve builder SQL generation")
    func primitiveQueryValuesDelegate() throws {
        let builder = SQLQueryBuilder(from: "items").select(
            ProjectionItem(.literal(.queryValue(.primitive(.int64(42)))), alias: "answer")
        )
        #expect(try builder.toSQL() == "SELECT 42 AS \"answer\" FROM \"items\"")
        #expect(try builder.toSQL() == builder.buildAST().toSQL())
    }

    @Test("Rich query literals fail in projection, nested filters, sources and CTEs")
    func richQueryValuesAreUnsupported() throws {
        let limits = QueryValueLimits(
            maximumRetainedBytes: 4096, maximumTotalNodes: 32,
            maximumNestingDepth: 8, maximumCollectionElements: 32
        )
        let values: [QueryValue] = [
            .numeric(try QueryNumericValue(
                value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 3
            )),
            .json(try QueryJSONValue(bytes: ByteString(utf8: "null"), limits: limits)),
            .jsonb(try QueryJSONBValue(root: .null, limits: limits)),
            .array(try QueryArrayValue(
                elementType: .integer, dimensions: [], elements: [], limits: limits
            )),
            .interval(try QueryIntervalValue(
                period: CalendarPeriod(months: 2, days: 3), time: TimeSpan(seconds: 4)
            )),
        ]
        for value in values {
            let literal = Literal.queryValue(value)
            let rich = SQLQueryBuilder(from: "items").select(
                ProjectionItem(.literal(literal), alias: "value")
            )
            let nested = SQLQueryBuilder(from: "items").where(
                .equal(.literal(.int(1)), .literal(.array([literal])))
            )
            let source = SQLQueryBuilder(source: .subquery(rich.buildAST(), alias: "nested"))
            let cte = SQLQueryBuilder(from: "nested").with("nested", as: rich.buildAST())
            for builder in [rich, nested, source, cte] {
                #expect(throws: QueryLiteralConversionError.unsupportedSQLLiteral) {
                    _ = try builder.toSQL()
                }
            }
        }
    }
}
