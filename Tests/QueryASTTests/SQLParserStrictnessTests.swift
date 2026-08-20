import Testing
import TestHeartbeat
@testable import QueryAST

@Suite("SQL parser strictness", .serialized, .heartbeat)
struct SQLParserStrictnessTests {
    @Test("Unterminated string literals are rejected at their opening quote")
    func rejectsUnterminatedStringLiteral() {
        #expect(
            throws: SQLParser.ParseError.invalidSyntax(
                message: "Unterminated string literal",
                position: 7
            )
        ) {
            _ = try SQLParser().parseSelect("SELECT 'unterminated")
        }
    }

    @Test("Unterminated block comments are rejected at their opening delimiter")
    func rejectsUnterminatedBlockComment() {
        #expect(
            throws: SQLParser.ParseError.invalidSyntax(
                message: "Unterminated block comment",
                position: 9
            )
        ) {
            _ = try SQLParser().parseSelect("SELECT 1 /* unterminated")
        }
    }

    @Test("CTE modifiers are parsed as contextual SQL words")
    func parsesCommonTableExpressionModifiers() throws {
        let materialized = try SQLParser().parseSelect(
            "WITH RECURSIVE data AS MATERIALIZED (SELECT 1) SELECT * FROM data"
        )
        #expect(materialized.subqueries?.first?.materialized == .materialized)

        let notMaterialized = try SQLParser().parseSelect(
            "WITH data AS NOT MATERIALIZED (SELECT 1) SELECT * FROM data"
        )
        #expect(
            notMaterialized.subqueries?.first?.materialized == .notMaterialized
        )
    }

    @Test("CAST produces typed expression nodes for scalar and array targets")
    func parsesCastExpressions() throws {
        let query = try SQLParser().parseSelect(
            "SELECT CAST('12.50' AS DECIMAL(10, 2)), "
                + "CAST(1 AS DOUBLE PRECISION), "
                + "CAST('x' AS VARCHAR(32) ARRAY), "
                + "CAST('POINT(0 0)' AS geography)"
        )
        guard case .items(let items) = query.projection,
              items.count == 4 else {
            Issue.record("Expected four CAST projection items")
            return
        }

        guard case .cast(_, let decimalType) = items[0].expression else {
            Issue.record("Expected DECIMAL CAST")
            return
        }
        #expect(decimalType == .decimal(precision: 10, scale: 2))

        guard case .cast(_, let doubleType) = items[1].expression else {
            Issue.record("Expected DOUBLE PRECISION CAST")
            return
        }
        #expect(doubleType == .doublePrecision)

        guard case .cast(_, let arrayType) = items[2].expression else {
            Issue.record("Expected ARRAY CAST")
            return
        }
        #expect(arrayType == .array(.varchar(length: 32)))

        guard case .cast(_, let customType) = items[3].expression else {
            Issue.record("Expected custom CAST")
            return
        }
        #expect(customType == .custom("GEOGRAPHY"))
    }

    @Test("CAST rejects invalid intrinsic type parameters")
    func rejectsInvalidCastTypeParameters() {
        #expect(throws: (any Error).self) {
            _ = try SQLParser().parseSelect(
                "SELECT CAST(1 AS DECIMAL(2, 3))"
            )
        }
        #expect(throws: (any Error).self) {
            _ = try SQLParser().parseSelect(
                "SELECT CAST('x' AS VARCHAR(0))"
            )
        }
    }
}
