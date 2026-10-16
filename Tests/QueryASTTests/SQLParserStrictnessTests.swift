import Testing
import DatabaseKit
import TestHeartbeat
@testable import QueryAST

@Suite("SQL parser strictness", .serialized, .heartbeat)
struct SQLParserStrictnessTests {
    @Test("Unterminated string literals are rejected at their opening quote")
    func rejectsUnterminatedStringLiteral() {
        #expect(
            throws: SQLParser.ParseError.invalidSyntax(
                message: "Unterminated quoted value",
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
                + "CAST('{1}' AS INTEGER ARRAY), "
                + "CAST('POINT(0 0)' AS geography)"
        )
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }),
              items.count == 4 else {
            Issue.record("Expected four CAST projection items")
            return
        }

        guard case .literal(.queryValue(.numeric(let numeric), let decimalType)) = try projectionExpression(items[0]) else {
            Issue.record("Expected normalized DECIMAL constant")
            return
        }
        #expect(decimalType == .sql(.decimal(precision: 10, scale: 2)))
        #expect(numeric.displayScale == 2)

        guard case .literal(.queryValue(.primitive(.float64(1)), let doubleType)) = try projectionExpression(items[1]) else {
            Issue.record("Expected normalized DOUBLE PRECISION constant")
            return
        }
        #expect(doubleType == .sql(.doublePrecision))

        guard case .literal(.queryValue(.array, let arrayType)) = try projectionExpression(items[2]) else {
            Issue.record("Expected normalized ARRAY constant")
            return
        }
        #expect(arrayType == .sql(.array(.integer)))

        guard case .cast(_, let customType) = try projectionExpression(items[3]) else {
            Issue.record("Expected custom CAST")
            return
        }
        #expect(customType == .custom("geography"))
    }

    @Test("CAST rejects invalid intrinsic type parameters")
    func rejectsInvalidCastTypeParameters() {
        #expect(throws: (any Error).self) {
            _ = try SQLParser().parseSelect(
                "SELECT CAST(1 AS DECIMAL(39, 3))"
            )
        }
        #expect(throws: (any Error).self) {
            _ = try SQLParser().parseSelect(
                "SELECT CAST('x' AS VARCHAR(0))"
            )
        }
    }
}
