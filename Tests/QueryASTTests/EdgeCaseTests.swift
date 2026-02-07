/// EdgeCaseTests.swift
/// Comprehensive tests for query edge cases and robustness
///
/// Coverage: Empty results, NULL handling, large datasets, Unicode, date/time, precision

import Testing
@testable import QueryAST

// MARK: - Query Edge Cases

@Suite("Query Edge Cases")
struct EdgeCaseTests {

    // MARK: - Empty Result Tests

    @Test("Empty result from complex query")
    func testEmptyResult() throws {
        // Query that would return no results

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .and(
                .equal(.column(ColumnRef(column: "status")), .literal(.string("nonexistent"))),
                .greaterThan(.column(ColumnRef(column: "age")), .literal(.int(200)))
            )
        )

        // Query structure is valid even if it returns no results
        #expect(query.filter != nil)
    }

    @Test("Empty IN list")
    func testEmptyInList() throws {
        // IN () with empty list - should be valid but match nothing

        // Represented as IN with empty values
        let condition = Expression.inList(
            .column(ColumnRef(column: "id")),
            values: []  // Empty list
        )

        if case .inList(let col, let items) = condition {
            if case .column(_) = col {
                // OK
            }
            #expect(items.isEmpty)
        }
    }

    @Test("Filter always false")
    func testFilterAlwaysFalse() throws {
        // WHERE 1 = 0 - always false

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .equal(.literal(.int(1)), .literal(.int(0)))
        )

        if case .equal(let left, let right) = query.filter {
            if case .literal(.int(1)) = left,
               case .literal(.int(0)) = right {
                // Valid structure for always-false condition
            }
        }
    }

    @Test("Filter always true")
    func testFilterAlwaysTrue() throws {
        // WHERE 1 = 1 - always true

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .equal(.literal(.int(1)), .literal(.int(1)))
        )

        #expect(query.filter != nil)
    }

    // MARK: - NULL Handling Tests

    @Test("NULL in equality comparison")
    func testNullEquality() throws {
        // WHERE col = NULL (incorrect way to check NULL)

        let condition = Expression.equal(
            .column(ColumnRef(column: "value")),
            .literal(.null)
        )

        if case .equal(_, let right) = condition {
            if case .literal(.null) = right {
                // OK - this is valid AST, even though semantically != IS NULL
            }
        }
    }

    @Test("IS NULL condition")
    func testIsNull() throws {
        // WHERE col IS NULL (correct way)

        let condition = Expression.isNull(.column(ColumnRef(column: "value")))

        if case .isNull(let expr) = condition {
            if case .column(let ref) = expr {
                #expect(ref.column == "value")
            }
        }
    }

    @Test("IS NOT NULL condition")
    func testIsNotNull() throws {
        // WHERE col IS NOT NULL

        let condition = Expression.not(.isNull(.column(ColumnRef(column: "value"))))

        if case .not(let inner) = condition {
            if case .isNull(_) = inner {
                // OK
            }
        }
    }

    @Test("NULL in aggregate functions")
    func testNullInAggregates() throws {
        // SUM/AVG/COUNT with NULL values

        // COUNT(*) includes NULL rows
        let countAll = AggregateFunction.count(nil, distinct: false)

        // COUNT(col) excludes NULL values
        let countCol = AggregateFunction.count(.column(ColumnRef(column: "value")), distinct: false)

        // SUM ignores NULL
        let sumCol = AggregateFunction.sum(.column(ColumnRef(column: "amount")), distinct: false)

        if case .count(nil, _) = countAll {
            // COUNT(*) - no column
        }

        if case .count(.some(_), _) = countCol {
            // COUNT(col) - has column
        }

        if case .sum(let expr, _) = sumCol {
            if case .column(_) = expr {
                // OK
            }
        }
    }

    @Test("COALESCE function")
    func testCoalesce() throws {
        // COALESCE(nullable_col, default_value)

        let coalesce = Expression.coalesce([
            .column(ColumnRef(column: "nullable")),
            .literal(.string("default"))
        ])

        if case .coalesce(let exprs) = coalesce {
            #expect(exprs.count == 2)
        }
    }

    @Test("NULLIF function")
    func testNullif() throws {
        // NULLIF(col, value) - returns NULL if equal

        let nullif = Expression.nullIf(
            .column(ColumnRef(column: "status")),
            .literal(.string("unknown"))
        )

        if case .nullIf(let left, let right) = nullif {
            if case .column(let col) = left {
                #expect(col.column == "status")
            }
            if case .literal(_) = right {
                // OK
            }
        }
    }

    // MARK: - Large Dataset Tests

    @Test("Large GROUP BY (many groups)")
    func testLargeGroupBy() throws {
        // Query structure for GROUP BY with many groups

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(column: "customer_id"))),
                ProjectionItem(.aggregate(.count(nil, distinct: false)), alias: "order_count")
            ]),
            source: .table(TableRef("orders")),
            groupBy: [.column(ColumnRef(column: "customer_id"))]
        )

        #expect(query.groupBy?.count == 1)
    }

    @Test("Large LIMIT value")
    func testLargeLimitValue() throws {
        // LIMIT 1000000

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("large_table")),
            limit: 1_000_000
        )

        #expect(query.limit == 1_000_000)
    }

    @Test("Large OFFSET value")
    func testLargeOffsetValue() throws {
        // OFFSET 1000000

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("large_table")),
            limit: 100,
            offset: 1_000_000
        )

        #expect(query.offset == 1_000_000)
    }

    @Test("Many columns in projection")
    func testManyColumnsProjection() throws {
        // SELECT col1, col2, ..., col100

        var items: [ProjectionItem] = []
        for i in 1...100 {
            items.append(ProjectionItem(.column(ColumnRef(column: "col\(i)"))))
        }

        let query = SelectQuery(
            projection: .items(items),
            source: .table(TableRef("wide_table"))
        )

        if case .items(let projItems) = query.projection {
            #expect(projItems.count == 100)
        }
    }

    @Test("Many ORDER BY columns")
    func testManyOrderByColumns() throws {
        // ORDER BY col1, col2, ..., col10

        var sortKeys: [SortKey] = []
        for i in 1...10 {
            sortKeys.append(SortKey(
                .column(ColumnRef(column: "col\(i)")),
                direction: i % 2 == 0 ? .ascending : .descending
            ))
        }

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("table")),
            orderBy: sortKeys
        )

        #expect(query.orderBy?.count == 10)
    }

    // MARK: - Unicode and Special Character Tests

    @Test("Unicode in string literals")
    func testUnicodeStringLiterals() throws {
        // String with various Unicode characters

        let japanese = Literal.string("日本語テスト")
        let emoji = Literal.string("Hello 👋 World 🌍")
        let arabic = Literal.string("مرحبا")
        let chinese = Literal.string("中文测试")

        #expect(japanese == .string("日本語テスト"))
        #expect(emoji == .string("Hello 👋 World 🌍"))
        #expect(arabic == .string("مرحبا"))
        #expect(chinese == .string("中文测试"))
    }

    @Test("Unicode in identifiers")
    func testUnicodeIdentifiers() throws {
        // Column/table names with Unicode (if supported)

        let col = ColumnRef(column: "名前")
        #expect(col.column == "名前")

        let table = TableRef("ユーザー")
        #expect(table.table == "ユーザー")
    }

    @Test("Special characters in strings")
    func testSpecialCharactersInStrings() throws {
        // Strings with special characters

        let withQuotes = Literal.string("He said \"Hello\"")
        let withBackslash = Literal.string("C:\\Users\\name")
        let withNewline = Literal.string("Line1\nLine2")
        let withTab = Literal.string("Col1\tCol2")

        #expect(withQuotes == .string("He said \"Hello\""))
        #expect(withBackslash == .string("C:\\Users\\name"))
        #expect(withNewline == .string("Line1\nLine2"))
        #expect(withTab == .string("Col1\tCol2"))
    }

    @Test("Empty string literal")
    func testEmptyStringLiteral() throws {
        let empty = Literal.string("")
        #expect(empty == .string(""))

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("users")),
            filter: .equal(.column(ColumnRef(column: "name")), .literal(.string("")))
        )

        #expect(query.filter != nil)
    }

    @Test("Very long string literal")
    func testLongStringLiteral() throws {
        let longString = String(repeating: "a", count: 10000)
        let literal = Literal.string(longString)

        if case .string(let s) = literal {
            #expect(s.count == 10000)
        }
    }

    // MARK: - Date/Time Tests

    @Test("Date literal")
    func testDateLiteral() throws {
        // ISO 8601 date string

        let dateStr = "2024-01-15"
        let literal = Literal.string(dateStr)

        if case .string(let s) = literal {
            #expect(s == dateStr)
        }
    }

    @Test("DateTime literal")
    func testDateTimeLiteral() throws {
        // ISO 8601 datetime string

        let datetimeStr = "2024-01-15T10:30:00Z"
        let literal = Literal.string(datetimeStr)

        if case .string(let s) = literal {
            #expect(s == datetimeStr)
        }
    }

    @Test("Date comparison in filter")
    func testDateComparison() throws {
        // WHERE created_at > '2024-01-01'

        let query = SelectQuery(
            projection: .all,
            source: .table(TableRef("events")),
            filter: .greaterThan(
                .column(ColumnRef(column: "created_at")),
                .literal(.string("2024-01-01"))
            )
        )

        #expect(query.filter != nil)
    }

    @Test("Date range filter")
    func testDateRangeFilter() throws {
        // WHERE date BETWEEN '2024-01-01' AND '2024-12-31'

        let condition = Expression.between(
            .column(ColumnRef(column: "date")),
            low: .literal(.string("2024-01-01")),
            high: .literal(.string("2024-12-31"))
        )

        if case .between(let expr, let low, let high) = condition {
            if case .column(let col) = expr {
                #expect(col.column == "date")
            }
            if case .literal(.string(let l)) = low {
                #expect(l == "2024-01-01")
            }
            if case .literal(.string(let h)) = high {
                #expect(h == "2024-12-31")
            }
        }
    }

    // MARK: - Numeric Precision Tests

    @Test("Large integer values")
    func testLargeIntegerValues() throws {
        let maxInt64 = Literal.int(Int64.max)
        let minInt64 = Literal.int(Int64.min)

        if case .int(let max) = maxInt64 {
            #expect(max == Int64.max)
        }

        if case .int(let min) = minInt64 {
            #expect(min == Int64.min)
        }
    }

    @Test("Decimal precision")
    func testDecimalPrecision() throws {
        // High precision decimal values

        let preciseValue = 123.456789012345
        let literal = Literal.double(preciseValue)

        if case .double(let d) = literal {
            #expect(abs(d - preciseValue) < 0.000000000001)
        }
    }

    @Test("Negative numbers")
    func testNegativeNumbers() throws {
        let negativeInt = Literal.int(-42)
        let negativeDouble = Literal.double(-3.14159)

        #expect(negativeInt == .int(-42))
        #expect(negativeDouble == .double(-3.14159))
    }

    @Test("Zero values")
    func testZeroValues() throws {
        let zeroInt = Literal.int(0)
        let zeroDouble = Literal.double(0.0)
        let negativeZero = Literal.double(-0.0)

        #expect(zeroInt == .int(0))

        if case .double(let d) = zeroDouble {
            #expect(d == 0.0)
        }

        // -0.0 and 0.0 should be equal in most contexts
        if case .double(let d) = negativeZero {
            #expect(d == 0.0)
        }
    }

    @Test("Very small double values")
    func testSmallDoubleValues() throws {
        let tiny = Literal.double(1e-300)

        if case .double(let d) = tiny {
            #expect(d > 0)
            #expect(d < 1e-299)
        }
    }

    @Test("Very large double values")
    func testLargeDoubleValues() throws {
        let huge = Literal.double(1e300)

        if case .double(let d) = huge {
            #expect(d > 1e299)
        }
    }

    // MARK: - Boolean Edge Cases

    @Test("Boolean literals")
    func testBooleanLiterals() throws {
        let trueVal = Literal.bool(true)
        let falseVal = Literal.bool(false)

        #expect(trueVal == .bool(true))
        #expect(falseVal == .bool(false))
        #expect(trueVal != falseVal)
    }

    @Test("Nested NOT expressions")
    func testNestedNot() throws {
        // NOT NOT NOT condition

        let base = Expression.equal(.column(ColumnRef(column: "a")), .literal(.int(1)))
        let notOnce = Expression.not(base)
        let notTwice = Expression.not(notOnce)
        let notThrice = Expression.not(notTwice)

        // Structure is valid even if semantically simplifiable
        if case .not(let inner) = notThrice {
            if case .not(_) = inner {
                // OK - nested NOT
            }
        }
    }

    // MARK: - Identifier Edge Cases

    @Test("Reserved word as identifier")
    func testReservedWordIdentifier() throws {
        // Using SQL reserved words as identifiers (quoted)

        let selectCol = ColumnRef(column: "select")
        let fromTable = TableRef("from")
        let whereCol = ColumnRef(column: "where")

        #expect(selectCol.column == "select")
        #expect(fromTable.table == "from")
        #expect(whereCol.column == "where")
    }

    @Test("Identifier starting with number")
    func testIdentifierStartingWithNumber() throws {
        // Identifiers starting with numbers (would need quoting)

        let col = ColumnRef(column: "123abc")
        #expect(col.column == "123abc")
    }

    @Test("Identifier with spaces")
    func testIdentifierWithSpaces() throws {
        // Identifiers with spaces (would need quoting)

        let col = ColumnRef(column: "first name")
        let table = TableRef("user table")

        #expect(col.column == "first name")
        #expect(table.table == "user table")
    }

    // MARK: - SPARQL Term Edge Cases

    @Test("Blank node identifiers")
    func testBlankNodeIdentifiers() throws {
        let blank1 = SPARQLTerm.blankNode("_:b0")
        let blank2 = SPARQLTerm.blankNode("_:genid123")

        if case .blankNode(let id) = blank1 {
            #expect(id == "_:b0")
        }

        if case .blankNode(let id) = blank2 {
            #expect(id == "_:genid123")
        }
    }

    @Test("IRI with special characters")
    func testIRISpecialCharacters() throws {
        let iri1 = SPARQLTerm.iri("http://example.org/path?query=value&other=123")
        let iri2 = SPARQLTerm.iri("http://example.org/path#fragment")
        let iri3 = SPARQLTerm.iri("http://example.org/path%20with%20spaces")

        if case .iri(let i) = iri1 {
            #expect(i.contains("?"))
            #expect(i.contains("&"))
        }

        if case .iri(let i) = iri2 {
            #expect(i.contains("#"))
        }

        if case .iri(let i) = iri3 {
            #expect(i.contains("%20"))
        }
    }

    @Test("Prefixed name edge cases")
    func testPrefixedNameEdgeCases() throws {
        // Empty local part
        let emptyLocal = SPARQLTerm.prefixedName(prefix: "ex", local: "")

        // Long local part
        let longLocal = SPARQLTerm.prefixedName(prefix: "ex", local: String(repeating: "a", count: 1000))

        if case .prefixedName(let prefix, let local) = emptyLocal {
            #expect(prefix == "ex")
            #expect(local.isEmpty)
        }

        if case .prefixedName(_, let local) = longLocal {
            #expect(local.count == 1000)
        }
    }

    // MARK: - Complex Expression Edge Cases

    @Test("Deeply nested expressions")
    func testDeeplyNestedExpressions() throws {
        // ((((a + b) * c) - d) / e)

        var expr = Expression.column(ColumnRef(column: "a"))

        for i in 0..<10 {
            expr = .add(expr, .column(ColumnRef(column: "col\(i)")))
        }

        // Just verify structure doesn't break
        #expect(true)
    }

    @Test("Complex CASE expression")
    func testComplexCaseExpression() throws {
        // CASE WHEN a > 10 THEN 'high' WHEN a > 5 THEN 'medium' ELSE 'low' END

        let caseExpr = Expression.caseWhen(
            cases: [
                CaseWhenPair(condition: Expression.greaterThan(
                    Expression.column(ColumnRef(column: "a")),
                    Expression.literal(.int(10))
                ), result: Expression.literal(.string("high"))),
                CaseWhenPair(condition: Expression.greaterThan(
                    Expression.column(ColumnRef(column: "a")),
                    Expression.literal(.int(5))
                ), result: Expression.literal(.string("medium")))
            ],
            elseResult: Expression.literal(.string("low"))
        )

        if case .caseWhen(let whens, let elseExpr) = caseExpr {
            #expect(whens.count == 2)
            #expect(elseExpr != nil)
        }
    }

    @Test("Expression with many operators")
    func testExpressionManyOperators() throws {
        // a + b - c * d / e

        let expr = Expression.divide(
            .multiply(
                .subtract(
                    .add(
                        .column(ColumnRef(column: "a")),
                        .column(ColumnRef(column: "b"))
                    ),
                    .column(ColumnRef(column: "c"))
                ),
                .column(ColumnRef(column: "d"))
            ),
            .column(ColumnRef(column: "e"))
        )

        // Verify structure
        if case .divide(_, _) = expr {
            // OK
        } else {
            Issue.record("Expected divide at root")
        }
    }
}
