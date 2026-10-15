import DatabaseKit
@testable import QueryAST
import Testing

@Suite("SQL semantic occurrence source annotations")
struct SQLSourceAnnotationTests {
    private func child(
        _ arena: QueryOccurrenceArena, _ parent: QueryOccurrenceID,
        _ field: QueryOccurrenceField, _ index: UInt32? = nil
    ) throws -> QueryOccurrenceID {
        try #require(arena.child(of: parent, field: field, index: index))
    }

    private func text(_ sql: String, _ range: SQLSourceRange) -> String {
        String(decoding: Array(sql.utf8)[range.start..<range.end], as: UTF8.self)
    }

    @Test("Equal expressions retain different source occurrences")
    func repeatedEqualExpressions() throws {
        let sql = "SELECT missing, missing FROM records WHERE missing = missing"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let first = try child(parsed.occurrences, parsed.root, .projection, 0)
        let second = try child(parsed.occurrences, parsed.root, .projection, 1)
        let filter = try child(parsed.occurrences, parsed.root, .filter)
        let left = try child(parsed.occurrences, filter, .operand, 0)
        let right = try child(parsed.occurrences, filter, .operand, 1)
        let ids = [first, second, left, right]
        let starts = try ids.map { try #require(parsed.sourceMap[$0]).range.start }
        #expect(Set(ids).count == 4)
        #expect(starts == [7, 16, 43, 53])
        for id in ids { #expect(text(sql, try #require(parsed.sourceMap[id]).range) == "missing") }
    }

    @Test("Nested arguments and CASE children keep decisive identifier spans")
    func nestedArguments() throws {
        let sql = "SELECT outer_fn(1, inner_fn(missing)), CASE WHEN flag THEN missing ELSE other END"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let outer = try child(parsed.occurrences, parsed.root, .projection, 0)
        let inner = try child(parsed.occurrences, outer, .argument, 1)
        let missing = try child(parsed.occurrences, inner, .argument, 0)
        let annotation = try #require(parsed.sourceMap[missing])
        #expect(text(sql, try #require(annotation.identifierRange)) == "missing")
        #expect(annotation.range.start == 28)
        let conditional = try child(parsed.occurrences, parsed.root, .projection, 1)
        let result = try child(parsed.occurrences, conditional, .caseResult, 0)
        #expect(text(sql, try #require(parsed.sourceMap[result]).range) == "missing")
        #expect(result != missing)
        let wildcardSQL = "SELECT missing.* FROM records"
        let wildcard = try SQLParser().parseSelectWithSource(wildcardSQL)
        let qualifier = try child(wildcard.occurrences, wildcard.root, .projection, 0)
        let qualifierAnnotation = try #require(wildcard.sourceMap[qualifier])
        #expect(text(wildcardSQL, qualifierAnnotation.range) == "missing.*")
        #expect(text(wildcardSQL, try #require(qualifierAnnotation.identifierRange)) == "missing")
    }

    @Test("CTE and scalar subquery equal identifiers have independent addresses")
    func nestedQueryOccurrences() throws {
        let sql = "WITH q AS (SELECT missing FROM records) SELECT (SELECT missing FROM records) FROM q"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let cte = try child(parsed.occurrences, parsed.root, .cte, 0)
        let first = try child(parsed.occurrences, cte, .projection, 0)
        let projection = try child(parsed.occurrences, parsed.root, .projection, 0)
        let subquery = try child(parsed.occurrences, projection, .subquery)
        let second = try child(parsed.occurrences, subquery, .projection, 0)
        #expect(first != second)
        #expect(try #require(parsed.sourceMap[first]).range.start == 18)
        #expect(try #require(parsed.sourceMap[second]).range.start == 55)
    }

    @Test("UTF-8 offsets remain absolute through CRLF comments and statement batches")
    func utf8BatchOffsets() throws {
        let sql = "-- É\r\nSELECT \"名前\"; /* 😀 */\r\n SELECT missing"
        let parsed = try SQLParser().parseStatementsWithSource(sql)
        #expect(parsed.roots.count == 2)
        let first = try child(parsed.occurrences, parsed.roots[0], .projection, 0)
        let second = try child(parsed.occurrences, parsed.roots[1], .projection, 0)
        let expected = try #require(sql.range(of: "missing"))
        let byteOffset = sql[..<expected.lowerBound].utf8.count
        #expect(text(sql, try #require(parsed.sourceMap[first]).range) == "\"名前\"")
        #expect(try #require(parsed.sourceMap[second]).range.start == byteOffset)
        #expect(parsed.occurrences.occurrence(parsed.roots[0])?.index == 0)
        #expect(parsed.occurrences.occurrence(parsed.roots[1])?.index == 1)
    }

    @Test("Normalization keeps complete original cast ranges and later occurrences")
    func normalizationProvenance() throws {
        let sql = "SELECT '1'::integer, CAST('2' AS integer), DATE '2024-01-01', missing"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let expected = ["'1'::integer", "CAST('2' AS integer)", "DATE '2024-01-01'", "missing"]
        for index in expected.indices {
            let occurrence = try child(parsed.occurrences, parsed.root, .projection, UInt32(index))
            #expect(text(sql, try #require(parsed.sourceMap[occurrence]).range) == expected[index])
        }
    }

    @Test("Lookahead type probing cannot add semantic occurrences")
    func lookaheadDoesNotCapture() throws {
        let parsed = try SQLParser().parseSelectWithSource("SELECT timestamp(3) '2024-01-01 00:00:00', missing")
        let missing = try child(parsed.occurrences, parsed.root, .projection, 1)
        #expect(parsed.sourceMap[missing]?.identifierRange?.start == 43)
    }

    @Test("Reuse after success and failure does not change earlier returned annotations")
    func parserReuseSnapshots() throws {
        let parser = SQLParser()
        let first = try parser.parseSelectWithSource("SELECT first_name FROM records")
        let occurrence = try child(first.occurrences, first.root, .projection, 0)
        let annotation = try #require(first.sourceMap[occurrence])
        #expect(throws: SQLParser.ParseError.self) { _ = try parser.parseSelectWithSource("SELECT (") }
        let second = try parser.parseSelectWithSource("SELECT x")
        #expect(first.sourceMap[occurrence] == annotation)
        #expect(annotation.range == SQLSourceRange(start: 7, end: 17))
        let secondOccurrence = try child(second.occurrences, second.root, .projection, 0)
        #expect(second.sourceMap[secondOccurrence]?.range == SQLSourceRange(start: 7, end: 8))
        #expect(try parser.parseSelect("SELECT 1").projection != .all)
    }

    @Test("Source nodes and map entries share the cumulative node admission ledger")
    func annotationNodeBudgetBoundary() throws {
        let admittedLimits = QueryStructuralLimits(maximumTotalNodes: 2)
        var admitted = SQLSourceCaptureCollector(limits: admittedLimits)
        var admittedLedger = QueryStructuralResourceLedger(limits: admittedLimits)
        try admitted.record(Expression.col("x"), start: 7, end: 8, identifierEnd: 8, ledger: &admittedLedger)
        let root = try admitted.finishStatement(ledger: &admittedLedger)
        #expect(admitted.snapshot.sourceMap[root]?.range == SQLSourceRange(start: 7, end: 8))
        let rejectedLimits = QueryStructuralLimits(maximumTotalNodes: 1)
        var rejected = SQLSourceCaptureCollector(limits: rejectedLimits)
        var rejectedLedger = QueryStructuralResourceLedger(limits: rejectedLimits)
        try rejected.record(Expression.col("x"), start: 7, end: 8, identifierEnd: 8, ledger: &rejectedLedger)
        #expect(throws: QueryStructuralValidationError.self) { _ = try rejected.finishStatement(ledger: &rejectedLedger) }
        #expect(rejected.snapshot.occurrences.count == 0)
    }

    @Test("Output position lowering keeps exact occurrence roles and cast children")
    func outputPositionProvenance() throws {
        let sql = "SELECT 7 GROUP BY (-1), CAST(1 AS INTEGER) ORDER BY (2), 1::BIGINT"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let group = try child(parsed.occurrences, parsed.root, .groupBy, 0)
        let groupCast = try child(parsed.occurrences, parsed.root, .groupBy, 1)
        let order = try child(parsed.occurrences, parsed.root, .orderBy, 0)
        let orderCast = try child(parsed.occurrences, parsed.root, .orderBy, 1)
        #expect(text(sql, try #require(parsed.sourceMap[group]).range) == "(-1)")
        #expect(text(sql, try #require(parsed.sourceMap[order]).range) == "(2)")
        #expect(parsed.occurrences.child(of: group, field: .operand, index: 0) == nil)
        #expect(parsed.occurrences.child(of: order, field: .operand, index: 0) == nil)
        for cast in [groupCast, orderCast] {
            let operand = try child(parsed.occurrences, cast, .operand, 0)
            _ = try child(parsed.occurrences, cast, .castType)
            #expect(text(sql, try #require(parsed.sourceMap[operand]).range) == "1")
        }
        #expect(text(sql, try #require(parsed.sourceMap[groupCast]).range) == "CAST(1 AS INTEGER)")
        #expect(text(sql, try #require(parsed.sourceMap[orderCast]).range) == "1::BIGINT")
        #expect(parsed.occurrences.count == 11)
    }
}
