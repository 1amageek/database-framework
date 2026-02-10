/// SPARQLParserBugFixTests.swift
/// Tests for Phase 1: Bug fixes and quick wins

import Testing
import Foundation
@testable import QueryAST

// MARK: - Helper

private func parseQuery(_ sparql: String) throws -> QueryIR.SelectQuery {
    let parser = SPARQLParser()
    return try parser.parseSelect(sparql)
}

private func parsePattern(_ sparql: String) throws -> GraphPattern {
    let query = try parseQuery(sparql)
    guard case .graphPattern(let pattern) = query.source else {
        throw SPARQLParser.ParseError.invalidSyntax(
            message: "Expected graphPattern source", position: 0
        )
    }
    return pattern
}

private func parseExpression(_ sparql: String) throws -> QueryIR.Expression {
    let pattern = try parsePattern(sparql)
    func findFilter(_ p: GraphPattern) -> QueryIR.Expression? {
        switch p {
        case .filter(_, let expr): return expr
        case .join(let l, let r): return findFilter(l) ?? findFilter(r)
        default: return nil
        }
    }
    guard let expr = findFilter(pattern) else {
        throw SPARQLParser.ParseError.invalidSyntax(
            message: "No FILTER found", position: 0
        )
    }
    return expr
}

// MARK: - C1: ABS/CEIL/FLOOR/ROUND

@Suite("C1: Math Functions")
struct MathFunctionTests {

    @Test("ABS function parses correctly")
    func testABS() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (ABS(?o) > 5) }
            """)
        guard case .greaterThan(let lhs, _) = expr else {
            Issue.record("Expected .greaterThan, got \(expr)")
            return
        }
        guard case .function(let call) = lhs else {
            Issue.record("Expected .function, got \(lhs)")
            return
        }
        #expect(call.name.uppercased() == "ABS")
        #expect(call.arguments.count == 1)
    }

    @Test("CEIL function parses correctly")
    func testCEIL() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (CEIL(?o) = 3) }
            """)
        guard case .equal(let lhs, _) = expr else {
            Issue.record("Expected .equal")
            return
        }
        guard case .function(let call) = lhs else {
            Issue.record("Expected .function")
            return
        }
        #expect(call.name.uppercased() == "CEIL")
    }

    @Test("FLOOR function parses correctly")
    func testFLOOR() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (FLOOR(?o) = 2) }
            """)
        guard case .equal(let lhs, _) = expr else {
            Issue.record("Expected .equal")
            return
        }
        guard case .function(let call) = lhs else {
            Issue.record("Expected .function")
            return
        }
        #expect(call.name.uppercased() == "FLOOR")
    }

    @Test("ROUND function parses correctly")
    func testROUND() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (ROUND(?o) = 4) }
            """)
        guard case .equal(let lhs, _) = expr else {
            Issue.record("Expected .equal")
            return
        }
        guard case .function(let call) = lhs else {
            Issue.record("Expected .function")
            return
        }
        #expect(call.name.uppercased() == "ROUND")
    }
}

// MARK: - B7: Unary Plus

@Suite("B7: Unary Plus")
struct UnaryPlusTests {

    @Test("Unary + in FILTER")
    func testUnaryPlus() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (+?o > 0) }
            """)
        // Unary + is identity, so the expression should be greaterThan(variable, literal)
        guard case .greaterThan(let lhs, _) = expr else {
            Issue.record("Expected .greaterThan, got \(expr)")
            return
        }
        guard case .variable(let v) = lhs else {
            Issue.record("Expected .variable, got \(lhs)")
            return
        }
        #expect(v.name == "o")
    }

    @Test("Unary + with expression")
    func testUnaryPlusExpression() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (+42 = 42) }
            """)
        guard case .equal(let lhs, let rhs) = expr else {
            Issue.record("Expected .equal")
            return
        }
        guard case .literal(.int(42)) = lhs else {
            Issue.record("Expected literal 42, got \(lhs)")
            return
        }
        guard case .literal(.int(42)) = rhs else {
            Issue.record("Expected literal 42, got \(rhs)")
            return
        }
    }
}

// MARK: - B6: Long String lang/datatype

@Suite("B6: Long String Suffixes")
struct LongStringSuffixTests {

    @Test("Long string with language tag")
    func testLongStringLang() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p """hello"""@en }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        #expect(triples.count == 1)
        guard case .literal(.langLiteral(let value, let lang)) = triples[0].object else {
            Issue.record("Expected langLiteral, got \(triples[0].object)")
            return
        }
        #expect(value == "hello")
        #expect(lang == "en")
    }

    @Test("Long string with datatype")
    func testLongStringDatatype() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p '''42'''^^<http://www.w3.org/2001/XMLSchema#integer> }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        #expect(triples.count == 1)
        guard case .literal(.typedLiteral(let value, let datatype)) = triples[0].object else {
            Issue.record("Expected typedLiteral, got \(triples[0].object)")
            return
        }
        #expect(value == "42")
        #expect(datatype == "http://www.w3.org/2001/XMLSchema#integer")
    }

    @Test("Long string without suffix still works")
    func testLongStringPlain() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p """multiline text""" }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .literal(.string(let value)) = triples[0].object else {
            Issue.record("Expected .string literal")
            return
        }
        #expect(value == "multiline text")
    }
}

// MARK: - B8: Unicode Escapes

@Suite("B8: Unicode Escapes")
struct UnicodeEscapeTests {

    @Test("\\u escape in string literal")
    func testUnicodeEscape4() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p "caf\u00E9" }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .literal(.string(let value)) = triples[0].object else {
            Issue.record("Expected .string literal, got \(triples[0].object)")
            return
        }
        #expect(value == "caf\u{00E9}")  // café
    }

    @Test("\\U escape for emoji")
    func testUnicodeEscape8() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p "hello\U0001F600" }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .literal(.string(let value)) = triples[0].object else {
            Issue.record("Expected .string literal")
            return
        }
        #expect(value == "hello\u{1F600}")
    }

    @Test("\\u escape for Japanese")
    func testUnicodeEscapeJapanese() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p "\u65E5\u672C" }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .literal(.string(let value)) = triples[0].object else {
            Issue.record("Expected .string literal")
            return
        }
        #expect(value == "\u{65E5}\u{672C}")  // 日本
    }
}

// MARK: - B4: GROUP BY Bare Functions

@Suite("B4: GROUP BY Bare Functions")
struct GroupByBareFunctionTests {

    @Test("GROUP BY STR(?x) — bare function call")
    func testGroupByBareFunction() throws {
        let query = try parseQuery("""
            SELECT (COUNT(*) AS ?count) ?type WHERE { ?s a ?type } GROUP BY STR(?type)
            """)
        #expect(query.groupBy != nil)
        guard let groupBy = query.groupBy, groupBy.count == 1 else {
            Issue.record("Expected 1 GROUP BY expression")
            return
        }
        guard case .function(let call) = groupBy[0] else {
            Issue.record("Expected .function, got \(groupBy[0])")
            return
        }
        #expect(call.name.uppercased() == "STR")
    }

    @Test("GROUP BY with variable still works")
    func testGroupByVariable() throws {
        let query = try parseQuery("""
            SELECT ?type (COUNT(*) AS ?count) WHERE { ?s a ?type } GROUP BY ?type
            """)
        #expect(query.groupBy != nil)
        guard let groupBy = query.groupBy, groupBy.count == 1 else {
            Issue.record("Expected 1 GROUP BY expression")
            return
        }
        guard case .variable(let v) = groupBy[0] else {
            Issue.record("Expected .variable")
            return
        }
        #expect(v.name == "type")
    }

    @Test("GROUP BY with bracketed expression still works")
    func testGroupByBracketed() throws {
        let query = try parseQuery("""
            SELECT ?x WHERE { ?s ?p ?x } GROUP BY (STR(?x))
            """)
        #expect(query.groupBy != nil)
        #expect(query.groupBy?.count == 1)
    }
}

// MARK: - B5: ORDER BY Bare Functions

@Suite("B5: ORDER BY Bare Functions")
struct OrderByBareFunctionTests {

    @Test("ORDER BY STRLEN(?name) — bare function call")
    func testOrderByBareFunction() throws {
        let query = try parseQuery("""
            SELECT * WHERE { ?s ?p ?name } ORDER BY STRLEN(?name)
            """)
        #expect(query.orderBy != nil)
        guard let orderBy = query.orderBy, orderBy.count == 1 else {
            Issue.record("Expected 1 ORDER BY key")
            return
        }
        guard case .function(let call) = orderBy[0].expression else {
            Issue.record("Expected .function, got \(orderBy[0].expression)")
            return
        }
        #expect(call.name.uppercased() == "STRLEN")
    }

    @Test("ORDER BY with ASC/DESC and bare function")
    func testOrderByDescBareFunction() throws {
        let query = try parseQuery("""
            SELECT * WHERE { ?s ?p ?name } ORDER BY DESC STRLEN(?name)
            """)
        #expect(query.orderBy != nil)
        guard let orderBy = query.orderBy, orderBy.count == 1 else {
            Issue.record("Expected 1 ORDER BY key")
            return
        }
        #expect(orderBy[0].direction == .descending)
    }

    @Test("ORDER BY with variable still works")
    func testOrderByVariable() throws {
        let query = try parseQuery("""
            SELECT * WHERE { ?s ?p ?o } ORDER BY ?o
            """)
        #expect(query.orderBy != nil)
        guard let orderBy = query.orderBy, orderBy.count == 1 else {
            Issue.record("Expected 1 ORDER BY key")
            return
        }
        guard case .variable(let v) = orderBy[0].expression else {
            Issue.record("Expected .variable")
            return
        }
        #expect(v.name == "o")
    }

    @Test("ORDER BY with bracketed expression still works")
    func testOrderByBracketed() throws {
        let query = try parseQuery("""
            SELECT * WHERE { ?s ?p ?o } ORDER BY (STRLEN(?o))
            """)
        #expect(query.orderBy != nil)
        #expect(query.orderBy?.count == 1)
    }
}

// MARK: - A13: Double Negation

@Suite("A13: Double Negation")
struct DoubleNegationTests {

    @Test("!! double negation parses correctly")
    func testDoubleNegation() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (!!BOUND(?s)) }
            """)
        guard case .not(let inner1) = expr else {
            Issue.record("Expected outer .not")
            return
        }
        guard case .not(let inner2) = inner1 else {
            Issue.record("Expected inner .not")
            return
        }
        guard case .bound = inner2 else {
            Issue.record("Expected .bound, got \(inner2)")
            return
        }
    }
}
