// SPARQLParserRDFStarTests.swift
// Tests for RDF-star parsing in SPARQLParser

import Testing
import TestHeartbeat
@testable import QueryAST

@Suite("SPARQLParser RDF-star", .heartbeat)
struct SPARQLParserRDFStarTests {

    private let parser = SPARQLParser()

    // MARK: - Quoted Triple in Triple Patterns

    @Test("Parse quoted triple as subject")
    func testQuotedTripleAsSubject() throws {
        let sparql = """
        SELECT ?source WHERE {
            << <http://example.org/s> <http://example.org/p> <http://example.org/o> >> <http://example.org/source> ?source .
        }
        """
        let statement = try parser.parse(sparql)

        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard case .basic(let basicGraphPattern) = pattern else {
            Issue.record("Expected basic pattern, got: \(pattern)")
            return
        }
        #expect(basicGraphPattern.count == 1)
        guard case .triple(let triple) = basicGraphPattern.elements[0] else {
            Issue.record("Expected triple pattern")
            return
        }
        // Subject should be a quoted triple
        guard case .tripleTerm(let s, let p, let o) = triple.subject else {
            Issue.record("Expected tripleTerm subject, got: \(triple.subject)")
            return
        }
        #expect(s == .iri("http://example.org/s"))
        #expect(p == .iri("http://example.org/p"))
        #expect(o == .iri("http://example.org/o"))
    }

    @Test("Parse quoted triple as object")
    func testQuotedTripleAsObject() throws {
        let sparql = """
        SELECT ?s WHERE {
            ?s <http://example.org/claims> << <http://example.org/a> <http://example.org/b> <http://example.org/c> >> .
        }
        """
        let statement = try parser.parse(sparql)

        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard case .basic(let basicGraphPattern) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(basicGraphPattern.count == 1)
        guard case .triple(let triple) = basicGraphPattern.elements[0] else {
            Issue.record("Expected triple pattern")
            return
        }
        if case .tripleTerm = triple.object {
            // OK: object is a quoted triple
        } else {
            Issue.record("Expected tripleTerm object, got: \(triple.object)")
        }
    }

    @Test("Parse quoted triple with variables inside")
    func testQuotedTripleWithVariables() throws {
        let sparql = """
        SELECT ?s ?p ?o WHERE {
            << ?s ?p ?o >> <http://example.org/source> <http://example.org/wiki> .
        }
        """
        let statement = try parser.parse(sparql)

        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard case .basic(let basicGraphPattern) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(basicGraphPattern.count == 1)
        guard case .triple(let triple) = basicGraphPattern.elements[0],
              case .tripleTerm(let s, let p, let o) = triple.subject else {
            Issue.record("Expected tripleTerm subject with variables")
            return
        }
        #expect(s == .variable("s"))
        #expect(p == .variable("p"))
        #expect(o == .variable("o"))
    }

    @Test("Parse nested quoted triple")
    func testNestedQuotedTriple() throws {
        let sparql = """
        SELECT ?source WHERE {
            << << <http://example.org/a> <http://example.org/b> <http://example.org/c> >> <http://example.org/meta> <http://example.org/value> >> <http://example.org/source> ?source .
        }
        """
        let statement = try parser.parse(sparql)

        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard case .basic(let basicGraphPattern) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(basicGraphPattern.count == 1)
        guard case .triple(let triple) = basicGraphPattern.elements[0],
              case .tripleTerm(let s, _, _) = triple.subject else {
            Issue.record("Expected tripleTerm subject")
            return
        }
        // Inner subject should also be a quoted triple
        if case .tripleTerm = s {
            // OK: nested quoted triple
        } else {
            Issue.record("Expected nested tripleTerm, got: \(s)")
        }
    }

    // MARK: - RDF-star Built-in Functions

    @Test("Parse ISTRIPLE() in FILTER")
    func testParseIsTriple() throws {
        let sparql = """
        SELECT ?x WHERE {
            ?x <http://example.org/value> ?v .
            FILTER(ISTRIPLE(?v))
        }
        """
        let statement = try parser.parse(sparql)
        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard let filterExpr = findFilterExpression(pattern) else {
            Issue.record("Expected FILTER in the pattern tree")
            return
        }
        if case .isTriple = filterExpr {
            // correct
        } else {
            Issue.record("Expected .isTriple expression, got: \(filterExpr)")
        }
    }

    @Test("Parse SUBJECT() in BIND")
    func testParseSubject() throws {
        let sparql = """
        SELECT ?t ?s WHERE {
            ?t <http://example.org/type> <http://example.org/Statement> .
            BIND(SUBJECT(?t) AS ?s)
        }
        """
        let statement = try parser.parse(sparql)
        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard let bindExpr = findBindExpression(pattern) else {
            Issue.record("Expected BIND in the pattern tree")
            return
        }
        if case .subject = bindExpr {
            // correct
        } else {
            Issue.record("Expected .subject expression, got: \(bindExpr)")
        }
    }

    @Test("Parse PREDICATE() in BIND")
    func testParsePredicate() throws {
        let sparql = """
        SELECT ?t ?p WHERE {
            ?t <http://example.org/type> <http://example.org/Statement> .
            BIND(PREDICATE(?t) AS ?p)
        }
        """
        let statement = try parser.parse(sparql)
        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard let bindExpr = findBindExpression(pattern) else {
            Issue.record("Expected BIND in the pattern tree")
            return
        }
        if case .predicate = bindExpr {
            // correct
        } else {
            Issue.record("Expected .predicate expression, got: \(bindExpr)")
        }
    }

    @Test("Parse OBJECT() in BIND")
    func testParseObject() throws {
        let sparql = """
        SELECT ?t ?o WHERE {
            ?t <http://example.org/type> <http://example.org/Statement> .
            BIND(OBJECT(?t) AS ?o)
        }
        """
        let statement = try parser.parse(sparql)
        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard let bindExpr = findBindExpression(pattern) else {
            Issue.record("Expected BIND in the pattern tree")
            return
        }
        if case .object = bindExpr {
            // correct
        } else {
            Issue.record("Expected .object expression, got: \(bindExpr)")
        }
    }

    @Test("Parse TRIPLE() in BIND")
    func testParseTriple() throws {
        let sparql = """
        SELECT ?result WHERE {
            BIND(TRIPLE(<http://example.org/s>, <http://example.org/p>, <http://example.org/o>) AS ?result)
        }
        """
        let statement = try parser.parse(sparql)
        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard let bindExpr = findBindExpression(pattern) else {
            Issue.record("Expected BIND in the pattern tree")
            return
        }
        if case .triple = bindExpr {
            // correct
        } else {
            Issue.record("Expected .triple expression, got: \(bindExpr)")
        }
    }

    // MARK: - Quoted Triple in Expression

    @Test("Parse quoted triple as expression value")
    func testQuotedTripleInExpression() throws {
        let sparql = """
        SELECT ?x WHERE {
            ?x <http://example.org/claims> ?t .
            FILTER(?t = << <http://example.org/a> <http://example.org/b> <http://example.org/c> >>)
        }
        """
        let statement = try parser.parse(sparql)
        guard case .select = statement else {
            Issue.record("Expected SELECT query")
            return
        }
    }

    // MARK: - Prefixed Names in Quoted Triples

    @Test("Parse quoted triple with prefixed names")
    func testQuotedTripleWithPrefixes() throws {
        let sparql = """
        PREFIX ex: <http://example.org/>
        SELECT ?source WHERE {
            << ex:Toyota ex:type ex:Company >> ex:source ?source .
        }
        """
        let statement = try parser.parse(sparql)

        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard case .basic(let basicGraphPattern) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(basicGraphPattern.count == 1)
        guard case .triple(let triple) = basicGraphPattern.elements[0],
              case .tripleTerm(let s, let p, let o) = triple.subject else {
            Issue.record("Expected tripleTerm subject")
            return
        }
        #expect(s == .iri("http://example.org/Toyota"))
        #expect(p == .iri("http://example.org/type"))
        #expect(o == .iri("http://example.org/Company"))
    }

    // MARK: - Helpers

    /// Recursively search for a FILTER expression in a GraphPattern tree
    private func findFilterExpression(
        _ pattern: GraphPattern
    ) -> DatabaseKit.Expression? {
        switch pattern {
        case .filter(_, let expr):
            return expr
        case .join(let left, let right):
            return findFilterExpression(left) ?? findFilterExpression(right)
        case .optional(let left, let right):
            return findFilterExpression(left) ?? findFilterExpression(right)
        case .union(let left, let right):
            return findFilterExpression(left) ?? findFilterExpression(right)
        case .bind(let inner, _, _):
            return findFilterExpression(inner)
        default:
            return nil
        }
    }

    /// Recursively search for a BIND expression in a GraphPattern tree
    private func findBindExpression(
        _ pattern: GraphPattern
    ) -> DatabaseKit.Expression? {
        switch pattern {
        case .bind(_, _, let expr):
            return expr
        case .join(let left, let right):
            return findBindExpression(left) ?? findBindExpression(right)
        case .optional(let left, let right):
            return findBindExpression(left) ?? findBindExpression(right)
        case .union(let left, let right):
            return findBindExpression(left) ?? findBindExpression(right)
        case .filter(let inner, _):
            return findBindExpression(inner)
        default:
            return nil
        }
    }
}
