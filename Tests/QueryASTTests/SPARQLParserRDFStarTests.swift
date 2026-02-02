// SPARQLParserRDFStarTests.swift
// Tests for RDF-star parsing in SPARQLParser

import Testing
@testable import QueryAST

@Suite("SPARQLParser RDF-star")
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
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected basic pattern, got: \(pattern)")
            return
        }
        #expect(triples.count == 1)
        let triple = triples[0]
        // Subject should be a quoted triple
        guard case .quotedTriple(let s, let p, let o) = triple.subject else {
            Issue.record("Expected quotedTriple subject, got: \(triple.subject)")
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
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(triples.count == 1)
        if case .quotedTriple = triples[0].object {
            // OK: object is a quoted triple
        } else {
            Issue.record("Expected quotedTriple object, got: \(triples[0].object)")
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
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(triples.count == 1)
        guard case .quotedTriple(let s, let p, let o) = triples[0].subject else {
            Issue.record("Expected quotedTriple subject with variables")
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
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(triples.count == 1)
        guard case .quotedTriple(let s, _, _) = triples[0].subject else {
            Issue.record("Expected quotedTriple subject")
            return
        }
        // Inner subject should also be a quoted triple
        if case .quotedTriple = s {
            // OK: nested quoted triple
        } else {
            Issue.record("Expected nested quotedTriple, got: \(s)")
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
        // If parsing succeeds without error, ISTRIPLE() was recognized as a built-in function
        let statement = try parser.parse(sparql)
        guard case .select(let selectQuery) = statement else {
            Issue.record("Expected SELECT query")
            return
        }
        guard case .graphPattern(let pattern) = selectQuery.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        // Verify the pattern contains a filter (may be nested in join, filter, etc.)
        let found = containsFilter(pattern)
        #expect(found, "Expected FILTER in the pattern tree")
    }

    @Test("Parse SUBJECT() in BIND")
    func testParseSubject() throws {
        let sparql = """
        SELECT ?t ?s WHERE {
            ?t <http://example.org/type> <http://example.org/Statement> .
            BIND(SUBJECT(?t) AS ?s)
        }
        """
        // If parsing succeeds without error, the SUBJECT() function was recognized
        let statement = try parser.parse(sparql)
        guard case .select = statement else {
            Issue.record("Expected SELECT query")
            return
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
        guard case .select = statement else {
            Issue.record("Expected SELECT query")
            return
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
        guard case .select = statement else {
            Issue.record("Expected SELECT query")
            return
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
        guard case .select = statement else {
            Issue.record("Expected SELECT query")
            return
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
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(triples.count == 1)
        guard case .quotedTriple(let s, let p, let o) = triples[0].subject else {
            Issue.record("Expected quotedTriple subject")
            return
        }
        if case .prefixedName(let prefix, let local) = s {
            #expect(prefix == "ex")
            #expect(local == "Toyota")
        } else {
            Issue.record("Expected prefixedName subject, got: \(s)")
        }
        if case .prefixedName(let prefix, let local) = p {
            #expect(prefix == "ex")
            #expect(local == "type")
        } else {
            Issue.record("Expected prefixedName predicate, got: \(p)")
        }
        if case .prefixedName(let prefix, let local) = o {
            #expect(prefix == "ex")
            #expect(local == "Company")
        } else {
            Issue.record("Expected prefixedName object, got: \(o)")
        }
    }

    // MARK: - Helpers

    /// Recursively search for a filter pattern in a GraphPattern tree
    private func containsFilter(_ pattern: GraphPattern) -> Bool {
        switch pattern {
        case .filter:
            return true
        case .join(let left, let right):
            return containsFilter(left) || containsFilter(right)
        case .optional(let left, let right):
            return containsFilter(left) || containsFilter(right)
        case .union(let left, let right):
            return containsFilter(left) || containsFilter(right)
        default:
            return false
        }
    }
}
