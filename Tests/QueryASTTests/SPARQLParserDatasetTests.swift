/// SPARQLParserDatasetTests.swift
/// Tests for Phase 3: AST changes (B9: FROM/FROM NAMED, A12: dirLangLiteral)

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

// MARK: - B9: FROM / FROM NAMED

@Suite("B9: FROM / FROM NAMED")
struct DatasetClauseTests {

    @Test("FROM clause captures IRI")
    func testFromClause() throws {
        let query = try parseQuery("""
            SELECT * FROM <http://example.org/graph1> WHERE { ?s ?p ?o }
            """)
        #expect(query.from != nil)
        #expect(query.from?.count == 1)
        #expect(query.from?[0] == "http://example.org/graph1")
    }

    @Test("FROM NAMED clause captures IRI")
    func testFromNamedClause() throws {
        let query = try parseQuery("""
            SELECT * FROM NAMED <http://example.org/named1> WHERE { ?s ?p ?o }
            """)
        #expect(query.fromNamed != nil)
        #expect(query.fromNamed?.count == 1)
        #expect(query.fromNamed?[0] == "http://example.org/named1")
    }

    @Test("Multiple FROM and FROM NAMED")
    func testMultipleDatasetClauses() throws {
        let query = try parseQuery("""
            SELECT *
            FROM <http://example.org/g1>
            FROM <http://example.org/g2>
            FROM NAMED <http://example.org/n1>
            WHERE { ?s ?p ?o }
            """)
        #expect(query.from?.count == 2)
        #expect(query.fromNamed?.count == 1)
    }

    @Test("No FROM clause returns nil")
    func testNoFromClause() throws {
        let query = try parseQuery("""
            SELECT * WHERE { ?s ?p ?o }
            """)
        #expect(query.from == nil)
        #expect(query.fromNamed == nil)
    }
}

// MARK: - A12: Literal Direction

@Suite("A12: Literal Direction")
struct LiteralDirectionTests {

    @Test("RTL direction literal")
    func testRTLDirection() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p "text"@ar--rtl }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        #expect(triples.count == 1)
        guard case .literal(.dirLangLiteral(let value, let lang, let dir)) = triples[0].object else {
            Issue.record("Expected dirLangLiteral, got \(triples[0].object)")
            return
        }
        #expect(value == "text")
        #expect(lang == "ar")
        #expect(dir == "rtl")
    }

    @Test("LTR direction literal")
    func testLTRDirection() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p "hello"@en--ltr }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .literal(.dirLangLiteral(let value, let lang, let dir)) = triples[0].object else {
            Issue.record("Expected dirLangLiteral, got \(triples[0].object)")
            return
        }
        #expect(value == "hello")
        #expect(lang == "en")
        #expect(dir == "ltr")
    }

    @Test("Language without direction still works")
    func testLangWithoutDirection() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p "hello"@en }
            """#)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .literal(.langLiteral(let value, let lang)) = triples[0].object else {
            Issue.record("Expected langLiteral, got \(triples[0].object)")
            return
        }
        #expect(value == "hello")
        #expect(lang == "en")
    }
}
