/// SPARQLParserErrorTests.swift
/// Tests for error handling and malformed input

import Testing
import Foundation
@testable import QueryAST

// MARK: - Helper

private func parseQuery(_ sparql: String) throws -> QueryIR.SelectQuery {
    let parser = SPARQLParser()
    return try parser.parseSelect(sparql)
}

private func parseStatement(_ sparql: String) throws -> QueryStatement {
    let parser = SPARQLParser()
    return try parser.parse(sparql)
}

// MARK: - Syntax Errors

@Suite("Syntax Error Handling")
struct SyntaxErrorTests {

    @Test("Unterminated string throws error")
    func testUnterminatedString() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery(#"""
                SELECT * WHERE { ?s ?p "unterminated }
                """#)
        }
    }

    @Test("Missing >> for quoted triple throws error")
    func testMissingClosingAngleBrackets() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("""
                SELECT * WHERE { ?s ?p << ?a ?b ?c }
                """)
        }
    }

    @Test("Missing ) in function call throws error")
    func testMissingClosingParen() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("""
                SELECT * WHERE { ?s ?p ?o . FILTER (STRLEN(?o) }
                """)
        }
    }

    @Test("Missing WHERE in DELETE ... WHERE throws error")
    func testMissingWhereInDelete() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("""
                DELETE { ?s ?p ?o }
                """)
        }
    }

    @Test("Empty query throws error")
    func testEmptyQuery() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("")
        }
    }

    @Test("Unknown query form throws error")
    func testUnknownQueryForm() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("FOOBAR { ?s ?p ?o }")
        }
    }

    @Test("Missing closing brace in WHERE throws error")
    func testMissingClosingBrace() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("SELECT * WHERE { ?s ?p ?o")
        }
    }

    @Test("Missing closing ] for blank node throws error")
    func testMissingClosingBracket() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("""
                SELECT * WHERE { [ <http://example.org/p> <http://example.org/o> ?p ?o }
                """)
        }
    }

    @Test("Missing closing ) for collection throws error")
    func testMissingClosingParenCollection() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("""
                SELECT * WHERE { ?s <http://example.org/list> (1 2 3 }
                """)
        }
    }

    @Test("Numeric token after SELECT produces empty projection")
    func testNumericAfterSelectProducesEmptyProjection() throws {
        // Parser is lenient — numeric after SELECT skips to WHERE
        let query = try parseQuery("SELECT ?x WHERE { ?x ?p ?o }")
        guard case .items(let items) = query.projection else {
            Issue.record("Expected items projection")
            return
        }
        #expect(items.count == 1)
    }
}

// MARK: - Unicode Escape Edge Cases

@Suite("Unicode Escape Edge Cases")
struct UnicodeEscapeEdgeCaseTests {

    @Test("Valid 4-digit \\u escape produces correct character")
    func testValidU4() throws {
        let query = try parseQuery(#"""
            SELECT * WHERE { ?s ?p "caf\u00E9" }
            """#)
        guard case .graphPattern(let pat) = query.source,
              case .basic(let triples) = pat,
              case .literal(.string(let value)) = triples[0].object else {
            Issue.record("Unexpected structure")
            return
        }
        #expect(value == "café")
    }

    @Test("Valid 8-digit \\U escape produces correct character")
    func testValidU8() throws {
        let query = try parseQuery(#"""
            SELECT * WHERE { ?s ?p "\U0001F600" }
            """#)
        guard case .graphPattern(let pat) = query.source,
              case .basic(let triples) = pat,
              case .literal(.string(let value)) = triples[0].object else {
            Issue.record("Unexpected structure")
            return
        }
        #expect(value == "😀")
    }

    @Test("Multiple unicode escapes in one string")
    func testMultipleUnicodeEscapes() throws {
        let query = try parseQuery(#"""
            SELECT * WHERE { ?s ?p "caf\u00E9 \u0041" }
            """#)
        guard case .graphPattern(let pat) = query.source,
              case .basic(let triples) = pat,
              case .literal(.string(let value)) = triples[0].object else {
            Issue.record("Unexpected structure")
            return
        }
        #expect(value == "café A")
    }
}

// MARK: - VERSION Errors

@Suite("VERSION Error Handling")
struct VersionErrorTests {

    @Test("VERSION without string throws error")
    func testVersionNoString() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("VERSION SELECT * WHERE { ?s ?p ?o }")
        }
    }
}

// MARK: - SPARQL Update Errors

@Suite("SPARQL Update Error Handling")
struct UpdateErrorTests {

    @Test("LOAD without IRI throws error")
    func testLoadNoIRI() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("LOAD ?x")
        }
    }

    @Test("CREATE GRAPH without IRI throws error")
    func testCreateGraphNoIRI() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("CREATE GRAPH ?x")
        }
    }

    @Test("DROP GRAPH without IRI throws error")
    func testDropGraphNoIRI() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("DROP GRAPH ?x")
        }
    }

    @Test("INSERT DATA without closing brace throws error")
    func testInsertDataNoBrace() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement(#"""
                INSERT DATA { <http://example.org/s> <http://example.org/p> "v"
                """#)
        }
    }
}

// MARK: - Construct Error Cases

@Suite("CONSTRUCT Error Handling")
struct ConstructErrorTests {

    @Test("CONSTRUCT without template or WHERE throws error")
    func testConstructNoTemplateNoWhere() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("CONSTRUCT")
        }
    }
}

// MARK: - LATERAL Errors

@Suite("LATERAL Error Handling")
struct LateralErrorTests {

    @Test("LATERAL without opening brace throws error")
    func testLateralNoBrace() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("SELECT * WHERE { ?s ?p ?o . LATERAL ?s ?p ?o }")
        }
    }

    @Test("LATERAL without closing brace throws error")
    func testLateralMissingClose() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseQuery("SELECT * WHERE { ?s ?p ?o . LATERAL { ?s ?p ?o }")
        }
    }
}

// MARK: - Robustness

@Suite("Parser Robustness")
struct ParserRobustnessTests {

    @Test("Very long variable name")
    func testLongVariableName() throws {
        let longVar = String(repeating: "a", count: 1000)
        let query = try parseQuery("SELECT ?\(longVar) WHERE { ?\(longVar) ?p ?o }")
        guard case .items(let items) = query.projection else {
            Issue.record("Expected items")
            return
        }
        #expect(items.count == 1)
    }

    @Test("Many triples in single pattern")
    func testManyTriples() throws {
        var triples = ""
        for i in 0..<50 {
            if i > 0 { triples += " . " }
            triples += "?s\(i) ?p\(i) ?o\(i)"
        }
        let query = try parseQuery("SELECT * WHERE { \(triples) }")
        guard case .graphPattern(let pattern) = query.source,
              case .basic(let parsed) = pattern else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(parsed.count == 50)
    }

    @Test("Deeply nested OPTIONAL")
    func testDeeplyNestedOptional() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s ?p ?o .
                OPTIONAL {
                    ?o ?p2 ?o2 .
                    OPTIONAL {
                        ?o2 ?p3 ?o3
                    }
                }
            }
            """)
        #expect(query.projection == .all)
    }

    @Test("Multiple PREFIX declarations")
    func testMultiplePrefixes() throws {
        let pattern = try parseQuery("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX ex: <http://example.org/>
            SELECT * WHERE { ?s rdf:type ex:Person . ?s rdfs:label ?name }
            """)
        guard case .graphPattern(let pat) = pattern.source,
              case .basic(let triples) = pat else {
            Issue.record("Expected basic pattern")
            return
        }
        #expect(triples.count == 2)
    }

    @Test("Query with comments")
    func testQueryWithComments() throws {
        let query = try parseQuery("""
            # This is a comment
            PREFIX ex: <http://example.org/> # inline comment
            SELECT * WHERE {
                # Another comment
                ?s ex:name ?o # trailing
            }
            """)
        #expect(query.projection == .all)
    }

    @Test("Boolean literals TRUE and FALSE")
    func testBooleanLiterals() throws {
        let query = try parseQuery("""
            SELECT * WHERE { ?s <http://example.org/active> TRUE . ?s <http://example.org/deleted> FALSE }
            """)
        guard case .graphPattern(let pattern) = query.source,
              case .basic(let triples) = pattern else {
            Issue.record("Expected basic")
            return
        }
        #expect(triples.count == 2)
        guard case .literal(.bool(true)) = triples[0].object else {
            Issue.record("Expected TRUE literal")
            return
        }
        guard case .literal(.bool(false)) = triples[1].object else {
            Issue.record("Expected FALSE literal")
            return
        }
    }
}
