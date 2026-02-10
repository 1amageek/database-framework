/// SPARQLNotInAndPropertyPathTests.swift
/// Tests for SPARQL NOT IN operator and Property Path parsing

import Testing
import Foundation
@testable import QueryAST

// MARK: - Helper

private func parsePattern(_ sparql: String) throws -> GraphPattern {
    let parser = SPARQLParser()
    let query = try parser.parseSelect(sparql)
    guard case .graphPattern(let pattern) = query.source else {
        throw SPARQLParser.ParseError.invalidSyntax(
            message: "Expected graphPattern source",
            position: 0
        )
    }
    return pattern
}

private func parseExpression(_ sparql: String) throws -> QueryIR.Expression {
    let parser = SPARQLParser()
    let query = try parser.parseSelect(sparql)
    guard case .graphPattern(let pattern) = query.source else {
        throw SPARQLParser.ParseError.invalidSyntax(
            message: "Expected graphPattern source", position: 0
        )
    }
    // Extract FILTER expression from pattern
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

// MARK: - NOT IN Tests

@Suite("SPARQL NOT IN")
struct SPARQLNotInTests {

    @Test("Basic NOT IN with IRI")
    func testNotInWithIRI() throws {
        let expr = try parseExpression("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT * WHERE { ?s ?p ?o . FILTER (?p NOT IN (rdf:type)) }
            """)

        guard case .notInList(let inner, let values) = expr else {
            Issue.record("Expected .notInList, got \(expr)")
            return
        }
        guard case .variable(let v) = inner else {
            Issue.record("Expected variable inner")
            return
        }
        #expect(v.name == "p")
        #expect(values.count == 1)
    }

    @Test("NOT IN with multiple integer values")
    func testNotInMultipleValues() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (?o NOT IN (1, 2, 3)) }
            """)

        guard case .notInList(_, let values) = expr else {
            Issue.record("Expected .notInList")
            return
        }
        #expect(values.count == 3)
    }

    @Test("NOT IN with empty list")
    func testNotInEmptyList() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (?o NOT IN ()) }
            """)

        guard case .notInList(_, let values) = expr else {
            Issue.record("Expected .notInList")
            return
        }
        #expect(values.isEmpty)
    }

    @Test("IN and NOT IN combined")
    func testInAndNotInCombined() throws {
        let expr = try parseExpression("""
            SELECT * WHERE { ?s ?p ?o . FILTER (?p IN (1) && ?o NOT IN (2)) }
            """)

        guard case .and(let lhs, let rhs) = expr else {
            Issue.record("Expected .and, got \(expr)")
            return
        }
        guard case .inList = lhs else {
            Issue.record("Expected .inList for lhs")
            return
        }
        guard case .notInList = rhs else {
            Issue.record("Expected .notInList for rhs")
            return
        }
    }

    @Test("NOT EXISTS regression (must not be broken)")
    func testNotExistsRegression() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE {
                ?s ?p ?o .
                FILTER NOT EXISTS { ?s <http://example.org/deleted> ?any }
            }
            """)

        // Should parse without error — NOT EXISTS should still work
        func hasFilter(_ p: GraphPattern) -> Bool {
            switch p {
            case .filter: return true
            case .join(let l, let r): return hasFilter(l) || hasFilter(r)
            default: return false
            }
        }
        #expect(hasFilter(pattern))
    }

    @Test("NOT IN SPARQL serialization round-trip")
    func testNotInSerialization() throws {
        let expr = Expression.notInList(
            .variable(Variable("x")),
            values: [.literal(.int(1)), .literal(.int(2))]
        )
        let sparql = expr.toSPARQL(prefixes: [:])
        #expect(sparql.contains("NOT IN"))
    }

    @Test("NOT IN SQL serialization")
    func testNotInSQLSerialization() throws {
        let expr = Expression.notInList(
            .column(ColumnRef(column: "status")),
            values: [.literal(.string("deleted")), .literal(.string("archived"))]
        )
        let sql = expr.toSQL()
        #expect(sql.contains("NOT IN"))
    }

    @Test("NOT IN Codable round-trip")
    func testNotInCodable() throws {
        let original = Expression.notInList(
            .variable(Variable("x")),
            values: [.literal(.int(1)), .literal(.string("test"))]
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(QueryIR.Expression.self, from: data)
        #expect(original == decoded)
    }
}

// MARK: - Property Path Tests

@Suite("SPARQL Property Paths")
struct SPARQLPropertyPathTests {

    @Test("ZeroOrMore: ?s rdfs:subClassOf* ?ancestor")
    func testZeroOrMore() throws {
        let pattern = try parsePattern("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT * WHERE { ?s rdfs:subClassOf* ?ancestor }
            """)

        guard case .propertyPath(let s, let path, let o) = pattern else {
            Issue.record("Expected .propertyPath, got \(pattern)")
            return
        }
        guard case .variable("s") = s else {
            Issue.record("Expected variable s")
            return
        }
        guard case .variable("ancestor") = o else {
            Issue.record("Expected variable ancestor")
            return
        }
        guard case .zeroOrMore(let inner) = path else {
            Issue.record("Expected .zeroOrMore, got \(path)")
            return
        }
        guard case .iri(let iri) = inner else {
            Issue.record("Expected .iri inner")
            return
        }
        #expect(iri == "http://www.w3.org/2000/01/rdf-schema#subClassOf")
    }

    @Test("Sequence: ?s foaf:knows/foaf:name ?name")
    func testSequence() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:knows/foaf:name ?name }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath, got \(pattern)")
            return
        }
        guard case .sequence(let left, let right) = path else {
            Issue.record("Expected .sequence, got \(path)")
            return
        }
        guard case .iri(let l) = left, l.hasSuffix("knows") else {
            Issue.record("Expected foaf:knows, got \(left)")
            return
        }
        guard case .iri(let r) = right, r.hasSuffix("name") else {
            Issue.record("Expected foaf:name, got \(right)")
            return
        }
    }

    @Test("Inverse: ?s ^foaf:knows ?follower")
    func testInverse() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s ^foaf:knows ?follower }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath, got \(pattern)")
            return
        }
        guard case .inverse(let inner) = path else {
            Issue.record("Expected .inverse, got \(path)")
            return
        }
        guard case .iri(let iri) = inner, iri.hasSuffix("knows") else {
            Issue.record("Expected foaf:knows, got \(inner)")
            return
        }
    }

    @Test("Alternative: ?s (foaf:knows|foaf:friendOf) ?person")
    func testAlternative() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s (foaf:knows|foaf:friendOf) ?person }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath, got \(pattern)")
            return
        }
        guard case .alternative(let left, let right) = path else {
            Issue.record("Expected .alternative, got \(path)")
            return
        }
        guard case .iri(let l) = left, l.hasSuffix("knows") else {
            Issue.record("Expected foaf:knows")
            return
        }
        guard case .iri(let r) = right, r.hasSuffix("friendOf") else {
            Issue.record("Expected foaf:friendOf")
            return
        }
    }

    @Test("OneOrMore: ?s foaf:knows+ ?friend")
    func testOneOrMore() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:knows+ ?friend }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath")
            return
        }
        guard case .oneOrMore(let inner) = path else {
            Issue.record("Expected .oneOrMore, got \(path)")
            return
        }
        guard case .iri = inner else {
            Issue.record("Expected .iri inner")
            return
        }
    }

    @Test("ZeroOrOne: ?s foaf:knows? ?friend")
    func testZeroOrOne() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:knows? ?friend }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath")
            return
        }
        guard case .zeroOrOne(let inner) = path else {
            Issue.record("Expected .zeroOrOne, got \(path)")
            return
        }
        guard case .iri = inner else {
            Issue.record("Expected .iri inner")
            return
        }
    }

    @Test("NegatedPropertySet: ?s !(rdf:type) ?val")
    func testNegatedPropertySet() throws {
        let pattern = try parsePattern("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT * WHERE { ?s !(rdf:type) ?val }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath, got \(pattern)")
            return
        }
        guard case .negation(let iris) = path else {
            Issue.record("Expected .negation, got \(path)")
            return
        }
        #expect(iris.count == 1)
        #expect(iris[0] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    }

    @Test("NegatedPropertySet with alternatives: ?s !(rdf:type|rdfs:label) ?val")
    func testNegatedPropertySetAlternatives() throws {
        let pattern = try parsePattern("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT * WHERE { ?s !(rdf:type|rdfs:label) ?val }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath")
            return
        }
        guard case .negation(let iris) = path else {
            Issue.record("Expected .negation, got \(path)")
            return
        }
        #expect(iris.count == 2)
    }

    @Test("Combined: Inverse + Sequence: ?s ^(foaf:knows/foaf:name) ?x")
    func testInverseSequence() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s ^(foaf:knows/foaf:name) ?x }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath, got \(pattern)")
            return
        }
        guard case .inverse(let inner) = path else {
            Issue.record("Expected .inverse, got \(path)")
            return
        }
        guard case .sequence = inner else {
            Issue.record("Expected .sequence inside inverse, got \(inner)")
            return
        }
    }

    @Test("Regression: simple 'a' predicate still works")
    func testSimpleAPredicate() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s a ?type }
            """)

        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        #expect(triples.count == 1)
        // 'a' → SPARQLTerm.rdfType which is .prefixedName(prefix: "rdf", local: "type")
        #expect(triples[0].predicate == SPARQLTerm.rdfType)
    }

    @Test("Regression: simple IRI predicate still works")
    func testSimpleIRIPredicate() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:name ?name }
            """)

        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        #expect(triples.count == 1)
    }

    @Test("Mixed: triple and property path in same subject")
    func testMixedTripleAndPath() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:name ?name ; foaf:knows+ ?friend }
            """)

        // Should contain both a basic triple and a propertyPath
        func hasPropertyPath(_ p: GraphPattern) -> Bool {
            switch p {
            case .propertyPath: return true
            case .join(let l, let r): return hasPropertyPath(l) || hasPropertyPath(r)
            default: return false
            }
        }
        func hasBasic(_ p: GraphPattern) -> Bool {
            switch p {
            case .basic: return true
            case .join(let l, let r): return hasBasic(l) || hasBasic(r)
            default: return false
            }
        }
        #expect(hasPropertyPath(pattern))
        #expect(hasBasic(pattern))
    }

    @Test("Path with 'a' as path primary: ?s a* ?type")
    func testAInPath() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s a* ?type }
            """)

        guard case .propertyPath(_, let path, _) = pattern else {
            Issue.record("Expected .propertyPath, got \(pattern)")
            return
        }
        guard case .zeroOrMore(let inner) = path else {
            Issue.record("Expected .zeroOrMore, got \(path)")
            return
        }
        guard case .iri(let iri) = inner else {
            Issue.record("Expected .iri, got \(inner)")
            return
        }
        #expect(iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    }

    @Test("Multiple triples block with path")
    func testMultipleTriplesWithPath() throws {
        let pattern = try parsePattern("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT * WHERE {
                ?class a rdfs:Class .
                ?class rdfs:subClassOf* ?parent
            }
            """)

        // Should successfully parse both a regular triple and a path pattern
        func countPatterns(_ p: GraphPattern) -> Int {
            switch p {
            case .basic(let t): return t.count
            case .propertyPath: return 1
            case .join(let l, let r): return countPatterns(l) + countPatterns(r)
            default: return 0
            }
        }
        #expect(countPatterns(pattern) >= 2)
    }
}
