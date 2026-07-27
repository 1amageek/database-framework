#if FOUNDATION_DB
/// SPARQLNotInAndPropertyPathTests.swift
/// Tests for SPARQL NOT IN operator and Property Path parsing

import Testing
import TestHeartbeat
import Foundation
import DatabaseTypes
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

private func parseExpression(
    _ sparql: String
) throws -> DatabaseKit.Expression {
    let parser = SPARQLParser()
    let query = try parser.parseSelect(sparql)
    guard case .graphPattern(let pattern) = query.source else {
        throw SPARQLParser.ParseError.invalidSyntax(
            message: "Expected graphPattern source", position: 0
        )
    }
    // Extract FILTER expression from pattern
    func findFilter(
        _ p: GraphPattern
    ) -> DatabaseKit.Expression? {
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

private enum GraphPatternInspectionError: Error {
    case expectedBasicGraphPattern(GraphPattern)
    case expectedSinglePropertyPath(BasicGraphPattern)
    case expectedSingleTriple(BasicGraphPattern)
}

private func requireBasicGraphPattern(
    _ pattern: GraphPattern
) throws -> BasicGraphPattern {
    guard case .basic(let basicGraphPattern) = pattern else {
        throw GraphPatternInspectionError.expectedBasicGraphPattern(pattern)
    }
    return basicGraphPattern
}

private func requireSinglePropertyPath(
    _ pattern: GraphPattern
) throws -> SPARQLPropertyPathPattern {
    let basicGraphPattern = try requireBasicGraphPattern(pattern)
    guard basicGraphPattern.elements.count == 1,
          case .propertyPath(let propertyPath) = basicGraphPattern.elements[0] else {
        throw GraphPatternInspectionError.expectedSinglePropertyPath(basicGraphPattern)
    }
    return propertyPath
}

private func requireSingleTriple(
    _ pattern: GraphPattern
) throws -> TriplePattern {
    let basicGraphPattern = try requireBasicGraphPattern(pattern)
    guard basicGraphPattern.elements.count == 1,
          case .triple(let triple) = basicGraphPattern.elements[0] else {
        throw GraphPatternInspectionError.expectedSingleTriple(basicGraphPattern)
    }
    return triple
}

// MARK: - NOT IN Tests

@Suite("SPARQL NOT IN", .heartbeat)
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

}

// MARK: - Property Path Tests

@Suite("SPARQL Property Paths", .heartbeat)
struct SPARQLPropertyPathTests {

    @Test("ZeroOrMore: ?s rdfs:subClassOf* ?ancestor")
    func testZeroOrMore() throws {
        let pattern = try parsePattern("""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT * WHERE { ?s rdfs:subClassOf* ?ancestor }
            """)

        let propertyPath = try requireSinglePropertyPath(pattern)
        let s = propertyPath.subject
        let path = propertyPath.path
        let o = propertyPath.object
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
        #expect(iri.rawValue == "http://www.w3.org/2000/01/rdf-schema#subClassOf")
    }

    @Test("Sequence: ?s foaf:knows/foaf:name ?name")
    func testSequence() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:knows/foaf:name ?name }
            """)

        let path = try requireSinglePropertyPath(pattern).path
        guard case .sequence(let left, let right) = path else {
            Issue.record("Expected .sequence, got \(path)")
            return
        }
        guard case .iri(let l) = left, l.rawValue.hasSuffix("knows") else {
            Issue.record("Expected foaf:knows, got \(left)")
            return
        }
        guard case .iri(let r) = right, r.rawValue.hasSuffix("name") else {
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

        let path = try requireSinglePropertyPath(pattern).path
        guard case .inverse(let inner) = path else {
            Issue.record("Expected .inverse, got \(path)")
            return
        }
        guard case .iri(let iri) = inner, iri.rawValue.hasSuffix("knows") else {
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

        let path = try requireSinglePropertyPath(pattern).path
        guard case .alternative(let left, let right) = path else {
            Issue.record("Expected .alternative, got \(path)")
            return
        }
        guard case .iri(let l) = left, l.rawValue.hasSuffix("knows") else {
            Issue.record("Expected foaf:knows")
            return
        }
        guard case .iri(let r) = right, r.rawValue.hasSuffix("friendOf") else {
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

        let path = try requireSinglePropertyPath(pattern).path
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

        let path = try requireSinglePropertyPath(pattern).path
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

        let path = try requireSinglePropertyPath(pattern).path
        guard case .negatedPropertySet(let exclusions) = path else {
            Issue.record("Expected .negatedPropertySet, got \(path)")
            return
        }
        #expect(exclusions.forward?.count == 1)
        #expect(exclusions.forward?.first?.rawValue == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        #expect(exclusions.inverse == nil)
    }

    @Test("NegatedPropertySet with alternatives: ?s !(rdf:type|rdfs:label) ?val")
    func testNegatedPropertySetAlternatives() throws {
        let pattern = try parsePattern("""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT * WHERE { ?s !(rdf:type|rdfs:label) ?val }
            """)

        let path = try requireSinglePropertyPath(pattern).path
        guard case .negatedPropertySet(let exclusions) = path else {
            Issue.record("Expected .negatedPropertySet, got \(path)")
            return
        }
        #expect(exclusions.forward?.count == 2)
        #expect(exclusions.inverse == nil)
    }

    @Test("Direct forward negated predicate preserves direction")
    func testDirectForwardNegatedPredicate() throws {
        let pattern = try parsePattern("""
            PREFIX ex: <http://example.org/>
            SELECT * WHERE { ?s !ex:p ?o }
            """)

        let path = try requireSinglePropertyPath(pattern).path
        guard case .negatedPropertySet(let exclusions) = path else {
            Issue.record("Expected a negated property set")
            return
        }
        #expect(exclusions.forward?.map(\.rawValue) == ["http://example.org/p"])
        #expect(exclusions.inverse == nil)
    }

    @Test("Direct inverse negated predicate preserves direction")
    func testDirectInverseNegatedPredicate() throws {
        let pattern = try parsePattern("""
            PREFIX ex: <http://example.org/>
            SELECT * WHERE { ?s !^ex:p ?o }
            """)

        let path = try requireSinglePropertyPath(pattern).path
        guard case .negatedPropertySet(let exclusions) = path else {
            Issue.record("Expected a negated property set")
            return
        }
        #expect(exclusions.forward == nil)
        #expect(exclusions.inverse?.map(\.rawValue) == ["http://example.org/p"])
    }

    @Test("Mixed negated predicate set preserves forward and inverse members")
    func testMixedDirectionNegatedPredicateSet() throws {
        let pattern = try parsePattern("""
            PREFIX ex: <http://example.org/>
            SELECT * WHERE { ?s !(ex:p|^ex:q) ?o }
            """)

        let path = try requireSinglePropertyPath(pattern).path
        guard case .negatedPropertySet(let exclusions) = path else {
            Issue.record("Expected a negated property set")
            return
        }
        #expect(exclusions.forward?.map(\.rawValue) == ["http://example.org/p"])
        #expect(exclusions.inverse?.map(\.rawValue) == ["http://example.org/q"])
    }

    @Test("Property path rejects an undefined prefix")
    func testUndefinedPropertyPathPrefix() {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parsePattern("SELECT * WHERE { ?s missing:p+ ?o }")
        }
    }

    @Test("Property path rejects a relative IRI without a base")
    func testRelativePropertyPathIRIWithoutBase() {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parsePattern("SELECT * WHERE { ?s <relative/path>+ ?o }")
        }
    }

    @Test("Combined: Inverse + Sequence: ?s ^(foaf:knows/foaf:name) ?x")
    func testInverseSequence() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s ^(foaf:knows/foaf:name) ?x }
            """)

        let path = try requireSinglePropertyPath(pattern).path
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

        let triple = try requireSingleTriple(pattern)
        // 'a' resolves directly to the canonical RDF type IRI.
        #expect(triple.predicate == SPARQLTerm.rdfType)
    }

    @Test("Regression: simple IRI predicate still works")
    func testSimpleIRIPredicate() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:name ?name }
            """)

        _ = try requireSingleTriple(pattern)
    }

    @Test("Mixed: triple and property path in same subject")
    func testMixedTripleAndPath() throws {
        let pattern = try parsePattern("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT * WHERE { ?s foaf:name ?name ; foaf:knows+ ?friend }
            """)

        let basicGraphPattern = try requireBasicGraphPattern(pattern)
        var tripleCount = 0
        var propertyPathCount = 0
        for element in basicGraphPattern.elements {
            switch element {
            case .triple:
                tripleCount += 1
            case .propertyPath:
                propertyPathCount += 1
            }
        }
        #expect(tripleCount == 1)
        #expect(propertyPathCount == 1)
    }

    @Test("Path with 'a' as path primary: ?s a* ?type")
    func testAInPath() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s a* ?type }
            """)

        let path = try requireSinglePropertyPath(pattern).path
        guard case .zeroOrMore(let inner) = path else {
            Issue.record("Expected .zeroOrMore, got \(path)")
            return
        }
        guard case .iri(let iri) = inner else {
            Issue.record("Expected .iri, got \(inner)")
            return
        }
        #expect(iri.rawValue == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
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

        let basicGraphPattern = try requireBasicGraphPattern(pattern)
        var tripleCount = 0
        var propertyPathCount = 0
        for element in basicGraphPattern.elements {
            switch element {
            case .triple:
                tripleCount += 1
            case .propertyPath:
                propertyPathCount += 1
            }
        }
        #expect(tripleCount == 1)
        #expect(propertyPathCount == 1)
    }
}
#endif
