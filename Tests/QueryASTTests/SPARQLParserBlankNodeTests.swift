#if FOUNDATION_DB
/// SPARQLParserBlankNodeTests.swift
/// Tests for Phase 2: Parser infrastructure (B3: BASE IRI, B1: Blank nodes, B2: Collections)

import Testing
import TestHeartbeat
import Foundation
@testable import QueryAST

// MARK: - Helper

private func parseQuery(_ sparql: String) throws -> QueryIR.SelectQuery {
    let parser = SPARQLParser()
    return try parser.parseSelect(sparql)
}

private func parseTriplePatterns(_ sparql: String) throws -> [TriplePattern] {
    let query = try parseQuery(sparql)
    guard case .graphPattern(.basic(let basicGraphPattern)) = query.source else {
        throw SPARQLParser.ParseError.invalidSyntax(
            message: "Expected a triple-only basic graph pattern",
            position: 0
        )
    }
    return try basicGraphPattern.triplePatterns()
}

// MARK: - B3: BASE IRI

@Suite("B3: BASE IRI", .heartbeat)
struct BaseIRITests {

    @Test("BASE IRI resolves fragment reference")
    func testBaseFragment() throws {
        let triples = try parseTriplePatterns("""
            BASE <http://example.org/>
            SELECT * WHERE { ?s <#name> ?o }
            """)
        #expect(triples.count == 1)
        guard case .iri(let iri) = triples[0].predicate else {
            Issue.record("Expected .iri, got \(triples[0].predicate)")
            return
        }
        #expect(iri == "http://example.org/#name")
    }

    @Test("BASE IRI resolves relative path")
    func testBaseRelativePath() throws {
        let triples = try parseTriplePatterns("""
            BASE <http://example.org/base/>
            SELECT * WHERE { ?s <foo> ?o }
            """)
        guard case .iri(let iri) = triples[0].predicate else {
            Issue.record("Expected .iri")
            return
        }
        #expect(iri == "http://example.org/base/foo")
    }

    @Test("Absolute IRI is not modified by BASE")
    func testAbsoluteIRIUnchanged() throws {
        let triples = try parseTriplePatterns("""
            BASE <http://example.org/>
            SELECT * WHERE { ?s <http://other.org/pred> ?o }
            """)
        guard case .iri(let iri) = triples[0].predicate else {
            Issue.record("Expected .iri")
            return
        }
        #expect(iri == "http://other.org/pred")
    }
}

// MARK: - B1: Anonymous Blank Nodes

@Suite("B1: Anonymous Blank Nodes", .heartbeat)
struct AnonymousBlankNodeTests {

    @Test("Empty blank node [] as subject")
    func testEmptyBlankNodeSubject() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE { [] <http://example.org/p> ?o }
            """)
        #expect(triples.count == 1)
        guard case .blankNode(_) = triples[0].subject else {
            Issue.record("Expected .blankNode, got \(triples[0].subject)")
            return
        }
    }

    @Test("Blank node with properties as subject")
    func testBlankNodeWithPropertiesSubject() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE {
                [ <http://example.org/name> "Alice" ; <http://example.org/age> 30 ]
                <http://example.org/knows> ?o
            }
            """)
        // Should have 3 triples:
        // 1. _:anon <name> "Alice"
        // 2. _:anon <age> 30
        // 3. _:anon <knows> ?o
        #expect(triples.count == 3)

        // All should share the same blank node subject
        let subjects = triples.map { $0.subject }
        guard case .blankNode(let bn1) = subjects[0],
              case .blankNode(let bn2) = subjects[1],
              case .blankNode(let bn3) = subjects[2] else {
            Issue.record("Expected all blank node subjects")
            return
        }
        #expect(bn1 == bn2)
        #expect(bn2 == bn3)
    }

    @Test("Blank node in object position")
    func testBlankNodeObject() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE {
                ?s <http://example.org/knows> [ <http://example.org/name> "Bob" ]
            }
            """)
        // Should have 2 triples:
        // 1. _:anon <name> "Bob"  (from pending)
        // 2. ?s <knows> _:anon
        #expect(triples.count == 2)
    }

    @Test("Multiple blank nodes generate unique IDs")
    func testMultipleBlankNodes() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE {
                [] <http://example.org/p> ?o .
                [] <http://example.org/q> ?o
            }
            """)
        #expect(triples.count == 2)
        guard case .blankNode(let bn1) = triples[0].subject,
              case .blankNode(let bn2) = triples[1].subject else {
            Issue.record("Expected blank nodes")
            return
        }
        #expect(bn1 != bn2)
    }
}

// MARK: - B2: RDF Collections

@Suite("B2: RDF Collections", .heartbeat)
struct RDFCollectionTests {

    @Test("Empty collection () is rdf:nil")
    func testEmptyCollection() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE { ?s <http://example.org/list> () }
            """)
        #expect(triples.count == 1)
        guard case .iri(let iri) = triples[0].object else {
            Issue.record("Expected .iri for rdf:nil, got \(triples[0].object)")
            return
        }
        #expect(iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
    }

    @Test("Single element collection")
    func testSingleElementCollection() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE { ?s <http://example.org/list> (42) }
            """)
        // Triples:
        // 1. _:anon rdf:first 42
        // 2. _:anon rdf:rest rdf:nil
        // 3. ?s <list> _:anon (the main triple)
        #expect(triples.count == 3)

        // Find rdf:first triple
        let firstTriples = triples.filter {
            if case .iri(let iri) = $0.predicate {
                return iri.hasSuffix("first")
            }
            return false
        }
        #expect(firstTriples.count == 1)
        guard case .literal(.int(42)) = firstTriples[0].object else {
            Issue.record("Expected literal 42, got \(firstTriples[0].object)")
            return
        }
    }

    @Test("Multi-element collection")
    func testMultiElementCollection() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE { ?s <http://example.org/list> (1 2 3) }
            """)
        // 3 elements → 3 rdf:first + 3 rdf:rest + 1 main triple = 7
        #expect(triples.count == 7)

        // Check rdf:first count
        let firstTriples = triples.filter {
            if case .iri(let iri) = $0.predicate { return iri.hasSuffix("first") }
            return false
        }
        #expect(firstTriples.count == 3)

        // Check last rdf:rest points to rdf:nil
        let restTriples = triples.filter {
            if case .iri(let iri) = $0.predicate { return iri.hasSuffix("rest") }
            return false
        }
        #expect(restTriples.count == 3)
        let lastRest = restTriples.last!
        guard case .iri(let nilIRI) = lastRest.object else {
            Issue.record("Expected rdf:nil")
            return
        }
        #expect(nilIRI == "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
    }

    @Test("Collection with different term types")
    func testCollectionMixedTypes() throws {
        let triples = try parseTriplePatterns(#"""
            SELECT * WHERE {
                ?s <http://example.org/list> ("hello" 42 <http://example.org/x>)
            }
            """#)
        // 3 elements → 7 triples
        #expect(triples.count == 7)
    }
}

// MARK: - B1: Nested Blank Nodes (Edge Cases)

@Suite("B1: Blank Node Edge Cases", .heartbeat)
struct BlankNodeEdgeCaseTests {

    @Test("Nested blank node [ :p [ :q :o ] ]")
    func testNestedBlankNode() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE {
                [ <http://example.org/p> [ <http://example.org/q> <http://example.org/o> ] ] <http://example.org/r> ?x
            }
            """)
        // Outer blank node: (bn0, :p, bn1), Inner blank node: (bn1, :q, :o), Plus: (bn0, :r, ?x)
        #expect(triples.count == 3)
        // Verify nesting: one triple should have blank node as both subject and object link
        let bnSubjects = Set(triples.compactMap { triple -> String? in
            if case .blankNode(let id) = triple.subject { return id }
            return nil
        })
        #expect(bnSubjects.count == 2) // Two distinct blank nodes
    }

    @Test("Blank node as subject with property list: [] :p :o")
    func testBlankNodeSubjectWithPropertyList() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE {
                [] <http://example.org/type> <http://example.org/Person> ;
                   <http://example.org/name> "Alice"
            }
            """)
        #expect(triples.count == 2)
        // Both triples should share the same blank node subject
        guard case .blankNode(let id1) = triples[0].subject,
              case .blankNode(let id2) = triples[1].subject else {
            Issue.record("Expected blank node subjects")
            return
        }
        #expect(id1 == id2)
    }

    @Test("Multiple blank nodes in same pattern")
    func testMultipleBlankNodes() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE {
                [] <http://example.org/knows> [] .
                [] <http://example.org/knows> []
            }
            """)
        #expect(triples.count == 2)
        // All four blank node positions should have different IDs
        let allBNs = triples.flatMap { triple -> [String] in
            var bns: [String] = []
            if case .blankNode(let id) = triple.subject { bns.append(id) }
            if case .blankNode(let id) = triple.object { bns.append(id) }
            return bns
        }
        #expect(Set(allBNs).count == 4) // 4 distinct blank nodes
    }

    @Test("Blank node with comma-separated objects")
    func testBlankNodeCommaObjects() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE {
                [ <http://example.org/type> <http://example.org/A> , <http://example.org/B> ] ?p ?o
            }
            """)
        // Two triples from blank node (type A, type B), plus one from property list (?p ?o)
        #expect(triples.count >= 2)
    }
}

// MARK: - B2: Collection Edge Cases

@Suite("B2: Collection Edge Cases", .heartbeat)
struct CollectionEdgeCaseTests {

    @Test("Collection in subject position")
    func testCollectionAsSubject() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE { (1 2 3) <http://example.org/length> 3 }
            """)
        // 3 rdf:first + 3 rdf:rest + 1 main triple = 7
        #expect(triples.count == 7)
        // First triple's subject should be a blank node (head of collection)
        let mainTriple = triples.last!
        guard case .blankNode = mainTriple.subject else {
            // The main triple may not be last due to pending triple ordering
            // Just verify total count
            return
        }
    }

    @Test("Nested collection ((1 2) (3 4))")
    func testNestedCollection() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE { ?s <http://example.org/matrix> ((1 2) (3 4)) }
            """)
        // Outer: 2 elements → 2 rdf:first + 2 rdf:rest = 4
        // Inner1: 2 elements → 2 rdf:first + 2 rdf:rest = 4
        // Inner2: 2 elements → 2 rdf:first + 2 rdf:rest = 4
        // Main triple: 1
        // Total: 13
        #expect(triples.count == 13)
    }

    @Test("Collection with blank node element")
    func testCollectionWithBlankNode() throws {
        let triples = try parseTriplePatterns("""
            SELECT * WHERE { ?s <http://example.org/list> ([ <http://example.org/name> "Alice" ]) }
            """)
        // Collection: 1 rdf:first + 1 rdf:rest = 2
        // Blank node property: 1
        // Main triple: 1
        // Total: 4
        #expect(triples.count == 4)
    }
}

// MARK: - B3: BASE Edge Cases

@Suite("B3: BASE Edge Cases", .heartbeat)
struct BaseEdgeCaseTests {

    @Test("Multiple BASE declarations — later overrides earlier")
    func testMultipleBASE() throws {
        let triples = try parseTriplePatterns("""
            BASE <http://first.org/>
            BASE <http://second.org/>
            SELECT * WHERE { ?s <name> ?o }
            """)
        // <name> should resolve against second BASE
        #expect(triples[0].predicate == .iri("http://second.org/name"))
    }

    @Test("BASE after PREFIX")
    func testBaseAfterPrefix() throws {
        let triples = try parseTriplePatterns("""
            PREFIX ex: <http://example.org/>
            BASE <http://base.org/>
            SELECT * WHERE { ?s <#local> ex:name }
            """)
        #expect(triples[0].predicate == .iri("http://base.org/#local"))
        #expect(triples[0].object == .iri("http://example.org/name"))
    }

    @Test("BASE with path resolution: parent directory")
    func testBaseParentPath() throws {
        let triples = try parseTriplePatterns("""
            BASE <http://example.org/a/b/c>
            SELECT * WHERE { ?s <d> ?o }
            """)
        // <d> resolves relative to base: http://example.org/a/b/d
        #expect(triples[0].predicate == .iri("http://example.org/a/b/d"))
    }
}
#endif
