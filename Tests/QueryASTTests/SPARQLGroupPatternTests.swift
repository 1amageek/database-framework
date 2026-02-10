/// SPARQLGroupPatternTests.swift
/// Comprehensive tests for SPARQL 1.1 GroupGraphPattern parsing
/// Validates rules [54]-[67] per W3C SPARQL 1.1 specification

import Testing
@testable import QueryAST

// MARK: - Helper

/// Parse a SPARQL SELECT query and extract the GraphPattern from the WHERE clause
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

/// Extract triples from a .basic pattern
private func extractTriples(_ pattern: GraphPattern) -> [TriplePattern]? {
    if case .basic(let triples) = pattern { return triples }
    return nil
}

// MARK: - Group 1: Bug Reproduction Tests

@Suite("SPARQL GroupPattern - Bug Reproduction", .serialized)
struct SPARQLGroupPatternBugTests {

    init() {
        SPARQLParser.enableDebug(false)
    }

    @Test("Optional dot Optional — rule [55] dot consumption")
    func testOptionalDotOptional() throws {
        // Bug #1: Dot after OPTIONAL was not consumed, causing parse error on second OPTIONAL
        let sparql = """
            SELECT * WHERE {
                ?x <http://example.org/foo> ?y .
                OPTIONAL { ?x <http://example.org/name> ?name } .
                OPTIONAL { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: optional(optional(basic, name-block), type-block)
        guard case .optional(let inner, let typeOpt) = pattern else {
            Issue.record("Expected outer .optional, got: \(pattern)")
            return
        }
        guard case .optional(let base, let nameOpt) = inner else {
            Issue.record("Expected inner .optional, got: \(inner)")
            return
        }
        // base should be the triples block
        #expect(extractTriples(base) != nil)
        #expect(extractTriples(nameOpt) != nil)
        #expect(extractTriples(typeOpt) != nil)
    }

    @Test("FILTER dot Triples — dot after FILTER consumed")
    func testFilterDotTriples() throws {
        // Bug #1: Dot after FILTER was not consumed
        let sparql = """
            SELECT * WHERE {
                ?x ?p ?o .
                FILTER(?o = "test") .
                ?x <http://example.org/a> ?a
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: join(filter(basic, expr), basic)
        guard case .join(let filtered, let trailing) = pattern else {
            Issue.record("Expected .join, got: \(pattern)")
            return
        }
        guard case .filter(let base, _) = filtered else {
            Issue.record("Expected .filter, got: \(filtered)")
            return
        }
        #expect(extractTriples(base) != nil)
        #expect(extractTriples(trailing) != nil)
    }

    @Test("UNION dot Triples — dot after UNION consumed")
    func testUnionDotTriples() throws {
        // Bug #1 + #2: UNION handling + dot consumption
        let sparql = """
            SELECT * WHERE {
                { ?x <http://example.org/a> ?a } UNION { ?x <http://example.org/b> ?b } .
                ?x <http://example.org/c> ?c
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: join(union(basic, basic), basic)
        guard case .join(let unionPat, let trailing) = pattern else {
            Issue.record("Expected .join, got: \(pattern)")
            return
        }
        guard case .union(_, _) = unionPat else {
            Issue.record("Expected .union, got: \(unionPat)")
            return
        }
        #expect(extractTriples(trailing) != nil)
    }

    @Test("No dot still works — dot is optional per [55]")
    func testNoDotStillWorks() throws {
        // Bug #1: Verify that omitting dot doesn't break parsing
        let sparql = """
            SELECT * WHERE {
                ?x ?p ?o
                OPTIONAL { ?x <http://example.org/a> ?a }
                OPTIONAL { ?x <http://example.org/b> ?b }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: optional(optional(basic, a-block), b-block)
        guard case .optional(let inner, _) = pattern else {
            Issue.record("Expected outer .optional, got: \(pattern)")
            return
        }
        guard case .optional(let base, _) = inner else {
            Issue.record("Expected inner .optional, got: \(inner)")
            return
        }
        #expect(extractTriples(base) != nil)
    }
}

// MARK: - Group 2: TriplesBlock Recursion Tests

@Suite("SPARQL GroupPattern - TriplesBlock", .serialized)
struct SPARQLTriplesBlockTests {

    init() {
        SPARQLParser.enableDebug(false)
    }

    @Test("Multiple subjects separated by dots — rule [56]")
    func testMultipleSubjects() throws {
        // Bug #3: parseTriplesBlock must handle multiple TriplesSameSubjectPath
        let sparql = "SELECT * WHERE { ?s ?p ?o . ?x ?y ?z }"
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        #expect(triples.count == 2)
        #expect(triples[0].subject == .variable("s"))
        #expect(triples[1].subject == .variable("x"))
    }

    @Test("Three subjects in one TriplesBlock")
    func testThreeSubjects() throws {
        let sparql = "SELECT * WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i }"
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        #expect(triples.count == 3)
        #expect(triples[0].subject == .variable("a"))
        #expect(triples[1].subject == .variable("d"))
        #expect(triples[2].subject == .variable("g"))
    }

    @Test("Trailing dot after triples — rule [56]")
    func testTrailingDot() throws {
        // Trailing dot: consumed by parseTriplesBlock, then canStartTriple() returns false → break
        let sparql = "SELECT * WHERE { ?s ?p ?o . }"
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        #expect(triples.count == 1)
    }

    @Test("Trailing semicolon in predicate-object list")
    func testTrailingSemicolon() throws {
        let sparql = """
            SELECT * WHERE {
                ?s <http://example.org/a> ?o1 ;
                   <http://example.org/b> ?o2 ;
            }
            """
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        // Same subject, two predicate-object pairs
        #expect(triples.count == 2)
        #expect(triples[0].subject == triples[1].subject)
        #expect(triples[0].predicate != triples[1].predicate)
    }

    @Test("Comma in object list — multiple objects for same predicate")
    func testCommaInObjectList() throws {
        let sparql = "SELECT * WHERE { ?s <http://example.org/p> ?o1 , ?o2 , ?o3 }"
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        // Same subject & predicate, three objects
        #expect(triples.count == 3)
        #expect(triples[0].subject == triples[1].subject)
        #expect(triples[0].predicate == triples[1].predicate)
        #expect(triples[0].object != triples[1].object)
    }

    @Test("Semicolon and comma combined")
    func testSemicolonAndComma() throws {
        let sparql = """
            SELECT * WHERE {
                ?s <http://example.org/a> ?o1 , ?o2 ;
                   <http://example.org/b> ?o3
            }
            """
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        // 2 from first predicate (comma) + 1 from second predicate
        #expect(triples.count == 3)
        #expect(triples[0].subject == triples[2].subject) // same subject
    }
}

// MARK: - Group 3: UNION (GroupOrUnionGraphPattern [67])

@Suite("SPARQL GroupPattern - UNION", .serialized)
struct SPARQLUnionTests {

    init() {
        SPARQLParser.enableDebug(false)
    }

    @Test("Simple UNION — rule [67]")
    func testSimpleUnion() throws {
        let sparql = """
            SELECT * WHERE {
                { ?a ?p ?o } UNION { ?b ?p ?o }
            }
            """
        let pattern = try parsePattern(sparql)

        guard case .union(let left, let right) = pattern else {
            Issue.record("Expected .union, got: \(pattern)")
            return
        }
        #expect(extractTriples(left) != nil)
        #expect(extractTriples(right) != nil)
    }

    @Test("Three-way UNION — left-associative per [67]")
    func testThreeWayUnion() throws {
        let sparql = """
            SELECT * WHERE {
                { ?a ?p ?o } UNION { ?b ?p ?o } UNION { ?c ?p ?o }
            }
            """
        let pattern = try parsePattern(sparql)

        // Left-associative: union(union(a, b), c)
        guard case .union(let leftUnion, let cPat) = pattern else {
            Issue.record("Expected outer .union, got: \(pattern)")
            return
        }
        guard case .union(let aPat, let bPat) = leftUnion else {
            Issue.record("Expected inner .union, got: \(leftUnion)")
            return
        }
        #expect(extractTriples(aPat) != nil)
        #expect(extractTriples(bPat) != nil)
        #expect(extractTriples(cPat) != nil)
    }

    @Test("UNION then triples — join after union")
    func testUnionThenTriples() throws {
        let sparql = """
            SELECT * WHERE {
                { ?a ?p ?o } UNION { ?b ?p ?o } .
                ?c ?d ?e
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: join(union(a, b), basic(c))
        guard case .join(let unionPat, let triples) = pattern else {
            Issue.record("Expected .join, got: \(pattern)")
            return
        }
        guard case .union(_, _) = unionPat else {
            Issue.record("Expected .union, got: \(unionPat)")
            return
        }
        #expect(extractTriples(triples) != nil)
    }

    @Test("Triples before UNION — join before union")
    func testTriplesBeforeUnion() throws {
        let sparql = """
            SELECT * WHERE {
                ?x ?p ?o .
                { ?a ?b ?c } UNION { ?d ?e ?f }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: join(basic(x), union(a, d))
        guard case .join(let triples, let unionPat) = pattern else {
            Issue.record("Expected .join, got: \(pattern)")
            return
        }
        #expect(extractTriples(triples) != nil)
        guard case .union(_, _) = unionPat else {
            Issue.record("Expected .union, got: \(unionPat)")
            return
        }
    }
}

// MARK: - Group 4: SubSelect Tests

@Suite("SPARQL GroupPattern - SubSelect", .serialized)
struct SPARQLSubSelectTests {

    init() {
        SPARQLParser.enableDebug(false)
    }

    @Test("SubSelect in GroupGraphPattern — rule [54]")
    func testSubSelect() throws {
        let sparql = """
            SELECT * WHERE {
                { SELECT ?x WHERE { ?x ?p ?o } }
            }
            """
        let pattern = try parsePattern(sparql)

        guard case .subquery(let subQuery) = pattern else {
            Issue.record("Expected .subquery, got: \(pattern)")
            return
        }
        #expect(subQuery.projection != .all)
    }

    @Test("SubSelect with LIMIT")
    func testSubSelectWithLimit() throws {
        let sparql = """
            SELECT * WHERE {
                { SELECT ?x WHERE { ?x ?p ?o } LIMIT 10 }
            }
            """
        let pattern = try parsePattern(sparql)

        guard case .subquery(let subQuery) = pattern else {
            Issue.record("Expected .subquery, got: \(pattern)")
            return
        }
        #expect(subQuery.limit == 10)
    }

    @Test("SubSelect joined with triples")
    func testSubSelectWithTriples() throws {
        let sparql = """
            SELECT * WHERE {
                ?s ?p ?o .
                { SELECT ?x WHERE { ?x <http://example.org/a> ?b } }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: join(basic, subquery)
        guard case .join(let triples, let sub) = pattern else {
            Issue.record("Expected .join, got: \(pattern)")
            return
        }
        #expect(extractTriples(triples) != nil)
        guard case .subquery(_) = sub else {
            Issue.record("Expected .subquery, got: \(sub)")
            return
        }
    }
}

// MARK: - Group 5: Complex Combination Tests

@Suite("SPARQL GroupPattern - Complex Patterns", .serialized)
struct SPARQLComplexPatternTests {

    init() {
        SPARQLParser.enableDebug(false)
    }

    @Test("OPTIONAL then FILTER — chained modifiers")
    func testOptionalThenFilter() throws {
        let sparql = """
            SELECT * WHERE {
                ?s <http://example.org/name> ?n .
                OPTIONAL { ?s <http://example.org/age> ?a } .
                FILTER(?n != "x")
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: filter(optional(basic, basic), expr)
        guard case .filter(let optPat, _) = pattern else {
            Issue.record("Expected .filter, got: \(pattern)")
            return
        }
        guard case .optional(let base, let opt) = optPat else {
            Issue.record("Expected .optional, got: \(optPat)")
            return
        }
        #expect(extractTriples(base) != nil)
        #expect(extractTriples(opt) != nil)
    }

    @Test("Multiple FILTERs — stacked filters")
    func testMultipleFilters() throws {
        let sparql = """
            SELECT * WHERE {
                ?s ?p ?o .
                FILTER(?p != <http://example.org/x>) .
                FILTER(?o != "bad")
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: filter(filter(basic, expr1), expr2)
        guard case .filter(let inner, _) = pattern else {
            Issue.record("Expected outer .filter, got: \(pattern)")
            return
        }
        guard case .filter(let base, _) = inner else {
            Issue.record("Expected inner .filter, got: \(inner)")
            return
        }
        #expect(extractTriples(base) != nil)
    }

    @Test("BIND then triples — extend pattern")
    func testBindThenTriples() throws {
        let sparql = """
            SELECT * WHERE {
                BIND(1 AS ?x) .
                ?x ?p ?o
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: join(bind(basic([]), ...), basic([...]))
        guard case .join(let bindPat, let triples) = pattern else {
            Issue.record("Expected .join, got: \(pattern)")
            return
        }
        guard case .bind(_, variable: let varName, expression: _) = bindPat else {
            Issue.record("Expected .bind, got: \(bindPat)")
            return
        }
        #expect(varName == "x")
        #expect(extractTriples(triples) != nil)
    }

    @Test("MINUS pattern")
    func testMinus() throws {
        let sparql = """
            SELECT * WHERE {
                ?s ?p ?o .
                MINUS { ?s <http://example.org/type> <http://example.org/Bad> }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: minus(basic, basic)
        guard case .minus(let base, let excluded) = pattern else {
            Issue.record("Expected .minus, got: \(pattern)")
            return
        }
        #expect(extractTriples(base) != nil)
        #expect(extractTriples(excluded) != nil)
    }

    @Test("GRAPH named graph pattern")
    func testGraph() throws {
        let sparql = """
            SELECT * WHERE {
                GRAPH <http://example.org/g1> { ?s ?p ?o }
            }
            """
        let pattern = try parsePattern(sparql)

        guard case .graph(name: let name, pattern: let graphPat) = pattern else {
            Issue.record("Expected .graph, got: \(pattern)")
            return
        }
        #expect(name == .iri("http://example.org/g1"))
        #expect(extractTriples(graphPat) != nil)
    }

    @Test("Nested OPTIONAL — inner group with its own OPTIONAL")
    func testNestedOptional() throws {
        let sparql = """
            SELECT * WHERE {
                ?s ?p ?o .
                OPTIONAL {
                    ?s <http://example.org/a> ?a .
                    OPTIONAL { ?a <http://example.org/b> ?b }
                }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: optional(basic, optional(basic, basic))
        guard case .optional(let base, let outerOpt) = pattern else {
            Issue.record("Expected outer .optional, got: \(pattern)")
            return
        }
        #expect(extractTriples(base) != nil)
        guard case .optional(let innerBase, let innerOpt) = outerOpt else {
            Issue.record("Expected inner .optional, got: \(outerOpt)")
            return
        }
        #expect(extractTriples(innerBase) != nil)
        #expect(extractTriples(innerOpt) != nil)
    }

    @Test("Empty group graph pattern — rule [55]")
    func testEmptyGroup() throws {
        let sparql = "SELECT * WHERE { }"
        let pattern = try parsePattern(sparql)

        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic([]), got: \(pattern)")
            return
        }
        #expect(triples.isEmpty)
    }

    @Test("OPTIONAL without preceding triples")
    func testOptionalWithoutPrecedingTriples() throws {
        let sparql = """
            SELECT * WHERE {
                OPTIONAL { ?s <http://example.org/a> ?a }
            }
            """
        let pattern = try parsePattern(sparql)

        guard case .optional(let base, let opt) = pattern else {
            Issue.record("Expected .optional, got: \(pattern)")
            return
        }
        // base should be empty basic
        guard case .basic(let triples) = base else {
            Issue.record("Expected .basic for base, got: \(base)")
            return
        }
        #expect(triples.isEmpty)
        #expect(extractTriples(opt) != nil)
    }

    @Test("Multiple GraphPatternNotTriples with dots between them")
    func testMultipleGraphPatternNotTriplesWithDots() throws {
        let sparql = """
            SELECT * WHERE {
                ?s ?p ?o .
                OPTIONAL { ?s <http://example.org/a> ?a } .
                FILTER(?o != "bad") .
                MINUS { ?s <http://example.org/type> <http://example.org/Hidden> }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: minus(filter(optional(basic, basic), expr), basic)
        guard case .minus(let filtered, _) = pattern else {
            Issue.record("Expected .minus at top, got: \(pattern)")
            return
        }
        guard case .filter(let optPat, _) = filtered else {
            Issue.record("Expected .filter, got: \(filtered)")
            return
        }
        guard case .optional(_, _) = optPat else {
            Issue.record("Expected .optional, got: \(optPat)")
            return
        }
    }

    @Test("Triples between two OPTIONALs without dots")
    func testTriplesBetweenOptionals() throws {
        let sparql = """
            SELECT * WHERE {
                ?s ?p ?o
                OPTIONAL { ?s <http://example.org/a> ?a }
                ?x ?y ?z
                OPTIONAL { ?x <http://example.org/b> ?b }
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: optional(join(optional(basic(s), basic(a)), basic(x)), basic(b))
        guard case .optional(let inner, _) = pattern else {
            Issue.record("Expected outer .optional, got: \(pattern)")
            return
        }
        guard case .join(let optInner, let midTriples) = inner else {
            Issue.record("Expected .join, got: \(inner)")
            return
        }
        guard case .optional(_, _) = optInner else {
            Issue.record("Expected inner .optional, got: \(optInner)")
            return
        }
        #expect(extractTriples(midTriples) != nil)
    }
}

// MARK: - Group 6: Edge Cases and Regression Guards

@Suite("SPARQL GroupPattern - Edge Cases", .serialized)
struct SPARQLEdgeCaseTests {

    init() {
        SPARQLParser.enableDebug(false)
    }

    @Test("Single triple without dot")
    func testSingleTripleNoDot() throws {
        let sparql = "SELECT * WHERE { ?s ?p ?o }"
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        #expect(triples.count == 1)
    }

    @Test("rdf:type shorthand 'a' in triples")
    func testRdfTypeShorthand() throws {
        let sparql = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT * WHERE { ?s a <http://example.org/Person> }
            """
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        #expect(triples.count == 1)
        // Parser stores 'a' as prefixedName — full IRI expansion happens at execution time
        #expect(triples[0].predicate == .prefixedName(prefix: "rdf", local: "type"))
    }

    @Test("UNION without surrounding triples")
    func testUnionOnly() throws {
        let sparql = """
            SELECT * WHERE {
                { ?a <http://example.org/p> ?b }
                UNION
                { ?c <http://example.org/q> ?d }
            }
            """
        let pattern = try parsePattern(sparql)

        guard case .union(_, _) = pattern else {
            Issue.record("Expected .union, got: \(pattern)")
            return
        }
    }

    @Test("FILTER in parentheses — constraint expression")
    func testFilterParenthesized() throws {
        let sparql = """
            SELECT * WHERE {
                ?s ?p ?o
                FILTER (?o > 10)
            }
            """
        let pattern = try parsePattern(sparql)

        guard case .filter(let base, _) = pattern else {
            Issue.record("Expected .filter, got: \(pattern)")
            return
        }
        #expect(extractTriples(base) != nil)
    }

    @Test("Dot only between triples, not after last triple")
    func testDotOnlyBetweenTriples() throws {
        // Common pattern: dot as separator, not terminator
        let sparql = """
            SELECT * WHERE {
                ?a ?b ?c .
                ?d ?e ?f .
                ?g ?h ?i
            }
            """
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        #expect(triples.count == 3)
    }

    @Test("PREFIX usage in triples")
    func testPrefixInTriples() throws {
        let sparql = """
            PREFIX ex: <http://example.org/>
            SELECT * WHERE {
                ?s ex:name ?name .
                ?s ex:age ?age
            }
            """
        let pattern = try parsePattern(sparql)

        guard let triples = extractTriples(pattern) else {
            Issue.record("Expected .basic, got: \(pattern)")
            return
        }
        #expect(triples.count == 2)
    }

    @Test("Complex real-world query — multiple patterns mixed")
    func testComplexRealWorldQuery() throws {
        let sparql = """
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT ?name ?email WHERE {
                ?person rdf:type foaf:Person .
                ?person foaf:name ?name .
                OPTIONAL { ?person foaf:mbox ?email } .
                FILTER(LANG(?name) = "en")
            }
            """
        let pattern = try parsePattern(sparql)

        // Structure: filter(optional(basic([2 triples]), basic([1 triple])), expr)
        guard case .filter(let optPat, _) = pattern else {
            Issue.record("Expected .filter, got: \(pattern)")
            return
        }
        guard case .optional(let base, let opt) = optPat else {
            Issue.record("Expected .optional, got: \(optPat)")
            return
        }
        guard let baseTriples = extractTriples(base) else {
            Issue.record("Expected base .basic, got: \(base)")
            return
        }
        #expect(baseTriples.count == 2)
        #expect(extractTriples(opt) != nil)
    }
}
