/// SPARQLParser12Tests.swift
/// Tests for Phase 4: SPARQL 1.2 features (A4, A5, A6, A7, A8-A11)

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

private func parsePattern(_ sparql: String) throws -> GraphPattern {
    let query = try parseQuery(sparql)
    guard case .graphPattern(let pattern) = query.source else {
        throw SPARQLParser.ParseError.invalidSyntax(
            message: "Expected graphPattern source", position: 0
        )
    }
    return pattern
}

// MARK: - A7: VERSION Declaration

@Suite("A7: VERSION Declaration")
struct VersionDeclarationTests {

    @Test("VERSION 1.2 parses without error")
    func testVersion12() throws {
        let query = try parseQuery(#"""
            VERSION "1.2"
            SELECT * WHERE { ?s ?p ?o }
            """#)
        #expect(query.projection == .all)
    }

    @Test("VERSION with PREFIX")
    func testVersionWithPrefix() throws {
        let query = try parseQuery(#"""
            VERSION "1.2"
            PREFIX ex: <http://example.org/>
            SELECT * WHERE { ?s ex:name ?o }
            """#)
        #expect(query.projection == .all)
    }

    @Test("VERSION via parse() statement")
    func testVersionStatement() throws {
        let stmt = try parseStatement(#"""
            VERSION "1.2"
            SELECT * WHERE { ?s ?p ?o }
            """#)
        guard case .select = stmt else {
            Issue.record("Expected SELECT statement")
            return
        }
    }
}

// MARK: - A8-A11: SPARQL 1.2 Functions

@Suite("A8-A11: SPARQL 1.2 Functions")
struct SPARQL12FunctionTests {

    @Test("LANGDIR function parses")
    func testLangDir() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p ?o . FILTER (LANGDIR(?o) = "rtl") }
            """)
        guard case .filter(_, let expr) = pattern else {
            Issue.record("Expected filter pattern")
            return
        }
        // Should be equal(function("LANGDIR", [variable]), literal("rtl"))
        guard case .equal(let lhs, _) = expr else {
            Issue.record("Expected equality, got \\(expr)")
            return
        }
        guard case .function(let call) = lhs else {
            Issue.record("Expected function call, got \\(lhs)")
            return
        }
        #expect(call.name == "LANGDIR")
        #expect(call.arguments.count == 1)
    }

    @Test("hasLANG function parses")
    func testHasLang() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p ?o . FILTER (hasLANG(?o)) }
            """)
        guard case .filter(_, let expr) = pattern else {
            Issue.record("Expected filter pattern")
            return
        }
        guard case .function(let call) = expr else {
            Issue.record("Expected function call, got \\(expr)")
            return
        }
        #expect(call.name == "HASLANG")
        #expect(call.arguments.count == 1)
    }

    @Test("hasLANGDIR function parses")
    func testHasLangDir() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p ?o . FILTER (hasLANGDIR(?o)) }
            """)
        guard case .filter(_, let expr) = pattern else {
            Issue.record("Expected filter pattern")
            return
        }
        guard case .function(let call) = expr else {
            Issue.record("Expected function call, got \\(expr)")
            return
        }
        #expect(call.name == "HASLANGDIR")
        #expect(call.arguments.count == 1)
    }

    @Test("STRLANGDIR function parses with 3 args")
    func testStrLangDir() throws {
        let query = try parseQuery(#"""
            SELECT (STRLANGDIR("text", "ar", "rtl") AS ?lit) WHERE {}
            """#)
        guard case .items(let items) = query.projection else {
            Issue.record("Expected projection items, got \(query.projection)")
            return
        }
        #expect(items.count == 1)
        if let proj = items.first {
            #expect(proj.alias == "lit")
            guard case .function(let call) = proj.expression else {
                Issue.record("Expected function call")
                return
            }
            #expect(call.name == "STRLANGDIR")
            #expect(call.arguments.count == 3)
        }
    }
}

// MARK: - A4: Triple Terms <<( s p o )>>

@Suite("A4: Triple Terms")
struct TripleTermTests {

    @Test("Triple term <<( s p o )>> in object position")
    func testTripleTermObject() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p <<( <http://example.org/a> <http://example.org/b> 42 )>> }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \\(pattern)")
            return
        }
        #expect(triples.count == 1)
        guard case .quotedTriple(let s, let p, let o) = triples[0].object else {
            Issue.record("Expected quotedTriple, got \\(triples[0].object)")
            return
        }
        #expect(s == .iri("http://example.org/a"))
        #expect(p == .iri("http://example.org/b"))
        guard case .literal(.int(42)) = o else {
            Issue.record("Expected integer literal 42, got \\(o)")
            return
        }
    }

    @Test("Triple term <<( )>> is distinct from quoted << >>")
    func testTripleTermVsQuoted() throws {
        // Both should produce .quotedTriple
        let pattern1 = try parsePattern("""
            SELECT * WHERE { ?s ?p << ?a ?b ?c >> }
            """)
        let pattern2 = try parsePattern("""
            SELECT * WHERE { ?s ?p <<( ?a ?b ?c )>> }
            """)
        guard case .basic(let triples1) = pattern1,
              case .basic(let triples2) = pattern2 else {
            Issue.record("Expected .basic")
            return
        }
        #expect(triples1[0].object == triples2[0].object)
    }
}

// MARK: - A5: Reified Triples

@Suite("A5: Reified Triples")
struct ReifiedTripleTests {

    @Test("Reified triple << s p o ~?r >> as subject")
    func testReifiedTriple() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { << ?s ?p ?o ~?r >> <http://example.org/meta> "annotated" }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        #expect(triples.count == 1)
        guard case .reifiedTriple(let s, let p, let o, let r) = triples[0].subject else {
            Issue.record("Expected reifiedTriple subject, got \(triples[0].subject)")
            return
        }
        #expect(s == .variable("s"))
        #expect(p == .variable("p"))
        #expect(o == .variable("o"))
        #expect(r == .variable("r"))
        #expect(triples[0].predicate == .iri("http://example.org/meta"))
    }

    @Test("Reified triple in object position")
    func testReifiedTripleObject() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?x <http://example.org/ref> << ?a ?b ?c ~?r >> }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        #expect(triples.count == 1)
        guard case .reifiedTriple(let s, let p, let o, let r) = triples[0].object else {
            Issue.record("Expected reifiedTriple object, got \(triples[0].object)")
            return
        }
        #expect(s == .variable("a"))
        #expect(p == .variable("b"))
        #expect(o == .variable("c"))
        #expect(r == .variable("r"))
    }

    @Test("Reified triple in expression")
    func testReifiedTripleExpression() throws {
        let query = try parseQuery("""
            SELECT * WHERE { ?x ?y ?z . FILTER (ISTRIPLE(<< ?a ?b ?c ~?r >>)) }
            """)
        #expect(query.projection == .all)
    }
}

// MARK: - A6: Annotation Syntax

@Suite("A6: Annotation Syntax")
struct AnnotationTests {

    @Test("Simple annotation {| p o |}")
    func testSimpleAnnotation() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p ?o {| <http://example.org/confidence> 0.9 |} }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \\(pattern)")
            return
        }
        // Should have 2 triples: the original (s, p, o) and the annotation (<<(s,p,o)>>, confidence, 0.9)
        #expect(triples.count == 2)

        // First triple: original
        #expect(triples[0].subject == .variable("s"))
        #expect(triples[0].predicate == .variable("p"))
        #expect(triples[0].object == .variable("o"))

        // Second triple: annotation
        guard case .quotedTriple(let as1, let ap, let ao) = triples[1].subject else {
            Issue.record("Expected quotedTriple annotation subject, got \\(triples[1].subject)")
            return
        }
        #expect(as1 == .variable("s"))
        #expect(ap == .variable("p"))
        #expect(ao == .variable("o"))
        #expect(triples[1].predicate == .iri("http://example.org/confidence"))
    }

    @Test("Annotation with multiple predicates")
    func testAnnotationMultiplePredicates() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE {
                ?s ?p ?o {|
                    <http://example.org/confidence> 0.9 ;
                    <http://example.org/source> <http://example.org/wiki>
                |}
            }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \\(pattern)")
            return
        }
        // 1 original + 2 annotation triples
        #expect(triples.count == 3)
    }

    @Test("No annotation present")
    func testNoAnnotation() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p ?o }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        #expect(triples.count == 1)
    }

    @Test("Annotation with comma-separated objects")
    func testAnnotationCommaObjects() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE {
                ?s ?p ?o {|
                    <http://example.org/source> <http://example.org/wiki> , <http://example.org/web>
                |}
            }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        // 1 original + 2 annotation triples (same predicate, two objects)
        #expect(triples.count == 3)
    }

    @Test("Annotation with trailing semicolon")
    func testAnnotationTrailingSemicolon() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE {
                ?s ?p ?o {|
                    <http://example.org/confidence> 0.9 ;
                |}
            }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        #expect(triples.count == 2)
    }
}

// MARK: - A5: Reified Triple Edge Cases

@Suite("A5: Reified Triple Edge Cases")
struct ReifiedTripleEdgeCaseTests {

    @Test("Reifier with IRI instead of variable")
    func testReifierIRI() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { << ?s ?p ?o ~<http://example.org/reifier1> >> <http://example.org/meta> "info" }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic, got \(pattern)")
            return
        }
        #expect(triples.count == 1)
        guard case .reifiedTriple(_, _, _, let reifier) = triples[0].subject else {
            Issue.record("Expected reifiedTriple subject")
            return
        }
        #expect(reifier == .iri("http://example.org/reifier1"))
    }

    @Test("Reifier with blank node")
    func testReifierBlankNode() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { << ?s ?p ?o ~_:b0 >> <http://example.org/meta> "info" }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        #expect(triples.count == 1)
        guard case .reifiedTriple(_, _, _, let reifier) = triples[0].subject else {
            Issue.record("Expected reifiedTriple subject")
            return
        }
        guard case .blankNode = reifier else {
            Issue.record("Expected blank node reifier, got \(reifier)")
            return
        }
    }

    @Test("Quoted triple without reifier still works")
    func testQuotedTripleNoReifier() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { << ?s ?p ?o >> <http://example.org/meta> "info" }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .quotedTriple = triples[0].subject else {
            Issue.record("Expected quotedTriple subject, got \(triples[0].subject)")
            return
        }
    }
}

// MARK: - A4: Triple Term Edge Cases

@Suite("A4: Triple Term Edge Cases")
struct TripleTermEdgeCaseTests {

    @Test("Triple term in subject position")
    func testTripleTermSubject() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { <<( <http://example.org/a> <http://example.org/b> <http://example.org/c> )>> <http://example.org/meta> "info" }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        #expect(triples.count == 1)
        guard case .quotedTriple = triples[0].subject else {
            Issue.record("Expected quotedTriple subject")
            return
        }
    }

    @Test("Triple term with variables")
    func testTripleTermVariables() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?x <http://example.org/ref> <<( ?a ?b ?c )>> }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        guard case .quotedTriple(let s, let p, let o) = triples[0].object else {
            Issue.record("Expected quotedTriple object")
            return
        }
        #expect(s == .variable("a"))
        #expect(p == .variable("b"))
        #expect(o == .variable("c"))
    }

    @Test("Triple term in FILTER expression")
    func testTripleTermInFilter() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p ?o . FILTER (?o = <<( <http://example.org/a> <http://example.org/b> 42 )>>) }
            """)
        guard case .filter = pattern else {
            Issue.record("Expected filter pattern, got \(pattern)")
            return
        }
    }
}

// MARK: - A7: VERSION Edge Cases

@Suite("A7: VERSION Edge Cases")
struct VersionEdgeCaseTests {

    @Test("VERSION before BASE and PREFIX")
    func testVersionBeforeBaseAndPrefix() throws {
        let query = try parseQuery(#"""
            VERSION "1.2"
            BASE <http://example.org/>
            PREFIX ex: <http://example.org/ns/>
            SELECT * WHERE { ?s <#name> ?o }
            """#)
        #expect(query.projection == .all)
    }

    @Test("VERSION 1.1")
    func testVersion11() throws {
        let query = try parseQuery(#"""
            VERSION "1.1"
            SELECT * WHERE { ?s ?p ?o }
            """#)
        #expect(query.projection == .all)
    }
}

// MARK: - A8-A11: Function Edge Cases

@Suite("A8-A11: Function Edge Cases")
struct FunctionEdgeCaseTests {

    @Test("LANGDIR in FILTER comparison")
    func testLangDirInFilter() throws {
        let pattern = try parsePattern(#"""
            SELECT * WHERE { ?s ?p ?o . FILTER (LANGDIR(?o) != "") }
            """#)
        guard case .filter = pattern else {
            Issue.record("Expected filter")
            return
        }
    }

    @Test("hasLANG combined with LANG")
    func testHasLangWithLang() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE { ?s ?p ?o . FILTER (hasLANG(?o) && LANG(?o) = "en") }
            """)
        guard case .filter(_, let expr) = pattern else {
            Issue.record("Expected filter")
            return
        }
        guard case .and = expr else {
            Issue.record("Expected AND expression, got \(expr)")
            return
        }
    }

    @Test("STRLANGDIR in SELECT projection")
    func testStrLangDirProjection() throws {
        let query = try parseQuery(#"""
            SELECT (STRLANGDIR(STR(?label), "en", "ltr") AS ?directed) WHERE { ?s <http://example.org/label> ?label }
            """#)
        guard case .items(let items) = query.projection else {
            Issue.record("Expected projection items")
            return
        }
        #expect(items.count == 1)
        #expect(items[0].alias == "directed")
    }

    @Test("Nested function calls: STRLEN(STRLANGDIR(...))")
    func testNestedFunctionCalls() throws {
        let query = try parseQuery(#"""
            SELECT (STRLEN(STRLANGDIR("text", "en", "ltr")) AS ?len) WHERE {}
            """#)
        guard case .items(let items) = query.projection else {
            Issue.record("Expected projection items")
            return
        }
        #expect(items.count == 1)
        guard case .function(let call) = items[0].expression else {
            Issue.record("Expected function call")
            return
        }
        #expect(call.name == "STRLEN")
    }
}

// MARK: - Combined Features

@Suite("Combined SPARQL 1.2 Features")
struct CombinedFeatureTests {

    @Test("VERSION + PREFIX + FROM + direction literal")
    func testCombinedFeatures() throws {
        let query = try parseQuery(#"""
            VERSION "1.2"
            PREFIX ex: <http://example.org/>
            SELECT * FROM <http://example.org/graph1> WHERE { ?s ex:label "text"@ar--rtl }
            """#)
        #expect(query.from?.count == 1)
        guard case .graphPattern(let pattern) = query.source else {
            Issue.record("Expected graphPattern source")
            return
        }
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected basic")
            return
        }
        guard case .literal(.dirLangLiteral(let val, let lang, let dir)) = triples[0].object else {
            Issue.record("Expected dirLangLiteral, got \(triples[0].object)")
            return
        }
        #expect(val == "text")
        #expect(lang == "ar")
        #expect(dir == "rtl")
    }

    @Test("Annotation + Triple term together")
    func testAnnotationWithTripleTerm() throws {
        let pattern = try parsePattern("""
            SELECT * WHERE {
                ?s <http://example.org/ref> <<( <http://example.org/a> <http://example.org/b> 42 )>> {|
                    <http://example.org/confidence> 0.95
                |}
            }
            """)
        guard case .basic(let triples) = pattern else {
            Issue.record("Expected .basic")
            return
        }
        // 1 original triple + 1 annotation triple
        #expect(triples.count == 2)
        // Original: ?s ref <<(a b 42)>>
        guard case .quotedTriple = triples[0].object else {
            Issue.record("Expected quotedTriple object")
            return
        }
        // Annotation subject should be quotedTriple of the full triple
        guard case .quotedTriple = triples[1].subject else {
            Issue.record("Expected quotedTriple annotation subject")
            return
        }
    }

    @Test("CONSTRUCT WHERE with VERSION")
    func testConstructWhereWithVersion() throws {
        let stmt = try parseStatement(#"""
            VERSION "1.2"
            CONSTRUCT WHERE { ?s ?p ?o }
            """#)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.template.count == 1)
    }
}

// MARK: - LATERAL Join (SPARQL 1.2)

@Suite("SPARQL 1.2: LATERAL Join")
struct LateralJoinTests {

    @Test("LATERAL with basic pattern")
    func testLateralBasic() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    ?s <http://example.org/name> ?name
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source,
              case .lateral(let left, let right) = pattern else {
            Issue.record("Expected .lateral pattern")
            return
        }
        // Left: BGP with one triple
        guard case .basic(let leftTriples) = left else {
            Issue.record("Expected .basic left, got \(left)")
            return
        }
        #expect(leftTriples.count == 1)
        // Right: BGP with one triple
        guard case .basic(let rightTriples) = right else {
            Issue.record("Expected .basic right, got \(right)")
            return
        }
        #expect(rightTriples.count == 1)
    }

    @Test("LATERAL with subquery containing LIMIT")
    func testLateralSubquery() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    SELECT ?label WHERE {
                        ?s <http://example.org/label> ?label
                    } LIMIT 1
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source,
              case .lateral(_, let right) = pattern else {
            Issue.record("Expected .lateral pattern")
            return
        }
        // Right should be a subquery
        guard case .subquery(let subquery) = right else {
            Issue.record("Expected .subquery right, got \(right)")
            return
        }
        #expect(subquery.limit == 1)
    }

    @Test("LATERAL with OPTIONAL inside")
    func testLateralWithOptional() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    OPTIONAL {
                        ?s <http://example.org/email> ?email
                    }
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source,
              case .lateral(_, let right) = pattern else {
            Issue.record("Expected .lateral pattern")
            return
        }
        guard case .optional = right else {
            Issue.record("Expected .optional right, got \(right)")
            return
        }
    }

    @Test("Multiple LATERAL blocks")
    func testMultipleLateral() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    ?s <http://example.org/name> ?name
                }
                LATERAL {
                    ?s <http://example.org/age> ?age
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source else {
            Issue.record("Expected graphPattern")
            return
        }
        // Second LATERAL wraps the first LATERAL result
        guard case .lateral(let outer, _) = pattern,
              case .lateral = outer else {
            Issue.record("Expected nested .lateral, got \(pattern)")
            return
        }
    }

    @Test("LATERAL variables in scope")
    func testLateralVariablesInScope() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    ?s <http://example.org/name> ?name
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source else {
            Issue.record("Expected graphPattern")
            return
        }
        let vars = pattern.variables
        #expect(vars.contains("s"))
        #expect(vars.contains("name"))
    }

    @Test("LATERAL with FILTER")
    func testLateralWithFilter() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    ?s <http://example.org/age> ?age .
                    FILTER (?age > 18)
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source,
              case .lateral(_, let right) = pattern else {
            Issue.record("Expected .lateral pattern")
            return
        }
        guard case .filter = right else {
            Issue.record("Expected .filter right, got \(right)")
            return
        }
    }

    @Test("LATERAL toSPARQL roundtrip")
    func testLateralToSPARQL() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    ?s <http://example.org/name> ?name
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source else {
            Issue.record("Expected graphPattern")
            return
        }
        let sparql = pattern.toSPARQL()
        #expect(sparql.contains("LATERAL"))
    }

    @Test("LATERAL empty right side produces lateral with empty basic")
    func testLateralEmptyRight() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s ?p ?o .
                LATERAL { }
            }
            """)
        guard case .graphPattern(let pattern) = query.source,
              case .lateral(_, let right) = pattern else {
            Issue.record("Expected .lateral pattern")
            return
        }
        guard case .basic(let triples) = right else {
            Issue.record("Expected empty .basic right, got \(right)")
            return
        }
        #expect(triples.isEmpty)
    }

    @Test("LATERAL with UNION inside")
    func testLateralWithUnion() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    { ?s <http://example.org/name> ?name }
                    UNION
                    { ?s <http://example.org/label> ?name }
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source,
              case .lateral(_, let right) = pattern else {
            Issue.record("Expected .lateral pattern")
            return
        }
        guard case .union = right else {
            Issue.record("Expected .union right, got \(right)")
            return
        }
    }

    @Test("LATERAL with VALUES inside")
    func testLateralWithValues() throws {
        let query = try parseQuery(#"""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    VALUES (?lang) { ("en") ("ja") }
                    ?s <http://example.org/label> ?label
                }
            }
            """#)
        guard case .graphPattern(let pattern) = query.source,
              case .lateral(_, _) = pattern else {
            Issue.record("Expected .lateral pattern")
            return
        }
    }

    @Test("LATERAL followed by triples block")
    func testLateralFollowedByTriples() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    ?s <http://example.org/name> ?name
                }
                ?s <http://example.org/age> ?age
            }
            """)
        guard case .graphPattern(let pattern) = query.source else {
            Issue.record("Expected graphPattern")
            return
        }
        // Structure: join(lateral(basic, basic), basic)
        // The trailing triple is joined after the LATERAL
        let vars = pattern.variables
        #expect(vars.contains("s"))
        #expect(vars.contains("name"))
        #expect(vars.contains("age"))
    }

    @Test("LATERAL requiredVariables includes both sides")
    func testLateralRequiredVariables() throws {
        let query = try parseQuery("""
            SELECT * WHERE {
                ?s <http://example.org/type> <http://example.org/Person> .
                LATERAL {
                    ?s <http://example.org/name> ?name
                }
            }
            """)
        guard case .graphPattern(let pattern) = query.source else {
            Issue.record("Expected graphPattern")
            return
        }
        let required = pattern.requiredVariables
        #expect(required.contains("s"))
        #expect(required.contains("name"))
    }
}
