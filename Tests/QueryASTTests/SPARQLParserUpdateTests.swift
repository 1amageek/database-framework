/// SPARQLParserUpdateTests.swift
/// Tests for Phase 5: CONSTRUCT improvements (B10, B11) + SPARQL Update (B12)

import Testing
import Foundation
@testable import QueryAST

// MARK: - Helper

private func parseStatement(_ sparql: String) throws -> QueryStatement {
    let parser = SPARQLParser()
    return try parser.parse(sparql)
}

// MARK: - B10: CONSTRUCT WHERE

@Suite("B10: CONSTRUCT WHERE")
struct ConstructWhereTests {

    @Test("CONSTRUCT WHERE shortcut uses pattern as template")
    func testConstructWhere() throws {
        let stmt = try parseStatement("""
            CONSTRUCT WHERE { ?s <http://example.org/name> ?o }
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT, got \(stmt)")
            return
        }
        #expect(query.template.count == 1)
        #expect(query.template[0].subject == .variable("s"))
        #expect(query.template[0].predicate == .iri("http://example.org/name"))
        #expect(query.template[0].object == .variable("o"))
    }

    @Test("CONSTRUCT WHERE with multiple triples")
    func testConstructWhereMultiple() throws {
        let stmt = try parseStatement("""
            CONSTRUCT WHERE { ?s ?p ?o . ?o <http://example.org/type> ?t }
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.template.count == 2)
    }
}

// MARK: - B11: CONSTRUCT Modifiers

@Suite("B11: CONSTRUCT Modifiers")
struct ConstructModifierTests {

    @Test("CONSTRUCT with ORDER BY")
    func testConstructOrderBy() throws {
        let stmt = try parseStatement("""
            CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } ORDER BY ?s
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.orderBy != nil)
        #expect(query.orderBy?.count == 1)
    }

    @Test("CONSTRUCT with LIMIT and OFFSET")
    func testConstructLimitOffset() throws {
        let stmt = try parseStatement("""
            CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 10 OFFSET 5
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.limit == 10)
        #expect(query.offset == 5)
    }

    @Test("CONSTRUCT with ORDER BY, LIMIT, OFFSET")
    func testConstructAllModifiers() throws {
        let stmt = try parseStatement("""
            CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 100 OFFSET 20
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.orderBy?.count == 1)
        #expect(query.limit == 100)
        #expect(query.offset == 20)
    }

    @Test("CONSTRUCT without modifiers still works")
    func testConstructNoModifiers() throws {
        let stmt = try parseStatement("""
            CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.orderBy == nil)
        #expect(query.limit == nil)
        #expect(query.offset == nil)
    }
}

// MARK: - B12: SPARQL Update

@Suite("B12: INSERT DATA")
struct InsertDataTests {

    @Test("INSERT DATA with single triple")
    func testInsertDataSingle() throws {
        let stmt = try parseStatement(#"""
            INSERT DATA { <http://example.org/s> <http://example.org/p> "value" }
            """#)
        guard case .insertData(let query) = stmt else {
            Issue.record("Expected insertData, got \(stmt)")
            return
        }
        #expect(query.quads.count == 1)
        #expect(query.quads[0].graph == nil)
        #expect(query.quads[0].triple.subject == .iri("http://example.org/s"))
    }

    @Test("INSERT DATA with GRAPH clause")
    func testInsertDataGraph() throws {
        let stmt = try parseStatement(#"""
            INSERT DATA {
                GRAPH <http://example.org/g1> {
                    <http://example.org/s> <http://example.org/p> "value"
                }
            }
            """#)
        guard case .insertData(let query) = stmt else {
            Issue.record("Expected insertData")
            return
        }
        #expect(query.quads.count == 1)
        #expect(query.quads[0].graph == .iri("http://example.org/g1"))
    }
}

@Suite("B12: DELETE DATA")
struct DeleteDataTests {

    @Test("DELETE DATA with single triple")
    func testDeleteDataSingle() throws {
        let stmt = try parseStatement(#"""
            DELETE DATA { <http://example.org/s> <http://example.org/p> "old" }
            """#)
        guard case .deleteData(let query) = stmt else {
            Issue.record("Expected deleteData, got \(stmt)")
            return
        }
        #expect(query.quads.count == 1)
    }
}

@Suite("B12: DELETE/INSERT WHERE")
struct DeleteInsertTests {

    @Test("DELETE INSERT WHERE pattern")
    func testDeleteInsertWhere() throws {
        let stmt = try parseStatement(#"""
            DELETE { ?s <http://example.org/p> ?old }
            INSERT { ?s <http://example.org/p> "new" }
            WHERE { ?s <http://example.org/p> ?old }
            """#)
        guard case .deleteInsert(let query) = stmt else {
            Issue.record("Expected deleteInsert, got \(stmt)")
            return
        }
        #expect(query.deletePattern?.count == 1)
        #expect(query.insertPattern?.count == 1)
    }

    @Test("DELETE-only WHERE pattern")
    func testDeleteOnlyWhere() throws {
        let stmt = try parseStatement("""
            DELETE { ?s <http://example.org/p> ?o }
            WHERE { ?s <http://example.org/p> ?o . FILTER (?o = "obsolete") }
            """)
        guard case .deleteInsert(let query) = stmt else {
            Issue.record("Expected deleteInsert, got \(stmt)")
            return
        }
        #expect(query.deletePattern?.count == 1)
        #expect(query.insertPattern == nil)
    }

    @Test("INSERT-only WHERE pattern")
    func testInsertOnlyWhere() throws {
        let stmt = try parseStatement(#"""
            INSERT { ?s <http://example.org/label> "default" }
            WHERE { ?s <http://example.org/type> <http://example.org/Thing> }
            """#)
        guard case .deleteInsert(let query) = stmt else {
            Issue.record("Expected deleteInsert, got \(stmt)")
            return
        }
        #expect(query.deletePattern == nil)
        #expect(query.insertPattern?.count == 1)
    }
}

@Suite("B12: LOAD")
struct LoadTests {

    @Test("LOAD source IRI")
    func testLoadBasic() throws {
        let stmt = try parseStatement("""
            LOAD <http://example.org/data.ttl>
            """)
        guard case .load(let query) = stmt else {
            Issue.record("Expected load, got \(stmt)")
            return
        }
        #expect(query.source == "http://example.org/data.ttl")
        #expect(query.destination == nil)
        #expect(query.silent == false)
    }

    @Test("LOAD SILENT INTO GRAPH")
    func testLoadSilentIntoGraph() throws {
        let stmt = try parseStatement("""
            LOAD SILENT <http://example.org/data.ttl> INTO GRAPH <http://example.org/g1>
            """)
        guard case .load(let query) = stmt else {
            Issue.record("Expected load")
            return
        }
        #expect(query.source == "http://example.org/data.ttl")
        #expect(query.destination == "http://example.org/g1")
        #expect(query.silent == true)
    }
}

@Suite("B12: CLEAR")
struct ClearTests {

    @Test("CLEAR DEFAULT")
    func testClearDefault() throws {
        let stmt = try parseStatement("CLEAR DEFAULT")
        guard case .clear(let query) = stmt else {
            Issue.record("Expected clear, got \(stmt)")
            return
        }
        #expect(query.target == .default)
        #expect(query.silent == false)
    }

    @Test("CLEAR ALL")
    func testClearAll() throws {
        let stmt = try parseStatement("CLEAR ALL")
        guard case .clear(let query) = stmt else {
            Issue.record("Expected clear")
            return
        }
        #expect(query.target == .all)
    }

    @Test("CLEAR SILENT GRAPH <iri>")
    func testClearSilentGraph() throws {
        let stmt = try parseStatement("CLEAR SILENT GRAPH <http://example.org/g1>")
        guard case .clear(let query) = stmt else {
            Issue.record("Expected clear")
            return
        }
        #expect(query.target == .graph("http://example.org/g1"))
        #expect(query.silent == true)
    }

    @Test("CLEAR NAMED")
    func testClearNamed() throws {
        let stmt = try parseStatement("CLEAR NAMED")
        guard case .clear(let query) = stmt else {
            Issue.record("Expected clear")
            return
        }
        #expect(query.target == .named)
    }
}

@Suite("B12: CREATE/DROP GRAPH")
struct CreateDropGraphTests {

    @Test("CREATE GRAPH")
    func testCreateGraph() throws {
        let stmt = try parseStatement("CREATE GRAPH <http://example.org/g1>")
        guard case .createSPARQLGraph(let iri, let silent) = stmt else {
            Issue.record("Expected createSPARQLGraph, got \(stmt)")
            return
        }
        #expect(iri == "http://example.org/g1")
        #expect(silent == false)
    }

    @Test("CREATE SILENT GRAPH")
    func testCreateSilentGraph() throws {
        let stmt = try parseStatement("CREATE SILENT GRAPH <http://example.org/g1>")
        guard case .createSPARQLGraph(let iri, let silent) = stmt else {
            Issue.record("Expected createSPARQLGraph")
            return
        }
        #expect(iri == "http://example.org/g1")
        #expect(silent == true)
    }

    @Test("DROP GRAPH")
    func testDropGraph() throws {
        let stmt = try parseStatement("DROP GRAPH <http://example.org/g1>")
        guard case .dropSPARQLGraph(let iri, let silent) = stmt else {
            Issue.record("Expected dropSPARQLGraph, got \(stmt)")
            return
        }
        #expect(iri == "http://example.org/g1")
        #expect(silent == false)
    }

    @Test("DROP SILENT GRAPH")
    func testDropSilentGraph() throws {
        let stmt = try parseStatement("DROP SILENT GRAPH <http://example.org/g1>")
        guard case .dropSPARQLGraph(_, let silent) = stmt else {
            Issue.record("Expected dropSPARQLGraph")
            return
        }
        #expect(silent == true)
    }
}

// MARK: - B12: Edge Cases

@Suite("B12: INSERT DATA Edge Cases")
struct InsertDataEdgeCaseTests {

    @Test("INSERT DATA with multiple triples")
    func testInsertDataMultiple() throws {
        let stmt = try parseStatement(#"""
            INSERT DATA {
                <http://example.org/s1> <http://example.org/p> "value1" .
                <http://example.org/s2> <http://example.org/p> "value2"
            }
            """#)
        guard case .insertData(let query) = stmt else {
            Issue.record("Expected insertData")
            return
        }
        #expect(query.quads.count == 2)
        #expect(query.quads[0].graph == nil)
        #expect(query.quads[1].graph == nil)
    }

    @Test("INSERT DATA with mixed GRAPH and default")
    func testInsertDataMixedGraphDefault() throws {
        let stmt = try parseStatement(#"""
            INSERT DATA {
                <http://example.org/s> <http://example.org/p> "default" .
                GRAPH <http://example.org/g1> {
                    <http://example.org/s> <http://example.org/p> "named"
                }
            }
            """#)
        guard case .insertData(let query) = stmt else {
            Issue.record("Expected insertData")
            return
        }
        #expect(query.quads.count == 2)
        #expect(query.quads[0].graph == nil)
        #expect(query.quads[1].graph == .iri("http://example.org/g1"))
    }

    @Test("INSERT DATA with multiple GRAPH blocks")
    func testInsertDataMultipleGraphs() throws {
        let stmt = try parseStatement(#"""
            INSERT DATA {
                GRAPH <http://example.org/g1> {
                    <http://example.org/s> <http://example.org/p> "v1"
                }
                GRAPH <http://example.org/g2> {
                    <http://example.org/s> <http://example.org/p> "v2"
                }
            }
            """#)
        guard case .insertData(let query) = stmt else {
            Issue.record("Expected insertData")
            return
        }
        #expect(query.quads.count == 2)
        #expect(query.quads[0].graph == .iri("http://example.org/g1"))
        #expect(query.quads[1].graph == .iri("http://example.org/g2"))
    }
}

@Suite("B12: DELETE/INSERT Edge Cases")
struct DeleteInsertEdgeCaseTests {

    @Test("DELETE with multiple patterns")
    func testDeleteMultiplePatterns() throws {
        let stmt = try parseStatement("""
            DELETE { ?s <http://example.org/p1> ?o1 . ?s <http://example.org/p2> ?o2 }
            WHERE { ?s <http://example.org/p1> ?o1 . ?s <http://example.org/p2> ?o2 }
            """)
        guard case .deleteInsert(let query) = stmt else {
            Issue.record("Expected deleteInsert")
            return
        }
        #expect(query.deletePattern?.count == 2)
        #expect(query.insertPattern == nil)
    }

    @Test("DELETE/INSERT with USING clause")
    func testDeleteInsertUsing() throws {
        let stmt = try parseStatement(#"""
            DELETE { ?s <http://example.org/p> ?old }
            INSERT { ?s <http://example.org/p> "new" }
            USING <http://example.org/source>
            WHERE { ?s <http://example.org/p> ?old }
            """#)
        guard case .deleteInsert(let query) = stmt else {
            Issue.record("Expected deleteInsert")
            return
        }
        #expect(query.using?.count == 1)
        #expect(query.using?[0].iri == "http://example.org/source")
        #expect(query.using?[0].isNamed == false)
    }

    @Test("DELETE/INSERT with USING NAMED clause")
    func testDeleteInsertUsingNamed() throws {
        let stmt = try parseStatement(#"""
            DELETE { ?s <http://example.org/p> ?old }
            INSERT { ?s <http://example.org/p> "new" }
            USING NAMED <http://example.org/source>
            WHERE { ?s <http://example.org/p> ?old }
            """#)
        guard case .deleteInsert(let query) = stmt else {
            Issue.record("Expected deleteInsert")
            return
        }
        #expect(query.using?.count == 1)
        #expect(query.using?[0].isNamed == true)
    }
}

@Suite("B10: CONSTRUCT WHERE Edge Cases")
struct ConstructWhereEdgeCaseTests {

    @Test("CONSTRUCT WHERE with FILTER")
    func testConstructWhereFilter() throws {
        let stmt = try parseStatement("""
            CONSTRUCT WHERE { ?s <http://example.org/age> ?o . FILTER (?o > 18) }
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        // Template should contain the BGP triples (FILTER is not a triple)
        #expect(query.template.count == 1)
    }

    @Test("CONSTRUCT WHERE with modifiers")
    func testConstructWhereWithModifiers() throws {
        let stmt = try parseStatement("""
            CONSTRUCT WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 5
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.template.count == 1)
        #expect(query.orderBy?.count == 1)
        #expect(query.limit == 5)
    }

    @Test("CONSTRUCT with ORDER BY DESC")
    func testConstructOrderByDesc() throws {
        let stmt = try parseStatement("""
            CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } ORDER BY DESC(?s)
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.orderBy?.count == 1)
        #expect(query.orderBy?[0].direction == .descending)
    }

    @Test("CONSTRUCT with ORDER BY expression")
    func testConstructOrderByExpression() throws {
        let stmt = try parseStatement("""
            CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } ORDER BY STRLEN(?s)
            """)
        guard case .construct(let query) = stmt else {
            Issue.record("Expected CONSTRUCT")
            return
        }
        #expect(query.orderBy?.count == 1)
    }
}
