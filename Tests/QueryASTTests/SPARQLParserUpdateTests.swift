/// SPARQLParserUpdateTests.swift
/// Tests for Phase 5: CONSTRUCT improvements (B10, B11) + SPARQL Update (B12)

import Testing
import TestHeartbeat
import Foundation
@testable import QueryAST

// MARK: - Helper

private func parseStatement(_ sparql: String) throws -> QueryStatement {
    let parser = SPARQLParser()
    return try parser.parse(sparql)
}

// MARK: - B10: CONSTRUCT WHERE

@Suite("B10: CONSTRUCT WHERE", .heartbeat)
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

@Suite("B11: CONSTRUCT Modifiers", .heartbeat)
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
        #expect(query.modifiers.orderBy.count == 1)
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
        #expect(query.modifiers.limit == 10)
        #expect(query.modifiers.offset == 5)
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
        #expect(query.modifiers.orderBy.count == 1)
        #expect(query.modifiers.limit == 100)
        #expect(query.modifiers.offset == 20)
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
        #expect(query.modifiers.orderBy.isEmpty)
        #expect(query.modifiers.limit == nil)
        #expect(query.modifiers.offset == nil)
    }
}

// MARK: - B12: SPARQL Update

@Suite("B12: INSERT DATA", .heartbeat)
struct InsertDataTests {

    @Test("INSERT DATA with single triple")
    func testInsertDataSingle() throws {
        let stmt = try parseStatement(#"""
            INSERT DATA { <http://example.org/s> <http://example.org/p> "value" }
            """#)
        guard case .insertData(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
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
        guard case .insertData(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected insertData")
            return
        }
        #expect(query.quads.count == 1)
        #expect(query.quads[0].graph == .iri("http://example.org/g1"))
    }
}

@Suite("B12: DELETE DATA", .heartbeat)
struct DeleteDataTests {

    @Test("DELETE DATA with single triple")
    func testDeleteDataSingle() throws {
        let stmt = try parseStatement(#"""
            DELETE DATA { <http://example.org/s> <http://example.org/p> "old" }
            """#)
        guard case .deleteData(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected deleteData, got \(stmt)")
            return
        }
        #expect(query.quads.count == 1)
    }
}

@Suite("B12: DELETE/INSERT WHERE", .heartbeat)
struct SPARQLModifyTests {

    @Test("DELETE INSERT WHERE pattern")
    func testDeleteInsertWhere() throws {
        let stmt = try parseStatement(#"""
            DELETE { ?s <http://example.org/p> ?old }
            INSERT { ?s <http://example.org/p> "new" }
            WHERE { ?s <http://example.org/p> ?old }
            """#)
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(stmt),
              case .deleteAndInsert(let deletePattern, let insertPattern) = query.action else {
            Issue.record("Expected SPARQL Modify, got \(stmt)")
            return
        }
        #expect(deletePattern.count == 1)
        #expect(insertPattern.count == 1)
    }

    @Test("DELETE-only WHERE pattern")
    func testDeleteOnlyWhere() throws {
        let stmt = try parseStatement("""
            DELETE { ?s <http://example.org/p> ?o }
            WHERE { ?s <http://example.org/p> ?o . FILTER (?o = "obsolete") }
            """)
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(stmt),
              case .delete(let deletePattern) = query.action else {
            Issue.record("Expected SPARQL Modify, got \(stmt)")
            return
        }
        #expect(deletePattern.count == 1)
    }

    @Test("INSERT-only WHERE pattern")
    func testInsertOnlyWhere() throws {
        let stmt = try parseStatement(#"""
            INSERT { ?s <http://example.org/label> "default" }
            WHERE { ?s <http://example.org/type> <http://example.org/Thing> }
            """#)
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(stmt),
              case .insert(let insertPattern) = query.action else {
            Issue.record("Expected SPARQL Modify, got \(stmt)")
            return
        }
        #expect(insertPattern.count == 1)
    }
}

@Suite("B12: WITH and DELETE WHERE", .heartbeat)
struct ModifyShortcutTests {
    @Test("WITH remains distinct from USING")
    func testWithModify() throws {
        let stmt = try parseStatement("""
            WITH <urn:target>
            DELETE { ?s <urn:old> ?o }
            INSERT { ?s <urn:new> ?o }
            USING <urn:dataset>
            WHERE { ?s <urn:old> ?o }
            """)
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected SPARQL Modify")
            return
        }
        #expect(query.withGraph == "urn:target")
        #expect(query.using == [GraphRef(iri: "urn:dataset")])
    }

    @Test("DELETE WHERE stores one quad pattern")
    func testDeleteWhere() throws {
        let stmt = try parseStatement("""
            DELETE WHERE { GRAPH ?g { ?s <urn:p> ?o } }
            """)
        guard case .deleteWhere(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected deleteWhere")
            return
        }
        #expect(query.pattern.count == 1)
        #expect(query.pattern.first?.graph == .variable("g"))
    }
}

@Suite("B12: LOAD", .heartbeat)
struct LoadTests {

    @Test("LOAD source IRI")
    func testLoadBasic() throws {
        let stmt = try parseStatement("""
            LOAD <http://example.org/data.ttl>
            """)
        guard case .load(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
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
        guard case .load(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected load")
            return
        }
        #expect(query.source == "http://example.org/data.ttl")
        #expect(query.destination == "http://example.org/g1")
        #expect(query.silent == true)
    }
}

@Suite("B12: CLEAR", .heartbeat)
struct ClearTests {

    @Test("CLEAR DEFAULT")
    func testClearDefault() throws {
        let stmt = try parseStatement("CLEAR DEFAULT")
        guard case .clear(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected clear, got \(stmt)")
            return
        }
        #expect(query.target == .default)
        #expect(query.silent == false)
    }

    @Test("CLEAR ALL")
    func testClearAll() throws {
        let stmt = try parseStatement("CLEAR ALL")
        guard case .clear(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected clear")
            return
        }
        #expect(query.target == .all)
    }

    @Test("CLEAR SILENT GRAPH <iri>")
    func testClearSilentGraph() throws {
        let stmt = try parseStatement("CLEAR SILENT GRAPH <http://example.org/g1>")
        guard case .clear(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected clear")
            return
        }
        #expect(query.target == .graph("http://example.org/g1"))
        #expect(query.silent == true)
    }

    @Test("CLEAR NAMED")
    func testClearNamed() throws {
        let stmt = try parseStatement("CLEAR NAMED")
        guard case .clear(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected clear")
            return
        }
        #expect(query.target == .named)
    }
}

@Suite("B12: CREATE/DROP GRAPH", .heartbeat)
struct CreateDropGraphTests {

    @Test("CREATE GRAPH")
    func testCreateGraph() throws {
        let stmt = try parseStatement("CREATE GRAPH <http://example.org/g1>")
        guard case .createGraph(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected createSPARQLGraph, got \(stmt)")
            return
        }
        #expect(query.graph == "http://example.org/g1")
        #expect(query.silent == false)
    }

    @Test("CREATE SILENT GRAPH")
    func testCreateSilentGraph() throws {
        let stmt = try parseStatement("CREATE SILENT GRAPH <http://example.org/g1>")
        guard case .createGraph(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected createSPARQLGraph")
            return
        }
        #expect(query.graph == "http://example.org/g1")
        #expect(query.silent == true)
    }

    @Test("DROP GRAPH")
    func testDropGraph() throws {
        let stmt = try parseStatement("DROP GRAPH <http://example.org/g1>")
        guard case .drop(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected DROP, got \(stmt)")
            return
        }
        #expect(query.target == .graph("http://example.org/g1"))
        #expect(query.silent == false)
    }

    @Test("DROP SILENT GRAPH")
    func testDropSilentGraph() throws {
        let stmt = try parseStatement("DROP SILENT GRAPH <http://example.org/g1>")
        guard case .drop(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected DROP")
            return
        }
        #expect(query.silent == true)
    }
}

@Suite("B12: ADD/COPY/MOVE", .heartbeat)
struct GraphTransferTests {
    @Test("Every graph transfer operation retains typed endpoints")
    func testGraphTransfers() throws {
        let statements = try [
            parseStatement("ADD DEFAULT TO GRAPH <urn:destination>"),
            parseStatement("COPY GRAPH <urn:source> TO DEFAULT"),
            parseStatement(
                "MOVE SILENT GRAPH <urn:source> TO GRAPH <urn:destination>"
            ),
        ]
        let operations = try statements.map(requireSingleSPARQLUpdateOperation)
        #expect(operations == [
            .graphTransfer(
                GraphTransferQuery(
                    operation: .add,
                    source: .default,
                    destination: .graph("urn:destination")
                )
            ),
            .graphTransfer(
                GraphTransferQuery(
                    operation: .copy,
                    source: .graph("urn:source"),
                    destination: .default
                )
            ),
            .graphTransfer(
                GraphTransferQuery(
                    operation: .move,
                    source: .graph("urn:source"),
                    destination: .graph("urn:destination"),
                    silent: true
                )
            ),
        ])
    }

    @Test("GRAPH is optional before transfer endpoint IRIs")
    func testOptionalGraphKeyword() throws {
        let request = try SPARQLParser().parseUpdate(
            """
            PREFIX ex: <https://example.invalid/>
            ADD ex:source TO GRAPH ex:destination
            """
        )

        guard case .graphTransfer(let transfer) = request.firstOperation else {
            Issue.record("Expected graph transfer")
            return
        }
        #expect(transfer.source == .graph("https://example.invalid/source"))
        #expect(
            transfer.destination
                == .graph("https://example.invalid/destination")
        )
    }
}

@Suite("B12: ordered Update request", .heartbeat)
struct SPARQLUpdateRequestTests {
    @Test("Semicolon sequence preserves order and accepts a trailing separator")
    func sequenceAndTrailingSemicolon() throws {
        let statement = try parseStatement(
            """
            LOAD <urn:source> INTO GRAPH <urn:stage>;
            CLEAR DEFAULT;
            DROP SILENT GRAPH <urn:old>;
            """
        )
        guard case .sparqlUpdate(let request) = statement else {
            Issue.record("Expected SPARQL Update request")
            return
        }

        #expect(request.count == 3)
        guard case .load(let load) = request[0],
              case .clear(let clear) = request[1],
              case .drop(let drop) = request[2] else {
            Issue.record("Expected LOAD, CLEAR, DROP order")
            return
        }
        #expect(load.destination == "urn:stage")
        #expect(clear.target == .default)
        #expect(drop.target == .graph("urn:old"))
        #expect(drop.silent)
    }

    @Test("A prologue after a separator applies to the following operation")
    func perOperationPrologue() throws {
        let request = try SPARQLParser().parseUpdate(
            """
            PREFIX first: <urn:first:>
            INSERT DATA { first:s first:p first:o };
            PREFIX second: <urn:second:>
            DELETE DATA { second:s second:p second:o };
            """
        )

        #expect(request.count == 2)
        guard case .insertData(let insert) = request[0],
              case .deleteData(let delete) = request[1] else {
            Issue.record("Expected INSERT DATA then DELETE DATA")
            return
        }
        #expect(insert.quads.first?.triple.subject == .iri("urn:first:s"))
        #expect(delete.quads.first?.triple.subject == .iri("urn:second:s"))
    }

    @Test("Empty, recursive, and mixed request states are rejected")
    func invalidRequestShapes() {
        let invalidRequests = [
            "",
            "PREFIX ex: <urn:example:>",
            ";",
            "INSERT DATA { <urn:s> <urn:p> <urn:o> };; CLEAR DEFAULT",
            "INSERT DATA { <urn:s> <urn:p> <urn:o> }; SELECT * WHERE { ?s ?p ?o }",
            "SELECT * WHERE { ?s ?p ?o }; CLEAR DEFAULT",
        ]

        for request in invalidRequests {
            #expect(throws: SPARQLParser.ParseError.self) {
                _ = try SPARQLParser().parse(request)
            }
        }

        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parseUpdate(
                "SELECT * WHERE { ?subject ?predicate ?object }"
            )
        }
    }
}

// MARK: - B12: Edge Cases

@Suite("B12: INSERT DATA Edge Cases", .heartbeat)
struct InsertDataEdgeCaseTests {

    @Test("INSERT DATA with multiple triples")
    func testInsertDataMultiple() throws {
        let stmt = try parseStatement(#"""
            INSERT DATA {
                <http://example.org/s1> <http://example.org/p> "value1" .
                <http://example.org/s2> <http://example.org/p> "value2"
            }
            """#)
        guard case .insertData(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
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
        guard case .insertData(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
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
        guard case .insertData(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected insertData")
            return
        }
        #expect(query.quads.count == 2)
        #expect(query.quads[0].graph == .iri("http://example.org/g1"))
        #expect(query.quads[1].graph == .iri("http://example.org/g2"))
    }
}

@Suite("B12: DELETE/INSERT Edge Cases", .heartbeat)
struct SPARQLModifyEdgeCaseTests {

    @Test("DELETE with multiple patterns")
    func testDeleteMultiplePatterns() throws {
        let stmt = try parseStatement("""
            DELETE { ?s <http://example.org/p1> ?o1 . ?s <http://example.org/p2> ?o2 }
            WHERE { ?s <http://example.org/p1> ?o1 . ?s <http://example.org/p2> ?o2 }
            """)
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(stmt),
              case .delete(let deletePattern) = query.action else {
            Issue.record("Expected SPARQL Modify")
            return
        }
        #expect(deletePattern.count == 2)
    }

    @Test("DELETE/INSERT with USING clause")
    func testDeleteInsertUsing() throws {
        let stmt = try parseStatement(#"""
            DELETE { ?s <http://example.org/p> ?old }
            INSERT { ?s <http://example.org/p> "new" }
            USING <http://example.org/source>
            WHERE { ?s <http://example.org/p> ?old }
            """#)
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected SPARQL Modify")
            return
        }
        #expect(query.using.count == 1)
        #expect(query.using[0].iri == "http://example.org/source")
        #expect(query.using[0].isNamed == false)
    }

    @Test("DELETE/INSERT with USING NAMED clause")
    func testDeleteInsertUsingNamed() throws {
        let stmt = try parseStatement(#"""
            DELETE { ?s <http://example.org/p> ?old }
            INSERT { ?s <http://example.org/p> "new" }
            USING NAMED <http://example.org/source>
            WHERE { ?s <http://example.org/p> ?old }
            """#)
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(stmt) else {
            Issue.record("Expected SPARQL Modify")
            return
        }
        #expect(query.using.count == 1)
        #expect(query.using[0].isNamed == true)
    }
}

@Suite("B10: CONSTRUCT WHERE Edge Cases", .heartbeat)
struct ConstructWhereEdgeCaseTests {

    @Test("CONSTRUCT WHERE rejects non-basic graph patterns")
    func testConstructWhereFilter() {
        #expect(throws: SPARQLParser.ParseError.self) {
            try parseStatement("""
                CONSTRUCT WHERE { ?s <http://example.org/age> ?o . FILTER (?o > 18) }
                """)
        }
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
        #expect(query.modifiers.orderBy.count == 1)
        #expect(query.modifiers.limit == 5)
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
        #expect(query.modifiers.orderBy.count == 1)
        #expect(query.modifiers.orderBy[0].direction == .descending)
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
        #expect(query.modifiers.orderBy.count == 1)
    }
}
