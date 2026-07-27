#if FOUNDATION_DB
import DatabaseKit
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("SPARQL parser blank node scope validation", .heartbeat)
struct SPARQLParserBlankNodeScopeValidationTests {
    @Test("A blank node label remains valid inside one basic graph pattern")
    func oneBasicGraphPatternIsValid() throws {
        _ = try SPARQLParser().parseSelect(
            """
            SELECT * WHERE {
                _:shared <urn:first> ?first .
                _:shared <urn:second> ?second
            }
            """
        )
    }

    @Test("A triple and property path share one basic graph pattern")
    func tripleAndPropertyPathShareBasicGraphPattern() throws {
        let query = try SPARQLParser().parseSelect(
            """
            SELECT * WHERE {
                _:shared <urn:first> ?first .
                _:shared <urn:step>/<urn:last> ?last
            }
            """
        )
        guard case .graphPattern(let graphPattern) = query.source,
              case .basic(let basicGraphPattern) = graphPattern else {
            Issue.record("Expected one basic graph pattern")
            return
        }

        #expect(basicGraphPattern.count == 2)
        guard case .triple = basicGraphPattern.elements[0],
              case .propertyPath = basicGraphPattern.elements[1] else {
            Issue.record("Expected one triple followed by one property path")
            return
        }
    }

    @Test("A blank node label cannot cross basic graph patterns")
    func labelAcrossBasicGraphPatternsIsRejected() {
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parseSelect(
                """
                SELECT * WHERE {
                    _:shared <urn:first> ?first
                    OPTIONAL { _:shared <urn:second> ?second }
                }
                """
            )
        }
    }

    @Test("A blank node label cannot cross INSERT DATA operations")
    func labelAcrossInsertDataOperationsIsRejected() {
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parseUpdate(
                """
                INSERT DATA { _:shared <urn:first> <urn:value> };
                INSERT DATA { _:shared <urn:second> <urn:value> }
                """
            )
        }
    }

    @Test("A blank node label cannot cross WHERE clauses")
    func labelAcrossWhereClausesIsRejected() {
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parseUpdate(
                """
                INSERT { ?subject <urn:copy> ?value }
                WHERE { _:shared <urn:first> ?value };
                INSERT { ?subject <urn:copy> ?value }
                WHERE { _:shared <urn:second> ?value }
                """
            )
        }
    }
}
#endif
