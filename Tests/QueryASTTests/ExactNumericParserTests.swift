import DatabaseKit
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("Exact numeric parser", .heartbeat)
struct ExactNumericParserTests {
    @Test("SQL preserves signed widths and numeric display scale")
    func sqlPreservesExactNumerics() throws {
        let query = try SQLParser().parseSelect(
            "SELECT 18446744073709551615, 123.4500 FROM Event"
        )

        #expect(query.projection == .items([
            ProjectionItem(.literal(.queryValue(.numeric(try QueryNumericValue(
                value: .init(coefficient: 18_446_744_073_709_551_615, scale: 0), displayScale: 0
            )), type: .sql(.decimal(precision: nil, scale: nil))))),
            ProjectionItem(.literal(.queryValue(.numeric(try QueryNumericValue(
                value: .init(coefficient: 12_345, scale: 2), displayScale: 4
            )), type: .sql(.decimal(precision: nil, scale: nil))))),
        ]))
    }

    @Test("SPARQL preserves unsigned and decimal literals")
    func sparqlPreservesExactNumerics() throws {
        let query = try SPARQLParser().parseSelect(
            """
            SELECT * WHERE {
                <urn:event:1> <urn:count> 18446744073709551615 .
                <urn:event:1> <urn:amount> 123.4500 .
            }
            """
        )
        guard case .graphPattern(.basic(let basicGraphPattern)) = query.source else {
            Issue.record("Expected a basic graph pattern")
            return
        }
        let triples = try basicGraphPattern.triplePatterns()

        #expect(triples.count == 2)
        #expect(triples[0].object == .literal(.uint(UInt64.max)))
        #expect(
            triples[1].object
                == .literal(.decimal(coefficient: 12_345, scale: 2))
        )
    }

    @Test("numeric overflow is rejected instead of becoming zero")
    func overflowIsRejected() {
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parseSelect(
                "SELECT 123456789012345678901234567890123456789 FROM Event"
            )
        }
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parseSelect(
                "SELECT * WHERE { <urn:s> <urn:p> 18446744073709551616 }"
            )
        }
    }
}
