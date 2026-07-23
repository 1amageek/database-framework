#if FOUNDATION_DB
import Testing
import TestHeartbeat
@testable import QueryAST

@Suite("SQL parameter parser", .heartbeat)
struct SQLParameterParserTests {
    @Test("anonymous parameters receive stable one-based positions")
    func anonymousParametersAreNumbered() throws {
        let statement = try SQLParser().parse(
            "SELECT ? AS firstValue, ? AS secondValue FROM Event WHERE id = ?"
        )
        guard case .select(let query) = statement else {
            Issue.record("Expected a SELECT statement")
            return
        }

        #expect(query.projection == .items([
            ProjectionItem(.parameter(.position(1)), alias: "firstValue"),
            ProjectionItem(.parameter(.position(2)), alias: "secondValue"),
        ]))
        #expect(query.filter == .equal(.col("id"), .parameter(.position(3))))
    }

    @Test("named and explicit positional parameters preserve identity")
    func namedAndExplicitParametersParse() throws {
        let statement = try SQLParser().parse(
            "SELECT :title AS title FROM Event WHERE id = $1 AND source = $2;"
        )
        guard case .select(let query) = statement else {
            Issue.record("Expected a SELECT statement")
            return
        }

        #expect(query.projection == .items([
            ProjectionItem(.parameter(.name("title")), alias: "title"),
        ]))
        #expect(query.filter == .and(
            .equal(.col("id"), .parameter(.position(1))),
            .equal(.col("source"), .parameter(.position(2)))
        ))
    }

    @Test("parser reuse resets anonymous positions")
    func parserReuseResetsPositions() throws {
        let parser = SQLParser()
        _ = try parser.parse("SELECT ? FROM Event")
        let statement = try parser.parse("SELECT ? FROM Event")
        guard case .select(let query) = statement else {
            Issue.record("Expected a SELECT statement")
            return
        }

        #expect(query.projection == .items([
            ProjectionItem(.parameter(.position(1))),
        ]))
    }

    @Test("malformed and mixed positional markers are rejected")
    func malformedMarkersAreRejected() {
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("SELECT $0 FROM Event")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("SELECT $1suffix FROM Event")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("SELECT ?, $2 FROM Event")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("SELECT id FROM Event AS event trailing")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("SELECT : FROM Event")
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("SELECT :1 FROM Event")
        }
    }
}
#endif
