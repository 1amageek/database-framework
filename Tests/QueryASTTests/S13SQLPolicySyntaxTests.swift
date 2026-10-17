import DatabaseKit
import Testing
@testable import QueryAST

@Suite("S13 SQL policy syntax", .timeLimit(.minutes(1)))
struct S13SQLPolicySyntaxTests {
    @Test("DEFAULT and identity override preserve mutation intent through text")
    func identityOverrideRoundTrip() throws {
        for (word, policy) in [("SYSTEM", SQLIdentityOverride.systemValue), ("USER", .userValue)] {
            let statement = try SQLParser().parse("INSERT INTO items (id, value) OVERRIDING \(word) VALUE VALUES (DEFAULT, 8)")
            guard case .insert(let insert) = statement else { Issue.record("Expected INSERT"); return }
            #expect(insert.overriding == policy)
            guard case .values(let rows) = insert.source else { Issue.record("Expected VALUES"); return }
            #expect(rows[0][0] == .defaultValue)
            #expect(try SQLParser().parse(SQLSerializer().serialize(statement)) == statement)
        }
        let update = try SQLParser().parse("UPDATE items SET id = DEFAULT")
        #expect(try SQLParser().parse(SQLSerializer().serialize(update)) == update)
        for malformed in ["INSERT INTO items OVERRIDING VALUE VALUES (1)",
                          "INSERT INTO items OVERRIDING SYSTEM VALUES (1)",
                          "INSERT INTO items OVERRIDING 'SYSTEM' VALUE VALUES (1)"] {
            #expect(throws: SQLParser.ParseError.self) { try SQLParser().parse(malformed) }
        }
    }

    @Test("Bare session expressions normalize without losing quoted column identity")
    func sessionExpressions() throws {
        let query = try SQLParser().parseSelect("SELECT CURRENT_USER, SESSION_USER, CURRENT_SCHEMA, CURRENT_SCHEMA(), CURRENT_CATALOG, CURRENT_ROLE, \"current_user\"")
        #expect(query.projection == [
            ProjectionItem(.function(FunctionCall(name: "current_user", arguments: []))),
            ProjectionItem(.function(FunctionCall(name: "session_user", arguments: []))),
            ProjectionItem(.function(FunctionCall(name: "current_schema", arguments: []))),
            ProjectionItem(.function(FunctionCall(name: "current_schema", arguments: []))),
            ProjectionItem(.function(FunctionCall(name: "current_database", arguments: []))),
            ProjectionItem(.function(FunctionCall(name: "current_user", arguments: []))),
            ProjectionItem(.column(ColumnRef("current_user")))])
        #expect(try SQLParser().parseSelect(SQLSerializer().serialize(query)) == query)
    }
}
