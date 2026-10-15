/// Canonical SQL identifier and string escaping behavior.

import Testing
import TestHeartbeat
@testable import QueryAST

@Suite("Canonical SQL escaping", .heartbeat)
struct SQLEscapeTests {

    // MARK: - identifier

    @Test func identifierSimple() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "column_name")))
        #expect(result == "\"column_name\"")
    }

    @Test func identifierEscapesDoubleQuotes() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "user\"name")))
        #expect(result == "\"user\"\"name\"")
    }

    @Test func identifierMultipleDoubleQuotes() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "a\"b\"c")))
        #expect(result == "\"a\"\"b\"\"c\"")
    }

    @Test func identifierEmpty() throws {
        #expect(throws: SQLSerializationError.invalidStructure("Empty or NUL SQL identifier")) {
            _ = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "")))
        }
    }

    @Test func identifierWithSpaces() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "column name")))
        #expect(result == "\"column name\"")
    }

    // MARK: - string

    @Test func stringSimple() throws {
        let result = try SQLSerializer().serialize(Literal.sqlUnknownString("hello"))
        #expect(result == "'hello'")
    }

    @Test func stringEscapesSingleQuotes() throws {
        let result = try SQLSerializer().serialize(Literal.sqlUnknownString("it's"))
        #expect(result == "'it''s'")
    }

    @Test func stringMultipleSingleQuotes() throws {
        let result = try SQLSerializer().serialize(Literal.sqlUnknownString("a'b'c"))
        #expect(result == "'a''b''c'")
    }

    @Test func stringEmpty() throws {
        let result = try SQLSerializer().serialize(Literal.sqlUnknownString(""))
        #expect(result == "''")
    }

    @Test func stringWithDoubleQuotes() throws {
        // Double quotes should NOT be escaped in string literals
        let result = try SQLSerializer().serialize(Literal.sqlUnknownString("say \"hello\""))
        #expect(result == "'say \"hello\"'")
    }

    // MARK: - Exact identifier spelling

    @Test func identifierAlwaysQuotesSimpleNames() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "column_name")))
        #expect(result == "\"column_name\"")
    }

    @Test func identifierAlwaysQuotesSpaces() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "column name")))
        #expect(result == "\"column name\"")
    }

    @Test func identifierPreservesUppercaseReservedWords() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "SELECT")))
        #expect(result == "\"SELECT\"")
    }

    @Test func identifierPreservesLowercaseReservedWords() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "select")))
        #expect(result == "\"select\"")
    }

    @Test func identifierStartsWithNumber() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "123abc")))
        #expect(result == "\"123abc\"")
    }

    @Test func identifierWithHyphen() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "column-name")))
        #expect(result == "\"column-name\"")
    }

    @Test func identifierStartsWithUnderscore() throws {
        let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: "_private")))
        #expect(result == "\"_private\"")
    }

    // MARK: - Reserved Words Coverage

    @Test func reservedWordsAreQuoted() throws {
        let reservedWords = ["FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE"]
        for word in reservedWords {
            let result = try SQLSerializer().serialize(Expression.column(ColumnRef(column: word)))
            #expect(result.hasPrefix("\""), "Expected \(word) to be quoted")
        }
    }
}
