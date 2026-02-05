/// SQLEscapeTests.swift
/// Tests for SQL and SPARQL escape utilities

import Testing
@testable import QueryIR

@Suite("SQLEscape")
struct SQLEscapeTests {

    // MARK: - identifier

    @Test func identifierSimple() {
        let result = SQLEscape.identifier("column_name")
        #expect(result == "\"column_name\"")
    }

    @Test func identifierEscapesDoubleQuotes() {
        let result = SQLEscape.identifier("user\"name")
        #expect(result == "\"user\"\"name\"")
    }

    @Test func identifierMultipleDoubleQuotes() {
        let result = SQLEscape.identifier("a\"b\"c")
        #expect(result == "\"a\"\"b\"\"c\"")
    }

    @Test func identifierEmpty() {
        let result = SQLEscape.identifier("")
        #expect(result == "\"\"")
    }

    @Test func identifierWithSpaces() {
        let result = SQLEscape.identifier("column name")
        #expect(result == "\"column name\"")
    }

    // MARK: - string

    @Test func stringSimple() {
        let result = SQLEscape.string("hello")
        #expect(result == "'hello'")
    }

    @Test func stringEscapesSingleQuotes() {
        let result = SQLEscape.string("it's")
        #expect(result == "'it''s'")
    }

    @Test func stringMultipleSingleQuotes() {
        let result = SQLEscape.string("a'b'c")
        #expect(result == "'a''b''c'")
    }

    @Test func stringEmpty() {
        let result = SQLEscape.string("")
        #expect(result == "''")
    }

    @Test func stringWithDoubleQuotes() {
        // Double quotes should NOT be escaped in string literals
        let result = SQLEscape.string("say \"hello\"")
        #expect(result == "'say \"hello\"'")
    }

    // MARK: - identifierIfNeeded

    @Test func identifierIfNeededSimple() {
        // Simple identifier without special chars should not be quoted
        let result = SQLEscape.identifierIfNeeded("column_name")
        #expect(result == "column_name")
    }

    @Test func identifierIfNeededWithSpace() {
        let result = SQLEscape.identifierIfNeeded("column name")
        #expect(result == "\"column name\"")
    }

    @Test func identifierIfNeededReservedWord() {
        let result = SQLEscape.identifierIfNeeded("SELECT")
        #expect(result == "\"SELECT\"")
    }

    @Test func identifierIfNeededReservedWordLowercase() {
        let result = SQLEscape.identifierIfNeeded("select")
        #expect(result == "\"select\"")
    }

    @Test func identifierIfNeededStartsWithNumber() {
        let result = SQLEscape.identifierIfNeeded("123abc")
        #expect(result == "\"123abc\"")
    }

    @Test func identifierIfNeededWithHyphen() {
        let result = SQLEscape.identifierIfNeeded("column-name")
        #expect(result == "\"column-name\"")
    }

    @Test func identifierIfNeededUnderscore() {
        // Underscore at start is valid
        let result = SQLEscape.identifierIfNeeded("_private")
        #expect(result == "_private")
    }

    // MARK: - Reserved Words Coverage

    @Test func reservedWordsAreQuoted() {
        let reservedWords = ["FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE"]
        for word in reservedWords {
            let result = SQLEscape.identifierIfNeeded(word)
            #expect(result.hasPrefix("\""), "Expected \(word) to be quoted")
        }
    }
}

@Suite("SPARQLEscape")
struct SPARQLEscapeTests {

    // MARK: - ncName

    @Test func ncNameValid() throws {
        let result = try SPARQLEscape.ncName("validName")
        #expect(result == "validName")
    }

    @Test func ncNameWithUnderscore() throws {
        let result = try SPARQLEscape.ncName("_prefix")
        #expect(result == "_prefix")
    }

    @Test func ncNameWithNumbers() throws {
        let result = try SPARQLEscape.ncName("name123")
        #expect(result == "name123")
    }

    @Test func ncNameWithDot() throws {
        let result = try SPARQLEscape.ncName("name.suffix")
        #expect(result == "name.suffix")
    }

    @Test func ncNameWithHyphen() throws {
        let result = try SPARQLEscape.ncName("name-suffix")
        #expect(result == "name-suffix")
    }

    @Test func ncNameEmpty() {
        #expect(throws: SPARQLEscapeError.emptyNCName) {
            _ = try SPARQLEscape.ncName("")
        }
    }

    @Test func ncNameStartsWithNumber() {
        #expect(throws: SPARQLEscapeError.self) {
            _ = try SPARQLEscape.ncName("123abc")
        }
    }

    @Test func ncNameWithColon() {
        #expect(throws: SPARQLEscapeError.self) {
            _ = try SPARQLEscape.ncName("ns:local")
        }
    }

    @Test func ncNameWithSpace() {
        #expect(throws: SPARQLEscapeError.self) {
            _ = try SPARQLEscape.ncName("invalid name")
        }
    }

    // MARK: - ncNameOrNil

    @Test func ncNameOrNilValid() {
        let result = SPARQLEscape.ncNameOrNil("validName")
        #expect(result == "validName")
    }

    @Test func ncNameOrNilInvalid() {
        let result = SPARQLEscape.ncNameOrNil("123invalid")
        #expect(result == nil)
    }

    // MARK: - iri

    @Test func iriSimple() {
        let result = SPARQLEscape.iri("http://example.org/resource")
        #expect(result == "<http://example.org/resource>")
    }

    @Test func iriEscapesLessThan() {
        let result = SPARQLEscape.iri("http://example.org/<test>")
        #expect(result == "<http://example.org/%3Ctest%3E>")
    }

    @Test func iriEscapesBackslash() {
        let result = SPARQLEscape.iri("http://example.org/path\\file")
        #expect(result == "<http://example.org/path%5Cfile>")
    }

    @Test func iriEscapesCurlyBraces() {
        let result = SPARQLEscape.iri("http://example.org/{id}")
        #expect(result == "<http://example.org/%7Bid%7D>")
    }

    @Test func iriEscapesPipe() {
        let result = SPARQLEscape.iri("http://example.org/a|b")
        #expect(result == "<http://example.org/a%7Cb>")
    }

    @Test func iriEscapesCaret() {
        let result = SPARQLEscape.iri("http://example.org/a^b")
        #expect(result == "<http://example.org/a%5Eb>")
    }

    @Test func iriEscapesBacktick() {
        let result = SPARQLEscape.iri("http://example.org/`test`")
        #expect(result == "<http://example.org/%60test%60>")
    }

    // MARK: - string

    @Test func stringSimple() {
        let result = SPARQLEscape.string("hello")
        #expect(result == "\"hello\"")
    }

    @Test func stringEscapesBackslash() {
        let result = SPARQLEscape.string("path\\file")
        #expect(result == "\"path\\\\file\"")
    }

    @Test func stringEscapesDoubleQuote() {
        let result = SPARQLEscape.string("say \"hello\"")
        #expect(result == "\"say \\\"hello\\\"\"")
    }

    @Test func stringEscapesNewline() {
        let result = SPARQLEscape.string("line1\nline2")
        #expect(result == "\"line1\\nline2\"")
    }

    @Test func stringEscapesTab() {
        let result = SPARQLEscape.string("col1\tcol2")
        #expect(result == "\"col1\\tcol2\"")
    }

    @Test func stringEscapesCarriageReturn() {
        let result = SPARQLEscape.string("line1\rline2")
        #expect(result == "\"line1\\rline2\"")
    }

    // MARK: - prefixedName

    @Test func prefixedNameValid() throws {
        let result = try SPARQLEscape.prefixedName(prefix: "ex", local: "resource")
        #expect(result == "ex:resource")
    }

    @Test func prefixedNameEmptyLocal() throws {
        let result = try SPARQLEscape.prefixedName(prefix: "rdf", local: "")
        #expect(result == "rdf:")
    }

    @Test func prefixedNameInvalidPrefix() {
        #expect(throws: SPARQLEscapeError.self) {
            _ = try SPARQLEscape.prefixedName(prefix: "123", local: "resource")
        }
    }

    @Test func prefixedNameInvalidLocal() {
        #expect(throws: SPARQLEscapeError.self) {
            _ = try SPARQLEscape.prefixedName(prefix: "ex", local: "invalid name")
        }
    }
}
