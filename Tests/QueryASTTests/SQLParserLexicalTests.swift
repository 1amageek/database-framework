import DatabaseKit
import QueryAST
import Testing

@Suite("SQL UTF-8 lexical contract")
struct SQLParserLexicalTests {
    private func expressions(_ sql: String) throws -> [Expression] {
        let query = try SQLParser().parseSelect(sql)
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }) else {
            Issue.record("Expected projection items")
            return []
        }
        return try projectionExpressions(items)
    }

    @Test("Unquoted ASCII folds while quoted and Unicode names remain exact")
    func identifierIdentity() throws {
        let query = try SQLParser().parseSelect("SELECT MiXeD, \"MiXeD\", ÉTÉ, \"a\"\"b\" FROM \"Schema\".TABLE_NAME")
        #expect(query.projection == [
            ProjectionItem(.col("mixed")), ProjectionItem(.col("MiXeD")),
            ProjectionItem(.col("ÉtÉ")), ProjectionItem(.col("a\"b")),
        ])
        #expect(query.source == .table(TableRef(schema: "Schema", table: "table_name")))
        #expect(try expressions("SELECT 'a''\u{301}', \"a\"\"\u{301}\"") == [.literal(.sqlUnknownString("a'\u{301}")), .col("a\"\u{301}")])
    }

    @Test("Reserved words require quotes and Unicode lookalikes remain names")
    func reservedAndContextualNames() throws {
        #expect(try expressions("SELECT \"select\", materialized, ſelect") == [.col("select"), .col("materialized"), .col("ſelect")])
        #expect(try expressions("SELECT count, a.first FROM sum AS a") == [.col("count"), .column(ColumnRef(table: "a", column: "first"))])
        #expect(try expressions("SELECT insert, update, drop, over, between, exists, values, property FROM graph_table") == [
            .col("insert"), .col("update"), .col("drop"), .col("over"), .col("between"), .col("exists"), .col("values"), .col("property"),
        ])
        let reserved = "ALL ANALYSE ANALYZE AND ANY ARRAY AS ASC ASYMMETRIC BOTH CASE CAST CHECK COLLATE COLUMN CONSTRAINT CREATE CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER DEFAULT DEFERRABLE DESC DISTINCT DO ELSE END EXCEPT FALSE FETCH FOR FOREIGN FROM GRANT GROUP HAVING IN INITIALLY INTERSECT INTO LATERAL LEADING LIMIT LOCALTIME LOCALTIMESTAMP NOT NULL OFFSET ON ONLY OR ORDER PLACING PRIMARY REFERENCES RETURNING SELECT SESSION_USER SOME SYMMETRIC SYSTEM_USER TABLE THEN TO TRAILING TRUE UNION UNIQUE USER USING VARIADIC WHEN WHERE WINDOW WITH".split(separator: " ")
        for keyword in reserved {
            let name = keyword.lowercased()
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT id FROM " + name) }
            #expect(try expressions("SELECT \"" + name + "\"") == [.col(name)])
            #expect(try expressions("SELECT t." + name + " FROM t") == [.column(ColumnRef(table: "t", column: name))])
            let alias = try SQLParser().parseSelect("SELECT NULL AS " + name)
            #expect(alias.projection == [ProjectionItem(.literal(.null), alias: name)])
        }
        let typeFunctions = "AUTHORIZATION BINARY COLLATION CONCURRENTLY CROSS CURRENT_SCHEMA FREEZE FULL ILIKE INNER IS ISNULL JOIN LEFT LIKE NATURAL NOTNULL OUTER OVERLAPS RIGHT SIMILAR TABLESAMPLE VERBOSE".split(separator: " ")
        for keyword in typeFunctions {
            let name = keyword.lowercased()
            if keyword == "CURRENT_SCHEMA" {
                #expect(try expressions("SELECT " + name) == [.function(FunctionCall(name: name, arguments: []))])
            } else {
                #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT " + name) }
            }
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT id FROM " + name) }
            #expect(try expressions("SELECT t." + name + " FROM t") == [.column(ColumnRef(table: "t", column: name))])
        }
        #expect(try expressions("SELECT authorization(:x)") == [.function(FunctionCall(name: "authorization", arguments: [.parameter(.name("x"))]))])
        #expect(try expressions("SELECT binary(:x)") == [.function(FunctionCall(name: "binary", arguments: [.parameter(.name("x"))]))])
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT check") }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT NULL::check") }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT id FROM t AS check") }
        #expect(try SQLParser().parseSelect("SELECT id FROM public.check").source == .table(TableRef(schema: "public", table: "check")))
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT SELECT") }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT \"\"") }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT \"a\u{0}b\"") }
    }

    @Test("Ordinary strings preserve backslashes and doubled quotes")
    func ordinaryStrings() throws {
        #expect(try expressions(#"SELECT 'a\nb', 'it''s'"#) == [.literal(.sqlUnknownString(#"a\nb"#)), .literal(.sqlUnknownString("it's"))])
    }

    @Test("Escape strings decode bytes, Unicode and surrogate pairs")
    func escapeStrings() throws {
        #expect(try expressions(#"SELECT E'\n\t\141\x62\u00e9\U0001F600\uD83D\uDE00\U0000D83D\U0000DE00'"#) == [.literal(.sqlUnknownString("\n\tabé😀😀😀"))])
        #expect(try expressions(#"SELECT E'\x', E'\xG', E'\z'"#) == [
            .literal(.sqlUnknownString("x")), .literal(.sqlUnknownString("xG")), .literal(.sqlUnknownString("z")),
        ])
    }

    @Test("Escape strings reject invalid UTF-8, NUL and incomplete escapes")
    func invalidEscapeStrings() {
        for sql in [#"SELECT E'\xFF'"#, #"SELECT E'\000'"#, #"SELECT E'\uD800'"#, #"SELECT E'\u12'"#] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
    }

    @Test("Unicode strings and identifiers support UESCAPE and scalar escapes")
    func unicodeStringsAndIdentifiers() throws {
        #expect(try expressions(#"SELECT U&'d\0061t\+000061', U&'!+00D83D!+00DE00' UESCAPE '!', U&"A!0042" UESCAPE '!'"#) == [
            .literal(.sqlUnknownString("data")), .literal(.sqlUnknownString("😀")), .col("AB"),
        ])
    }

    @Test("Unicode malformed escapes and invalid UESCAPE characters fail")
    func invalidUnicodeStrings() {
        for sql in [#"SELECT U&'\D800'"#, #"SELECT U&'\0000'"#, #"SELECT U&'\+110000'"#, #"SELECT U&'a' UESCAPE '0'"#, #"SELECT U&'a' UESCAPE 'xx'"#, #"SELECT U&'a' UESCAPE 'é'"#] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
    }

    @Test("Dollar delimiters are exact and do not expose SQL markers")
    func dollarStrings() throws {
        #expect(try expressions(#"SELECT $$a;'$1 -- /*$$, $Tag$x$tag$y$Tag$, $1"#) == [
            .literal(.sqlUnknownString("a;'$1 -- /*")), .literal(.sqlUnknownString("x$tag$y")), .parameter(.position(1)),
        ])
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT $Tag$body$tag$") }
    }

    @Test("SQL newline concatenation accepts line comments and stops at block comments")
    func newlineConcatenation() throws {
        #expect(try expressions("SELECT 'one'\r\n-- ; hidden delimiter\n 'two'") == [.literal(.sqlUnknownString("onetwo"))])
        for sql in [
            "SELECT 'one'\n/* nested /* hidden */ comment */ 'two'",
            "SELECT 'one' /* newline\ninside */ 'two'",
            "SELECT E'one'\n/* hidden */ 'two'",
            "SELECT U&'one'\n/* hidden */ 'two'",
        ] {
            #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect(sql) }
        }
        #expect(try expressions("SELECT 'one' /* nested /* hidden */ comment */, 'two'") == [.literal(.sqlUnknownString("one")), .literal(.sqlUnknownString("two"))])
        #expect(try expressions("SELECT U&'a'\n'b'") == [.literal(.sqlUnknownString("ab"))])
        #expect(try expressions("SELECT U&'!0061'\n'!0062' UESCAPE '!'") == [.literal(.sqlUnknownString("ab"))])
        #expect(try expressions("SELECT E'\\401'") == [.literal(.sqlUnknownString("\u{1}"))])
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT 'one' 'two'") }
    }

    @Test("Nested comments hide every apparent statement and parameter")
    func nestedComments() throws {
        let query = try SQLParser().parseSelect("/* a /* SELECT ?; */ b */ SELECT :ExactName -- $0;\r\n")
        #expect(query.projection == [ProjectionItem(.parameter(.name("ExactName")))])
    }

    @Test("Diagnostic positions are UTF-8 token starts and EOF byte offsets")
    func diagnosticBytePositions() {
        #expect(throws: SQLParser.ParseError.unexpectedToken(expected: "projection alias", found: "symbol ','", position: 15)) {
            _ = try SQLParser().parseSelect("SELECT 'é' AS ,")
        }
        #expect(throws: SQLParser.ParseError.invalidSyntax(message: "Unterminated quoted value", position: 13)) {
            _ = try SQLParser().parseSelect("SELECT \"é\", 'bad")
        }
        #expect(throws: SQLParser.ParseError.unexpectedEndOfInput(expected: ")", position: 12)) {
            _ = try SQLParser().parseSelect("SELECT ('é'")
        }
    }

    @Test("Lexical lookahead errors are restored and sequential reuse resets state")
    func parserReuseAfterLookaheadFailure() throws {
        let parser = SQLParser()
        #expect(throws: SQLParser.ParseError.invalidSyntax(message: "Unterminated quoted value", position: 12)) {
            _ = try parser.parseSelect("SELECT name 'bad")
        }
        #expect(throws: SQLParser.ParseError.self) { _ = try parser.parseSelect("SELECT ?, $1") }
        let query = try parser.parseSelect("SELECT ?")
        #expect(query.projection == [ProjectionItem(.parameter(.position(1)))])
    }

    @Test("Single entries reject additional statements and batches preserve delimiters")
    func completeStatementBatch() throws {
        let sql = #";;SELECT $$a;b$$;/* ; */SELECT U&'\003b';;SELECT 'c;d';"#
        let batch = try SQLParser().parseStatements(sql)
        #expect(batch.count == 3)
        #expect(try SQLParser().parseStatements("; /* empty */ ;").isEmpty)
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parse("SELECT 1;SELECT 2") }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseSelect("SELECT 1;SELECT 2") }
        #expect(throws: SQLParser.ParseError.self) { _ = try SQLParser().parseStatements("SELECT 1; SELECT") }
    }

    @Test("Source UTF-8 admission accepts exact bytes and rejects one past before token growth")
    func sourceByteBoundary() throws {
        _ = try SQLParser(structuralLimits: .init(maximumRetainedBytes: 9)).parseSelect("SELECT id")
        #expect(throws: QueryStructuralValidationError.resourceLimitExceeded(resource: .retainedBytes, actual: 9, maximum: 8)) {
            _ = try SQLParser(structuralLimits: .init(maximumRetainedBytes: 8)).parseSelect("SELECT id")
        }
        #expect(throws: QueryStructuralValidationError.self) {
            // TEST_SCALE_SEMANTICS: correctness rejects source bytes before token allocation.
            _ = try SQLParser(structuralLimits: .init(maximumRetainedBytes: 32)).parseSelect("SELECT '" + String(repeating: "x", count: 100_000) + "'")
        }
    }

    @Test("Comment depth is independently bounded at exact and one-past nesting")
    func commentDepthBoundary() throws {
        let sql = "/* a /* b */ */ SELECT id"
        _ = try SQLParser(structuralLimits: .init(maximumNestingDepth: 2)).parseSelect(sql)
        #expect(throws: QueryStructuralValidationError.resourceLimitExceeded(resource: .nestingDepth, actual: 2, maximum: 1)) {
            _ = try SQLParser(structuralLimits: .init(maximumNestingDepth: 1)).parseSelect(sql)
        }
    }

    @Test("Batch token and collection ledgers are cumulative")
    func batchResourceBoundary() throws {
        _ = try SQLParser(structuralLimits: .init(maximumInputTokens: 5)).parseStatements("SELECT id;SELECT id")
        #expect(throws: QueryStructuralValidationError.resourceLimitExceeded(resource: .inputTokens, actual: 5, maximum: 4)) {
            _ = try SQLParser(structuralLimits: .init(maximumInputTokens: 4)).parseStatements("SELECT id;SELECT id")
        }
        #expect(throws: QueryStructuralValidationError.self) {
            _ = try SQLParser(structuralLimits: .init(maximumCollectionElements: 3)).parseStatements("SELECT id;SELECT id")
        }
        _ = try SQLParser(structuralLimits: .init(maximumTotalNodes: 8)).parseStatements("SELECT id;SELECT id")
        #expect(throws: QueryStructuralValidationError.self) {
            _ = try SQLParser(structuralLimits: .init(maximumTotalNodes: 7)).parseStatements("SELECT id;SELECT id")
        }
        let rich = "SELECT JSONB '[1,2]';SELECT JSONB '[3,4]'"
        _ = try SQLParser(structuralLimits: .init(maximumCollectionElements: 8)).parseStatements(rich)
        #expect(throws: (any Error).self) {
            _ = try SQLParser(structuralLimits: .init(maximumCollectionElements: 7)).parseStatements(rich)
        }
    }
}
