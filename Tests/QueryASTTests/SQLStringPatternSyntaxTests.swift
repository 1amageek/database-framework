import DatabaseKit
import Testing
@testable import QueryAST

@Suite("S05 string and pattern SQL syntax")
struct SQLStringPatternSyntaxTests {
    @Test("Every SQL pattern kind and NOT form preserves explicit escape meaning")
    func patternOperators() throws {
        let cases: [(String, PatternMatchKind, Bool)] = [
            ("s LIKE $1 ESCAPE $2", .like, false), ("s NOT LIKE $1 ESCAPE $2", .like, true),
            ("s ILIKE $1 ESCAPE $2", .caseInsensitiveLike, false), ("s NOT ILIKE $1 ESCAPE $2", .caseInsensitiveLike, true),
            ("s SIMILAR TO $1 ESCAPE $2", .similarTo, false), ("s NOT SIMILAR TO $1 ESCAPE $2", .similarTo, true),
            ("s ~ $1", .regularExpression, false), ("s !~ $1", .regularExpression, true),
            ("s ~* $1", .caseInsensitiveRegularExpression, false), ("s !~* $1", .caseInsensitiveRegularExpression, true),
        ]
        for (sql, kind, negated) in cases {
            let selected = try roundTrip(sql)
            let match: Expression
            if negated, case .not(let inner) = selected { match = inner } else { match = selected }
            #expect(match == .patternMatch(subject: .col("s"), pattern: .parameter(.position(1)),
                escape: kind.permitsEscape ? .parameter(.position(2)) : nil, kind: kind))
        }
        let omitted = try roundTrip("s LIKE p")
        #expect(omitted == .patternMatch(subject: .col("s"), pattern: .col("p"),
            escape: .literal(.queryValue(.primitive(.string("\\")), type: .sql(.text))), kind: .like))
        let disabled = try roundTrip("s LIKE p ESCAPE ''")
        #expect(disabled == .patternMatch(subject: .col("s"), pattern: .col("p"), escape: nil, kind: .like))
        #expect(try SQLSerializer().serialize(disabled).contains("ESCAPE ''"))
        let nullEscape = try roundTrip("s LIKE p ESCAPE NULL")
        #expect(nullEscape != disabled)
        #expect(throws: SQLParser.ParseError.self) { try expression("s ~ p ESCAPE '!' ") }
    }

    @Test("SUBSTRING POSITION and TRIM normalize special grammar into selected argument order")
    func specialStringGrammar() throws {
        let cases: [(String, String, [Expression])] = [
            ("SUBSTRING(s FROM start FOR amount)", "substring", [.col("s"), .col("start"), .col("amount")]),
            ("SUBSTRING(s FROM start)", "substring", [.col("s"), .col("start")]),
            ("SUBSTRING(s FOR amount FROM start)", "substring", [.col("s"), .col("start"), .col("amount")]),
            ("SUBSTRING(s FOR amount)", "substring", [.col("s"), integer(1), .col("amount")]),
            ("SUBSTRING(s, start, amount)", "substring", [.col("s"), .col("start"), .col("amount")]),
            ("SUBSTRING(s FROM 'a(.*)' FOR '#')", "substring", [.col("s"), .literal(.sqlUnknownString("a(.*)")), .literal(.sqlUnknownString("#"))]),
            ("POSITION(needle IN s)", "strpos", [.col("s"), .col("needle")]),
            ("TRIM(s)", "btrim", [.col("s")]),
            ("TRIM(FROM s)", "btrim", [.col("s")]),
            ("TRIM(chars FROM s)", "btrim", [.col("s"), .col("chars")]),
            ("TRIM(BOTH chars FROM s)", "btrim", [.col("s"), .col("chars")]),
            ("TRIM(LEADING chars FROM s)", "ltrim", [.col("s"), .col("chars")]),
            ("TRIM(TRAILING FROM s)", "rtrim", [.col("s")]),
            ("TRIM(s, chars)", "btrim", [.col("s"), .col("chars")]),
        ]
        for (sql, name, arguments) in cases {
            #expect(try roundTrip(sql) == .function(FunctionCall(name: name, arguments: arguments)))
        }
    }

    @Test("Quoted and schema qualified calls retain their exact ordinary function identity")
    func exactGenericCalls() throws {
        for name in ["substring", "position", "trim", "regexp_match", "regexp_matches", "regexp_split_to_table"] {
            #expect(try roundTrip("\"\(name)\"(s, p)") == .function(FunctionCall(name: name, arguments: [.col("s"), .col("p")])))
            #expect(try roundTrip("pg_catalog.\(name)(s, p)") == .function(FunctionCall(name: name, arguments: [.col("s"), .col("p")], schema: "pg_catalog")))
        }
        #expect(throws: SQLParser.ParseError.self) { try expression("\"substring\"(s FROM p)") }
        #expect(throws: SQLParser.ParseError.self) { try expression("TRIM(DISTINCT s)") }
    }

    @Test("Dynamic pattern children and reordered special arguments retain their original UTF8 positions")
    func sourceOccurrences() throws {
        let sql = "SELECT subject LIKE $1 ESCAPE $2"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let root = try child(parsed, parsed.root, .projection, 0)
        #expect(try source(sql, parsed, child(parsed, root, .operand, 0)) == "subject")
        #expect(try source(sql, parsed, child(parsed, root, .argument, 0)) == "$1")
        #expect(try source(sql, parsed, child(parsed, root, .argument, 1)) == "$2")
        for (text, expected) in [
            ("SELECT POSITION($1 IN $2)", ["$2", "$1"]),
            ("SELECT TRIM(LEADING $1 FROM $2)", ["$2", "$1"]),
            ("SELECT SUBSTRING($1 FOR $2 FROM $3)", ["$1", "$3", "$2"]),
            ("SELECT SUBSTRING($1 FOR $2)", ["$1", "FOR", "$2"]),
        ] {
            let value = try SQLParser().parseSelectWithSource(text)
            let call = try child(value, value.root, .projection, 0)
            for index in expected.indices {
                #expect(try source(text, value, child(value, call, .argument, UInt32(index))) == expected[index])
            }
        }
    }

    @Test("Text and binary concatenation use one strict operator with stable precedence")
    func concatenation() throws {
        #expect(try roundTrip("a || b || c") == .concatenate(.concatenate(.col("a"), .col("b")), .col("c")))
        #expect(try roundTrip("a || b + 1") == .concatenate(.col("a"), .add(.col("b"), integer(1))))
        #expect(try roundTrip("CONCAT(a, b)") == .function(FunctionCall(name: "concat", arguments: [.col("a"), .col("b")])))
        _ = try roundTrip("BYTEA '\\x00' || BYTEA '\\xff'")
    }

    @Test("Deferred relation functions and malformed predicate structure fail explicitly")
    func deferredAndMalformedSyntax() throws {
        for sql in ["SELECT * FROM regexp_matches(s, p)", "SELECT * FROM regexp_split_to_table(s, p)"] {
            #expect(throws: SQLParser.ParseError.unsupportedFeature("SQL table functions require S10 relation composition")) {
                try SQLParser().parseSelect(sql)
            }
        }
        for sql in ["s SIMILAR p", "s LIKE", "s NOT ILIKE", "SUBSTRING(s FROM)", "POSITION(s IN)", "TRIM(LEADING FROM)"] {
            #expect(throws: SQLParser.ParseError.self) { try expression(sql) }
        }
        #expect(throws: SQLSerializationError.invalidStructure("ERE operators do not accept ESCAPE")) {
            try SQLSerializer().serialize(.patternMatch(subject: .col("s"), pattern: .col("p"), escape: .null, kind: .regularExpression))
        }
        let value = Expression.patternMatch(subject: .col("s"), pattern: .col("p"), escape: nil, kind: .like)
        let output = try SQLSerializer().serialize(value)
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: UInt64(output.utf8.count - 1))).serialize(value)
        }
    }

    private func integer(_ value: Int32) -> Expression { .literal(.queryValue(.primitive(.int32(value)), type: .sql(.integer))) }
    private func expression(_ sql: String) throws -> Expression {
        let query = try SQLParser().parseSelect("SELECT " + sql)
        guard case .items(let items) = query.projection, items.count == 1 else {
            throw SQLSerializationError.invalidStructure("Expected one projection")
        }
        return items[0].expression
    }
    private func roundTrip(_ sql: String) throws -> Expression {
        let original = try expression(sql)
        let emitted = try SQLSerializer().serialize(original)
        #expect(try expression(emitted) == original)
        return original
    }
    private func child(_ parsed: SQLParsedSelect, _ parent: QueryOccurrenceID,
                       _ field: QueryOccurrenceField, _ index: UInt32) throws -> QueryOccurrenceID {
        try #require(parsed.occurrences.child(of: parent, field: field, index: index))
    }
    private func source(_ sql: String, _ parsed: SQLParsedSelect, _ occurrence: QueryOccurrenceID) throws -> String {
        let range = try #require(parsed.sourceMap[occurrence]).range
        return String(decoding: Array(sql.utf8)[range.start..<range.end], as: UTF8.self)
    }
}
