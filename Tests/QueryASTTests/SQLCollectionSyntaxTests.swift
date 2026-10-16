import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("SQL collection syntax and canonical carriage", .heartbeat)
struct SQLCollectionSyntaxTests {
    private func column(_ name: String) -> Expression { .column(ColumnRef(name)) }

    private func expression(_ source: String) throws -> Expression {
        let query = try SQLParser().parseSelect("SELECT " + source)
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }), let first = items.first else {
            throw SQLParser.ParseError.invalidSyntax(message: "Expected one projection", position: 0)
        }
        return try projectionExpression(first)
    }

    private func roundtrip(_ value: Expression) throws {
        let text = try SQLSerializer().serialize(value)
        let parsed = try expression(text)
        #expect(parsed == value)
        #expect(try SQLSerializer().serialize(parsed) == text)
    }

    @Test("All eleven collection operators retain their closed identity")
    func collectionOperators() throws {
        let cases: [(String, CollectionOperator)] = [
            ("@>", .contains), ("<@", .containedBy), ("&&", .overlaps),
            ("->", .jsonField), ("->>", .jsonFieldText), ("#>", .jsonPath),
            ("#>>", .jsonPathText), ("?", .jsonHasKey), ("?|", .jsonHasAnyKey),
            ("?&", .jsonHasAllKeys), ("#-", .jsonDeletePath),
        ]
        for (token, operation) in cases {
            let expected = Expression.collectionOperation(operation, left: column("a"), right: column("b"))
            #expect(try expression("a " + token + " b") == expected)
            try roundtrip(expected)
        }
    }

    @Test("Longest tokens remain distinct without surrounding whitespace")
    func longestTokens() throws {
        #expect(try expression("a->>b#>>c") == .collectionOperation(.jsonPathText,
            left: .collectionOperation(.jsonFieldText, left: column("a"), right: column("b")), right: column("c")))
        #expect(try expression("a?|b?&c") == .collectionOperation(.jsonHasAllKeys,
            left: .collectionOperation(.jsonHasAnyKey, left: column("a"), right: column("b")), right: column("c")))
        #expect(try expression("a#-b->c") == .collectionOperation(.jsonField,
            left: .collectionOperation(.jsonDeletePath, left: column("a"), right: column("b")), right: column("c")))
    }

    @Test("Generic operators share left association below arithmetic and above comparison")
    func genericPrecedence() throws {
        let value = Expression.equal(.collectionOperation(.contains,
            left: .concatenate(.collectionOperation(.jsonField, left: column("a"), right: .add(column("b"), column("c"))), column("d")),
            right: column("e")), column("f"))
        #expect(try expression("a -> b + c || d @> e = f") == value)
        try roundtrip(value)
        #expect(try expression("a IS DISTINCT FROM b -> c") == .isDistinctFrom(column("a"),
            .collectionOperation(.jsonField, left: column("b"), right: column("c")), negated: false))
        #expect(try expression("a -> b - c") == .collectionOperation(.jsonField,
            left: column("a"), right: .subtract(column("b"), column("c"))))
    }

    @Test("Symbolic ERE shares the generic level while LIKE retains predicate grammar")
    func regularExpressionPrecedence() throws {
        let match = Expression.patternMatch(subject: column("a"), pattern: column("b"), escape: nil, kind: .regularExpression)
        #expect(try expression("a ~ b || c") == .concatenate(match, column("c")))
        #expect(try expression("a !~* b -> c") == .collectionOperation(.jsonField,
            left: .not(.patternMatch(subject: column("a"), pattern: column("b"), escape: nil, kind: .caseInsensitiveRegularExpression)),
            right: column("c")))
        guard case .patternMatch(let subject, _, _, .like) = try expression("a || b LIKE c") else {
            Issue.record("Expected LIKE predicate"); return
        }
        #expect(subject == .concatenate(column("a"), column("b")))
    }

    @Test("Contiguous access groups stay distinct across parentheses and casts")
    func accessGroups() throws {
        let group = Expression.arrayAccess(column("a"), subscripts: [.index(column("i")), .index(column("j"))])
        #expect(try expression("a[i][j]") == group)
        let nested = Expression.arrayAccess(.arrayAccess(column("a"), subscripts: [.index(column("i"))]), subscripts: [.index(column("j"))])
        #expect(try expression("(a[i])[j]") == nested)
        #expect(group != nested)
        try roundtrip(group)
        try roundtrip(nested)
        #expect(try expression("((a[i])::integer[])[j]") == .arrayAccess(
            .cast(.arrayAccess(column("a"), subscripts: [.index(column("i"))]), targetType: .array(.integer)),
            subscripts: [.index(column("j"))]))
    }

    @Test("Slices preserve omitted bounds and explicit NULL expressions")
    func sliceBounds() throws {
        let value = Expression.arrayAccess(column("a"), subscripts: [
            .slice(lower: nil, upper: column("p")), .slice(lower: column("lo"), upper: nil),
            .slice(lower: nil, upper: nil), .slice(lower: .null, upper: column("hi")),
        ])
        #expect(try expression("a[:p][lo:][:][NULL:hi]") == value)
        try roundtrip(value)
        #expect(try expression("a[i][:j]") == .arrayAccess(column("a"), subscripts: [
            .index(column("i")), .slice(lower: nil, upper: column("j")),
        ]))
    }

    @Test("Named operands are grouped and anonymous prefix parameters survive infix question mark")
    func parameterDisambiguation() throws {
        let named = Expression.arrayAccess(column("a"), subscripts: [
            .index(.parameter(.name("p"))),
            .slice(lower: .parameter(.name("lo")), upper: .parameter(.name("hi"))),
        ])
        #expect(try expression("a[(:p)][(:lo):(:hi)]") == named)
        try roundtrip(named)
        #expect(try expression("? ? ?") == .collectionOperation(.jsonHasKey,
            left: .parameter(.position(1)), right: .parameter(.position(2))))
        #expect(try expression(":Name") == .parameter(.name("Name")))
        #expect(throws: SQLParser.ParseError.self) { try expression("? ? $1") }
        let parser = SQLParser()
        #expect(throws: SQLParser.ParseError.self) { try parser.parseSelect("SELECT a[lo: :p]") }
        #expect(try parser.parseSelect("SELECT :p").projection == [ProjectionItem(.parameter(.name("p")))])
    }

    @Test("Collection source occurrences use canonical sparse edges and UTF-8 ranges")
    func sourceOccurrences() throws {
        let source = "SELECT a[(:名前):hi][:] -> key"
        let parsed = try SQLParser().parseSelectWithSource(source)
        let operation = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        let access = try #require(parsed.occurrences.child(of: operation, field: .left))
        let key = try #require(parsed.occurrences.child(of: operation, field: .right))
        let base = try #require(parsed.occurrences.child(of: access, field: .operand))
        let lower = try #require(parsed.occurrences.child(of: access, field: .argument, index: 0))
        let upper = try #require(parsed.occurrences.child(of: access, field: .argument, index: 1))
        #expect(parsed.occurrences.child(of: access, field: .argument, index: 2) == nil)
        #expect(parsed.occurrences.child(of: access, field: .argument, index: 3) == nil)
        for (id, expected) in [(base, "a"), (lower, ":名前"), (upper, "hi"), (key, "key")] {
            let range = try #require(parsed.sourceMap[id]).range
            #expect(String(decoding: Array(source.utf8)[range.start..<range.end], as: UTF8.self) == expected)
        }
    }

    @Test("Explicit VARIADIC is final-array metadata rather than eager expansion")
    func variadicCarriage() throws {
        for source in ["json_build_array(VARIADIC a)", "pg_catalog.jsonb_extract_path(doc, VARIADIC path)",
                       "json_build_object(VARIADIC NULL::text[])", "json_build_array(VARIADIC ARRAY[]::text[])"] {
            let value = try expression(source)
            guard case .function(let call) = value else { Issue.record("Expected generic call"); continue }
            #expect(call.finalArrayIsVariadic)
            #expect(!call.arguments.isEmpty)
            try roundtrip(value)
        }
        guard case .function(let ordinary) = try expression("json_build_array(a)"),
              case .function(let empty) = try expression("json_build_array()") else {
            Issue.record("Expected ordinary builder calls"); return
        }
        #expect(!ordinary.finalArrayIsVariadic)
        #expect(!empty.finalArrayIsVariadic && empty.arguments.isEmpty)
        let source = "SELECT jsonb_extract_path(doc, VARIADIC :path)"
        let parsed = try SQLParser().parseSelectWithSource(source)
        let call = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        let path = try #require(parsed.occurrences.child(of: call, field: .argument, index: 1))
        let range = try #require(parsed.sourceMap[path]).range
        #expect(String(decoding: Array(source.utf8)[range.start..<range.end], as: UTF8.self) == ":path")
    }

    @Test("Malformed suffixes and VARIADIC forms fail explicitly")
    func malformedSyntax() throws {
        for source in ["a[]", "a[lo:hi:other]", "a[1 + :p]", "a[lo: :p]", "a[i", "a[i][j][k][l][m][n][o]", "a ->>",
                       "json_build_array(VARIADIC)", "json_build_array(VARIADIC a, b)",
                       "json_build_array(a, VARIADIC b, VARIADIC c)", "COALESCE(VARIADIC a)"] {
            #expect(throws: SQLParser.ParseError.self) { try expression(source) }
        }
        guard case .arraySubquery(let query) = try expression("ARRAY(SELECT a FROM t)") else {
            Issue.record("Expected a complete ARRAY subquery operand"); return
        }
        #expect(query.projection == [ProjectionItem(.column(ColumnRef("a")))])
        #expect(query.source == .table(TableRef("t")))
        #expect(query.quantifier == .all && query.pagination == QueryPagination())
        try roundtrip(.arraySubquery(query))
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer().serialize(Expression.function(FunctionCall(name: "json_build_array", arguments: [], finalArrayIsVariadic: true)))
        }
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer().serialize(Expression.arrayAccess(column("a"), subscripts: []))
        }
    }

    @Test("Shared array grammar keeps intrinsic values and runtime-owned temporal CAST")
    func arrayConstantCapability() throws {
        #expect(SQLConstantError.value(.malformedArrayInput(offset: 17, reason: "Missing cell")).description
            == "Invalid array input at byte 17: Missing cell")
        #expect(SQLConstantError.value(.invalidArrayInputIndex(index: 2, count: 1)).description
            == "Invalid array input index: 2, count 1")
        let intrinsic = try expression("'[-2:-1]={1,NULL}'::integer[]")
        guard case .literal(.queryValue(.array(let array), _)) = intrinsic else {
            Issue.record("Expected pure integer array"); return
        }
        #expect(array.dimensions.map(\.lowerBound) == [-2])
        #expect(array.elements.elementsEqual([QueryValue.primitive(.int32(1)), .primitive(.null)]))
        try roundtrip(intrinsic)
        let temporal = try expression("'{2026-10-16,infinity}'::date[]")
        #expect(temporal == .cast(.literal(.sqlUnknownString("{2026-10-16,infinity}")), targetType: .array(.date)))
        try roundtrip(temporal)
        for source in ["'{{1},{2,3}}'::date[]", "'{bad}'::integer[]", "'[2147483647:2147483647]={1}'::integer[]"] {
            #expect(throws: SQLParser.ParseError.self) { try expression(source) }
        }
    }

    @Test("Structural limits and non-SQL serialization reject without placeholder success")
    func limitsAndLanguageGates() throws {
        let value = Expression.arrayAccess(column("a"), subscripts: Array(repeating: .index(column("i")), count: 6))
        try roundtrip(value)
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: 2)).serialize(value)
        }
        for sqlOnly in [value, .collectionOperation(.contains, left: column("a"), right: column("b")),
                        .function(FunctionCall(name: "json_build_array", arguments: [column("a")], finalArrayIsVariadic: true))] {
            #expect(throws: QueryLiteralConversionError.self) {
                try SPARQLQueryBuilder().select(sqlOnly, as: "value").toSPARQL()
            }
        }
    }

    @Test("Ordinary temporal UUID and BYTEA array cells reuse canonical scalar output")
    func ordinaryArrayCellOutput() throws {
        let date = try CivilDate(year: 2026, month: 10, day: 16)
        let bc = try CivilDate(year: 0, month: 2, day: 29)
        let time = try CivilTime(hour: 1, minute: 2, second: 3, nanoseconds: 123_000_000)
        let uuid = try #require(UUID(canonicalString: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
        let fixtures: [(DataType, QueryValue, String, Bool)] = [
            (.date, .primitive(.date(date)), "2026-10-16", true),
            (.date, .primitive(.date(bc)), "0001-02-29 BC", true),
            (.timestamp(withTimeZone: false), .primitive(.dateTime(CivilDateTime(date: date, time: time))), "2026-10-16T01:02:03.123", true),
            (.timestamp(withTimeZone: false), .primitive(.dateTime(CivilDateTime(date: bc, time: time))), "0001-02-29T01:02:03.123 BC", true),
            (.timestamp(withTimeZone: true), .primitive(.timestamp(Timestamp(secondsSinceUnixEpoch: 0))), "1970-01-01T00:00:00Z", true),
            (.timestamp(withTimeZone: true), .primitive(.timestamp(Timestamp(secondsSinceUnixEpoch: -62_167_219_200))), "0001-01-01T00:00:00Z BC", true),
            (.time(withTimeZone: false), .primitive(.time(time)), "01:02:03.123", true),
            (.uuid, .primitive(.uuid(uuid)), uuid.description, false),
            (.blob, .primitive(.bytes([0, 255, 92])), #"\x00FF5C"#, false),
        ]
        let limits = QueryStructuralLimits(maximumRetainedBytes: 65536)
        for (type, value, expectedText, runtime) in fixtures {
            #expect(try SQLConstantSyntax.elementText(value, limits: limits) == expectedText)
            let array = try QueryArrayValue(elementType: type,
                dimensions: [QueryArrayDimension(lowerBound: -2, length: 1)], elements: [value], limits: SQLConstantSyntax.valueLimits(limits))
            let text = try SQLConstantSyntax.arrayText(array, limits: limits)
            let source = try SQLSerializer().serialize(Expression.cast(.literal(.sqlUnknownString(text)), targetType: .array(type)))
            let parsed = try expression(source)
            if runtime {
                #expect(parsed == .cast(.literal(.sqlUnknownString(text)), targetType: .array(type)))
            } else {
                #expect(parsed == .literal(.queryValue(.array(array), type: .sql(.array(type)))))
            }
            try roundtrip(parsed)
        }
    }

    @Test("Array output rejects submicrosecond carriers and preadmits BYTEA expansion")
    func arrayOutputFailures() throws {
        let limits = QueryStructuralLimits(maximumRetainedBytes: 5)
        #expect(throws: SQLConstantError.self) {
            try SQLConstantSyntax.elementText(.primitive(.bytes([0, 255])), limits: limits)
        }
        #expect(try SQLConstantSyntax.elementText(.primitive(.bytes([0, 255])),
            limits: QueryStructuralLimits(maximumRetainedBytes: 6)) == #"\x00FF"#)
        let submicrosecond = try Timestamp(secondsSinceUnixEpoch: 0, nanoseconds: 1)
        #expect(throws: SQLConstantError.self) {
            try SQLConstantSyntax.elementText(.primitive(.timestamp(submicrosecond)), limits: limits)
        }
        let date = try CivilDate(year: 2026, month: 10, day: 16)
        let time = try CivilTime(hour: 1, minute: 2, second: 3, nanoseconds: 1)
        #expect(throws: SQLConstantError.self) {
            try SQLConstantSyntax.elementText(.primitive(.dateTime(CivilDateTime(date: date, time: time))), limits: limits)
        }
        #expect(try SQLConstantSyntax.sqlEraText("-0001-01-01T00:00:00Z") == "0002-01-01T00:00:00Z BC")
    }
}
