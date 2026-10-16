import DatabaseKit
import DatabaseTypes
import QueryAST
import Testing

@Suite("SQL temporal syntax and source carriage")
struct SQLTemporalSyntaxTests {
    @Test func currentValuesAndQuotedIdentity() throws {
        let spellings = ["CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP"]
        for (spelling, kind) in zip(spellings, SQLCurrentValueKind.allCases) {
            #expect(try roundTrip(spelling) == .currentValue(kind: kind, precision: nil))
            if kind != .date {
                for precision in [0, 3, 6] {
                    #expect(try roundTrip(spelling + "(\(precision))") == .currentValue(kind: kind, precision: precision))
                }
            }
        }
        let generic = Expression.function(FunctionCall(name: "current_date", arguments: []))
        #expect(try roundTrip(#""current_date"()"#) == generic)
        #expect(try SQLSerializer().serialize(generic) == #""current_date"()"#)
        for sql in ["CURRENT_DATE(0)", "CURRENT_TIME()", "CURRENT_TIME(-1)", "CURRENT_TIMESTAMP(7)", "LOCALTIME($1)", "LOCALTIMESTAMP(1.0)"] {
            #expect(throws: SQLParser.ParseError.self) { try expression(sql) }
        }
    }

    @Test func extractAndOrdinaryFunctions() throws {
        let source = Expression.col("source")
        #expect(try roundTrip("EXTRACT(YEAR FROM source)") == .function(FunctionCall(name: "extract", arguments: [text("year"), source])))
        #expect(try roundTrip("EXTRACT('epoch' FROM source)") == .function(FunctionCall(name: "extract", arguments: [text("epoch"), source])))
        for name in ["date_part", "date_trunc", "date_bin", "age", "make_date", "make_time", "make_timestamp", "make_timestamptz", "to_char", "to_date", "to_timestamp", "now", "transaction_timestamp", "statement_timestamp", "clock_timestamp"] {
            #expect(try roundTrip(name + "(source)") == .function(FunctionCall(name: name, arguments: [source])))
        }
        for sql in ["EXTRACT(FROM source)", "EXTRACT(year source)", "EXTRACT(year FROM)"] {
            #expect(throws: SQLParser.ParseError.self) { try expression(sql) }
        }
    }

    @Test func timezonePrecedenceAndChaining() throws {
        func zone(_ value: Expression, _ zone: Expression) -> Expression {
            .function(FunctionCall(name: "timezone", arguments: [zone, value]))
        }
        #expect(try roundTrip("a AT TIME ZONE b AT TIME ZONE c") == zone(zone(.col("a"), .col("b")), .col("c")))
        #expect(try roundTrip("a + b AT TIME ZONE c * d") == .add(.col("a"), .multiply(zone(.col("b"), .col("c")), .col("d"))))
        #expect(try roundTrip("-a AT TIME ZONE b") == zone(.negate(.col("a")), .col("b")))
        #expect(throws: SQLParser.ParseError.self) { try expression("a AT TIME b") }
    }

    @Test func intervalModifiersAndLexicalBoundary() throws {
        let names = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "YEAR TO MONTH", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND", "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE TO SECOND"]
        for (name, field) in zip(names, SQLIntervalFields.allCases) {
            let type = DataType.interval(fields: field)
            #expect(try roundTrip("NULL::INTERVAL " + name) == .literal(.queryValue(.primitive(.null), type: .sql(type))))
            #expect(try roundTrip("INTERVAL '1' " + name) == .cast(.literal(.sqlUnknownString("1")), targetType: type))
        }
        #expect(try roundTrip("INTERVAL(3) '1.2345'") == .cast(.literal(.sqlUnknownString("1.2345")), targetType: .interval(precision: 3)))
        #expect(try roundTrip("INTERVAL '1.2345' DAY TO SECOND(3)") == .cast(.literal(.sqlUnknownString("1.2345")), targetType: .interval(fields: .dayToSecond, precision: 3)))
        for sql in ["NULL::INTERVAL YEAR TO DAY", "NULL::INTERVAL YEAR(3)", "NULL::INTERVAL SECOND(7)", "INTERVAL(3) '1' SECOND(2)", "INTERVAL '1' MONTH TO YEAR"] {
            #expect(throws: SQLParser.ParseError.self) { try expression(sql) }
        }
        #expect(try roundTrip("DATE '2026-01-01'") == .cast(.literal(.sqlUnknownString("2026-01-01")), targetType: .date))
        #expect(try roundTrip("TIMESTAMPTZ 'infinity'") == .cast(.literal(.sqlUnknownString("infinity")), targetType: .timestamp(withTimeZone: true)))
    }

    @Test func makeIntervalNamedArguments() throws {
        func call(_ arguments: [Expression], schema: String? = nil) -> Expression {
            .function(FunctionCall(name: "make_interval", arguments: arguments, schema: schema))
        }
        var expected = Array(repeating: integer(0), count: 6) + [.literal(.queryValue(.primitive(.float64(0)), type: .sql(.doublePrecision)))]
        expected[3] = integer(2)
        expected[4] = .parameter(.position(1))
        #expect(try roundTrip("make_interval(hours => $1, days := 2)") == call(expected))
        #expect(try roundTrip(#"pg_catalog."make_interval"(hours => $1, days => 2)"#) == call(expected, schema: "pg_catalog"))
        #expect(try roundTrip("make_interval(1, 2)") == call([integer(1), integer(2)]))
        for sql in ["make_interval(days => 1, days => 2)", "make_interval(bad => 1)", "make_interval(days => 1, 2)", "make_interval(1, years => 2)", #""MAKE_INTERVAL"(days => 1)"#] {
            #expect(throws: SQLParser.ParseError.self) { try expression(sql) }
        }
    }

    @Test func temporalSourceOccurrences() throws {
        let sql = "SELECT $1 AT TIME ZONE $2"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let call = try child(parsed, parsed.root, .projection, 0)
        #expect(try source(sql, parsed, child(parsed, call, .argument, 0)) == "$2")
        #expect(try source(sql, parsed, child(parsed, call, .argument, 1)) == "$1")
        let extractSQL = "SELECT EXTRACT(YEAR FROM $1)"
        let extract = try SQLParser().parseSelectWithSource(extractSQL)
        let extractCall = try child(extract, extract.root, .projection, 0)
        #expect(try source(extractSQL, extract, child(extract, extractCall, .argument, 0)) == "YEAR")
        #expect(try source(extractSQL, extract, child(extract, extractCall, .argument, 1)) == "$1")
        let namedSQL = "SELECT make_interval(hours => $1, days => $2)"
        let named = try SQLParser().parseSelectWithSource(namedSQL)
        let namedCall = try child(named, named.root, .projection, 0)
        #expect(try source(namedSQL, named, child(named, namedCall, .argument, 3)) == "$2")
        #expect(try source(namedSQL, named, child(named, namedCall, .argument, 4)) == "$1")
        #expect(try source(namedSQL, named, child(named, namedCall, .argument, 0)) == "make_interval")
        let current = try SQLParser().parseSelectWithSource("SELECT CURRENT_TIMESTAMP(3)")
        let value = try child(current, current.root, .projection, 0)
        #expect(current.occurrences.child(of: value, field: .argument, index: 0) == nil)
    }

    @Test func temporalLiteralEmissionAndBounds() throws {
        let endpoint = QueryValue.time(try QueryTimeValue(microsecondsSinceMidnight: 86_400_000_000))
        #expect(try SQLSerializer().serialize(Literal.queryValue(endpoint)) == "TIME '24:00:00'")
        let normal = QueryTimeWithTimeZoneValue(time: try QueryTimeValue(microsecondsSinceMidnight: 1), utcOffsetSeconds: -1)
        #expect(try SQLSerializer().serialize(Literal.queryValue(.timeWithTimeZone(normal))) == "TIME WITH TIME ZONE '00:00:00.000001-00:00:01'")
        let wide = QueryTimeWithTimeZoneValue(time: try QueryTimeValue(microsecondsSinceMidnight: 0), utcOffsetSeconds: .max)
        let wideSQL = try SQLSerializer().serialize(Literal.queryValue(.timeWithTimeZone(wide)))
        #expect(wideSQL.contains("timezone"))
        #expect(wideSQL.contains("2147483647000000 microseconds"))
        _ = try expression(wideSQL)
        let wideEndpoint = QueryTimeWithTimeZoneValue(time: try QueryTimeValue(microsecondsSinceMidnight: 86_400_000_000), utcOffsetSeconds: .min)
        #expect(throws: SQLSerializationError.self) { try SQLSerializer().serialize(Literal.queryValue(.timeWithTimeZone(wideEndpoint))) }
        for value: QueryValue in [.dateInfinity(.negative), .timestampInfinity(.positive, withTimeZone: false), .timestampInfinity(.positive, withTimeZone: true), .intervalInfinity(.negative)] {
            let sql = try SQLSerializer().serialize(Literal.queryValue(value))
            #expect(sql.contains("infinity"))
            _ = try roundTrip(sql)
        }
        let value = Expression.currentValue(kind: .date, precision: nil)
        #expect(try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: 12)).serialize(value) == "CURRENT_DATE")
        #expect(throws: SQLSerializationError.self) { try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: 11)).serialize(value) }
    }

    private func text(_ value: String) -> Expression { .literal(.queryValue(.primitive(.string(value)), type: .sql(.text))) }
    private func integer(_ value: Int32) -> Expression { .literal(.queryValue(.primitive(.int32(value)), type: .sql(.integer))) }
    private func expression(_ sql: String) throws -> Expression {
        let query = try SQLParser().parseSelect("SELECT " + sql)
        guard case .items(let items) = query.projection, items.count == 1 else { throw SQLSerializationError.invalidStructure("Expected one projection") }
        return items[0].expression
    }
    private func roundTrip(_ sql: String) throws -> Expression {
        let value = try expression(sql)
        #expect(try expression(SQLSerializer().serialize(value)) == value)
        return value
    }
    private func child(_ parsed: SQLParsedSelect, _ parent: QueryOccurrenceID, _ field: QueryOccurrenceField, _ index: UInt32) throws -> QueryOccurrenceID {
        try #require(parsed.occurrences.child(of: parent, field: field, index: index))
    }
    private func source(_ sql: String, _ parsed: SQLParsedSelect, _ occurrence: QueryOccurrenceID) throws -> String {
        let range = try #require(parsed.sourceMap[occurrence]).range
        return String(decoding: Array(sql.utf8)[range.start..<range.end], as: UTF8.self)
    }
}
