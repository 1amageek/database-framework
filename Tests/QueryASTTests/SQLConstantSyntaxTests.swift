import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("SQL intrinsic constant syntax", .heartbeat)
struct SQLConstantSyntaxTests {
    private let limits = QueryStructuralLimits(maximumRetainedBytes: 65536, maximumTotalNodes: 2048,
                                               maximumCollectionElements: 1024)

    @Test("Direct signs classify PostgreSQL integer boundaries before negation")
    func integerBoundaries() throws {
        let cases: [(String, FieldValue, DataType)] = [
            ("2147483647", .int32(.max), .integer), ("-2147483648", .int32(.min), .integer),
            ("2147483648", .int64(2_147_483_648), .bigint),
            ("-9223372036854775808", .int64(.min), .bigint),
            ("0x_7f", .int32(127), .integer), ("-0x80000000", .int32(.min), .integer),
            ("0o_1_755", .int32(1005), .integer), ("0b1_010", .int32(10), .integer)
        ]
        for (text, value, type) in cases {
            #expect(try SQLConstantSyntax.numericLiteral(text, limits: limits) == .queryValue(.primitive(value), type: .sql(type)))
        }
    }

    @Test("Decimal and exponent lexemes always preserve exact NUMERIC and scale")
    func exactNumericForms() throws {
        let cases: [(String, ExactDecimal, Int32)] = [
            ("1.0", .init(coefficient: 1, scale: 0), 1), ("1e0", .init(coefficient: 1, scale: 0), 0),
            ("-0.000", .init(coefficient: 0, scale: 0), 3), ("1_234.500_0e-2", .init(coefficient: 12345, scale: 3), 6),
            ("9223372036854775808", .init(coefficient: 9_223_372_036_854_775_808, scale: 0), 0)
        ]
        for (text, value, scale) in cases {
            #expect(try SQLConstantSyntax.numericLiteral(text, limits: limits) == .queryValue(.numeric(QueryNumericValue(value: value, displayScale: scale)), type: .sql(.decimal(precision: nil, scale: nil))))
        }
    }

    @Test("Malformed separators, radix digits and numeric overflow are typed failures")
    func rejectedNumerics() {
        for text in ["0x__1", "0b2", "0o8", "1__2", "1_.2", "1e_2", "1e", "1e999999999999999999999999", "1e-16384", "1e131072", "123456789012345678901234567890123456789"] {
            #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.numericLiteral(text, limits: limits) }
        }
        #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.numericLiteral("1234", limits: QueryStructuralLimits(maximumRetainedBytes: 3)) }
    }

    @Test("Numeric expansion validates PostgreSQL limits and budgets before allocation")
    func numericExpansion() throws {
        let value = try QueryNumericValue(value: .init(coefficient: 1, scale: -131071), displayScale: 0)
        #expect(try SQLConstantSyntax.numericText(value, maximum: 131072).utf8.count == 131072)
        #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.numericText(value, maximum: 131071) }
        let tooLarge = try QueryNumericValue(value: .init(coefficient: 1, scale: -131072), displayScale: 0)
        #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.numericText(tooLarge, maximum: 200000) }
    }

    @Test("NULL casts retain declared precision while runtime temporal input stays lexical")
    func temporalBoundary() throws {
        let type = DataType.timestamp(withTimeZone: true, precision: 2)
        #expect(try SQLConstantSyntax.normalize(.null, as: type, limits: limits) == .queryValue(.primitive(.null), type: .sql(type)))
        #expect(try SQLConstantSyntax.normalize(.literal(.string("2026-01-01 01:02:03 Europe/Paris")), as: type, limits: limits) == nil)
        #expect(try SQLConstantSyntax.normalize(.parameter(.position(1)), as: .integer, limits: limits) == nil)
        #expect(throws: SQLConstantError.self) {
            try SQLConstantSyntax.normalize(.null, as: .time(withTimeZone: false, precision: 7), limits: limits)
        }
    }

    @Test("Malformed supported JSON and canonical interval never fall back to string")
    func malformedIntrinsicConstants() {
        for type in [DataType.json, .jsonb] {
            #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.normalize(.literal(.string("{bad}")), as: type, limits: limits) }
        }
        #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.normalize(.literal(.string("1 mons 2 days bad microseconds")), as: .interval(), limits: limits) }
        for (text, type): (String, DataType) in [("1.2.3", .decimal(precision: nil, scale: nil)), ("bad", .uuid), (#"\xzz"#, .blob)] {
            #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.normalize(.literal(.string(text)), as: type, limits: limits) }
        }
    }

    @Test("Shared lexical constructors normalize valid typed constants and retain failure positions")
    func sharedScalarLexicalConstants() throws {
        let uuid = QueryValue.primitive(.uuid(UUID(canonicalString: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")!))
        let cases: [(String, Literal)] = [
            ("SELECT UUID '{a0eebc999c0b4ef8bb6d6bb9bd380a11}'", .queryValue(uuid, type: .sql(.uuid))),
            ("SELECT CAST('a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11' AS UUID)", .queryValue(uuid, type: .sql(.uuid))),
            (#"SELECT BYTEA '\001\377\\A'"#, .queryValue(.primitive(.bytes([1, 255, 92, 65])), type: .sql(.blob))),
            (#"SELECT '\x00 FF 80'::BYTEA"#, .queryValue(.primitive(.bytes([0, 255, 128])), type: .sql(.blob))),
            ("SELECT NUMERIC ' 1.2300e2 '", .queryValue(.numeric(try QueryNumericValue(value: .init(coefficient: 123, scale: 0), displayScale: 2)), type: .sql(.decimal(precision: nil, scale: nil)))),
        ]
        for (sql, literal) in cases {
            #expect(try SQLParser().parseSelect(sql).projection == [ProjectionItem(.literal(literal))])
            #expect(try SQLConstantSyntax.normalize(.literal(literal), as: literalType(literal), limits: limits) == literal)
        }
        for sql in ["SELECT NUMERIC '1.2.3'", "SELECT UUID 'bad'", #"SELECT BYTEA '\xzz'"#] {
            do { _ = try SQLParser().parseSelect(sql); Issue.record("Malformed typed constant succeeded") }
            catch SQLParser.ParseError.literalError(_, let position) { #expect(position == 7) }
            catch { Issue.record("Expected a positioned intrinsic literal failure: \(error)") }
        }
    }

    private func literalType(_ literal: Literal) -> DataType {
        if case .queryValue(_, .sql(let type)) = literal { return type }
        preconditionFailure("Fixture literals must carry SQL declarations")
    }

    @Test("Int64 minimum interval uses signed components without absolute-value overflow")
    func minimumInterval() throws {
        let literal = try SQLConstantSyntax.normalize(.literal(.string("-2147483648 mons 2147483647 days -9223372036854775808 microseconds")), as: .interval(), limits: limits)
        guard case .queryValue(.interval(let interval), _) = literal else { Issue.record("Expected interval"); return }
        #expect(interval.microseconds == .min)
        #expect(SQLConstantSyntax.intervalText(interval) == "-2147483648 mons 2147483647 days -9223372036854775808 microseconds")
    }

    @Test("Array parser rejects ragged shape, bad bounds and malformed escapes")
    func malformedArrays() throws {
        for input in ["{{1},{2,3}}", "[1:3]={1,2}", "[2:1]={}", "{1,}", "{\"unterminated}", "{{}}", "{1} trailing", "[2147483647:2147483648]={1}"] {
            #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.normalize(.literal(.string(input)), as: .array(.integer), limits: limits) }
        }
        #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.normalize(.literal(.string("{1,2}")), as: .array(.integer), limits: QueryStructuralLimits(maximumCollectionElements: 1)) }
        let richCells = "{\"[1,2]\",\"[3,4]\",\"not json\"}"
        do {
            _ = try SQLConstantSyntax.normalize(.literal(.string(richCells)), as: .array(.jsonb),
                limits: QueryStructuralLimits(maximumTotalNodes: 5))
            Issue.record("Expected the second rich cell to exceed the cumulative array budget")
        } catch SQLConstantError.value(.resourceLimit(let resource, let actual, let maximum)) {
            #expect(resource == "nodes")
            #expect(actual > maximum)
            #expect(maximum == 1)
        } catch {
            Issue.record("Expected a resource failure before the later malformed JSON cell")
        }
        let admitted = try SQLConstantSyntax.normalize(.literal(.string("{\"[1,2]\",\"[3,4]\"}")), as: .array(.jsonb),
            limits: QueryStructuralLimits(maximumTotalNodes: 7))
        guard case .queryValue(.array(let array), _) = admitted else { Issue.record("Expected the exact cumulative budget to admit two JSONB cells"); return }
        #expect(array.elements.count == 2)
    }

    @Test("Six-dimensional arrays preserve every explicit lower bound")
    func sixDimensionalArray() throws {
        let text = "[-3:-3][0:0][1:1][7:7][-1:-1][2:2]={{{{{{42}}}}}}"
        let literal = try SQLConstantSyntax.normalize(.literal(.string(text)), as: .array(.bigint), limits: limits)
        guard case .queryValue(.array(let array), _) = literal else { Issue.record("Expected array"); return }
        #expect(array.dimensions.map(\.lowerBound) == [-3, 0, 1, 7, -1, 2])
        #expect(array.elements.elementsEqual([QueryValue.primitive(.int64(42))]))
        #expect(try SQLConstantSyntax.arrayText(array, limits: limits) == "[-3:-3][0:0][1:1][7:7][-1:-1][2:2]={{{{{{\"42\"}}}}}}")
        #expect(throws: SQLConstantError.self) { try SQLConstantSyntax.normalize(.literal(.string("{{{{{{{42}}}}}}}")), as: .array(.integer), limits: limits) }
    }

    @Test("JSONB output preserves numeric scales and is independent of object input order")
    func canonicalJSONB() throws {
        let normalized = try SQLConstantSyntax.normalize(.literal(.string("{\"long\": 1.200, \"b\": 0.00, \"a\": [true, null]}")), as: .jsonb, limits: limits)
        guard case .queryValue(.jsonb(let json), _) = normalized else { Issue.record("Expected JSONB"); return }
        #expect(try SQLConstantSyntax.jsonbText(json, limits: limits) == "{\"a\":[true,null],\"b\":0.00,\"long\":1.200}")
    }
}
