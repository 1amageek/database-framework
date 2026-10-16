import DatabaseKit
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL C/POSIX strings")
struct CanonicalSQLStringEvaluatorTests {
    private func meter(bytes: UInt64 = 1_000_000, work: UInt64 = 1_000_000,
                       clock: any StorageMonotonicClock = SystemStorageClock()) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: work,
            maximumIntermediateRows: 100, maximumIntermediateBytes: bytes, timeoutMilliseconds: 100), monotonicClock: clock)
    }

    private func execute(_ name: String, _ values: [QueryValue], types: [QueryColumnType]? = nil,
                         workMeter: DatabaseWorkMeter? = nil) throws -> DatabaseQueryScopedFieldValue {
        let selected = types ?? values.map { value in
            if case .primitive(.int32) = value { return .sql(.integer) }
            return .sql(.text)
        }
        let integer = ["length", "char_length", "character_length", "octet_length", "bit_length", "strpos", "position", "ascii"].contains(name)
        let children = zip(values, selected).map { value, type in
            CanonicalBoundExpression(expression: .literal(.queryValue(value, type: type)), type: type, isNullable: value.isNull)
        }
        let bound = CanonicalBoundExpression(expression: .function(FunctionCall(name: name, arguments: children.map(\.expression))),
            type: .sql(integer ? .integer : .text), isNullable: true, children: children)
        return try CanonicalSQLStringEvaluator.evaluate(bound, operands: values, workMeter: workMeter ?? meter())
    }

    private func result(_ name: String, _ values: [QueryValue], types: [QueryColumnType]? = nil,
                        workMeter: DatabaseWorkMeter? = nil) throws -> FieldValue {
        let owner = try execute(name, values, types: types, workMeter: workMeter)
        return owner.withValue { copy $0 }
    }

    private func text(_ value: String) -> QueryValue { .primitive(.string(value)) }
    private func integer(_ value: Int32) -> QueryValue { .primitive(.int32(value)) }

    @Test("C/POSIX case conversion changes ASCII only")
    func asciiCase() throws {
        #expect(try result("lower", [text("ÄİΣAZ")]) == .string("ÄİΣaz"))
        #expect(try result("upper", [text("ßéaz")]) == .string("ßéAZ"))
        #expect(try result("upper", [text("")]) == .string(""))
        #expect(try result("lower", [text("A  ")], types: [.sql(.char(length: 3))]) == .string("a"))
    }

    @Test("Character counts and byte counts distinguish codepoints and CHAR padding")
    func lengths() throws {
        for name in ["length", "char_length", "character_length"] {
            #expect(try result(name, [text("e\u{301}😀")]) == .int32(3))
            #expect(try result(name, [text("x  ")], types: [.sql(.char(length: 3))]) == .int32(1))
            #expect(try result(name, [text("x  ")], types: [.sql(.varchar(length: 3))]) == .int32(3))
        }
        #expect(try result("octet_length", [text("e\u{301}😀")]) == .int32(7))
        #expect(try result("bit_length", [text("e\u{301}😀")]) == .int32(56))
        #expect(try result("octet_length", [text("x  ")], types: [.sql(.char(length: 3))]) == .int32(3))
    }

    @Test("SUBSTRING clips signed one-based intervals and rejects negative lengths")
    func substrings() throws {
        for name in ["substring", "substr"] {
            #expect(try result(name, [text("e\u{301}😀"), integer(2), integer(1)]) == .string("\u{301}"))
            #expect(try result(name, [text("abcd"), integer(0), integer(2)]) == .string("a"))
            #expect(try result(name, [text("abcd"), integer(-2), integer(4)]) == .string("a"))
            #expect(try result(name, [text("abcd"), integer(-2)]) == .string("abcd"))
            #expect(try result(name, [text("abcd"), integer(.max), integer(.max)]) == .string(""))
            #expect(try result(name, [text("abcd"), integer(1), integer(0)]) == .string(""))
            #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: name, reason: "negative substring length")) {
                try result(name, [text("abcd"), integer(1), integer(-1)])
            }
        }
    }

    @Test("POSITION and STRPOS return codepoint positions without normalization")
    func positions() throws {
        for name in ["strpos", "position"] {
            #expect(try result(name, [text("e\u{301}😀"), text("😀")]) == .int32(3))
            #expect(try result(name, [text("e\u{301}"), text("é")]) == .int32(0))
            #expect(try result(name, [text("abc"), text("")]) == .int32(1))
            #expect(try result(name, [text(""), text("a")]) == .int32(0))
        }
    }

    @Test("TRIM uses a codepoint set and removes only U0020 by default")
    func trimming() throws {
        #expect(try result("trim", [text(" \t x \n ")]) == .string("\t x \n"))
        #expect(try result("btrim", [text("😀ée\u{301}é😀"), text("é😀")]) == .string("e\u{301}"))
        #expect(try result("ltrim", [text("xxabcxx"), text("x")]) == .string("abcxx"))
        #expect(try result("rtrim", [text("xxabcxx"), text("x")]) == .string("xxabc"))
        #expect(try result("btrim", [text("abc"), text("")]) == .string("abc"))
        #expect(try result("btrim", [text("xxx"), text("x")]) == .string(""))
    }

    @Test("CONCAT skips NULL and preserves CHAR and scalar type-output formatting")
    func concatenation() throws {
        let number = QueryValue.numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 4))
        #expect(try result("concat", [text("x"), .primitive(.null), integer(42), .primitive(.bool(true))],
            types: [.sql(.text), .sql(.text), .sql(.integer), .sql(.boolean)]) == .string("x42t"))
        #expect(try result("concat", [number, text("x  ")], types: [.sql(.decimal(precision: nil, scale: nil)), .sql(.char(length: 3))]) == .string("1.2300x  "))
        #expect(try result("concat", [.primitive(.uint64(.max))], types: [.primitive(.uint64)]) == .string("18446744073709551615"))
        #expect(try result("concat", [.primitive(.int8(-1)), .primitive(.int16(-2)), .primitive(.int64(-3)),
            .primitive(.uint8(4)), .primitive(.uint16(5)), .primitive(.uint32(6)), .primitive(.bool(false))],
            types: [.primitive(.int8), .sql(.smallint), .sql(.bigint), .primitive(.uint8),
                .primitive(.uint16), .primitive(.uint32), .sql(.boolean)]) == .string("-1-2-3456f"))
        #expect(try result("concat", [.primitive(.float32(1.25)), .primitive(.float64(2.5))],
            types: [.sql(.real), .sql(.doublePrecision)]) == .string("1.252.5"))
        let parsedUUID = DatabaseTypes.UUID(canonicalString: "00112233-4455-6677-8899-aabbccddeeff")
        let uuid = try #require(parsedUUID)
        #expect(try result("concat", [.primitive(.uuid(uuid)), .primitive(.bytes(ByteString(utf8:"AZ")))],
            types: [.sql(.uuid), .sql(.blob)]) == .string("00112233-4455-6677-8899-aabbccddeeff\\x415a"))
        #expect(try result("concat", [.primitive(.null), .primitive(.null)]) == .string(""))
        #expect(try result("concat_ws", [text("|"), text("x"), .primitive(.null), text(""), integer(42)],
            types: [.sql(.text), .sql(.text), .sql(.text), .sql(.text), .sql(.integer)]) == .string("x||42"))
        #expect(try result("concat_ws", [.primitive(.null), text("x")]) == .null)
        let left = CanonicalBoundExpression(expression:.null,type:.sql(.text),isNullable:true)
        let right = CanonicalBoundExpression(expression:.literal(.string("b")),type:.sql(.text),isNullable:false)
        let concatenation = CanonicalBoundExpression(expression:.concatenate(left.expression,right.expression),
            type:.sql(.text),isNullable:true,children:[left,right])
        let joined = try CanonicalSQLStringEvaluator.evaluate(concatenation,operands:[text("a"),text("b")],workMeter:meter())
        #expect(joined.withValue { $0 == .string("ab") })
        let absent = try CanonicalSQLStringEvaluator.evaluate(concatenation,operands:[.primitive(.null),text("b")],workMeter:meter())
        #expect(absent.withValue { $0.isNull })
    }

    @Test("REPLACE is nonoverlapping and an empty pattern preserves input")
    func replacing() throws {
        #expect(try result("replace", [text("aaaaa"), text("aa"), text("😀")]) == .string("😀😀a"))
        #expect(try result("replace", [text("e\u{301}e\u{301}"), text("\u{301}"), text("")]) == .string("ee"))
        #expect(try result("replace", [text("abc"), text(""), text("x")]) == .string("abc"))
        #expect(try result("replace", [text("abc"), text("x"), text("z")]) == .string("abc"))
    }

    @Test("TRANSLATE maps codepoints using the first duplicate source and deletes missing targets")
    func translating() throws {
        #expect(try result("translate", [text("abca"), text("aab"), text("XYZ")]) == .string("XZcX"))
        #expect(try result("translate", [text("é😀e\u{301}"), text("é😀\u{301}"), text("界")]) == .string("界e"))
        #expect(try result("translate", [text("abc"), text(""), text("")]) == .string("abc"))
    }

    @Test("SPLIT_PART retains empty fields and supports signed positions")
    func splitting() throws {
        for (index, expected): (Int32, String) in [(1,"a"),(2,""),(3,"b"),(4,""),(-1,""),(-2,"b"),(-4,"a"),(-5,""),(.min,"")] {
            #expect(try result("split_part", [text("a,,b,"), text(","), integer(index)]) == .string(expected))
        }
        #expect(try result("split_part", [text("abc"), text(""), integer(-1)]) == .string("abc"))
        #expect(try result("split_part", [text("abc"), text(""), integer(2)]) == .string(""))
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "split_part", reason: "field index must not be zero")) {
            try result("split_part", [text("a,b"), text(","), integer(0)])
        }
    }

    @Test("LEFT RIGHT and REVERSE operate on codepoints including negative lengths")
    func selectingEnds() throws {
        #expect(try result("left", [text("e\u{301}😀"), integer(2)]) == .string("e\u{301}"))
        #expect(try result("right", [text("e\u{301}😀"), integer(2)]) == .string("\u{301}😀"))
        #expect(try result("left", [text("abcd"), integer(-1)]) == .string("abc"))
        #expect(try result("right", [text("abcd"), integer(-1)]) == .string("bcd"))
        #expect(try result("left", [text("abcd"), integer(.min)]) == .string(""))
        #expect(try result("reverse", [text("e\u{301}😀")]) == .string("😀\u{301}e"))
    }

    @Test("Padding and repeating derive exact UTF8 output bounds before growth")
    func paddingAndRepeating() throws {
        #expect(try result("lpad", [text("é"), integer(4), text("😀x")]) == .string("😀x😀é"))
        #expect(try result("rpad", [text("é"), integer(4), text("😀x")]) == .string("é😀x😀"))
        #expect(try result("lpad", [text("abc"), integer(2), text("")]) == .string("ab"))
        #expect(try result("rpad", [text("abc"), integer(5), text("")]) == .string("abc"))
        #expect(try result("lpad", [text("a"), integer(3)]) == .string("  a"))
        #expect(try result("rpad", [text("a"), integer(-1)]) == .string(""))
        #expect(try result("repeat", [text("é"), integer(3)]) == .string("ééé"))
        #expect(try result("repeat", [text(""), integer(.max)]) == .string(""))
        #expect(try result("repeat", [text("x"), integer(-1)]) == .string(""))
    }

    @Test("ASCII and CHR distinguish empty input and invalid Unicode")
    func unicodeScalars() throws {
        #expect(try result("ascii", [text("")]) == .int32(0))
        #expect(try result("ascii", [text("😀")]) == .int32(128512))
        #expect(try result("chr", [integer(128512)]) == .string("😀"))
        for value: Int32 in [0, -1, 0xD800, 0xDFFF, 0x110000, .max] {
            #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "chr", reason: "invalid Unicode scalar")) {
                try result("chr", [integer(value)])
            }
        }
    }

    @Test("Strict NULL handling preserves selected types and validates the signature first")
    func strictNulls() throws {
        let signatures: [(String, [QueryColumnType])] = [
            ("lower",[.sql(.text)]),("upper",[.sql(.text)]),("length",[.sql(.text)]),
            ("char_length",[.sql(.text)]),("character_length",[.sql(.text)]),("octet_length",[.sql(.text)]),
            ("bit_length",[.sql(.text)]),("ascii",[.sql(.text)]),("reverse",[.sql(.text)]),("chr",[.sql(.integer)]),
            ("substring",[.sql(.text),.sql(.integer),.sql(.integer)]),("substr",[.sql(.text),.sql(.integer)]),
            ("strpos",[.sql(.text),.sql(.text)]),("position",[.sql(.text),.sql(.text)]),
            ("trim",[.sql(.text)]),("btrim",[.sql(.text)]),("ltrim",[.sql(.text)]),("rtrim",[.sql(.text)]),
            ("replace",[.sql(.text),.sql(.text),.sql(.text)]),("translate",[.sql(.text),.sql(.text),.sql(.text)]),
            ("split_part",[.sql(.text),.sql(.text),.sql(.integer)]),("left",[.sql(.text),.sql(.integer)]),
            ("right",[.sql(.text),.sql(.integer)]),("repeat",[.sql(.text),.sql(.integer)]),
            ("lpad",[.sql(.text),.sql(.integer)]),("rpad",[.sql(.text),.sql(.integer)])]
        for (name, types) in signatures {
            #expect(try result(name, types.map { _ in .primitive(.null) }, types: types) == .null)
        }
        #expect(try result("substring", [.primitive(.null),integer(1),integer(-1)], types:[.sql(.text),.sql(.integer),.sql(.integer)]) == .null)
        #expect(throws: DatabaseExpressionEvaluationError.typeMismatch(operation: "chr")) {
            try result("chr", [.primitive(.null)], types:[.sql(.text)])
        }
        #expect(try result("concat", [.primitive(.null)], types:[.sql(.json)]) == .string(""))
        #expect(try result("concat", [.primitive(.null)], types:[.sql(.jsonb)]) == .string(""))
    }

    @Test("Exact output admission and last-owner release reject one byte less")
    func admissionAndOwnership() throws {
        let probe = meter()
        var first: DatabaseQueryScopedFieldValue? = try execute("repeat", [text("é"),integer(512)], workMeter:probe)
        let peak = probe.peakIntermediateBytes
        #expect(first?.withValue { $0 == .string(String(repeating:"é",count:512)) } == true)
        var alias = first
        first = nil
        #expect(probe.retainedIntermediateBytes > 0)
        withExtendedLifetime(alias) {}
        alias = nil
        #expect(probe.retainedIntermediateBytes == 0)
        let exact = meter(bytes:peak)
        #expect(try result("repeat",[text("é"),integer(512)],workMeter:exact) == .string(String(repeating:"é",count:512)))
        #expect(exact.retainedIntermediateBytes == 0)
        let short = meter(bytes:peak-1)
        #expect(throws:DatabaseWorkLimitError.self) { try result("repeat",[text("é"),integer(512)],workMeter:short) }
        #expect(short.retainedIntermediateBytes == 0)
        let oversized = meter(bytes:1024)
        #expect(throws:DatabaseWorkLimitError.self) { try result("repeat",[text("é"),integer(.max)],workMeter:oversized) }
        #expect(oversized.retainedIntermediateBytes == 0)
    }

    @Test("Work deadline and cancellation failures release unpublished string output")
    func operationFailures() async throws {
        let limited = meter(work:4)
        #expect(throws:DatabaseWorkLimitError.self) { try result("repeat",[text("x"),integer(100_000)],workMeter:limited) }
        #expect(limited.retainedIntermediateBytes == 0)
        let expired = meter(clock:StringDeadlineClock())
        #expect(throws:DatabaseWorkLimitError.self) { try result("repeat",[text("x"),integer(100_000)],workMeter:expired) }
        #expect(expired.retainedIntermediateBytes == 0)
        let cancelled = meter()
        await #expect(throws:CancellationError.self) {
            try await withThrowingTaskGroup(of:FieldValue.self) { group in
                group.cancelAll()
                group.addTask { try result("repeat",[text("x"),integer(100_000)],workMeter:cancelled) }
                _ = try await group.next()
            }
        }
        #expect(cancelled.retainedIntermediateBytes == 0)
        #expect(try result("repeat",[text("x"),integer(2)]) == .string("xx"))
    }
}

private final class StringDeadlineClock: StorageMonotonicClock, Sendable {
    private let ticks = Mutex(Int64(0))
    var now: StorageInstant {
        ticks.withLock { value in
            defer { value += 100 }
            return StorageInstant(durationSinceReference:.milliseconds(value))
        }
    }
    func sleep(until deadline:StorageInstant) async throws(StorageClockError) { }
}
