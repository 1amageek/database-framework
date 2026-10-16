import DatabaseKit
import DatabaseTypes
import StorageKit
import StorageKitSystemClock
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected scalar CAST and type output")
struct CanonicalSQLScalarCastTests {
    private let numeric = QueryColumnType.sql(.decimal(precision: nil, scale: nil))
    private func meter(bytes: UInt64 = 4_000_000, work: UInt64 = 20_000_000) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: work, maximumIntermediateBytes: bytes),
            monotonicClock: SystemStorageClock())
    }
    private func payload(_ text: String, to target: DataType,
        context: CanonicalQueryTypeBinder.CastContext = .explicit, workMeter: DatabaseWorkMeter? = nil) throws -> String {
        let scoped = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.string(text)), from: .sql(.text),
            to: .sql(target), context: context, workMeter: workMeter ?? meter())
        return try scoped.withValue { value in
            guard case .string(let text) = value else { throw DatabaseExpressionEvaluationError.invalidCast("test text") }
            return text
        }
    }
    private func value(_ text: String, to target: DataType, workMeter: DatabaseWorkMeter? = nil) throws -> QueryValue {
        try CanonicalSQLScalarCast.evaluateValue(.primitive(.string(text)), from: .sql(.text), to: .sql(target),
            context: .explicit, workMeter: workMeter ?? meter())
    }
    private func output(_ value: QueryValue, type: QueryColumnType, preserve: Bool = true,
        workMeter: DatabaseWorkMeter? = nil) throws -> String {
        try CanonicalSQLScalarText.withText(of: value, type: type, preservingCharacterPadding: preserve,
            workMeter: workMeter ?? meter()) { String($0) }
    }

    @Test("CHAR padding and VARCHAR truncation count codepoints, including combining marks")
    func characterCodepoints() throws {
        #expect(try payload("e\u{301}", to: .char(length: 3)) == "e\u{301} ")
        #expect(try payload("e\u{301}X", to: .varchar(length: 1)) == "e")
        #expect(try payload("😀Z", to: .char(length: 1)) == "😀")
        #expect(try payload("x", to: .char(length: nil)) == "x")
        #expect(try payload("xy", to: .char(length: nil)) == "x")
        #expect(try payload("x ", to: .char(length: nil), context: .assignment) == "x")
        #expect(try payload("xy", to: .varchar(length: nil)) == "xy")
    }

    @Test("Assignment rejects excess nonspaces while explicit truncates and space-only excess is legal")
    func assignmentLengths() throws {
        for target: DataType in [.char(length: 2), .varchar(length: 2)] {
            #expect(try payload("abc", to: target) == "ab")
            #expect(try payload("ab  ", to: target, context: .assignment) == "ab")
            #expect(throws: DatabaseExpressionEvaluationError.invalidCast("character length")) {
                try payload("abZ", to: target, context: .assignment)
            }
            #expect(throws: DatabaseExpressionEvaluationError.invalidCast("character length")) {
                try payload("ab\u{A0}", to: target, context: .assignment)
            }
        }
    }

    @Test("CHAR ordinary conversion trims only U+0020 while polymorphic output preserves padding")
    func characterOutput() throws {
        let value = QueryValue.primitive(.string("a\u{A0}  "))
        #expect(try output(value, type: .sql(.char(length: 4)), preserve: false) == "a\u{A0}")
        #expect(try output(value, type: .sql(.char(length: 4))) == "a\u{A0}  ")
        let scoped = try CanonicalSQLScalarCast.evaluatePayload(value, from: .sql(.char(length: 4)),
            to: .sql(.text), context: .explicit, workMeter: meter())
        scoped.withValue { #expect($0 == .string("a\u{A0}")) }
        let commonMeter = meter()
        let common = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.string("a  ")),
            from: .sql(.char(length: 3)), to: .sql(.char(length: nil)), context: .implicit, workMeter: commonMeter)
        common.withValue { #expect($0 == .string("a  ")) }
        #expect(common.retainedReservation == nil)
        #expect(commonMeter.peakIntermediateBytes == 0)
    }

    @Test("Lexical numeric preserves displayScale and delegates typmod rounding before overflow")
    func lexicalNumeric() throws {
        let inputs: [(String, Int128, Int32, Int32)] = [
            ("1.2300", 123, 2, 4), ("1.2300e2", 123, 0, 2),
            ("1.2300e-2", 123, 4, 6), (" +.50e+2  ", 5, -1, 0),
            ("-0.000", 0, 0, 3), ("1_000.230_0", 100023, 2, 4)]
        for (text, coefficient, scale, display) in inputs {
            #expect(try value(text, to: .decimal(precision: nil, scale: nil)) ==
                .numeric(QueryNumericValue(value: ExactDecimal(coefficient: coefficient, scale: scale), displayScale: display)))
        }
        #expect(try value("1.2350", to: .decimal(precision: 5, scale: 2)) ==
            .numeric(QueryNumericValue(value: ExactDecimal(coefficient: 124, scale: 2), displayScale: 2)))
        #expect(try value("-1.235", to: .decimal(precision: 5, scale: 2)) ==
            .numeric(QueryNumericValue(value: ExactDecimal(coefficient: -124, scale: 2), displayScale: 2)))
        #expect(try value("1499", to: .decimal(precision: 2, scale: -3)) ==
            .numeric(QueryNumericValue(value: ExactDecimal(coefficient: 1, scale: -3), displayScale: 0)))
        #expect(throws: DatabaseExpressionEvaluationError.numericOverflow) {
            try value("9.995", to: .decimal(precision: 3, scale: 2))
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("NUMERIC")) {
            try value("1.2.3", to: .decimal(precision: nil, scale: nil))
        }
    }

    @Test("Scalar type output preserves unsigned width and numeric scale; Boolean CAST differs from type output")
    func scalarFormatting() throws {
        #expect(try output(.primitive(.uint64(.max)), type: .primitive(.uint64)) == "18446744073709551615")
        #expect(try output(.primitive(.int64(.min)), type: .sql(.bigint)) == "-9223372036854775808")
        #expect(try output(.numeric(QueryNumericValue(value: ExactDecimal(coefficient: -123, scale: 2), displayScale: 5)),
            type: numeric) == "-1.23000")
        #expect(try output(.primitive(.bool(true)), type: .sql(.boolean)) == "t")
        let scoped = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.bool(false)), from: .sql(.boolean),
            to: .sql(.text), context: .explicit, workMeter: meter())
        scoped.withValue { #expect($0 == .string("false")) }
        #expect(try output(.primitive(.bytes(ByteString([0, 255, 92]))), type: .sql(.blob)) == "\\x00ff5c")
    }

    @Test("Floating type output follows PostgreSQL notation, shortest digits, strict midpoint and signed zero")
    func floatingOutput() throws {
        let doubles: [(Double, String)] = [
            (0, "0"), (-0.0, "-0"), (1, "1"), (1.2345678901234567, "1.2345678901234567"),
            (1e-4, "0.0001"), (1e-5, "1e-05"), (1e14, "100000000000000"), (1e15, "1e+15"),
            (Double.leastNonzeroMagnitude, "5e-324"), (Double.greatestFiniteMagnitude, "1.7976931348623157e+308"),
            (144115188075856384, "1.4411518807585638e+17")]
        for (number, expected) in doubles {
            let workMeter = meter()
            #expect(try output(.primitive(.float64(number)), type: .sql(.doublePrecision), workMeter: workMeter) == expected)
            #expect(workMeter.retainedIntermediateBytes == 0)
        }
        for (number, expected): (Float, String) in [(1e5, "100000"), (1e6, "1e+06"),
            (Float(bitPattern: 0x4c000004), "3.3554448e+07"), (Float.leastNonzeroMagnitude, "1e-45")] {
            #expect(try output(.primitive(.float32(number)), type: .sql(.real)) == expected)
        }
        #expect(throws: DatabaseExpressionEvaluationError.nonFiniteValue) {
            try output(.primitive(.float64(.infinity)), type: .primitive(.float64))
        }
    }

    @Test("Runtime UUID and BYTEA input use the same constructors as typed SQL constants")
    func sharedLexicalInput() throws {
        for text in ["a0eebc999c0b4ef8bb6d6bb9bd380a11", "{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}"] {
            let parsed = try value(text, to: .uuid)
            let expected = try QueryValue.uuid(sqlLexicalForm: text[...], maximumUTF8Count: 100)
            #expect(parsed == expected)
            #expect(try output(parsed, type: .sql(.uuid)) == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        }
        for text in ["\\x00 ff5c", "\\000\\377\\\\A"] {
            let scoped = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.string(text)), from: .sql(.text),
                to: .sql(.blob), context: .explicit, workMeter: meter())
            let expected = try QueryValue.binary(sqlLexicalForm: text[...], maximumUTF8Count: 100)
            scoped.withValue { #expect(QueryValue.primitive($0) == expected) }
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("UUID")) { try value("not-a-uuid", to: .uuid) }
    }

    @Test("Typed NULLs retain declarations and illegal descriptors fail before NULL propagation")
    func nullAndDeclarations() throws {
        #expect(try CanonicalSQLScalarCast.evaluateValue(.primitive(.null), from: .sql(.text),
            to: .sql(.decimal(precision: 5, scale: 2)), context: .explicit, workMeter: meter()) == .primitive(.null))
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("selected declaration")) {
            try CanonicalSQLScalarCast.evaluatePayload(.primitive(.null), from: .sql(.text),
                to: .sql(.char(length: 10_485_761)), context: .explicit, workMeter: meter())
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("UUID")) {
            try CanonicalSQLScalarCast.evaluateValue(.primitive(.null), from: .sql(.integer),
                to: .sql(.uuid), context: .explicit, workMeter: meter())
        }
        #expect(try value("Tr", to: .boolean) == .primitive(.bool(true)))
        #expect(try value("of", to: .boolean) == .primitive(.bool(false)))
        #expect(try value("0xff", to: .integer) == .primitive(.int32(255)))
        #expect(throws: DatabaseExpressionEvaluationError.invalidCast("integer")) { try value("1.5", to: .integer) }
        #expect(throws: DatabaseExpressionEvaluationError.numericUnderflow) { try value("1e-1000", to: .real) }
    }

    @Test("Output claims survive producer return and exact/one-short admission releases all failure state")
    func retainedAdmission() throws {
        let probe = meter()
        _ = try payload("x", to: .char(length: 1000), workMeter: probe)
        #expect(probe.retainedIntermediateBytes == 0)
        let exact = meter(bytes: probe.peakIntermediateBytes)
        do {
            let scoped = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.string("x")), from: .sql(.text),
                to: .sql(.char(length: 1000)), context: .explicit, workMeter: exact)
            #expect(exact.retainedIntermediateBytes > 0)
            scoped.withValue { #expect($0.stringValue?.utf8.count == 1000) }
        }
        #expect(exact.retainedIntermediateBytes == 0)
        let short = meter(bytes: probe.peakIntermediateBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) { try payload("x", to: .char(length: 1000), workMeter: short) }
        #expect(short.retainedIntermediateBytes == 0)
        let noWork = meter(work: 0)
        #expect(throws: DatabaseWorkLimitError.self) { try payload("x", to: .char(length: 1000), workMeter: noWork) }
        #expect(noWork.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation and deadline failures preserve causes and release temporary formatting owners")
    func cancellationAndDeadline() async throws {
        let cancelled = meter()
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try payload("x", to: .char(length: 1000), workMeter: cancelled)
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        #expect(cancelled.retainedIntermediateBytes == 0)
        let deadline = DatabaseWorkMeter(budget: ExecutionBudget(maximumWorkUnits: 20_000_000,
            maximumIntermediateBytes: 4_000_000, timeoutMilliseconds: 5), monotonicClock: CastDeadlineClock())
        #expect(throws: DatabaseWorkLimitError.deadline(stage: .expressionEvaluation)) {
            try payload("x", to: .char(length: 1000), workMeter: deadline)
        }
        #expect(deadline.retainedIntermediateBytes == 0)
    }
}

private final class CastDeadlineClock: StorageMonotonicClock, Sendable {
    private let ticks = Mutex(Int64(0))
    var now: StorageInstant {
        ticks.withLock { value in
            defer { value += 1 }
            return StorageInstant(durationSinceReference: .milliseconds(value))
        }
    }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) { }
}
