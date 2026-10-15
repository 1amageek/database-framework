import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Selected SQL decimal math")
struct CanonicalSQLDecimalMathTests {
    private func meter(bytes: UInt64 = 1_000_000, work: UInt64 = 100_000_000,
                       clock: DecimalMathClock = DecimalMathClock()) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 100, maximumWorkUnits: work,
            maximumIntermediateRows: 100, maximumIntermediateBytes: bytes,
            timeoutMilliseconds: 1_000), monotonicClock: clock)
    }

    private func numeric(_ coefficient: Int128, scale: Int32 = 0, display: Int32? = nil) throws -> QueryNumericValue {
        try QueryNumericValue(value: ExactDecimal(coefficient: coefficient, scale: scale),
                              displayScale: display ?? max(0, scale))
    }

    @Test("Retained PostgreSQL math vectors preserve exact values and scales")
    func referenceVectors() throws {
        let cases: [(String, [QueryNumericValue], QueryNumericValue)] = [
            ("SQRT", [try numeric(4, display: 4)], try numeric(2, display: 15)),
            ("SQRT", [try numeric(1, scale: 12)], try numeric(1, scale: 6, display: 21)),
            ("SQRT", [try numeric(25, scale: 1, display: 4)], try numeric(158113883008419, scale: 14, display: 15)),
            ("EXP", [try numeric(0, display: 3)], try numeric(1, display: 16)),
            ("EXP", [try numeric(25, scale: 1, display: 4)], try numeric(12182493960703473, scale: 15)),
            ("LN", [try numeric(1, display: 3)], try numeric(0, display: 16)),
            ("LN", [try numeric(25, scale: 1, display: 4)], try numeric(9162907318741551, scale: 16)),
            ("LOG", [try numeric(2, display: 2), try numeric(64, display: 3)], try numeric(6, display: 16)),
            ("LOG", [try numeric(100, display: 2)], try numeric(2, display: 16)),
            ("LOG", [try numeric(25, scale: 1, display: 4)], try numeric(3979400086720376, scale: 16)),
            ("LOG10", [try numeric(25, scale: 1, display: 4)], try numeric(3979400086720376, scale: 16)),
            ("POWER", [try numeric(2, display: 3), try numeric(5, scale: 1, display: 2)], try numeric(1414213562373095, scale: 15, display: 16)),
            ("POWER", [try numeric(2, display: 3), try numeric(3, display: 2)], try numeric(8, display: 16)),
            ("POWER", [try numeric(2, display: 3), try numeric(-3, display: 2)], try numeric(125, scale: 3, display: 16)),
            ("POWER", [try numeric(0, display: 3), try numeric(0, display: 2)], try numeric(1, display: 16)),
            ("POWER", [try numeric(9), try numeric(4)], try numeric(6561, display: 13)),
            ("POWER", [try numeric(1, scale: 15), try numeric(-2)], try numeric(1, scale: -30, display: 15)),
            ("POWER", [try numeric(-1, scale: 15), try numeric(-2)], try numeric(1, scale: -30, display: 15)),
            ("POWER", [try numeric(-1, scale: 10), try numeric(-3)], try numeric(-1, scale: -30, display: 10)),
            ("POWER", [try numeric(1, scale: 15), try numeric(2)], try numeric(1, scale: 30, display: 46)),
            ("POWER", [try numeric(1, scale: -15), try numeric(-2)], try numeric(1, scale: 30, display: 46)),
            ("POWER", [try numeric(-1, scale: -15), try numeric(3)], try numeric(-1, scale: -45)),
        ]
        for (function, operands, expected) in cases {
            let workMeter = meter()
            let actual = try CanonicalSQLDecimalMath.evaluate(function: function, operands: operands, workMeter: workMeter)
            #expect(actual == expected, "\(function): \(operands)")
            #expect(workMeter.retainedIntermediateBytes == 0)
            #expect(workMeter.consumedWorkUnits > 0)
        }
    }

    @Test("Decimal domains and logarithm base one fail explicitly")
    func domains() throws {
        let cases: [(String, [QueryNumericValue], DatabaseExpressionEvaluationError)] = [
            ("SQRT", [try numeric(-1)], .numericDomain(operation: "SQRT")),
            ("LN", [try numeric(0)], .numericDomain(operation: "LN")),
            ("LN", [try numeric(-1)], .numericDomain(operation: "LN")),
            ("LOG", [try numeric(1), try numeric(2)], .divisionByZero),
            ("LOG", [try numeric(-2), try numeric(2)], .numericDomain(operation: "LOG")),
            ("POWER", [try numeric(-2), try numeric(5, scale: 1)], .numericDomain(operation: "POWER")),
            ("POWER", [try numeric(0), try numeric(-1)], .numericDomain(operation: "POWER")),
        ]
        for (function, operands, expected) in cases {
            let workMeter = meter()
            #expect(throws: expected) {
                try CanonicalSQLDecimalMath.evaluate(function: function, operands: operands, workMeter: workMeter)
            }
            #expect(workMeter.retainedIntermediateBytes == 0)
        }
    }

    @Test("Exact widened arithmetic admits final cancellation and AVG")
    func widenedIntermediate() throws {
        let workMeter = meter()
        do {
            let maximum: Int128 = 99_999_999_999_999_999_999_999_999_999_999_999_999
            let input = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: maximum, scale: 0), workMeter: workMeter)
            let widened = try input.adding(input)
            #expect(widened.decimalDigitCount == 39)
            #expect(throws: ExactDecimalError.numericOverflow) { try widened.exactDecimal() }
            #expect(try widened.subtracting(input).exactDecimal() == ExactDecimal(coefficient: maximum, scale: 0))
            #expect(try widened.divided(by: 2, toScale: 0).exactDecimal() == ExactDecimal(coefficient: maximum, scale: 0))
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Division rounds signed ties and metadata follows normalized groups")
    func divisionAndMetadata() throws {
        let workMeter = meter()
        for coefficient: Int128 in [-25, 25] {
            let value = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: coefficient, scale: 1), workMeter: workMeter)
            #expect(try value.divided(by: 1, toScale: 0).exactDecimal().coefficient == (coefficient < 0 ? -3 : 3))
            #expect(try value.divided(by: 1, toScale: 0, rounding: .towardZero).exactDecimal().coefficient == (coefficient < 0 ? -2 : 2))
        }
        for value in [ExactDecimal(coefficient: 1, scale: 12), ExactDecimal(coefficient: 12345, scale: 2), ExactDecimal(coefficient: 5, scale: -3)] {
            let wide = try CanonicalSQLDecimalWorkspaceValue(value, workMeter: workMeter)
            let expected = CanonicalSQLNumericScale.leading(value)
            #expect(wide.leadingBase10000.weight == expected.weight)
            #expect(wide.leadingBase10000.digit == expected.digit)
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Immutable aliases retain the claim until the last owner ends")
    func copiedOwnership() throws {
        let workMeter = meter()
        var original: CanonicalSQLDecimalWorkspaceValue? = try .init(ExactDecimal(coefficient: 12, scale: 0), workMeter: workMeter)
        var alias = original
        let held = workMeter.retainedIntermediateBytes
        original = nil
        #expect(workMeter.retainedIntermediateBytes == held)
        #expect(try alias?.exactDecimal() == ExactDecimal(coefficient: 12, scale: 0))
        alias = nil
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Scalar scratch is admitted at the exact measured boundary")
    func exactScratchBoundary() throws {
        let cases = [("SQRT", [try numeric(25, scale: 1)]),
                     ("POWER", [try numeric(1, scale: 15), try numeric(-2)])]
        for (function, operands) in cases {
            let baseline = meter()
            _ = try CanonicalSQLDecimalMath.evaluate(function: function, operands: operands, workMeter: baseline)
            let peak = baseline.peakIntermediateBytes
            let exact = meter(bytes: peak)
            _ = try CanonicalSQLDecimalMath.evaluate(function: function, operands: operands, workMeter: exact)
            #expect(exact.retainedIntermediateBytes == 0)
            let short = meter(bytes: peak - 1)
            #expect(throws: DatabaseWorkLimitError.self) {
                try CanonicalSQLDecimalMath.evaluate(function: function, operands: operands, workMeter: short)
            }
            #expect(short.retainedIntermediateBytes == 0)
        }
    }

    @Test("Extreme scale alignment fails admission before proportional growth")
    func extremeScaleAdmission() throws {
        let workMeter = meter(bytes: 1_000)
        do {
            let left = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: 1, scale: Int32.min), workMeter: workMeter)
            let right = try CanonicalSQLDecimalWorkspaceValue(ExactDecimal(coefficient: 1, scale: Int32.max), workMeter: workMeter)
            #expect(throws: DatabaseWorkLimitError.self) { try left.adding(right) }
            #expect(workMeter.peakIntermediateBytes < 1_000)
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Work exhaustion and deadline propagate and release decimal scratch")
    func workAndDeadline() throws {
        let input = try numeric(2)
        let workMeter = meter(work: 100)
        #expect(throws: DatabaseWorkLimitError.self) {
            try CanonicalSQLDecimalMath.evaluate(function: "SQRT", operands: [input], workMeter: workMeter)
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
        let clock = DecimalMathClock()
        let expired = meter(clock: clock)
        clock.advance()
        #expect(throws: DatabaseWorkLimitError.deadline(stage: .expressionEvaluation)) {
            try CanonicalSQLDecimalMath.evaluate(function: "EXP", operands: [input], workMeter: expired)
        }
        #expect(expired.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation stays cancellation and releases all decimal owners")
    func cancellation() async throws {
        let workMeter = meter()
        let input = try numeric(2)
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try CanonicalSQLDecimalMath.evaluate(function: "SQRT", operands: [input], workMeter: workMeter)
        }
        do { _ = try await task.value; Issue.record("Expected cancellation") }
        catch is CancellationError { }
        #expect(workMeter.retainedIntermediateBytes == 0)
    }
}

private final class DecimalMathClock: StorageMonotonicClock, Sendable {
    private let instant = Mutex(StorageInstant(durationSinceReference: .zero))
    var now: StorageInstant { instant.withLock { $0 } }
    func advance() { instant.withLock { $0 = $0.advanced(by: .seconds(1)) } }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) {
        instant.withLock { $0 = deadline }
    }
}
