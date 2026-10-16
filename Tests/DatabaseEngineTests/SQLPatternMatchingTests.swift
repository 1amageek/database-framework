import DatabaseKit
import StorageKit
import Synchronization
import Testing
@testable import DatabaseEngine

@Suite("SQL pattern matching")
struct SQLPatternMatchingTests {
    private let limits = QueryValueLimits(maximumRetainedBytes: 10_000_000, maximumTotalNodes: 1_000_000,
                                         maximumNestingDepth: 100, maximumCollectionElements: 1_000_000)
    private func meter(bytes: UInt64 = 10_000_000, work: UInt64 = 10_000_000,
                       clock: PatternClock = PatternClock()) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1000, maximumWorkUnits: work,
            maximumIntermediateRows: 1000, maximumIntermediateBytes: bytes, timeoutMilliseconds: 1000), monotonicClock: clock)
    }
    private func program(_ pattern: String, flags: String? = nil, operation: SQLPatternOperation = .predicate,
                         meter: DatabaseWorkMeter) throws -> SQLPatternProgram {
        try .compile(pattern: .admitting(pattern, workMeter: meter), syntax: .ere(caseInsensitive: false),
                     flags: flags.map { try .admitting($0, workMeter: meter) }, operation: operation)
    }
    private func text(_ value: DatabaseQueryScopedFieldValue) -> String? {
        value.withValue { if case .string(let text) = $0 { return text }; return nil }
    }
    private func evaluate(_ name: String, _ values: [QueryValue], meter: DatabaseWorkMeter)
        throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let children = try values.map { value -> CanonicalBoundExpression in
            let type: QueryColumnType = value.isNull ? .sql(.text) : try CanonicalQueryTypeBinder.type(of: value)
            return .init(expression: .literal(.queryValue(value, type: type)), type: type, isNullable: value.isNull)
        }
        let expression = DatabaseKit.Expression.function(FunctionCall(name: name, arguments: children.map(\.expression)))
        return try CanonicalSQLPatternEvaluator.evaluate(.init(expression: expression, type: .sql(.text),
            isNullable: true, children: children), operands: values, workMeter: meter, limits: limits)
    }
    private func strings(_ value: QueryArrayValue) -> [String?] {
        value.elements.map { if case .primitive(.string(let string)) = $0 { return string }; return nil }
    }

    @Test("LIKE uses codepoints, explicit escapes and ASCII ILIKE")
    func likeSemantics() throws {
        let meter = meter()
        func matches(_ input: String, _ pattern: String, escape: String? = "\\", kind: PatternMatchKind = .like) throws -> Bool {
            try CanonicalSQLPatternEvaluator.predicate(subject: input, pattern: pattern, escape: escape, kind: kind, workMeter: meter)
        }
        #expect(try matches("e\u{301}", "__"))
        #expect(try !matches("e\u{301}", "_"))
        #expect(try matches("a%b", "a#%b", escape: "#"))
        #expect(try matches("a\\b", "a\\b", escape: nil))
        #expect(try matches("Ab", "aB", kind: .caseInsensitiveLike))
        #expect(try !matches("É", "é", kind: .caseInsensitiveLike))
        #expect(try matches("a\nb", "a_b"))
        let padded = QueryValue.primitive(.string("a  "))
        let subject = CanonicalBoundExpression(expression: .literal(.queryValue(padded, type: .sql(.char(length: 3)))),
            type: .sql(.char(length: 3)), isNullable: false)
        for (kind, pattern): (PatternMatchKind, String) in [(.like, "a"), (.regularExpression, "^a$")] {
            let value = QueryValue.primitive(.string(pattern))
            let operand = CanonicalBoundExpression(expression: .literal(.queryValue(value, type: .sql(.text))), type: .sql(.text), isNullable: false)
            let bound = CanonicalBoundExpression(expression: .patternMatch(subject: subject.expression,
                pattern: operand.expression, escape: nil, kind: kind), type: .sql(.boolean), isNullable: false,
                children: [subject, operand])
            #expect(try CanonicalSQLPatternEvaluator.evaluate(bound, operands: [padded, value], workMeter: meter, limits: limits).value == .primitive(.bool(false)))
        }
        let anchor = QueryValue.primitive(.string("^a$"))
        let anchorChild = CanonicalBoundExpression(expression: .literal(.queryValue(anchor, type: .sql(.text))), type: .sql(.text), isNullable: false)
        let function = CanonicalBoundExpression(expression: .function(FunctionCall(name: "regexp_like", arguments: [subject.expression, anchorChild.expression])),
            type: .sql(.boolean), isNullable: false, children: [subject, anchorChild])
        #expect(try CanonicalSQLPatternEvaluator.evaluate(function, operands: [padded, anchor], workMeter: meter, limits: limits).value == .primitive(.bool(true)))
    }

    @Test("LIKE trailing escape fails only when reached with input remaining")
    func deferredEscape() throws {
        let meter = meter()
        #expect(try !CanonicalSQLPatternEvaluator.predicate(subject: "a", pattern: "a\\", escape: "\\", kind: .like, workMeter: meter))
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArgument(function: "pattern predicate", reason: "invalid escape")) {
            try CanonicalSQLPatternEvaluator.predicate(subject: "ab", pattern: "a\\", escape: "\\", kind: .like, workMeter: meter)
        }
    }

    @Test("ERE grammar covers alternation repetition ranges and POSIX classes")
    func ereGrammar() throws {
        let meter = meter()
        for (pattern, source, expected) in [
            ("^(ab|cd){2,3}$", "abcdab", true), ("a{2,}", "xaaa", true),
            ("^[[:alpha:]][[:digit:]]+$", "A123", true), ("^[^a-c]+$", "xyz", true),
            ("^a?b+$", "bbb", true), ("^[=]$", "=", true), ("a.b", "a\nb", true),
            ("[[]", "[", true), ("^[a[]$", "[", true), ("^[a[]$", "a", true),
            ("^[A-[]$", "[", true), ("^[[]$", "]", false), ("^[[]$", "[[", false),
            ("^[[]$", "", false), ("a{0}", "", true),
            ("^a{255}$", String(repeating: "a", count: 255), true),
            ("^a{255,}$", String(repeating: "a", count: 256), true),
        ] {
            #expect(try program(pattern, meter: meter).matches(.admitting(source, workMeter: meter)) == expected)
        }
    }

    @Test("POSIX selects earliest longest whole match and capture priority")
    func capturePriority() throws {
        let meter = meter()
        let expression = try program("(a|ab)", operation: .match, meter: meter)
        var matches = try expression.makeMatchIterator(in: .admitting("zab", workMeter: meter))
        let nextMatch = try matches.next()
        let match = try #require(nextMatch)
        #expect(try text(match.wholeMatch.materialize()) == "ab")
        #expect(try text(match.capture(at: 1)!.materialize()) == "ab")
        let repeated = try program("(a*)(a*)", operation: .match, meter: meter)
        var second = try repeated.makeMatchIterator(in: .admitting("aa", workMeter: meter))
        let nextCapture = try second.next()
        let capture = try #require(nextCapture)
        #expect(try text(capture.capture(at: 1)!.materialize()) == "aa")
        #expect(try text(capture.capture(at: 2)!.materialize()) == "")
    }

    @Test("Unmatched captures remain NULL and empty participating groups remain text")
    func nullableCaptures() throws {
        let meter = meter()
        var iterator = try program("(a)?(b*)", operation: .match, meter: meter)
            .makeMatchIterator(in: .admitting("b", workMeter: meter))
        let nextMatch = try iterator.next()
        let match = try #require(nextMatch)
        let output = try match.materializeTextArray(limits: limits)
        #expect(strings(output.value) == [nil, "b"])
        #expect(output.value.elementType == .text)
        #expect(try match.capture(at: 1) == nil)
        #expect(throws: SQLPatternError.invalidCaptureIndex) { try match.capture(at: 3) }
    }

    @Test("C/POSIX classes and case exclude Unicode folding while literals remain Unicode")
    func asciiClasses() throws {
        let meter = meter()
        #expect(try !program("^[[:alpha:]]+$", meter: meter).matches(.admitting("é", workMeter: meter)))
        #expect(try program("^雪é$", meter: meter).matches(.admitting("雪é", workMeter: meter)))
        #expect(try !program("^é$", flags: "i", meter: meter).matches(.admitting("É", workMeter: meter)))
        #expect(try program("^[a-z]+$", flags: "i", meter: meter).matches(.admitting("ABC", workMeter: meter)))
    }

    @Test("Flags preserve order newline modes literal mode and official ERE e")
    func flags() throws {
        let meter = meter()
        #expect(try program("a", flags: "ci", meter: meter).matches(.admitting("A", workMeter: meter)))
        #expect(try !program("a", flags: "ic", meter: meter).matches(.admitting("A", workMeter: meter)))
        #expect(try program("(a|ab)", flags: "e", meter: meter).matches(.admitting("ab", workMeter: meter)))
        for flags in [nil, "e"] {
            #expect(try program("\\q", flags: flags, meter: meter).matches(.admitting("q", workMeter: meter)))
            #expect(try !program("^\\q$", flags: flags, meter: meter).matches(.admitting("\\q", workMeter: meter)))
        }
        #expect(try program("a.b", flags: "q", meter: meter).matches(.admitting("a.b", workMeter: meter)))
        #expect(try !program("^a.b$", flags: "n", meter: meter).matches(.admitting("a\nb", workMeter: meter)))
        #expect(try program("^b$", flags: "w", meter: meter).matches(.admitting("a\nb", workMeter: meter)))
        #expect(try !program("^b$", flags: "p", meter: meter).matches(.admitting("a\nb", workMeter: meter)))
        #expect(try program(" a # comment\n b ", flags: "x", meter: meter).matches(.admitting("ab", workMeter: meter)))
    }

    @Test("ARE constructs unknown flags and function-incompatible global flags fail typed")
    func unsupportedSyntax() throws {
        let meter = meter()
        for pattern in ["(?=a)", "(?<=a)", "(a)\\1", "a+?", "a++", "\\p{L}"] {
            #expect(throws: SQLPatternError.self) { try program(pattern, meter: meter) }
        }
        for operation: SQLPatternOperation in [.predicate, .match, .split] {
            #expect(throws: SQLPatternError.self) { try program("a", flags: "g", operation: operation, meter: meter) }
        }
        for pattern in ["[", "[[", "[[:alpha]", "[[.a]", "[[=a]", "[[:unknown:]]", "[A-[:digit:]]",
                        "a{256}", "a{0,256}", "a{256,}", "a{255,254}", "a{255"] {
            do {
                _ = try program(pattern, meter: meter)
                Issue.record("Expected invalid ERE pattern: \(pattern)")
            } catch let error as SQLPatternError {
                guard case .invalidPattern = error else { throw error }
            }
        }
        for pattern in ["\\d", "\\w", "\\P{L}"] {
            do {
                _ = try program(pattern, meter: meter)
                Issue.record("Expected explicitly unsupported ERE extension: \(pattern)")
            } catch let error as SQLPatternError {
                guard case .unsupportedPattern = error else { throw error }
            }
        }
        #expect(throws: SQLPatternError.self) { try program("a", flags: "b", meter: meter) }
    }

    @Test("Replacement defaults first and supports global start and explicit N")
    func replacementSelection() throws {
        let meter = meter()
        let source = try SQLPatternInput.admitting("abcabc", workMeter: meter)
        let replacement = try SQLPatternInput.admitting("X", workMeter: meter)
        let first = try program("a", operation: .replace, meter: meter)
        #expect(try text(first.replacing(in: source, with: replacement)) == "Xbcabc")
        let all = try program("a", flags: "g", operation: .replace, meter: meter)
        #expect(try text(all.replacing(in: source, with: replacement)) == "XbcXbc")
        #expect(try text(all.replacing(in: source, with: replacement, start: 2, occurrence: 1)) == "abcXbc")
        #expect(try text(first.replacing(in: source, with: replacement, occurrence: 0)) == "XbcXbc")
        #expect(throws: SQLPatternError.invalidStart) { try first.replacing(in: source, with: replacement, start: 0) }
        #expect(throws: SQLPatternError.invalidOccurrence) { try first.replacing(in: source, with: replacement, occurrence: -1) }
    }

    @Test("Replacement preserves unknown and trailing escapes and empties missing captures")
    func replacementEscapes() throws {
        let meter = meter()
        let pattern = try program("(a)(b)?", operation: .replace, meter: meter)
        let source = try SQLPatternInput.admitting("a", workMeter: meter)
        let replacement = try SQLPatternInput.admitting("\\&|\\1|\\2|\\9|\\q|\\\\|\\", workMeter: meter)
        #expect(try text(pattern.replacing(in: source, with: replacement)) == "a|a|||\\q|\\|\\")
    }

    @Test("Zero-width matches advance and split suppresses degenerate boundaries")
    func zeroWidthAndSplit() throws {
        let meter = meter()
        let input = try SQLPatternInput.admitting("ab", workMeter: meter)
        let expression = try program("", flags: "g", operation: .matches, meter: meter)
        var matches = try expression.makeMatchIterator(in: input)
        var count = 0
        while try matches.next() != nil { count += 1 }
        #expect(count == 3)
        let replacement = try program("", flags: "g", operation: .replace, meter: meter)
        #expect(try text(replacement.replacing(in: input, with: .admitting("X", workMeter: meter))) == "XaXbX")
        let split = try program("", operation: .split, meter: meter)
        #expect(try strings(split.splittingToArray(input, limits: limits).value) == ["a", "b"])
        let comma = try program(",", operation: .split, meter: meter)
        #expect(try strings(comma.splittingToArray(.admitting("a,,b,", workMeter: meter), limits: limits).value) == ["a", "", "b", ""])
        #expect(try strings(split.splittingToArray(.admitting("", workMeter: meter), limits: limits).value) == [""])
    }

    @Test("Selected regex substring uses first capture and strict NULL suppresses invalid syntax")
    func selectedScalar() throws {
        let meter = meter()
        #expect(try evaluate("substring", [.primitive(.string("zabc")), .primitive(.string("a(b)c"))], meter: meter).value == .primitive(.string("b")))
        #expect(try evaluate("substring", [.primitive(.string("zabc")), .primitive(.string("xyz"))], meter: meter).value.isNull)
        #expect(try evaluate("regexp_like", [.primitive(.null), .primitive(.string("["))], meter: meter).value.isNull)
        #expect(try evaluate("regexp_match", [.primitive(.string("a")), .primitive(.string("z"))], meter: meter).value.isNull)
    }

    @Test("SIMILAR SUBSTRING supports zero one two separators and shortest outer pieces")
    func similarSubstring() throws {
        let meter = meter()
        let input = try SQLPatternInput.admitting("abcde", workMeter: meter)
        for (pattern, expected) in [("a#\"b_d#\"e", "bcd"), ("abcde", "abcde"), ("a#\"bcde", "bcde"), ("%#\"%#\"%", "abcde")] {
            let result = try SQLPatternProgram.similarSubstring(input: input, pattern: .admitting(pattern, workMeter: meter), escape: "#")
            #expect(try text(result!.materialize()) == expected)
        }
    }

    @Test("Workspace exact byte peak succeeds and one byte below rejects without retained claims")
    func exactWorkspaceAdmission() throws {
        func run(_ meter: DatabaseWorkMeter) throws {
            var iterator = try program("(a|ab)(b*)", operation: .match, meter: meter)
                .makeMatchIterator(in: .admitting("abb", workMeter: meter))
            let nextMatch = try iterator.next()
            let match = try #require(nextMatch)
            _ = try match.materializeTextArray(limits: limits)
        }
        let measured = meter(); try run(measured)
        #expect(measured.retainedIntermediateBytes == 0)
        let exact = meter(bytes: measured.peakIntermediateBytes); try run(exact)
        #expect(exact.retainedIntermediateBytes == 0)
        let short = meter(bytes: measured.peakIntermediateBytes - 1)
        #expect(throws: DatabaseWorkLimitError.self) { try run(short) }
        #expect(short.retainedIntermediateBytes == 0)
    }

    @Test("Matches retain source and capture ownership after iterator lifetime")
    func retainedCaptureOwner() throws {
        let meter = meter()
        var retained: SQLPatternMatch?
        do {
            var iterator = try program("(ab)", operation: .match, meter: meter)
                .makeMatchIterator(in: .admitting("ab", workMeter: meter))
            retained = try iterator.next()
        }
        #expect(meter.retainedIntermediateBytes > 0)
        #expect(try text(retained!.capture(at: 1)!.materialize()) == "ab")
        retained = nil
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Compile expansion work and deadline failures release all temporary owners")
    func resourceFailures() throws {
        let small = meter(bytes: 4096)
        #expect(throws: DatabaseWorkLimitError.self) { try program("(a{255}){255}", meter: small) }
        #expect(small.retainedIntermediateBytes == 0)
        let exhausted = meter(work: 3)
        #expect(throws: DatabaseWorkLimitError.self) { try program("abcdef", meter: exhausted) }
        #expect(exhausted.retainedIntermediateBytes == 0)
        let clock = PatternClock(); let deadline = meter(clock: clock)
        do {
            let expression = try program("a+", meter: deadline)
            clock.advance()
            #expect(throws: DatabaseWorkLimitError.self) { try expression.matches(.admitting("aaa", workMeter: deadline)) }
        }
        #expect(deadline.retainedIntermediateBytes == 0)
    }

    @Test("Cancellation is preserved through the selected error mapper and releases owners")
    func cancellation() async throws {
        let meter = meter()
        let task = Task {
            let expression = try program("a+", meter: meter)
            withUnsafeCurrentTask { $0?.cancel() }
            _ = try expression.matches(.admitting("aaa", workMeter: meter))
        }
        do { try await task.value; Issue.record("Expected cancellation") } catch is CancellationError { }
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(CanonicalSQLPatternEvaluator.mapFailure(CancellationError(), function: "test") is CancellationError)
    }

    @Test("Input leases require identical meter and admit retained lease storage")
    func inputLeaseIdentity() throws {
        let meter = meter()
        do {
            let reservation = try meter.reserveIntermediate(bytes: 3, at: .expressionEvaluation)
            let lease = DatabaseQueryPayloadLease(retaining: reservation)
            let input = try SQLPatternInput.retaining("abc", leases: [lease], workMeter: meter)
            #expect(try program("abc", meter: meter).matches(input))
            #expect(throws: SQLPatternError.meterMismatch) {
                try SQLPatternInput.retaining("abc", leases: [lease], workMeter: self.meter())
            }
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }
}

private final class PatternClock: StorageMonotonicClock, Sendable {
    private let instant = Mutex(StorageInstant(durationSinceReference: .zero))
    var now: StorageInstant { instant.withLock { $0 } }
    func advance() { instant.withLock { $0 = $0.advanced(by: .seconds(1)) } }
    func sleep(until deadline: StorageInstant) async throws(StorageClockError) { instant.withLock { $0 = deadline } }
}
