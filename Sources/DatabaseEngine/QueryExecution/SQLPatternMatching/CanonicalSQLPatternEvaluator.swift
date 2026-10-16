import DatabaseKit

/// Selected SQL pattern policy and the sole conversion into public expression failures.
enum CanonicalSQLPatternEvaluator {
    static func predicate(subject: String, pattern: String, escape: String?, kind: PatternMatchKind,
                          workMeter: DatabaseWorkMeter) throws -> Bool {
        do {
            guard kind.permitsEscape || escape == nil else { throw SQLPatternError.invalidEscape }
            let input = try SQLPatternInput.admitting(subject, workMeter: workMeter)
            let pattern = try SQLPatternInput.admitting(pattern, workMeter: workMeter)
            let escape = try escape.map { try SQLPatternProgram.escape($0, workMeter: workMeter) } ?? nil
            return try SQLPatternProgram.compile(pattern: pattern, syntax: syntax(kind, escape: escape),
                flags: nil, operation: .predicate).matches(input)
        } catch { throw mapFailure(error, function: "pattern predicate") }
    }

    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue], workMeter: DatabaseWorkMeter,
                         limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard operands.count == bound.children.count else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected pattern argument count")
        }
        // All signatures are strict; caller evaluation/binding still occurs before this suppression.
        if operands.contains(where: { $0.isNull }) { return (.primitive(.null), nil) }
        var function = "pattern predicate"
        do {
            func input(_ index: Int, preservingCharacterPadding: Bool = false) throws -> SQLPatternInput {
                guard operands.indices.contains(index) else { throw SQLPatternError.invalidPattern(offset: 0, reason: "argument count") }
                return try CanonicalSQLScalarText.withText(of: operands[index], type: bound.children[index].type,
                    preservingCharacterPadding: preservingCharacterPadding, workMeter: workMeter) {
                        try SQLPatternInput.admitting($0, workMeter: workMeter)
                    }
            }
            func integer(_ index: Int) throws -> Int32 {
                let number = try CanonicalSQLNumericEvaluator.integer(operands[index])
                guard let result = Int32(exactly: number) else { throw SQLPatternError.invalidStart }
                return result
            }
            func scalar(_ value: DatabaseQueryScopedFieldValue) -> (QueryValue, DatabaseQueryPayloadLease?) {
                (value.withValue { .primitive(copy $0) }, value.retainedReservation.map { DatabaseQueryPayloadLease(retaining: $0) })
            }
            if case .patternMatch(_, _, let escapeExpression, let kind) = bound.expression {
                guard kind.permitsEscape || escapeExpression == nil else { throw SQLPatternError.invalidEscape }
                let subject = try input(0, preservingCharacterPadding: true); let pattern = try input(1)
                let escape = try escapeExpression == nil ? nil : SQLPatternProgram.escape(input(2).text, workMeter: workMeter)
                let program = try SQLPatternProgram.compile(pattern: pattern, syntax: syntax(kind, escape: escape), flags: nil, operation: .predicate)
                return (.primitive(.bool(try program.matches(subject))), nil)
            }
            guard case .function(let call) = bound.expression else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("selected pattern expression")
            }
            function = call.name
            let subject = try input(0); let pattern = try input(1)
            if function == "substring" {
                let slice: SQLPatternSlice?
                if operands.count == 3 {
                    let escape = try SQLPatternProgram.escape(input(2).text, workMeter: workMeter)
                    slice = try SQLPatternProgram.similarSubstring(input: subject, pattern: pattern, escape: escape)
                } else {
                    let program = try SQLPatternProgram.compile(pattern: pattern, syntax: .ere(caseInsensitive: false), flags: nil, operation: .match)
                    var matches = try program.makeMatchIterator(in: subject)
                    if let match = try matches.next() { slice = try match.captureCount > 0 ? match.capture(at: 1) : match.wholeMatch }
                    else { slice = nil }
                }
                guard let slice else { return (.primitive(.null), nil) }
                return scalar(try slice.materialize())
            }
            let operation: SQLPatternOperation
            switch function {
            case "regexp_like": operation = .predicate
            case "regexp_match": operation = .match
            case "regexp_replace": operation = .replace
            case "regexp_split_to_array": operation = .split
            default: throw DatabaseExpressionEvaluationError.unsupportedFunction(function)
            }
            var flags: SQLPatternInput?
            var start: Int32 = 1
            var occurrence: Int32?
            var replacement: SQLPatternInput?
            if function == "regexp_replace" {
                replacement = try input(2)
                if operands.count > 3 {
                    if bound.children[3].type == .sql(.text) { flags = try input(3) }
                    else { start = try integer(3) }
                }
                if operands.count > 4 { occurrence = try integer(4) }
                if operands.count > 5 { flags = try input(5) }
            } else if operands.count > 2 { flags = try input(2) }
            let program = try SQLPatternProgram.compile(pattern: pattern, syntax: .ere(caseInsensitive: false), flags: flags, operation: operation)
            switch operation {
            case .predicate: return (.primitive(.bool(try program.matches(subject))), nil)
            case .match:
                var matches = try program.makeMatchIterator(in: subject)
                guard let match = try matches.next() else { return (.primitive(.null), nil) }
                let result = try match.materializeTextArray(limits: limits)
                return (.array(result.value), result.lease)
            case .replace:
                guard let replacement else { throw SQLPatternError.invalidReplacement(offset: 0, reason: "missing replacement") }
                return scalar(try program.replacing(in: subject, with: replacement, start: start, occurrence: occurrence))
            case .split:
                let result = try program.splittingToArray(subject, limits: limits)
                return (.array(result.value), result.lease)
            case .matches: throw DatabaseExpressionEvaluationError.unsupportedFunction(function)
            }
        } catch { throw mapFailure(error, function: function) }
    }

    private static func syntax(_ kind: PatternMatchKind, escape: Unicode.Scalar?) -> SQLPatternSyntax {
        switch kind {
        case .like: .like(caseInsensitive: false, escape: escape)
        case .caseInsensitiveLike: .like(caseInsensitive: true, escape: escape)
        case .similarTo: .similar(escape: escape)
        case .regularExpression: .ere(caseInsensitive: false)
        case .caseInsensitiveRegularExpression: .ere(caseInsensitive: true)
        }
    }

    static func mapFailure(_ error: any Error, function: String) -> any Error {
        guard let failure = error as? SQLPatternError else { return error }
        func checked(_ offset: Int) -> UInt64 {
            guard let value = UInt64(exactly: offset) else { preconditionFailure("Pattern offsets are nonnegative") }
            return value
        }
        switch failure {
        case .invalidPattern(let offset, let reason): return DatabaseExpressionEvaluationError.invalidPattern(offset: checked(offset), reason: reason)
        case .unsupportedPattern(let offset, let construct): return DatabaseExpressionEvaluationError.unsupportedPattern(offset: checked(offset), reason: construct)
        case .unsupportedFlag(let offset, let flag): return DatabaseExpressionEvaluationError.unsupportedPattern(offset: checked(offset), reason: "flag \(flag)")
        case .invalidFlag(let offset, let flag): return DatabaseExpressionEvaluationError.invalidFunctionArgument(function: function, reason: "flag \(flag) at \(offset)")
        case .invalidEscape: return DatabaseExpressionEvaluationError.invalidFunctionArgument(function: function, reason: "invalid escape")
        case .invalidReplacement(let offset, let reason): return DatabaseExpressionEvaluationError.invalidFunctionArgument(function: function, reason: "replacement at \(offset): \(reason)")
        case .invalidStart: return DatabaseExpressionEvaluationError.invalidFunctionArgument(function: function, reason: "start must be positive")
        case .invalidOccurrence: return DatabaseExpressionEvaluationError.invalidFunctionArgument(function: function, reason: "occurrence must be nonnegative")
        case .invalidCaptureIndex: return DatabaseExpressionEvaluationError.invalidFunctionArgument(function: function, reason: "capture index out of range")
        case .meterMismatch: return DatabaseExpressionEvaluationError.invalidFunctionArgument(function: function, reason: "pattern owners belong to different requests")
        }
    }
}
