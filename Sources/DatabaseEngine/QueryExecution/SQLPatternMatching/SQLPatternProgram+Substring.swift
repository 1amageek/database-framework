import DatabaseKit

extension SQLPatternProgram {
    /// SQL SIMILAR SUBSTRING gives the first and third independent patterns shortest priority.
    static func similarSubstring(input: SQLPatternInput, pattern: SQLPatternInput,
                                 escape: Unicode.Scalar?) throws -> SQLPatternSlice? {
        guard input.meter === pattern.meter else { throw SQLPatternError.meterMismatch }
        var parts = try SQLPatternStorage<SQLPatternInput>(meter: input.meter)
        var position = pattern.text.startIndex
        var beginning = position
        while position != pattern.text.endIndex {
            try input.meter.consume(at: .expressionEvaluation)
            let scalar = pattern.text.unicodeScalars[position]
            let next = pattern.text.unicodeScalars.index(after: position)
            if scalar == escape, next != pattern.text.endIndex {
                let end = pattern.text.unicodeScalars.index(after: next)
                if pattern.text.unicodeScalars[next] == "\"" {
                    try parts.append(.admitting(pattern.text[beginning..<position], workMeter: input.meter))
                    guard parts.count <= 2 else { throw SQLPatternError.invalidPattern(offset: 0, reason: "more than two SIMILAR substring separators") }
                    beginning = end
                }
                position = end
            } else { position = next }
        }
        try parts.append(.admitting(pattern.text[beginning..<pattern.text.endIndex], workMeter: input.meter))
        let empty = try SQLPatternInput.admitting("", workMeter: input.meter)
        let prefix: SQLPatternInput; let middle: SQLPatternInput; let suffix: SQLPatternInput
        if parts.count == 1 { prefix = empty; middle = parts[0]; suffix = empty }
        else if parts.count == 2 { prefix = parts[0]; middle = parts[1]; suffix = empty }
        else { prefix = parts[0]; middle = parts[1]; suffix = parts[2] }
        let first = try compile(pattern: prefix, syntax: .similar(escape: escape), flags: nil, operation: .predicate)
        let second = try compile(pattern: middle, syntax: .similar(escape: escape), flags: nil, operation: .predicate)
        let third = try compile(pattern: suffix, syntax: .similar(escape: escape), flags: nil, operation: .predicate)
        var lower = input.bounds.lowerBound
        while true {
            try input.meter.consume(at: .expressionEvaluation)
            if try first.matches(input.bounded(input.bounds.lowerBound..<lower)) {
                var upper = input.bounds.upperBound
                while upper >= lower {
                    try input.meter.consume(at: .expressionEvaluation)
                    if try third.matches(input.bounded(upper..<input.bounds.upperBound)),
                       try second.matches(input.bounded(lower..<upper)) { return .init(input: input, range: lower..<upper) }
                    if upper == lower { break }
                    input.text.unicodeScalars.formIndex(before: &upper)
                }
            }
            if lower == input.bounds.upperBound { return nil }
            input.text.unicodeScalars.formIndex(after: &lower)
        }
    }
}
