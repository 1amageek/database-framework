import DatabaseKit

struct SQLPatternMatch: Sendable {
    struct Capture: Sendable {
        var lower: String.Index?
        var upper: String.Index?
    }
    let input: SQLPatternInput
    let range: Range<String.Index>
    let captures: DatabaseSharedRetainedArray<Capture>
    var wholeMatch: SQLPatternSlice { .init(input: input, range: range) }
    var captureCount: Int { captures.count }
    func capture(at index: Int) throws -> SQLPatternSlice? {
        guard index > 0, index <= captures.count else { throw SQLPatternError.invalidCaptureIndex }
        let capture = captures[index - 1]
        guard let lower = capture.lower, let upper = capture.upper else { return nil }
        return .init(input: input, range: lower..<upper)
    }
    func materializeTextArray(limits: QueryValueLimits) throws -> (value: QueryArrayValue, lease: DatabaseQueryPayloadLease) {
        var slices = try SQLPatternStorage<SQLPatternSlice?>(meter: input.meter)
        if captures.isEmpty { try slices.append(wholeMatch) }
        else { for index in 1...captures.count { try slices.append(capture(at: index)) } }
        return try SQLPatternOutput.array(slices.values, meter: input.meter, limits: limits)
    }
}
