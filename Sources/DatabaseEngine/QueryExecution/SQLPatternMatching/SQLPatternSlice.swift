import DatabaseKit

struct SQLPatternSlice: Sendable {
    let input: SQLPatternInput
    let range: Range<String.Index>
    func byteCount() throws -> UInt64 {
        var count: UInt64 = 0
        for scalar in input.text[range].unicodeScalars {
            try input.meter.consume(at: .expressionEvaluation)
            count = try DatabaseIntermediateFootprint(bytes: count).adding(.init(bytes: SQLPatternInput.utf8Width(scalar))).bytes
        }
        return count
    }
    func materialize() throws -> DatabaseQueryScopedFieldValue {
        let bytes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<String>.stride)).adding(.init(bytes: byteCount()))
        let reservation = try input.meter.reserveIntermediate(bytes: bytes.bytes, at: .expressionEvaluation)
        return .retaining(.string(String(input.text[range])), reservation: reservation)
    }
}
