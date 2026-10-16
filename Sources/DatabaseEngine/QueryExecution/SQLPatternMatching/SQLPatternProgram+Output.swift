import DatabaseKit

extension SQLPatternProgram {
    func splittingToArray(_ input: SQLPatternInput, limits: QueryValueLimits) throws
        -> (value: QueryArrayValue, lease: DatabaseQueryPayloadLease) {
        var iterator = try makeSplitIterator(in: input)
        var slices = try SQLPatternStorage<SQLPatternSlice?>(meter: meter)
        while let slice = try iterator.next() { try slices.append(slice) }
        return try SQLPatternOutput.array(slices.values, meter: meter, limits: limits)
    }

    func replacing(in input: SQLPatternInput, with replacement: SQLPatternInput,
                   start: Int32 = 1, occurrence: Int32? = nil) throws -> DatabaseQueryScopedFieldValue {
        guard input.meter === meter, replacement.meter === meter else { throw SQLPatternError.meterMismatch }
        guard occurrence == nil || occurrence! >= 0 else { throw SQLPatternError.invalidOccurrence }
        let selected = occurrence ?? (isGlobal ? 0 : 1)
        var iterator = try makeMatchIterator(in: input, start: start)
        var pieces = try SQLPatternStorage<SQLPatternSlice>(meter: meter)
        var emittedThrough = input.bounds.lowerBound
        var number: UInt64 = 0
        while let match = try iterator.next() {
            try meter.consume(at: .expressionEvaluation)
            number += 1
            if selected != 0 && number != UInt64(selected) { continue }
            try pieces.append(.init(input: input, range: emittedThrough..<match.range.lowerBound))
            try appendReplacement(replacement, match: match, into: &pieces)
            emittedThrough = match.range.upperBound
            if selected != 0 { break }
        }
        try pieces.append(.init(input: input, range: emittedThrough..<input.bounds.upperBound))
        return try SQLPatternOutput.text(pieces.values, meter: meter)
    }

    private func appendReplacement(_ source: SQLPatternInput, match: SQLPatternMatch,
                                   into pieces: inout SQLPatternStorage<SQLPatternSlice>) throws {
        var position = source.text.startIndex
        var literal = position
        while position != source.text.endIndex {
            try meter.consume(at: .expressionEvaluation)
            let scalar = source.text.unicodeScalars[position]
            guard scalar == "\\" else { source.text.unicodeScalars.formIndex(after: &position); continue }
            if literal != position { try pieces.append(.init(input: source, range: literal..<position)) }
            let slash = position
            source.text.unicodeScalars.formIndex(after: &position)
            if position == source.text.endIndex { try pieces.append(.init(input: source, range: slash..<position)); literal = position; break }
            let escaped = source.text.unicodeScalars[position]
            let end = source.text.unicodeScalars.index(after: position)
            if escaped == "&" { try pieces.append(match.wholeMatch) }
            else if (49...57).contains(escaped.value) {
                let group = Int(escaped.value - 48)
                if group <= match.captureCount, let capture = try match.capture(at: group) { try pieces.append(capture) }
            } else if escaped == "\\" { try pieces.append(.init(input: source, range: position..<end)) }
            else { try pieces.append(.init(input: source, range: slash..<end)) }
            position = end; literal = end
        }
        if literal != source.text.endIndex { try pieces.append(.init(input: source, range: literal..<source.text.endIndex)) }
    }
}
