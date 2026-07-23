enum FullTextTextUtilities {
    static func isTokenCharacter(_ character: Character) -> Bool {
        character.isLetter || character.isNumber
    }

    static func tokenSlices(in value: String) -> [Substring] {
        value.split(
            omittingEmptySubsequences: true,
            whereSeparator: { !isTokenCharacter($0) }
        )
    }

    static func trimmingWhitespace(_ value: String) -> Substring {
        var lowerBound = value.startIndex
        var upperBound = value.endIndex
        while lowerBound < upperBound, value[lowerBound].isWhitespace {
            lowerBound = value.index(after: lowerBound)
        }
        while lowerBound < upperBound {
            let previous = value.index(before: upperBound)
            guard value[previous].isWhitespace else {
                break
            }
            upperBound = previous
        }
        return value[lowerBound..<upperBound]
    }

    static func containsNonWhitespace(_ value: Substring) -> Bool {
        value.contains { !$0.isWhitespace }
    }
}
