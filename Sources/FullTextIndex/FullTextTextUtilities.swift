enum FullTextTextUtilities {
    static func isTokenCharacter(_ character: Character) -> Bool {
        character.isLetter || character.isNumber
    }

    static func forEachTokenSlice<Failure: Error>(
        in value: String,
        _ body: (Substring) throws(Failure) -> Void
    ) throws(Failure) {
        var tokenStart: String.Index?
        var cursor = value.startIndex
        while cursor < value.endIndex {
            let next = value.index(after: cursor)
            if isTokenCharacter(value[cursor]) {
                if tokenStart == nil {
                    tokenStart = cursor
                }
            } else if let start = tokenStart {
                try body(value[start..<cursor])
                tokenStart = nil
            }
            cursor = next
        }
        if let tokenStart {
            try body(value[tokenStart..<value.endIndex])
        }
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
