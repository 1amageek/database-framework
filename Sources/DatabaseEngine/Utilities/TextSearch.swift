/// Deterministic text searches shared by database execution modules.
package enum TextSearch {
    /// Returns whether `pattern` occurs as the same UTF-8 byte sequence.
    package static func contains(
        _ pattern: String,
        in value: String
    ) -> Bool {
        firstRange(of: pattern, in: value) != nil
    }

    /// Returns indices that are valid only for `value`.
    package static func firstRange(
        of pattern: String,
        in value: String
    ) -> Range<String.Index>? {
        firstRange(of: pattern, in: value[value.startIndex...])
    }

    package static func firstRange(
        of pattern: String,
        in value: Substring
    ) -> Range<String.Index>? {
        guard !pattern.isEmpty else {
            return value.startIndex..<value.startIndex
        }

        let patternBytes = pattern.utf8
        let valueBytes = value.utf8
        var candidate = valueBytes.startIndex
        while candidate < valueBytes.endIndex {
            var sourceIndex = candidate
            var patternIndex = patternBytes.startIndex
            while sourceIndex < valueBytes.endIndex,
                  patternIndex < patternBytes.endIndex,
                  valueBytes[sourceIndex] == patternBytes[patternIndex] {
                sourceIndex = valueBytes.index(after: sourceIndex)
                patternIndex = patternBytes.index(after: patternIndex)
            }
            if patternIndex == patternBytes.endIndex {
                return candidate..<sourceIndex
            }
            candidate = valueBytes.index(after: candidate)
        }
        return nil
    }

    package static func isEqualIgnoringASCIICase(
        _ left: String,
        _ right: String
    ) -> Bool {
        let leftBytes = left.utf8
        let rightBytes = right.utf8
        guard leftBytes.count == rightBytes.count else {
            return false
        }
        return leftBytes.elementsEqual(rightBytes) {
            asciiLowercased($0) == asciiLowercased($1)
        }
    }

    private static func asciiLowercased(_ byte: UInt8) -> UInt8 {
        byte >= 65 && byte <= 90 ? byte + 32 : byte
    }
}
