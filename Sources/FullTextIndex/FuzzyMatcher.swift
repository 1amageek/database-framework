// FuzzyMatcher.swift
// FullTextIndex - Fuzzy string matching with Levenshtein distance
//
// Reference: Levenshtein Automata
// http://blog.notdot.net/2010/07/Damn-Cool-Algorithms-Levenshtein-Automata

import Foundation

// MARK: - FuzzyMatcher

/// Levenshtein distance matcher for fuzzy search
///
/// Computes edit distance between strings with early termination optimization.
///
/// **Complexity**:
/// - Time: O(nm) where n, m are string lengths
/// - Space: O(min(n, m)) using space-optimized algorithm
///
/// **Usage**:
/// ```swift
/// let matcher = FuzzyMatcher(maxEdits: 2, prefixLength: 2)
///
/// // Check if strings match within edit distance
/// matcher.matches("swift", "swfit")  // true (1 edit)
/// matcher.matches("swift", "swiift")  // true (1 edit)
/// matcher.matches("swift", "xxxxx")  // false (5 edits)
///
/// // Get actual distance
/// let distance = matcher.distance("swift", "swfit")  // 1
/// ```
///
/// **Reference**: Lucene FuzzyQuery uses Levenshtein Automata for efficiency
/// when searching through an index. This implementation uses the simpler
/// dynamic programming approach suitable for individual comparisons.
public struct FuzzyMatcher: Sendable {
    /// Maximum edit distance to consider a match
    ///
    /// Lucene default is 2, which catches most typos.
    /// Higher values increase false positives.
    public let maxEdits: Int

    /// Number of prefix characters that must match exactly
    ///
    /// Optimization that reduces the search space.
    /// With prefixLength=2, "swift" only matches terms starting with "sw".
    public let prefixLength: Int

    /// Whether matches are case-sensitive
    public let caseSensitive: Bool

    public init(maxEdits: Int = 2, prefixLength: Int = 0, caseSensitive: Bool = false) {
        // Lucene caps maxEdits at 2 for performance reasons
        self.maxEdits = min(max(0, maxEdits), 2)
        self.prefixLength = max(0, prefixLength)
        self.caseSensitive = caseSensitive
    }

    // MARK: - Public API

    /// Check if two strings match within the edit distance threshold
    ///
    /// - Parameters:
    ///   - s1: First string (typically the query term)
    ///   - s2: Second string (typically the indexed term)
    /// - Returns: true if strings are within maxEdits of each other
    public func matches(_ s1: String, _ s2: String) -> Bool {
        distance(s1, s2) <= maxEdits
    }

    /// Calculate the Levenshtein edit distance between two strings
    ///
    /// Uses space-optimized dynamic programming with early termination.
    ///
    /// - Parameters:
    ///   - s1: First string
    ///   - s2: Second string
    /// - Returns: Edit distance (insertions + deletions + substitutions)
    public func distance(_ s1: String, _ s2: String) -> Int {
        let str1 = caseSensitive ? s1 : s1.lowercased()
        let str2 = caseSensitive ? s2 : s2.lowercased()

        // Check prefix constraint
        if prefixLength > 0 {
            let prefix1 = String(str1.prefix(prefixLength))
            let prefix2 = String(str2.prefix(prefixLength))
            if prefix1 != prefix2 {
                return Int.max
            }
        }

        // Early exit for identical strings
        if str1 == str2 {
            return 0
        }

        // Early exit if length difference exceeds maxEdits
        let lengthDiff = abs(str1.count - str2.count)
        if lengthDiff > maxEdits {
            return lengthDiff
        }

        // Use shorter string as the "column" to minimize space
        let (shorter, longer) = str1.count <= str2.count ? (str1, str2) : (str2, str1)
        let shorterChars = Array(shorter)
        let longerChars = Array(longer)
        let m = shorterChars.count
        let n = longerChars.count

        // Only need two rows for space optimization
        var previousRow = Array(0...m)
        var currentRow = Array(repeating: 0, count: m + 1)

        for j in 1...n {
            currentRow[0] = j

            // Track minimum in this row for early termination
            var rowMin = currentRow[0]

            for i in 1...m {
                let cost = shorterChars[i - 1] == longerChars[j - 1] ? 0 : 1

                currentRow[i] = min(
                    currentRow[i - 1] + 1,      // insertion
                    previousRow[i] + 1,         // deletion
                    previousRow[i - 1] + cost   // substitution
                )

                rowMin = min(rowMin, currentRow[i])
            }

            // Early termination: if minimum in row exceeds maxEdits,
            // the final distance will too
            if rowMin > maxEdits {
                return rowMin
            }

            swap(&previousRow, &currentRow)
        }

        return previousRow[m]
    }

    /// Find all matching terms from a dictionary
    ///
    /// - Parameters:
    ///   - query: The query term
    ///   - dictionary: Set of terms to search
    /// - Returns: Array of (term, distance) pairs sorted by distance
    public func findMatches(_ query: String, in dictionary: [String]) -> [(term: String, distance: Int)] {
        var matches: [(term: String, distance: Int)] = []

        for term in dictionary {
            let d = distance(query, term)
            if d <= maxEdits {
                matches.append((term: term, distance: d))
            }
        }

        // Sort by distance, then alphabetically
        matches.sort { ($0.distance, $0.term) < ($1.distance, $1.term) }
        return matches
    }
}

// MARK: - DamerauLevenshteinMatcher

/// Damerau-Levenshtein distance matcher
///
/// Extends Levenshtein with transposition as a single edit.
/// "ab" → "ba" is 1 edit (not 2).
///
/// **Complexity**: Same as Levenshtein
///
/// **Reference**: "A technique for computer detection and correction
/// of spelling errors" - Damerau, 1964
public struct DamerauLevenshteinMatcher: Sendable {
    public let maxEdits: Int
    public let caseSensitive: Bool

    public init(maxEdits: Int = 2, caseSensitive: Bool = false) {
        self.maxEdits = min(max(0, maxEdits), 2)
        self.caseSensitive = caseSensitive
    }

    /// Calculate Damerau-Levenshtein distance
    public func distance(_ s1: String, _ s2: String) -> Int {
        let str1 = caseSensitive ? s1 : s1.lowercased()
        let str2 = caseSensitive ? s2 : s2.lowercased()

        if str1 == str2 { return 0 }

        let chars1 = Array(str1)
        let chars2 = Array(str2)
        let m = chars1.count
        let n = chars2.count

        // Early exit if length difference exceeds maxEdits
        if abs(m - n) > maxEdits {
            return abs(m - n)
        }

        // Need 3 rows for transposition
        var prevPrevRow = Array(repeating: 0, count: n + 1)
        var previousRow = Array(0...n)
        var currentRow = Array(repeating: 0, count: n + 1)

        for i in 1...m {
            currentRow[0] = i

            for j in 1...n {
                let cost = chars1[i - 1] == chars2[j - 1] ? 0 : 1

                currentRow[j] = min(
                    currentRow[j - 1] + 1,      // insertion
                    previousRow[j] + 1,         // deletion
                    previousRow[j - 1] + cost   // substitution
                )

                // Check for transposition
                if i > 1 && j > 1 &&
                    chars1[i - 1] == chars2[j - 2] &&
                    chars1[i - 2] == chars2[j - 1] {
                    currentRow[j] = min(
                        currentRow[j],
                        prevPrevRow[j - 2] + cost  // transposition
                    )
                }
            }

            let temp = prevPrevRow
            prevPrevRow = previousRow
            previousRow = currentRow
            currentRow = temp
        }

        return previousRow[n]
    }

    public func matches(_ s1: String, _ s2: String) -> Bool {
        distance(s1, s2) <= maxEdits
    }
}

// MARK: - Phonetic Matching

/// Soundex phonetic algorithm
///
/// Encodes words by their sound, so similar sounding words get the same code.
///
/// **Reference**: US Census Bureau phonetic algorithm
public struct Soundex: Sendable {
    public init() {}

    /// Encode a string to its Soundex code
    ///
    /// - Parameter text: The string to encode
    /// - Returns: 4-character Soundex code (letter + 3 digits)
    public func encode(_ text: String) -> String {
        let upper = text.uppercased()
        guard let firstChar = upper.first, firstChar.isLetter else {
            return "0000"
        }

        var code = String(firstChar)
        var lastCode = soundexCode(for: firstChar)

        for char in upper.dropFirst() {
            guard char.isLetter else { continue }

            let charCode = soundexCode(for: char)
            if charCode != "0" && charCode != lastCode {
                code.append(charCode)
                if code.count == 4 {
                    break
                }
            }
            lastCode = charCode
        }

        // Pad with zeros if needed
        while code.count < 4 {
            code.append("0")
        }

        return code
    }

    /// Check if two strings have the same Soundex code
    public func matches(_ s1: String, _ s2: String) -> Bool {
        encode(s1) == encode(s2)
    }

    private func soundexCode(for char: Character) -> Character {
        switch char {
        case "B", "F", "P", "V": return "1"
        case "C", "G", "J", "K", "Q", "S", "X", "Z": return "2"
        case "D", "T": return "3"
        case "L": return "4"
        case "M", "N": return "5"
        case "R": return "6"
        default: return "0"  // A, E, I, O, U, H, W, Y
        }
    }
}

// MARK: - Jaro-Winkler Similarity

/// Jaro-Winkler string similarity
///
/// Measures similarity between two strings (0.0 - 1.0).
/// Gives higher scores to strings that match from the beginning.
///
/// **Reference**: Winkler, "String Comparator Metrics and Enhanced
/// Decision Rules in the Fellegi-Sunter Model of Record Linkage"
public struct JaroWinkler: Sendable {
    /// Prefix scale factor (typically 0.1)
    public let prefixScale: Double

    /// Maximum prefix length to consider
    public let maxPrefixLength: Int

    public init(prefixScale: Double = 0.1, maxPrefixLength: Int = 4) {
        self.prefixScale = prefixScale
        self.maxPrefixLength = maxPrefixLength
    }

    /// Calculate Jaro similarity (0.0 - 1.0)
    public func jaroSimilarity(_ s1: String, _ s2: String) -> Double {
        if s1 == s2 { return 1.0 }
        if s1.isEmpty || s2.isEmpty { return 0.0 }

        let chars1 = Array(s1.lowercased())
        let chars2 = Array(s2.lowercased())

        let matchDistance = max(chars1.count, chars2.count) / 2 - 1

        var s1Matches = Array(repeating: false, count: chars1.count)
        var s2Matches = Array(repeating: false, count: chars2.count)

        var matches = 0
        var transpositions = 0

        // Find matches
        for i in 0..<chars1.count {
            let start = max(0, i - matchDistance)
            let end = min(i + matchDistance + 1, chars2.count)

            for j in start..<end {
                if s2Matches[j] || chars1[i] != chars2[j] { continue }
                s1Matches[i] = true
                s2Matches[j] = true
                matches += 1
                break
            }
        }

        if matches == 0 { return 0.0 }

        // Count transpositions
        var k = 0
        for i in 0..<chars1.count {
            if !s1Matches[i] { continue }
            while !s2Matches[k] { k += 1 }
            if chars1[i] != chars2[k] { transpositions += 1 }
            k += 1
        }

        let m = Double(matches)
        return (m / Double(chars1.count) +
                m / Double(chars2.count) +
                (m - Double(transpositions) / 2) / m) / 3
    }

    /// Calculate Jaro-Winkler similarity (0.0 - 1.0)
    ///
    /// Adds prefix bonus to Jaro similarity.
    public func similarity(_ s1: String, _ s2: String) -> Double {
        let jaroSim = jaroSimilarity(s1, s2)

        // Calculate common prefix length
        let chars1 = Array(s1.lowercased())
        let chars2 = Array(s2.lowercased())

        var prefixLength = 0
        let maxPrefix = min(maxPrefixLength, min(chars1.count, chars2.count))

        for i in 0..<maxPrefix {
            if chars1[i] == chars2[i] {
                prefixLength += 1
            } else {
                break
            }
        }

        return jaroSim + Double(prefixLength) * prefixScale * (1 - jaroSim)
    }

    /// Check if similarity exceeds threshold
    public func matches(_ s1: String, _ s2: String, threshold: Double = 0.8) -> Bool {
        similarity(s1, s2) >= threshold
    }
}
