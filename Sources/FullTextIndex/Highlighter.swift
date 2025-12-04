// Highlighter.swift
// FullTextIndex - Search result highlighting
//
// Reference: Elasticsearch Unified Highlighter, Lucene Highlighter

import Foundation

// MARK: - Highlighter

/// Highlighter for search results
///
/// Extracts relevant fragments from text and highlights matching terms.
///
/// **Usage**:
/// ```swift
/// let highlighter = Highlighter(config: .html)
/// let fragments = highlighter.highlight(
///     text: "Swift is a powerful programming language.",
///     terms: ["swift", "powerful"]
/// )
/// // ["<em>Swift</em> is a <em>powerful</em> programming language."]
/// ```
///
/// **Reference**: Elasticsearch unified highlighter
public struct Highlighter: Sendable {
    /// Highlighting configuration
    public let config: HighlightConfig

    /// Analyzer for matching terms
    private let analyzer: StandardAnalyzer

    public init(config: HighlightConfig = .html) {
        self.config = config
        self.analyzer = StandardAnalyzer(stopwords: [], minLength: 1)
    }

    // MARK: - Types

    /// Match position in text
    public struct TermMatch: Comparable, Sendable {
        public let start: Int
        public let end: Int
        public let term: String

        public init(start: Int, end: Int, term: String) {
            self.start = start
            self.end = end
            self.term = term
        }

        public static func < (lhs: TermMatch, rhs: TermMatch) -> Bool {
            lhs.start < rhs.start
        }
    }

    // MARK: - Public API

    /// Highlight matching terms in text
    ///
    /// - Parameters:
    ///   - text: The full text to highlight
    ///   - terms: Terms to highlight
    /// - Returns: Array of highlighted fragments
    public func highlight(text: String, terms: [String]) -> [HighlightFragment] {
        guard !text.isEmpty && !terms.isEmpty else {
            return []
        }

        let normalizedTerms = Set(terms.map { $0.lowercased() })

        // Find all term positions
        let matches = findMatches(text: text, terms: normalizedTerms)

        guard !matches.isEmpty else {
            return []
        }

        // Extract fragments around matches
        var fragments = extractFragments(text: text, matches: matches)

        // Sort by score and limit
        fragments.sort { $0.score > $1.score }
        fragments = Array(fragments.prefix(config.numberOfFragments))

        return fragments
    }

    /// Highlight entire field (no fragmentation)
    ///
    /// - Parameters:
    ///   - text: The full text to highlight
    ///   - terms: Terms to highlight
    /// - Returns: Highlighted text
    public func highlightFull(text: String, terms: [String]) -> String {
        guard !text.isEmpty && !terms.isEmpty else {
            return text
        }

        let normalizedTerms = Set(terms.map { $0.lowercased() })
        let matches = findMatches(text: text, terms: normalizedTerms)

        return applyHighlights(text: text, matches: matches)
    }

    /// Create FieldHighlight for a field
    ///
    /// - Parameters:
    ///   - field: Field name
    ///   - text: Field value
    ///   - terms: Terms to highlight
    /// - Returns: FieldHighlight with fragments
    public func highlightField(
        field: String,
        text: String,
        terms: [String]
    ) -> FieldHighlight {
        let fragments = highlight(text: text, terms: terms)
        return FieldHighlight(field: field, fragments: fragments)
    }

    /// Extract fragments around matches (public for FVH)
    public func extractFragments(text: String, matches: [TermMatch]) -> [HighlightFragment] {
        var fragments: [HighlightFragment] = []
        let fragmentSize = config.fragmentSize
        var usedRanges: [Range<Int>] = []

        // Group nearby matches
        var matchGroups: [[TermMatch]] = []
        var currentGroup: [TermMatch] = []

        for match in matches {
            if let lastMatch = currentGroup.last {
                // Check if this match is close enough to group
                if match.start - lastMatch.end < fragmentSize / 2 {
                    currentGroup.append(match)
                } else {
                    if !currentGroup.isEmpty {
                        matchGroups.append(currentGroup)
                    }
                    currentGroup = [match]
                }
            } else {
                currentGroup.append(match)
            }
        }
        if !currentGroup.isEmpty {
            matchGroups.append(currentGroup)
        }

        // Extract fragment for each group
        for group in matchGroups {
            guard let firstMatch = group.first, let lastMatch = group.last else { continue }

            // Calculate fragment boundaries
            let matchCenter = (firstMatch.start + lastMatch.end) / 2
            var fragStart = max(0, matchCenter - fragmentSize / 2)
            var fragEnd = min(text.count, matchCenter + fragmentSize / 2)

            // Adjust to word boundaries
            fragStart = adjustToWordBoundary(text: text, position: fragStart, searchBackward: true)
            fragEnd = adjustToWordBoundary(text: text, position: fragEnd, searchBackward: false)

            // Check for overlap with existing fragments
            let fragRange = fragStart..<fragEnd
            var overlaps = false
            for used in usedRanges {
                if fragRange.overlaps(used) {
                    overlaps = true
                    break
                }
            }

            if overlaps && !config.mergeOverlapping { continue }

            usedRanges.append(fragRange)

            // Extract and highlight fragment
            let startIndex = text.index(text.startIndex, offsetBy: fragStart)
            let endIndex = text.index(text.startIndex, offsetBy: min(fragEnd, text.count))
            let fragmentText = String(text[startIndex..<endIndex])

            // Adjust match positions relative to fragment
            let fragmentMatches = group.compactMap { match -> TermMatch? in
                let relStart = match.start - fragStart
                let relEnd = match.end - fragStart
                guard relStart >= 0 && relEnd <= fragmentText.count else { return nil }
                return TermMatch(start: relStart, end: relEnd, term: match.term)
            }

            let highlightedText = applyHighlights(text: fragmentText, matches: fragmentMatches)

            // Create spans
            let spans = fragmentMatches.map { match in
                HighlightSpan(start: match.start, end: match.end, term: match.term)
            }

            // Score based on match density
            let fragmentLength = fragEnd - fragStart
            let score = Float(group.count) / Float(max(1, fragmentLength)) * 100

            fragments.append(HighlightFragment(
                text: highlightedText,
                offset: fragStart,
                score: score,
                spans: spans
            ))
        }

        return fragments
    }

    // MARK: - Private Methods

    /// Find all term matches in text
    private func findMatches(text: String, terms: Set<String>) -> [TermMatch] {
        var matches: [TermMatch] = []

        // Tokenize text to find word positions
        let tokens = analyzer.analyze(text)

        for token in tokens {
            if terms.contains(token.text.lowercased()) {
                matches.append(TermMatch(
                    start: token.startOffset,
                    end: token.endOffset,
                    term: token.text
                ))
            }
        }

        return matches.sorted()
    }

    /// Adjust position to word boundary
    private func adjustToWordBoundary(text: String, position: Int, searchBackward: Bool) -> Int {
        guard position > 0 && position < text.count else { return position }

        var pos = position
        let chars = Array(text)

        if searchBackward {
            while pos > 0 && !chars[pos - 1].isWhitespace {
                pos -= 1
            }
        } else {
            while pos < chars.count && !chars[pos].isWhitespace {
                pos += 1
            }
        }

        return pos
    }

    /// Apply highlight tags to text
    private func applyHighlights(text: String, matches: [TermMatch]) -> String {
        guard !matches.isEmpty else { return text }

        var result = ""
        var lastEnd = 0

        // Merge overlapping matches if configured
        let mergedMatches = config.mergeOverlapping ? mergeOverlapping(matches) : matches.sorted()

        for match in mergedMatches {
            // Add text before this match
            if match.start > lastEnd {
                let startIdx = text.index(text.startIndex, offsetBy: lastEnd)
                let endIdx = text.index(text.startIndex, offsetBy: match.start)
                result += String(text[startIdx..<endIdx])
            }

            // Add highlighted term
            let termStart = text.index(text.startIndex, offsetBy: match.start)
            let termEnd = text.index(text.startIndex, offsetBy: match.end)
            result += config.preTag + String(text[termStart..<termEnd]) + config.postTag

            lastEnd = match.end
        }

        // Add remaining text
        if lastEnd < text.count {
            let startIdx = text.index(text.startIndex, offsetBy: lastEnd)
            result += String(text[startIdx...])
        }

        return result
    }

    /// Merge overlapping matches
    private func mergeOverlapping(_ matches: [TermMatch]) -> [TermMatch] {
        guard !matches.isEmpty else { return [] }

        let sorted = matches.sorted()
        var merged: [TermMatch] = []

        var current = sorted[0]
        for match in sorted.dropFirst() {
            if match.start <= current.end {
                // Overlap - extend current match
                current = TermMatch(
                    start: current.start,
                    end: max(current.end, match.end),
                    term: current.term + "+" + match.term
                )
            } else {
                merged.append(current)
                current = match
            }
        }
        merged.append(current)

        return merged
    }
}

// MARK: - FastVectorHighlighter

/// Fast vector highlighter using pre-stored term vectors
///
/// Requires term vectors with positions and offsets stored at index time.
/// Faster for large documents.
///
/// **Reference**: Lucene FastVectorHighlighter
public struct FastVectorHighlighter: Sendable {
    public let config: HighlightConfig

    public init(config: HighlightConfig = .html) {
        self.config = config
    }

    /// Highlight using term vectors
    ///
    /// - Parameters:
    ///   - termVectors: Pre-computed term vectors with positions
    ///   - text: Original text
    ///   - terms: Query terms to highlight
    /// - Returns: Highlighted fragments
    public func highlight(
        termVectors: [String: [Int]],  // term -> positions
        text: String,
        terms: [String]
    ) -> [HighlightFragment] {
        // Collect all positions for query terms
        var allPositions: [(position: Int, term: String)] = []

        for term in terms {
            if let positions = termVectors[term.lowercased()] {
                for pos in positions {
                    allPositions.append((position: pos, term: term))
                }
            }
        }

        guard !allPositions.isEmpty else { return [] }

        // Sort by position
        allPositions.sort { $0.position < $1.position }

        // Convert positions to character offsets (simplified - real implementation
        // would use stored offsets)
        let tokens = StandardAnalyzer().analyze(text)
        var positionToOffset: [Int: (start: Int, end: Int)] = [:]
        for token in tokens {
            positionToOffset[token.position] = (token.startOffset, token.endOffset)
        }

        // Build matches
        var matches: [Highlighter.TermMatch] = []
        for (pos, term) in allPositions {
            if let offset = positionToOffset[pos] {
                matches.append(Highlighter.TermMatch(
                    start: offset.start,
                    end: offset.end,
                    term: term
                ))
            }
        }

        // Delegate to regular highlighter for fragment extraction
        let highlighter = Highlighter(config: config)
        return highlighter.extractFragments(text: text, matches: matches)
    }
}
