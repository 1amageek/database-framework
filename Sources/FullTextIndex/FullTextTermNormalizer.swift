// FullTextTermNormalizer.swift
// FullTextIndex - Shared full-text query and indexing token normalization

import DatabaseKit

internal struct FullTextTermNormalizer: Sendable {
    let tokenizer: TokenizationStrategy
    let ngramSize: Int
    let minTermLength: Int

    init(
        tokenizer: TokenizationStrategy,
        ngramSize: Int,
        minTermLength: Int
    ) {
        self.tokenizer = tokenizer
        self.ngramSize = ngramSize
        self.minTermLength = minTermLength
    }

    func normalizedTerms(from text: String) -> [String] {
        var terms: [String] = []
        forEachNormalizedTerm(from: text) { terms.append($0) }
        return terms
    }

    /// Streams normalized terms so request-scoped callers can admit each
    /// retained token before appending it to their own storage.
    func forEachNormalizedTerm<Failure: Error>(
        from text: String,
        _ body: (String) throws(Failure) -> Void
    ) throws(Failure) {
        switch tokenizer {
        case .simple:
            let normalized = text.lowercased()
            let consume: (Substring) throws(Failure) -> Void = { word in
                if word.count >= minTermLength {
                    try body(materializeTruncatedTerm(word))
                }
            }
            try FullTextTextUtilities.forEachTokenSlice(
                in: normalized,
                consume
            )
        case .stem:
            let normalized = text.lowercased()
            let consume: (Substring) throws(Failure) -> Void = { word in
                var stemmed = word
                if stemmed.hasSuffix("ing") && stemmed.count > 5 {
                    stemmed = stemmed.dropLast(3)
                } else if stemmed.hasSuffix("ed") && stemmed.count > 4 {
                    stemmed = stemmed.dropLast(2)
                } else if stemmed.hasSuffix("s"),
                          !stemmed.hasSuffix("ss"),
                          stemmed.count > 3 {
                    stemmed = stemmed.dropLast(1)
                }
                if stemmed.count >= minTermLength {
                    try body(materializeTruncatedTerm(stemmed))
                }
            }
            try FullTextTextUtilities.forEachTokenSlice(
                in: normalized,
                consume
            )
        case .ngram:
            guard ngramSize > 0 else { return }
            let lowered = text.lowercased()
            var windowStart = lowered.startIndex
            var windowEnd = windowStart
            for _ in 0..<ngramSize {
                guard windowEnd < lowered.endIndex else { return }
                windowEnd = lowered.index(after: windowEnd)
            }
            while true {
                let ngram = lowered[windowStart..<windowEnd]
                if ngram.count >= minTermLength,
                   FullTextTextUtilities.containsNonWhitespace(ngram) {
                    try body(materializeTruncatedTerm(ngram))
                }
                guard windowEnd < lowered.endIndex else { break }
                windowStart = lowered.index(after: windowStart)
                windowEnd = lowered.index(after: windowEnd)
            }
        case .keyword:
            let lowered = text.lowercased()
            let normalized = FullTextTextUtilities.trimmingWhitespace(lowered)
            guard normalized.count >= minTermLength else { return }
            try body(materializeTruncatedTerm(normalized))
        }
    }

    func tokenize(_ text: String) -> [(term: String, position: Int)] {
        switch tokenizer {
        case .simple:
            return simpleTokenize(text)
        case .stem:
            return stemTokenize(text)
        case .ngram:
            return ngramTokenize(text)
        case .keyword:
            return keywordTokenize(text)
        }
    }

    func truncateTerm(_ term: String) -> String {
        let utf8 = term.utf8
        guard utf8.count > fullTextMaxTermBytes else {
            return term
        }

        var end = utf8.index(
            utf8.startIndex,
            offsetBy: fullTextMaxTermBytes
        )
        while end > utf8.startIndex,
              utf8[end] & 0xC0 == 0x80 {
            end = utf8.index(before: end)
        }
        return String(decoding: utf8[..<end], as: UTF8.self)
    }

    /// Materializes a borrowed token exactly once at its retained-output
    /// boundary, including the overlong-token case.
    private func materializeTruncatedTerm(_ term: Substring) -> String {
        let utf8 = term.utf8
        guard utf8.count > fullTextMaxTermBytes else {
            return String(term)
        }

        var end = utf8.index(
            utf8.startIndex,
            offsetBy: fullTextMaxTermBytes
        )
        while end > utf8.startIndex,
              utf8[end] & 0xC0 == 0x80 {
            end = utf8.index(before: end)
        }
        return String(decoding: utf8[..<end], as: UTF8.self)
    }

    private func simpleTokenize(_ text: String) -> [(term: String, position: Int)] {
        let normalized = text.lowercased()
        var tokens: [(term: String, position: Int)] = []
        var position = 0

        FullTextTextUtilities.forEachTokenSlice(in: normalized) { word in
            if word.count >= minTermLength {
                tokens.append((String(word), position))
                position += 1
            }
        }

        return tokens
    }

    private func stemTokenize(_ text: String) -> [(term: String, position: Int)] {
        let normalized = text.lowercased()
        var tokens: [(term: String, position: Int)] = []
        var position = 0

        FullTextTextUtilities.forEachTokenSlice(in: normalized) { word in
            var stemmed = word

            if stemmed.hasSuffix("ing") && stemmed.count > 5 {
                stemmed = stemmed.dropLast(3)
            } else if stemmed.hasSuffix("ed") && stemmed.count > 4 {
                stemmed = stemmed.dropLast(2)
            } else if stemmed.hasSuffix("s") && !stemmed.hasSuffix("ss") && stemmed.count > 3 {
                stemmed = stemmed.dropLast(1)
            }

            if stemmed.count >= minTermLength {
                tokens.append((String(stemmed), position))
                position += 1
            }
        }

        return tokens
    }

    private func ngramTokenize(_ text: String) -> [(term: String, position: Int)] {
        guard ngramSize > 0 else {
            return []
        }

        let lowered = text.lowercased()
        var windowStart = lowered.startIndex
        var windowEnd = windowStart
        for _ in 0..<ngramSize {
            guard windowEnd < lowered.endIndex else {
                return []
            }
            windowEnd = lowered.index(after: windowEnd)
        }
        guard windowStart < windowEnd else {
            return []
        }

        var tokens: [(term: String, position: Int)] = []
        var position = 0

        while true {
            let ngram = lowered[windowStart..<windowEnd]
            if ngram.count >= minTermLength,
               FullTextTextUtilities.containsNonWhitespace(ngram) {
                tokens.append((String(ngram), position))
                position += 1
            }
            guard windowEnd < lowered.endIndex else {
                break
            }
            windowStart = lowered.index(after: windowStart)
            windowEnd = lowered.index(after: windowEnd)
        }

        return tokens
    }

    private func keywordTokenize(_ text: String) -> [(term: String, position: Int)] {
        let lowered = text.lowercased()
        let normalized = FullTextTextUtilities.trimmingWhitespace(lowered)
        guard normalized.count >= minTermLength else {
            return []
        }
        return [(String(normalized), 0)]
    }
}
