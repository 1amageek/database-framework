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
        tokenize(text).map { truncateTerm($0.term) }
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

    private func simpleTokenize(_ text: String) -> [(term: String, position: Int)] {
        let normalized = text.lowercased()
        let words = FullTextTextUtilities.tokenSlices(in: normalized)

        var tokens: [(String, Int)] = []
        tokens.reserveCapacity(words.count)
        var position = 0

        for word in words where word.count >= minTermLength {
                tokens.append((String(word), position))
                position += 1
        }

        return tokens
    }

    private func stemTokenize(_ text: String) -> [(term: String, position: Int)] {
        let normalized = text.lowercased()
        let words = FullTextTextUtilities.tokenSlices(in: normalized)

        var tokens: [(String, Int)] = []
        tokens.reserveCapacity(words.count)
        var position = 0

        for word in words {
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

        var tokens: [(String, Int)] = []
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
