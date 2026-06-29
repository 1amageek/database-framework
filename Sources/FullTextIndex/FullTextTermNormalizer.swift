// FullTextTermNormalizer.swift
// FullTextIndex - Shared full-text query and indexing token normalization

import Foundation
import FullText

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
        let data = Data(term.utf8)
        guard data.count > fullTextMaxTermBytes else {
            return term
        }

        var truncatedData = data.prefix(fullTextMaxTermBytes)
        while !truncatedData.isEmpty {
            if let string = String(data: truncatedData, encoding: .utf8) {
                return string
            }
            truncatedData = truncatedData.dropLast()
        }
        return ""
    }

    private func simpleTokenize(_ text: String) -> [(term: String, position: Int)] {
        let words = text.lowercased().components(separatedBy: CharacterSet.alphanumerics.inverted)

        var tokens: [(String, Int)] = []
        tokens.reserveCapacity(words.count)
        var position = 0

        for word in words {
            let trimmed = word.trimmingCharacters(in: .whitespaces)
            if trimmed.count >= minTermLength {
                tokens.append((trimmed, position))
                position += 1
            }
        }

        return tokens
    }

    private func stemTokenize(_ text: String) -> [(term: String, position: Int)] {
        let words = text.lowercased().components(separatedBy: CharacterSet.alphanumerics.inverted)

        var tokens: [(String, Int)] = []
        tokens.reserveCapacity(words.count)
        var position = 0

        for word in words {
            var stemmed = word.trimmingCharacters(in: .whitespaces)

            if stemmed.hasSuffix("ing") && stemmed.count > 5 {
                stemmed = String(stemmed.dropLast(3))
            } else if stemmed.hasSuffix("ed") && stemmed.count > 4 {
                stemmed = String(stemmed.dropLast(2))
            } else if stemmed.hasSuffix("s") && !stemmed.hasSuffix("ss") && stemmed.count > 3 {
                stemmed = String(stemmed.dropLast(1))
            }

            if stemmed.count >= minTermLength {
                tokens.append((stemmed, position))
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
        let characters = Array(lowered)
        guard characters.count >= ngramSize else {
            return []
        }

        var tokens: [(String, Int)] = []
        tokens.reserveCapacity(characters.count - ngramSize + 1)
        var position = 0

        for index in 0...(characters.count - ngramSize) {
            let ngram = String(characters[index..<index + ngramSize])
            if ngram.count >= minTermLength && !ngram.trimmingCharacters(in: .whitespaces).isEmpty {
                tokens.append((ngram, position))
                position += 1
            }
        }

        return tokens
    }

    private func keywordTokenize(_ text: String) -> [(term: String, position: Int)] {
        let normalized = text.lowercased().trimmingCharacters(in: .whitespaces)
        guard normalized.count >= minTermLength else {
            return []
        }
        return [(normalized, 0)]
    }
}
