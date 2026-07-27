import Testing
import DatabaseKit
@testable import FullTextIndex

@Suite("Full-text term normalizer")
struct FullTextTermNormalizerTests {
    @Test("stem tokenizer is shared by read and write paths")
    func stemTokenizerNormalizesQueryText() {
        let normalizer = FullTextTermNormalizer(tokenizer: .stem, ngramSize: 3, minTermLength: 2)

        #expect(normalizer.normalizedTerms(from: "Running jumped cats") == ["runn", "jump", "cat"])
    }

    @Test("ngram tokenizer expands query terms")
    func ngramTokenizerExpandsQueryTerms() {
        let normalizer = FullTextTermNormalizer(tokenizer: .ngram, ngramSize: 3, minTermLength: 2)

        #expect(normalizer.normalizedTerms(from: "Swift") == ["swi", "wif", "ift"])
    }

    @Test("keyword tokenizer preserves the normalized phrase")
    func keywordTokenizerPreservesPhrase() {
        let normalizer = FullTextTermNormalizer(tokenizer: .keyword, ngramSize: 3, minTermLength: 2)

        #expect(normalizer.normalizedTerms(from: "  App Store  ") == ["app store"])
    }
}
