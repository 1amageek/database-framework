import DatabaseEngine
import DatabaseKit
import TestSupport
import Testing
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

    @Test("Streaming normalization stops at the request work limit")
    func streamingNormalizationStopsAtWorkLimit() throws {
        let normalizer = FullTextTermNormalizer(
            tokenizer: .ngram,
            ngramSize: 2,
            minTermLength: 1
        )
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(maximumWorkUnits: 2),
            monotonicClock: TestProcessMonotonicClock()
        )

        #expect {
            try normalizer.forEachNormalizedTerm(from: "abcdefgh") { _ in
                try meter.consume(at: .indexScan)
            }
        } throws: { error in
            guard case .maximumWorkUnits(
                stage: .indexScan,
                consumed: 2,
                requested: 1,
                maximum: 2
            ) = error as? DatabaseWorkLimitError else {
                return false
            }
            return true
        }
    }
}
