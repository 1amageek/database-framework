/// TrigramSimilarityTests.swift
/// Unit tests for trigram Dice similarity and FilterExpression.similarTo

import Testing
import Foundation
import Core
import DatabaseEngine
@testable import GraphIndex

@Suite("Trigram Similarity Tests")
struct TrigramSimilarityTests {

    // MARK: - TrigramSimilarity.score (Dice)

    @Test("Identical strings have similarity 1.0")
    func testIdenticalStrings() {
        #expect(TrigramSimilarity.score("Google", "Google") == 1.0)
    }

    @Test("Completely different strings have zero similarity")
    func testDifferentStrings() {
        #expect(TrigramSimilarity.score("Apple", "Zebra") == 0.0)
    }

    @Test("Google vs Google LLC: Dice >= 0.45")
    func testGoogleLLC() {
        #expect(TrigramSimilarity.score("Google", "Google LLC") >= 0.45)
    }

    @Test("Apple vs Apple Inc.: Dice >= 0.45")
    func testAppleInc() {
        #expect(TrigramSimilarity.score("Apple", "Apple Inc.") >= 0.45)
    }

    @Test("Microsoft vs Microsoft Corporation: Dice >= 0.45")
    func testMicrosoftCorporation() {
        #expect(TrigramSimilarity.score("Microsoft", "Microsoft Corporation") >= 0.45)
    }

    @Test("Firebase vs Firebase (Google): Dice >= 0.45")
    func testFirebaseGoogle() {
        #expect(TrigramSimilarity.score("Firebase", "Firebase (Google)") >= 0.45)
    }

    @Test("Anthropic vs Anthropic PBC: Dice >= 0.45")
    func testAnthropicPBC() {
        #expect(TrigramSimilarity.score("Anthropic", "Anthropic PBC") >= 0.45)
    }

    @Test("Goldman Sachs vs Goldman Sachs Group: Dice >= 0.45")
    func testGoldmanSachs() {
        #expect(TrigramSimilarity.score("Goldman Sachs", "Goldman Sachs Group") >= 0.45)
    }

    @Test("Case insensitive")
    func testCaseInsensitive() {
        #expect(TrigramSimilarity.score("google", "GOOGLE") == 1.0)
    }

    @Test("Similarity is symmetric")
    func testSymmetric() {
        let ab = TrigramSimilarity.score("Apple", "Apple Inc.")
        let ba = TrigramSimilarity.score("Apple Inc.", "Apple")
        #expect(ab == ba)
    }

    @Test("Both empty strings have similarity 1.0")
    func testBothEmpty() {
        #expect(TrigramSimilarity.score("", "") == 1.0)
    }

    @Test("Cross-language names have no trigram overlap")
    func testCrossLanguage() {
        #expect(TrigramSimilarity.score("Toyota", "トヨタ自動車") == 0.0)
    }

    // MARK: - FilterExpression.similarTo

    @Test("similarTo filter matches: Google vs Google LLC at default threshold")
    func testFilterMatches() {
        let filter = FilterExpression.similarTo("?name", "Google LLC", 0.45)
        let binding = VariableBinding(["?name": .string("Google")])
        #expect(filter.evaluate(binding))
    }

    @Test("similarTo filter rejects unrelated names")
    func testFilterRejects() {
        let filter = FilterExpression.similarTo("?name", "Apple", 0.45)
        let binding = VariableBinding(["?name": .string("Zebra")])
        #expect(!filter.evaluate(binding))
    }

    @Test("similarTo filter handles unbound variable")
    func testFilterUnbound() {
        let filter = FilterExpression.similarTo("?name", "Google", 0.45)
        #expect(!filter.evaluate(VariableBinding()))
    }

    @Test("similarTo variables extraction")
    func testVariables() {
        let filter = FilterExpression.similarTo("?label", "test", 0.45)
        #expect(filter.variables == ["?label"])
    }

    @Test("similarTo description contains TRIGRAM_SIM")
    func testDescription() {
        let filter = FilterExpression.similarTo("?name", "Google", 0.45)
        #expect(filter.description.contains("TRIGRAM_SIM"))
    }

    @Test("similarTo equality")
    func testEquality() {
        let a = FilterExpression.similarTo("?name", "Google", 0.45)
        let b = FilterExpression.similarTo("?name", "Google", 0.45)
        let c = FilterExpression.similarTo("?name", "Apple", 0.45)
        #expect(a == b)
        #expect(a != c)
    }
}
