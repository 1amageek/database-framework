import Testing
@testable import OntologyIndex

@Suite("XSD 1.1 regular expressions")
struct XSDRegularExpressionTests {
    @Test("Matching is implicit whole-string matching and anchors are literals")
    func wholeMatchAndLiteralAnchors() throws {
        let plain = try XSDRegularExpression(pattern: "abc")
        #expect(try plain.wholeMatch("abc"))
        #expect(try !plain.wholeMatch("zabc"))
        #expect(try !plain.wholeMatch("abcz"))

        let anchors = try XSDRegularExpression(pattern: "^a$")
        #expect(try anchors.wholeMatch("^a$"))
        #expect(try !anchors.wholeMatch("a"))
    }

    @Test("Alternation, grouping, and every repetition form compose")
    func alternationGroupingAndRepetition() throws {
        let expression = try XSDRegularExpression(
            pattern: "(ab|c)+d?e{2,3}f{1,}"
        )
        #expect(try expression.wholeMatch("abccdeef"))
        #expect(try expression.wholeMatch("ceeef"))
        #expect(try expression.wholeMatch("abeeffff"))
        #expect(try !expression.wholeMatch("deef"))
        #expect(try !expression.wholeMatch("abde"))

        let zero = try XSDRegularExpression(pattern: "a{0}b{0,1}c{0,}")
        #expect(try zero.wholeMatch(""))
        #expect(try zero.wholeMatch("bccc"))
    }

    @Test("Character classes support negation, ranges, and subtraction")
    func characterClassSubtraction() throws {
        let consonants = try XSDRegularExpression(
            pattern: "[a-z-[aeiou]]+"
        )
        #expect(try consonants.wholeMatch("bcdf"))
        #expect(try !consonants.wholeMatch("face"))

        let outsideConsonants = try XSDRegularExpression(
            pattern: "[^a-z-[aeiou]]+"
        )
        #expect(try outsideConsonants.wholeMatch("AEIOU123"))
        #expect(try !outsideConsonants.wholeMatch("b"))
        #expect(try !outsideConsonants.wholeMatch("a"))

        let literalHyphenBeforeSubtraction = try XSDRegularExpression(
            pattern: "[a--[b]]+"
        )
        #expect(try literalHyphenBeforeSubtraction.wholeMatch("a-a"))
        #expect(try !literalHyphenBeforeSubtraction.wholeMatch("b"))

        let escapeThenHyphen = try XSDRegularExpression(pattern: "[\\d-a]+")
        #expect(try escapeThenHyphen.wholeMatch("1-a"))
    }

    @Test("XML NameStartChar and NameChar escapes use XML scalar rules")
    func xmlNameEscapes() throws {
        let name = try XSDRegularExpression(pattern: "\\i\\c*")
        #expect(try name.wholeMatch("name-1"))
        #expect(try name.wholeMatch(":名.1"))
        #expect(try !name.wholeMatch("1name"))
        #expect(try !name.wholeMatch("-name"))

        let notStart = try XSDRegularExpression(pattern: "\\I+")
        #expect(try notStart.wholeMatch("123-"))
        #expect(try !notStart.wholeMatch("a"))
    }

    @Test("Unicode category and XSD block escapes are strict")
    func unicodeCategoriesAndBlocks() throws {
        let categoryAndBlock = try XSDRegularExpression(
            pattern: "\\p{Lu}\\p{IsGreekandCoptic}+"
        )
        #expect(try categoryAndBlock.wholeMatch("AΩβ"))
        #expect(try !categoryAndBlock.wholeMatch("aΩ"))

        let notDigits = try XSDRegularExpression(pattern: "\\P{Nd}+")
        #expect(try notDigits.wholeMatch("abcΩ"))
        #expect(try !notDigits.wholeMatch("abc1"))

        let legacyPrivateUse = try XSDRegularExpression(
            pattern: "\\p{IsPrivateUse}"
        )
        #expect(try legacyPrivateUse.wholeMatch("\u{E000}"))
    }

    @Test("Wildcard excludes line feed and carriage return only")
    func wildcardSemantics() throws {
        let expression = try XSDRegularExpression(pattern: ".")
        #expect(try expression.wholeMatch("a"))
        #expect(try expression.wholeMatch("\t"))
        #expect(try !expression.wholeMatch("\n"))
        #expect(try !expression.wholeMatch("\r"))
    }

    @Test("Multi-character escapes follow XSD definitions")
    func multiCharacterEscapes() throws {
        let whitespace = try XSDRegularExpression(pattern: "\\s+")
        #expect(try whitespace.wholeMatch(" \t\n\r"))
        #expect(try !whitespace.wholeMatch("\u{00A0}"))

        let digits = try XSDRegularExpression(pattern: "\\d+")
        #expect(try digits.wholeMatch("123"))
        #expect(try digits.wholeMatch("١٢٣"))

        let words = try XSDRegularExpression(pattern: "\\w+")
        #expect(try words.wholeMatch("letters数字"))
        #expect(try !words.wholeMatch("letters_数字"))
        #expect(try !words.wholeMatch("word-space"))

        let complements = try XSDRegularExpression(
            pattern: "\\S\\D\\W\\C"
        )
        #expect(try complements.wholeMatch("aa_ "))
    }

    @Test("Single-character escapes represent their literal scalars")
    func singleCharacterEscapes() throws {
        let expression = try XSDRegularExpression(
            pattern: "\\n\\r\\t\\\\\\|\\.\\-\\^\\?\\*\\+\\{\\}\\(\\)\\[\\]"
        )
        #expect(
            try expression.wholeMatch("\n\r\t\\|.-^?*+{}()[]")
        )
    }

    @Test("Invalid escape, range, quantifier, and block fail compilation")
    func invalidSyntaxIsTyped() {
        let patterns = [
            "\\q",
            "[z-a]",
            "[--z]",
            "a{3,2}",
            "a{,2}",
            "a**",
            "\\p{Cs}",
            "\\p{IsNotAUnicodeBlock}",
            "[a-z-[b]c]",
            "\u{0000}",
        ]

        for pattern in patterns {
            let error = captureError {
                _ = try XSDRegularExpression(pattern: pattern)
            }
            guard case .invalidSyntax = error else {
                Issue.record("Expected invalidSyntax for pattern: \(pattern)")
                continue
            }
        }
    }

    @Test("Syntax offsets count Unicode scalars")
    func syntaxOffsetUsesUnicodeScalars() {
        let error = captureError {
            _ = try XSDRegularExpression(pattern: "é**")
        }
        guard case .invalidSyntax(let offset, _) = error else {
            Issue.record("Expected invalidSyntax")
            return
        }
        #expect(offset == 2)
    }

    @Test("Every compilation limit rejects excess input independently")
    func compilationLimits() {
        assertResourceLimit(
            "patternUTF8Bytes",
            pattern: "é",
            limits: .init(patternUTF8Bytes: 1)
        )
        assertResourceLimit(
            "patternScalars",
            pattern: "ab",
            limits: .init(patternScalars: 1)
        )
        assertResourceLimit(
            "nestingDepth",
            pattern: "((a))",
            limits: .init(nestingDepth: 1)
        )
        assertResourceLimit(
            "astNodes",
            pattern: "ab",
            limits: .init(astNodes: 2)
        )
        assertResourceLimit(
            "nfaStates",
            pattern: "a",
            limits: .init(nfaStates: 1)
        )
        assertResourceLimit(
            "quantifier",
            pattern: "a{3}",
            limits: .init(quantifier: 2)
        )
        assertResourceLimit(
            "nfaStates",
            pattern: "((ab){4096}){4096}",
            limits: .default
        )
    }

    @Test("Active transition work is bounded during matching")
    func activeTransitionWorkLimit() throws {
        let limits = XSDRegularExpression.Limits(activeTransitionWork: 1)
        let expression = try XSDRegularExpression(pattern: "a", limits: limits)
        let error = captureError {
            _ = try expression.wholeMatch("a")
        }
        assertResourceName("activeTransitionWork", error: error)
    }

    @Test("Adversarial ambiguous input fails by budget without backtracking")
    func adversarialInputIsBounded() throws {
        let limits = XSDRegularExpression.Limits(activeTransitionWork: 200)
        let expression = try XSDRegularExpression(
            pattern: "(a|aa)*b",
            limits: limits
        )
        let error = captureError {
            _ = try expression.wholeMatch(String(repeating: "a", count: 200))
        }
        assertResourceName("activeTransitionWork", error: error)
    }

    @Test("Empty-loop expressions terminate and preserve language semantics")
    func emptyLoopTerminates() throws {
        let expression = try XSDRegularExpression(pattern: "(a?)*")
        #expect(try expression.wholeMatch(""))
        #expect(try expression.wholeMatch("aaaa"))
        #expect(try !expression.wholeMatch("b"))
    }

    @Test("Nested empty quantifiers are normalized before NFA expansion")
    func nestedEmptyQuantifiersDoNotAmplifyCompilation() throws {
        let expression = try XSDRegularExpression(
            pattern: "(((){4096}){4096}){4096}"
        )
        #expect(try expression.wholeMatch(""))
        #expect(try !expression.wholeMatch("a"))
    }

    private func assertResourceLimit(
        _ expectedName: String,
        pattern: String,
        limits: XSDRegularExpression.Limits
    ) {
        let error = captureError {
            _ = try XSDRegularExpression(pattern: pattern, limits: limits)
        }
        assertResourceName(expectedName, error: error)
    }

    private func assertResourceName(
        _ expectedName: String,
        error: XSDRegularExpression.Error?
    ) {
        guard case .resourceLimit(let name, _, _) = error else {
            Issue.record("Expected resourceLimit named \(expectedName)")
            return
        }
        #expect(name == expectedName)
    }

    private func captureError(
        _ operation: () throws -> Void
    ) -> XSDRegularExpression.Error? {
        do {
            try operation()
            return nil
        } catch let error as XSDRegularExpression.Error {
            return error
        } catch {
            Issue.record("Unexpected error type: \(error)")
            return nil
        }
    }
}
