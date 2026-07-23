import Core
import DatabaseValue
import QueryIR
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("Bounded SPARQL regular expressions", .heartbeat)
struct SPARQLBoundedRegularExpressionTests {
    @Test(
        "Nested quantifiers do not backtrack catastrophically",
        .timeLimit(.minutes(1))
    )
    func nestedQuantifiersAreLinear() throws {
        let expression = try SPARQLRegularExpression(pattern: "^(a+)+$")
        let input = String(repeating: "a", count: 50_000) + "!"

        #expect(try !expression.matches(input))
    }

    @Test("Prioritized NFA preserves greedy capture semantics")
    func greedyCaptures() throws {
        let expression = try SPARQLRegularExpression(pattern: "(a+)(a)")

        #expect(
            try expression.replacingMatches(
                in: "aaa",
                with: "$1|$2"
            ) == "aa|a"
        )
    }

    @Test("REPLACE expands captures globally without source copies")
    func globalCaptureReplacement() throws {
        let expression = try SPARQLRegularExpression(
            pattern: "([A-Za-z]+)-([0-9]+)"
        )

        #expect(
            try expression.replacingMatches(
                in: "abc-12 def-34",
                with: "$2:$1"
            ) == "12:abc 34:def"
        )
    }

    @Test("Unmatched optional captures expand to an empty string")
    func optionalCaptureReplacement() throws {
        let expression = try SPARQLRegularExpression(pattern: "(a)?b")

        #expect(
            try expression.replacingMatches(in: "b", with: "<$1>")
                == "<>"
        )
    }

    @Test("Replacement escapes are parsed deterministically")
    func replacementEscapes() throws {
        let expression = try SPARQLRegularExpression(pattern: "(a)")

        #expect(
            try expression.replacingMatches(
                in: "a",
                with: "\\\\$1-\\$1"
            ) == "\\a-$1"
        )
    }

    @Test("A missing capture reference is rejected")
    func missingCaptureReferenceFails() throws {
        let expression = try SPARQLRegularExpression(pattern: "(a)")

        #expect(throws: SPARQLRegularExpression.Error.self) {
            try expression.replacingMatches(in: "a", with: "$2")
        }
    }

    @Test("Zero-length global matches always advance by one scalar")
    func zeroLengthGlobalReplacementAdvances() throws {
        let expression = try SPARQLRegularExpression(pattern: "")

        #expect(
            try expression.replacingMatches(in: "ab", with: "_")
                == "_a_b_"
        )
    }

    @Test("Anchored zero-length matches retain untouched source text")
    func anchoredZeroLengthReplacement() throws {
        let expression = try SPARQLRegularExpression(pattern: "^|$")

        #expect(
            try expression.replacingMatches(in: "ab", with: "_")
                == "_ab_"
        )
    }

    @Test("Unicode scalar matching preserves decomposed captures")
    func unicodeScalarCaptures() throws {
        let expression = try SPARQLRegularExpression(pattern: "(.)")

        #expect(
            try expression.replacingMatches(
                in: "e\u{301}",
                with: "<$1>"
            ) == "<e><\u{301}>"
        )
    }

    @Test("Unicode categories and blocks use scalar properties")
    func unicodeProperties() throws {
        let category = try SPARQLRegularExpression(pattern: "^\\p{L}+$")
        let block = try SPARQLRegularExpression(
            pattern: "^\\p{IsHiragana}+$"
        )

        #expect(try category.matches("Å雪"))
        #expect(try block.matches("ひらがな"))
        #expect(try !block.matches("カタカナ"))
    }

    @Test("The i flag performs Unicode scalar case matching")
    func caseInsensitiveFlag() throws {
        let expression = try SPARQLRegularExpression(
            pattern: "^é+$",
            flags: "i"
        )

        #expect(try expression.matches("Éé"))
    }

    @Test("The m flag applies anchors at line boundaries")
    func multilineFlag() throws {
        let plain = try SPARQLRegularExpression(pattern: "^β$")
        let multiline = try SPARQLRegularExpression(
            pattern: "^β$",
            flags: "m"
        )

        #expect(try !plain.matches("α\nβ\nγ"))
        #expect(try multiline.matches("α\nβ\nγ"))
    }

    @Test("The s flag permits wildcard line separators")
    func dotAllFlag() throws {
        let plain = try SPARQLRegularExpression(pattern: "a.b")
        let dotAll = try SPARQLRegularExpression(pattern: "a.b", flags: "s")

        #expect(try !plain.matches("a\nb"))
        #expect(try dotAll.matches("a\nb"))
    }

    @Test("The x flag ignores bounded pattern trivia and comments")
    func extendedFlag() throws {
        let expression = try SPARQLRegularExpression(
            pattern: " a  # comment\n b ",
            flags: "x"
        )
        let escapedSpace = try SPARQLRegularExpression(
            pattern: "a\\ b",
            flags: "x"
        )

        #expect(try expression.matches("ab"))
        #expect(try escapedSpace.matches("a b"))
    }

    @Test("Unsupported backtracking constructs fail compilation")
    func unsupportedSyntaxFails() {
        for pattern in ["(?=a)", "(a)\\1", "a+?", "a++", "^*"] {
            #expect(throws: SPARQLRegularExpression.Error.self) {
                try SPARQLRegularExpression(pattern: pattern)
            }
        }
    }

    @Test("Unknown flags fail through the typed evaluation contract")
    func invalidFlagFails() {
        do {
            _ = try SPARQLRegularExpression.evaluateMatch(
                "value",
                pattern: "value",
                flags: "q"
            )
            Issue.record("An unknown flag was accepted")
        } catch let error as SPARQLExpressionEvaluationError {
            #expect(error == .invalidFunctionArguments("REGEX flags"))
        } catch {
            Issue.record("An unknown flag produced an untyped error")
        }
    }

    @Test("NFA state expansion is rejected before allocation")
    func stateLimitFailsBeforeCompilation() {
        let limits = SPARQLRegularExpression.Limits(nfaStates: 32)

        #expect(throws: SPARQLRegularExpression.Error.self) {
            try SPARQLRegularExpression(
                pattern: "a{100}",
                limits: limits
            )
        }
    }

    @Test("Pattern bytes are bounded before parsing")
    func patternLimitFailsBeforeParsing() {
        let limits = SPARQLRegularExpression.Limits(patternUTF8Bytes: 3)

        #expect(throws: SPARQLRegularExpression.Error.self) {
            try SPARQLRegularExpression(pattern: "abcd", limits: limits)
        }
    }

    @Test("Input bytes fail at the first over-limit Unicode scalar")
    func inputLimitFailsIncrementally() throws {
        let limits = SPARQLRegularExpression.Limits(inputUTF8Bytes: 3)
        let expression = try SPARQLRegularExpression(
            pattern: ".",
            limits: limits
        )

        do {
            _ = try expression.matches("雪a")
            Issue.record("An oversized regex input was accepted")
        } catch let error as SPARQLRegularExpression.Error {
            #expect(
                error == .resourceLimit(
                    name: "inputUTF8Bytes",
                    limit: 3,
                    actual: 4
                )
            )
        } catch {
            Issue.record("An oversized regex input produced an untyped error")
        }
    }

    @Test("Capture register count is fixed at nine")
    func captureLimitFailsBeforeCompilation() {
        #expect(throws: SPARQLRegularExpression.Error.self) {
            try SPARQLRegularExpression(
                pattern: "(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)"
            )
        }
    }

    @Test("Active transition work is shared across the complete search")
    func transitionWorkLimitFails() throws {
        let limits = SPARQLRegularExpression.Limits(
            activeTransitionWork: 32
        )
        let expression = try SPARQLRegularExpression(
            pattern: "z",
            limits: limits
        )

        do {
            _ = try expression.matches(String(repeating: "a", count: 100))
            Issue.record("A work-budget overflow was accepted")
        } catch let error as SPARQLRegularExpression.Error {
            guard case .resourceLimit(let name, let limit, let actual) = error
            else {
                Issue.record("A work-budget overflow produced the wrong error")
                return
            }
            #expect(name == "activeTransitionWork")
            #expect(limit == 32)
            #expect(actual > limit)
        } catch {
            Issue.record("A work-budget overflow produced an untyped error")
        }
    }

    @Test("REPLACE checks output bytes before appending")
    func outputLimitFailsBeforeAppend() throws {
        let limits = SPARQLRegularExpression.Limits(outputUTF8Bytes: 5)
        let expression = try SPARQLRegularExpression(
            pattern: "(.)",
            limits: limits
        )

        #expect(throws: SPARQLRegularExpression.Error.self) {
            try expression.replacingMatches(in: "abc", with: "$1$1")
        }
    }

    @Test("Global replacement has a shared match-count limit")
    func replacementMatchLimitFails() throws {
        let limits = SPARQLRegularExpression.Limits(replacementMatches: 2)
        let expression = try SPARQLRegularExpression(
            pattern: "",
            limits: limits
        )

        #expect(throws: SPARQLRegularExpression.Error.self) {
            try expression.replacingMatches(in: "ab", with: "")
        }
    }

    @Test("ExpressionEvaluator uses the bounded REPLACE engine")
    func expressionEvaluatorReplacementPath() throws {
        let expression = QueryIR.Expression.function(
            FunctionCall(
                name: "REPLACE",
                arguments: [
                    .literal(.string("alpha-12")),
                    .literal(.string("([a-z]+)-([0-9]+)")),
                    .literal(.string("$2:$1")),
                ]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(
                expression,
                binding: VariableBinding()
            ) == rdfString("12:alpha")
        )
    }

    @Test("LIKE escapes metacharacters through the bounded regex engine")
    func likeExpressionPath() throws {
        let wildcard = QueryIR.Expression.like(
            .literal(.string("雪だるま")),
            pattern: "雪%"
        )
        let literalMetacharacters = QueryIR.Expression.like(
            .literal(.string("a.[b]")),
            pattern: "a.[b]"
        )

        #expect(
            try ExpressionEvaluator.evaluate(
                wildcard,
                binding: VariableBinding()
            ) == rdfBoolean(true)
        )
        #expect(
            try ExpressionEvaluator.evaluate(
                literalMetacharacters,
                binding: VariableBinding()
            ) == rdfBoolean(true)
        )
    }

    @Test("LIKE rejects input and escaped output before materialization")
    func likePatternLimitsFailBeforeMaterialization() {
        assertLikePatternResourceLimit(
            String(
                repeating: "a",
                count: SPARQLExecutionLimits
                    .maximumRegularExpressionPatternUTF8Count + 1
            ),
            expectedRequired: UInt64(
                SPARQLExecutionLimits
                    .maximumRegularExpressionPatternUTF8Count + 1
            )
        )
        assertLikePatternResourceLimit(
            String(repeating: "*", count: 8_192),
            expectedRequired: 16_386
        )
    }

    @Test("FilterExpression uses the bounded regex engine")
    func filterExpressionPath() throws {
        let binding = VariableBinding(["?value": .string("Éclair")])

        #expect(
            try FilterExpression.regexWithFlags(
                "?value",
                "^é",
                "i"
            ).evaluate(binding)
        )
        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try FilterExpression.regexWithFlags(
                "?value",
                "é",
                "q"
            ).evaluate(binding)
        }
    }

    @Test("Runtime expression plans use the bounded regex engine")
    func runtimeExpressionPlanPath() async throws {
        let plan = try SPARQLExpressionPlan(
            .regex(
                .literal(.string("Éclair")),
                pattern: "^é",
                flags: "i"
            )
        )
        let resolver = SPARQLRuntimeExpressionResolver(
            exists: { _, _ in
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "immutable REGEX attempted dataset resolution"
                )
            },
            function: { _, _, _ in
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "immutable REGEX attempted function resolution"
                )
            }
        )

        #expect(
            try await SPARQLRuntimeExpressionEvaluator.evaluate(
                plan,
                binding: VariableBinding(),
                resolver: resolver
            ) == rdfBoolean(true)
        )
    }

    private func rdfString(_ lexicalForm: String) throws -> FieldValue {
        .rdfTerm(
            .literal(
                try DatabaseRDFLiteral(
                    lexicalForm: lexicalForm,
                    datatype: "http://www.w3.org/2001/XMLSchema#string"
                )
            )
        )
    }

    private func rdfBoolean(_ value: Bool) throws -> FieldValue {
        .rdfTerm(
            .literal(
                try DatabaseRDFLiteral(
                    lexicalForm: value ? "true" : "false",
                    datatype: "http://www.w3.org/2001/XMLSchema#boolean"
                )
            )
        )
    }

    private func assertLikePatternResourceLimit(
        _ pattern: String,
        expectedRequired: UInt64
    ) {
        do {
            _ = try ExpressionEvaluator.evaluate(
                .like(.literal(.string("value")), pattern: pattern),
                binding: VariableBinding()
            )
            Issue.record("An oversized LIKE pattern was accepted")
        } catch let error as SPARQLExpressionEvaluationError {
            guard case .resourceLimitExceeded(
                let stage,
                let required,
                let maximum
            ) = error else {
                Issue.record("An oversized LIKE pattern produced the wrong error")
                return
            }
            #expect(stage == "LIKE regular expression pattern")
            #expect(required == expectedRequired)
            #expect(
                maximum == UInt64(
                    SPARQLExecutionLimits
                        .maximumRegularExpressionPatternUTF8Count
                )
            )
        } catch {
            Issue.record("An oversized LIKE pattern produced an untyped error")
        }
    }
}
