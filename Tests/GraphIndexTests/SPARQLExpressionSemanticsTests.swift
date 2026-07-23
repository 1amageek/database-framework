import Core
import DatabaseValue
import QueryIR
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("SPARQL expression semantics", .heartbeat)
struct SPARQLExpressionSemanticsTests {
    @Test("Expression booleans are canonical RDF literals")
    func canonicalBooleanResult() throws {
        let result = try ExpressionEvaluator.evaluate(
            .equal(.literal(.int(1)), .literal(.decimal(coefficient: 10, scale: 1))),
            binding: VariableBinding()
        )
        let expected = try rdfLiteral("true", datatype: xsd("boolean"))

        #expect(result == expected)
    }

    @Test("Language-tagged literals have no effective boolean value")
    func languageLiteralEBVIsAnExpressionError() {
        let expression = QueryIR.Expression.not(
            .literal(.langLiteral(value: "text", language: "en"))
        )

        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                expression,
                binding: VariableBinding()
            )
        }
    }

    @Test("AND follows the SPARQL true false error table")
    func logicalAndTruthTable() throws {
        let error = QueryIR.Expression.variable(Variable("missing"))
        let falseValue = QueryIR.Expression.literal(.bool(false))
        let trueValue = QueryIR.Expression.literal(.bool(true))

        #expect(
            try ExpressionEvaluator.evaluate(
                .and(falseValue, error),
                binding: VariableBinding()
            ) == rdfLiteral("false", datatype: xsd("boolean"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(
                .and(error, falseValue),
                binding: VariableBinding()
            ) == rdfLiteral("false", datatype: xsd("boolean"))
        )
        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                .and(trueValue, error),
                binding: VariableBinding()
            )
        }
        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                .and(error, trueValue),
                binding: VariableBinding()
            )
        }
    }

    @Test("OR follows the SPARQL true false error table")
    func logicalOrTruthTable() throws {
        let error = QueryIR.Expression.variable(Variable("missing"))
        let falseValue = QueryIR.Expression.literal(.bool(false))
        let trueValue = QueryIR.Expression.literal(.bool(true))

        #expect(
            try ExpressionEvaluator.evaluate(
                .or(trueValue, error),
                binding: VariableBinding()
            ) == rdfLiteral("true", datatype: xsd("boolean"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(
                .or(error, trueValue),
                binding: VariableBinding()
            ) == rdfLiteral("true", datatype: xsd("boolean"))
        )
        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                .or(falseValue, error),
                binding: VariableBinding()
            )
        }
        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                .or(error, falseValue),
                binding: VariableBinding()
            )
        }
    }

    @Test("NaN comparisons return booleans rather than expression errors")
    func nanComparisonSemantics() throws {
        let nan = QueryIR.Expression.literal(.double(.nan))
        let one = QueryIR.Expression.literal(.double(1))
        let binding = VariableBinding()

        #expect(
            try ExpressionEvaluator.evaluate(.equal(nan, nan), binding: binding)
                == rdfLiteral("false", datatype: xsd("boolean"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(.notEqual(nan, nan), binding: binding)
                == rdfLiteral("true", datatype: xsd("boolean"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(.lessThan(nan, one), binding: binding)
                == rdfLiteral("false", datatype: xsd("boolean"))
        )
    }

    @Test("Floating-point division by zero follows IEEE 754")
    func floatingDivisionByZero() throws {
        let result = try ExpressionEvaluator.evaluate(
            .divide(.literal(.double(1)), .literal(.double(0))),
            binding: VariableBinding()
        )
        let expected = try rdfLiteral("INF", datatype: xsd("double"))

        #expect(result == expected)
    }

    @Test("STRLEN and SUBSTR count Unicode code points")
    func unicodeCodePointStringOperations() throws {
        let source = QueryIR.Expression.literal(.string("e\u{301}x"))
        let length = QueryIR.Expression.function(
            FunctionCall(name: "STRLEN", arguments: [source])
        )
        let substring = QueryIR.Expression.function(
            FunctionCall(
                name: "SUBSTR",
                arguments: [source, .literal(.int(2)), .literal(.int(1))]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(length, binding: VariableBinding())
                == rdfLiteral("3", datatype: xsd("integer"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(substring, binding: VariableBinding())
                == rdfLiteral("\u{301}", datatype: xsd("string"))
        )
    }

    @Test("Case conversion preserves language annotation")
    func caseConversionPreservesLanguage() throws {
        let expression = QueryIR.Expression.function(
            FunctionCall(
                name: "UCASE",
                arguments: [
                    .literal(.langLiteral(value: "hello", language: "en"))
                ]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(
                expression,
                binding: VariableBinding()
            )
                == .rdfTerm(
                    .literal(
                        DatabaseRDFLiteral(
                            lexicalForm: "HELLO",
                            language: .english
                        )
                    )
                )
        )
    }

    @Test("HASLANG and HASLANGDIR return false for non-literals")
    func languagePredicatesOnIRI() throws {
        let iri = QueryIR.Expression.literal(.iri("https://example.com/value"))
        for function in ["HASLANG", "HASLANGDIR"] {
            let expression = QueryIR.Expression.function(
                FunctionCall(name: function, arguments: [iri])
            )
            #expect(
                try ExpressionEvaluator.evaluate(
                    expression,
                    binding: VariableBinding()
                ) == rdfLiteral("false", datatype: xsd("boolean"))
            )
        }
    }

    @Test("ENCODE_FOR_URI percent-encodes UTF-8 and preserves unreserved bytes")
    func encodeForURIUsesUTF8() throws {
        let expression = QueryIR.Expression.function(
            FunctionCall(
                name: "ENCODE_FOR_URI",
                arguments: [.literal(.string("A /雪"))]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(
                expression,
                binding: VariableBinding()
            ) == rdfLiteral("A%20%2F%E9%9B%AA", datatype: xsd("string"))
        )
    }

    @Test("LANGMATCHES uses case-insensitive basic language filtering")
    func languageRangeMatching() throws {
        let binding = VariableBinding()
        let matches = QueryIR.Expression.function(
            FunctionCall(
                name: "LANGMATCHES",
                arguments: [
                    .literal(.string("en-GB")),
                    .literal(.string("EN")),
                ]
            )
        )
        let wildcardEmpty = QueryIR.Expression.function(
            FunctionCall(
                name: "LANGMATCHES",
                arguments: [
                    .literal(.string("")),
                    .literal(.string("*")),
                ]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(matches, binding: binding)
                == rdfLiteral("true", datatype: xsd("boolean"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(wildcardEmpty, binding: binding)
                == rdfLiteral("false", datatype: xsd("boolean"))
        )
    }

    @Test("STRBEFORE and STRAFTER preserve the first argument annotation")
    func stringBeforeAfterPreserveLanguage() throws {
        let source = QueryIR.Expression.literal(
            .langLiteral(value: "alpha-beta", language: "en")
        )
        let separator = QueryIR.Expression.literal(.string("-"))
        let before = QueryIR.Expression.function(
            FunctionCall(name: "STRBEFORE", arguments: [source, separator])
        )
        let after = QueryIR.Expression.function(
            FunctionCall(name: "STRAFTER", arguments: [source, separator])
        )

        #expect(
            try ExpressionEvaluator.evaluate(before, binding: VariableBinding())
                == .rdfTerm(
                    .literal(
                        DatabaseRDFLiteral(
                            lexicalForm: "alpha",
                            language: .english
                        )
                    )
                )
        )
        #expect(
            try ExpressionEvaluator.evaluate(after, binding: VariableBinding())
                == .rdfTerm(
                    .literal(
                        DatabaseRDFLiteral(
                            lexicalForm: "beta",
                            language: .english
                        )
                    )
                )
        )
    }

    @Test("sameTerm tests RDF identity rather than numeric value equality")
    func sameTermUsesRDFIdentity() throws {
        let expression = QueryIR.Expression.function(
            FunctionCall(
                name: "SAMETERM",
                arguments: [
                    .literal(.typedLiteral(value: "01", datatype: xsd("integer"))),
                    .literal(.typedLiteral(value: "1", datatype: xsd("integer"))),
                ]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(
                expression,
                binding: VariableBinding()
            ) == rdfLiteral("false", datatype: xsd("boolean"))
        )
    }

    @Test("STRDT and STRLANG construct canonical RDF literals")
    func stringLiteralConstructors() throws {
        let typed = QueryIR.Expression.function(
            FunctionCall(
                name: "STRDT",
                arguments: [
                    .literal(.string("42")),
                    .literal(.iri(xsd("integer"))),
                ]
            )
        )
        let language = QueryIR.Expression.function(
            FunctionCall(
                name: "STRLANG",
                arguments: [
                    .literal(.string("hello")),
                    .literal(.string("en")),
                ]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(typed, binding: VariableBinding())
                == rdfLiteral("42", datatype: xsd("integer"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(language, binding: VariableBinding())
                == .rdfTerm(
                    .literal(
                        DatabaseRDFLiteral(
                            lexicalForm: "hello",
                            language: .english
                        )
                    )
                )
        )
    }

    @Test("XSD operand constructors cast and validate their result")
    func xsdOperandConstructors() throws {
        let integer = QueryIR.Expression.function(
            FunctionCall(
                name: xsd("integer"),
                arguments: [.literal(.string("42"))]
            )
        )
        let boolean = QueryIR.Expression.function(
            FunctionCall(
                name: xsd("boolean"),
                arguments: [.literal(.int(0))]
            )
        )
        let string = QueryIR.Expression.function(
            FunctionCall(
                name: xsd("string"),
                arguments: [.literal(.iri("did:example:value"))]
            )
        )
        let invalidInteger = QueryIR.Expression.function(
            FunctionCall(
                name: xsd("integer"),
                arguments: [.literal(.string("1.5"))]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(
                integer,
                binding: VariableBinding()
            ) == rdfLiteral("42", datatype: xsd("integer"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(
                boolean,
                binding: VariableBinding()
            ) == rdfLiteral("false", datatype: xsd("boolean"))
        )
        #expect(
            try ExpressionEvaluator.evaluate(
                string,
                binding: VariableBinding()
            ) == rdfLiteral("did:example:value", datatype: xsd("string"))
        )
        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                invalidInteger,
                binding: VariableBinding()
            )
        }
    }

    @Test("Dynamic REGEX evaluates its pattern expression and rejects flags")
    func dynamicRegularExpression() throws {
        let binding = VariableBinding([
            "?pattern": try rdfLiteral("^a", datatype: xsd("string"))
        ])
        let expression = QueryIR.Expression.function(
            FunctionCall(
                name: "REGEX",
                arguments: [
                    .literal(.string("alpha")),
                    .variable(Variable("pattern")),
                ]
            )
        )
        let invalidFlags = QueryIR.Expression.function(
            FunctionCall(
                name: "REGEX",
                arguments: [
                    .literal(.string("alpha")),
                    .literal(.string("a")),
                    .literal(.string("q")),
                ]
            )
        )

        #expect(
            try ExpressionEvaluator.evaluate(expression, binding: binding)
                == rdfLiteral("true", datatype: xsd("boolean"))
        )
        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                invalidFlags,
                binding: VariableBinding()
            )
        }
    }

    private func rdfLiteral(
        _ lexicalForm: String,
        datatype: String
    ) throws -> FieldValue {
        .rdfTerm(
            .literal(
                try DatabaseRDFLiteral(
                    lexicalForm: lexicalForm,
                    datatype: datatype
                )
            )
        )
    }

    private func xsd(_ localName: String) -> String {
        "http://www.w3.org/2001/XMLSchema#\(localName)"
    }
}
