// RDFStarTests.swift
// Tests for RDF-star implementation: QuotedTripleEncoding, ExecutionTerm, ExpressionEvaluator

import Testing
import Foundation
import Core
import QueryIR
@testable import GraphIndex

// MARK: - QuotedTripleEncoding Tests

@Suite("QuotedTripleEncoding")
struct QuotedTripleEncodingTests {

    // MARK: Roundtrip

    @Test("String values roundtrip")
    func testStringRoundtrip() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:Toyota"),
            predicate: .string("rdf:type"),
            object: .string("ex:Company")
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.subject == .string("ex:Toyota"))
        #expect(decoded?.predicate == .string("rdf:type"))
        #expect(decoded?.object == .string("ex:Company"))
    }

    @Test("Int64 value roundtrip")
    func testInt64Roundtrip() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:count"),
            object: .int64(42)
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .int64(42))
    }

    @Test("Double value roundtrip")
    func testDoubleRoundtrip() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:price"),
            object: .double(3.14)
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .double(3.14))
    }

    @Test("Bool value roundtrip")
    func testBoolRoundtrip() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:active"),
            object: .bool(true)
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .bool(true))
    }

    @Test("Data value roundtrip")
    func testDataRoundtrip() {
        let data = Data([0x01, 0x02, 0x03])
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:data"),
            object: .data(data)
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .data(data))
    }

    @Test("Null value roundtrip")
    func testNullRoundtrip() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:value"),
            object: .null
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .null)
    }

    @Test("Negative int64 roundtrip")
    func testNegativeInt64Roundtrip() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:offset"),
            object: .int64(-9999)
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .int64(-9999))
    }

    // MARK: Nested Quoted Triple

    @Test("Nested quoted triple roundtrip")
    func testNestedRoundtrip() {
        // Inner triple: << ex:s ex:p ex:o >>
        let inner = QuotedTripleEncoding.encode(
            subject: .string("ex:inner_s"),
            predicate: .string("ex:inner_p"),
            object: .string("ex:inner_o")
        )

        // Outer triple uses inner as subject
        let outer = QuotedTripleEncoding.encode(
            subject: .string(inner),
            predicate: .string("ex:source"),
            object: .string("ex:Wikipedia")
        )

        let decoded = QuotedTripleEncoding.decode(outer)
        #expect(decoded != nil)
        #expect(decoded?.predicate == .string("ex:source"))
        #expect(decoded?.object == .string("ex:Wikipedia"))

        // Subject should be the inner encoded string
        if case .string(let subjectStr) = decoded?.subject {
            let innerDecoded = QuotedTripleEncoding.decode(subjectStr)
            #expect(innerDecoded != nil)
            #expect(innerDecoded?.subject == .string("ex:inner_s"))
            #expect(innerDecoded?.predicate == .string("ex:inner_p"))
            #expect(innerDecoded?.object == .string("ex:inner_o"))
        } else {
            Issue.record("Expected string subject for nested triple")
        }
    }

    @Test("Double-nested quoted triple roundtrip")
    func testDoubleNestedRoundtrip() {
        let level1 = QuotedTripleEncoding.encode(
            subject: .string("ex:a"),
            predicate: .string("ex:b"),
            object: .string("ex:c")
        )
        let level2 = QuotedTripleEncoding.encode(
            subject: .string(level1),
            predicate: .string("ex:d"),
            object: .string("ex:e")
        )
        let level3 = QuotedTripleEncoding.encode(
            subject: .string(level2),
            predicate: .string("ex:meta"),
            object: .string("ex:value")
        )

        let decoded = QuotedTripleEncoding.decode(level3)
        #expect(decoded != nil)
        #expect(decoded?.predicate == .string("ex:meta"))
    }

    // MARK: String Escaping

    @Test("String with < and > characters roundtrip")
    func testAngleBracketsInString() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:formula"),
            object: .string("x << 5 && y >> 3")
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .string("x << 5 && y >> 3"))
    }

    @Test("String with tab character roundtrip")
    func testTabInString() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:text"),
            object: .string("col1\tcol2\tcol3")
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .string("col1\tcol2\tcol3"))
    }

    @Test("String with percent character roundtrip")
    func testPercentInString() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:item"),
            predicate: .string("ex:rate"),
            object: .string("50% discount")
        )
        let decoded = QuotedTripleEncoding.decode(encoded)
        #expect(decoded != nil)
        #expect(decoded?.object == .string("50% discount"))
    }

    // MARK: isQuotedTriple

    @Test("isQuotedTriple returns true for valid encoding")
    func testIsQuotedTripleValid() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:s"),
            predicate: .string("ex:p"),
            object: .string("ex:o")
        )
        #expect(QuotedTripleEncoding.isQuotedTriple(encoded))
    }

    @Test("isQuotedTriple returns false for plain string")
    func testIsQuotedTriplePlainString() {
        #expect(!QuotedTripleEncoding.isQuotedTriple("hello world"))
    }

    @Test("isQuotedTriple returns false for string resembling delimiters")
    func testIsQuotedTripleFalsePositive() {
        // This looks like a quoted triple but isn't properly formatted
        #expect(!QuotedTripleEncoding.isQuotedTriple("<< hello >>"))
        #expect(!QuotedTripleEncoding.isQuotedTriple("<<not tagged>>"))
    }

    // MARK: Invalid Input

    @Test("Decode returns nil for invalid input")
    func testDecodeInvalid() {
        #expect(QuotedTripleEncoding.decode("hello") == nil)
        #expect(QuotedTripleEncoding.decode("") == nil)
        #expect(QuotedTripleEncoding.decode("<<>>") == nil)
        #expect(QuotedTripleEncoding.decode("<< >>") == nil)
    }
}

// MARK: - ExecutionTerm QuotedTriple Tests

@Suite("ExecutionTerm QuotedTriple")
struct ExecutionTermQuotedTripleTests {

    @Test("quotedTriple isBound when all components bound")
    func testIsBoundAllBound() {
        let term = ExecutionTerm.quotedTriple(
            subject: .value(.string("ex:s")),
            predicate: .value(.string("ex:p")),
            object: .value(.string("ex:o"))
        )
        #expect(term.isBound)
    }

    @Test("quotedTriple not isBound when contains variable")
    func testIsBoundWithVariable() {
        let term = ExecutionTerm.quotedTriple(
            subject: .variable("?s"),
            predicate: .value(.string("ex:p")),
            object: .value(.string("ex:o"))
        )
        #expect(!term.isBound)
    }

    @Test("quotedTriple literalValue encodes to canonical string")
    func testLiteralValue() {
        let term = ExecutionTerm.quotedTriple(
            subject: .value(.string("ex:s")),
            predicate: .value(.string("ex:p")),
            object: .value(.string("ex:o"))
        )
        let literal = term.literalValue
        #expect(literal != nil)
        if case .string(let s) = literal {
            #expect(QuotedTripleEncoding.isQuotedTriple(s))
            let decoded = QuotedTripleEncoding.decode(s)
            #expect(decoded?.subject == .string("ex:s"))
        }
    }

    @Test("quotedTriple literalValue returns nil with variable")
    func testLiteralValueWithVariable() {
        let term = ExecutionTerm.quotedTriple(
            subject: .variable("?s"),
            predicate: .value(.string("ex:p")),
            object: .value(.string("ex:o"))
        )
        #expect(term.literalValue == nil)
    }

    @Test("quotedTriple substitute replaces inner variables")
    func testSubstitute() {
        let term = ExecutionTerm.quotedTriple(
            subject: .variable("?s"),
            predicate: .value(.string("ex:p")),
            object: .variable("?o")
        )
        var binding = VariableBinding()
        binding = binding.binding("?s", to: .string("ex:Toyota"))
        binding = binding.binding("?o", to: .string("ex:Company"))

        let substituted = term.substitute(binding)
        if case .quotedTriple(let s, let p, let o) = substituted {
            #expect(s == .value(.string("ex:Toyota")))
            #expect(p == .value(.string("ex:p")))
            #expect(o == .value(.string("ex:Company")))
        } else {
            Issue.record("Expected quotedTriple after substitution")
        }
    }

    @Test("quotedTriple description")
    func testDescription() {
        let term = ExecutionTerm.quotedTriple(
            subject: .variable("?s"),
            predicate: .value(.string("ex:p")),
            object: .value(.string("ex:o"))
        )
        let desc = term.description
        #expect(desc.contains("<<"))
        #expect(desc.contains(">>"))
        #expect(desc.contains("?s"))
    }
}

// MARK: - ExecutionTriple Variables Tests

@Suite("ExecutionTriple QuotedTriple Variables")
struct ExecutionTripleQuotedTripleTests {

    @Test("Variables from quotedTriple in subject")
    func testVariablesInQuotedTripleSubject() {
        let triple = ExecutionTriple(
            subject: .quotedTriple(
                subject: .variable("?inner_s"),
                predicate: .value(.string("ex:p")),
                object: .variable("?inner_o")
            ),
            predicate: .value(.string("ex:source")),
            object: .variable("?source")
        )
        let vars = triple.variables
        #expect(vars.contains("?inner_s"))
        #expect(vars.contains("?inner_o"))
        #expect(vars.contains("?source"))
        #expect(vars.count == 3)
    }

    @Test("Variables from quotedTriple in object")
    func testVariablesInQuotedTripleObject() {
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: .value(.string("ex:related")),
            object: .quotedTriple(
                subject: .variable("?a"),
                predicate: .variable("?b"),
                object: .variable("?c")
            )
        )
        let vars = triple.variables
        #expect(vars.count == 4)
        #expect(vars.contains("?s"))
        #expect(vars.contains("?a"))
        #expect(vars.contains("?b"))
        #expect(vars.contains("?c"))
    }

    @Test("No duplicate variables from shared quotedTriple vars")
    func testSharedVariables() {
        // ?s appears in both outer subject and inner quotedTriple
        let triple = ExecutionTriple(
            subject: .variable("?s"),
            predicate: .value(.string("ex:about")),
            object: .quotedTriple(
                subject: .variable("?s"),
                predicate: .value(.string("ex:prop")),
                object: .variable("?o")
            )
        )
        let vars = triple.variables
        #expect(vars.count == 2) // ?s and ?o (no duplicates)
    }
}

// MARK: - ExpressionEvaluator RDF-star Tests

@Suite("ExpressionEvaluator RDF-star")
struct ExpressionEvaluatorRDFStarTests {

    @Test("TRIPLE() constructs a quoted triple string")
    func testTripleConstruction() {
        let expr: QueryIR.Expression = .triple(
            subject: .literal(.string("ex:s")),
            predicate: .literal(.string("ex:p")),
            object: .literal(.string("ex:o"))
        )
        let binding = VariableBinding()
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result != nil)
        if case .string(let s) = result {
            #expect(QuotedTripleEncoding.isQuotedTriple(s))
        } else {
            Issue.record("Expected string result from TRIPLE()")
        }
    }

    @Test("isTriple() returns true for quoted triple")
    func testIsTripleTrue() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:s"),
            predicate: .string("ex:p"),
            object: .string("ex:o")
        )
        let binding = VariableBinding(["?t": .string(encoded)])
        let expr: QueryIR.Expression = .isTriple(.variable(Variable("?t")))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .bool(true))
    }

    @Test("isTriple() returns false for non-triple")
    func testIsTripleFalse() {
        let binding = VariableBinding(["?x": .string("hello")])
        let expr: QueryIR.Expression = .isTriple(.variable(Variable("?x")))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .bool(false))
    }

    @Test("isTriple() returns false for non-string")
    func testIsTripleNonString() {
        let binding = VariableBinding(["?x": .int64(42)])
        let expr: QueryIR.Expression = .isTriple(.variable(Variable("?x")))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .bool(false))
    }

    @Test("SUBJECT() extracts subject from quoted triple")
    func testSubjectExtraction() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:Toyota"),
            predicate: .string("rdf:type"),
            object: .string("ex:Company")
        )
        let binding = VariableBinding(["?t": .string(encoded)])
        let expr: QueryIR.Expression = .subject(.variable(Variable("?t")))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .string("ex:Toyota"))
    }

    @Test("PREDICATE() extracts predicate from quoted triple")
    func testPredicateExtraction() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:Toyota"),
            predicate: .string("rdf:type"),
            object: .string("ex:Company")
        )
        let binding = VariableBinding(["?t": .string(encoded)])
        let expr: QueryIR.Expression = .predicate(.variable(Variable("?t")))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .string("rdf:type"))
    }

    @Test("OBJECT() extracts object from quoted triple")
    func testObjectExtraction() {
        let encoded = QuotedTripleEncoding.encode(
            subject: .string("ex:Toyota"),
            predicate: .string("rdf:type"),
            object: .string("ex:Company")
        )
        let binding = VariableBinding(["?t": .string(encoded)])
        let expr: QueryIR.Expression = .object(.variable(Variable("?t")))
        let result = ExpressionEvaluator.evaluate(expr, binding: binding)
        #expect(result == .string("ex:Company"))
    }

    @Test("SUBJECT/PREDICATE/OBJECT return nil for non-triple")
    func testAccessorsOnNonTriple() {
        let binding = VariableBinding(["?x": .string("not a triple")])
        #expect(ExpressionEvaluator.evaluate(.subject(.variable(Variable("?x"))), binding: binding) == nil)
        #expect(ExpressionEvaluator.evaluate(.predicate(.variable(Variable("?x"))), binding: binding) == nil)
        #expect(ExpressionEvaluator.evaluate(.object(.variable(Variable("?x"))), binding: binding) == nil)
    }

    @Test("Roundtrip: TRIPLE() then SUBJECT/PREDICATE/OBJECT")
    func testTripleRoundtrip() {
        // First construct a triple
        let tripleExpr: QueryIR.Expression = .triple(
            subject: .literal(.string("ex:A")),
            predicate: .literal(.string("ex:rel")),
            object: .literal(.string("ex:B"))
        )
        let binding = VariableBinding()
        guard let tripleValue = ExpressionEvaluator.evaluate(tripleExpr, binding: binding) else {
            Issue.record("TRIPLE() should not return nil")
            return
        }

        // Then extract components
        let binding2 = VariableBinding(["?t": tripleValue])
        let subject = ExpressionEvaluator.evaluate(.subject(.variable(Variable("?t"))), binding: binding2)
        let predicate = ExpressionEvaluator.evaluate(.predicate(.variable(Variable("?t"))), binding: binding2)
        let object = ExpressionEvaluator.evaluate(.object(.variable(Variable("?t"))), binding: binding2)

        #expect(subject == .string("ex:A"))
        #expect(predicate == .string("ex:rel"))
        #expect(object == .string("ex:B"))
    }

    @Test("TRIPLE() with typed object")
    func testTripleWithTypedObject() {
        let tripleExpr: QueryIR.Expression = .triple(
            subject: .literal(.string("ex:item")),
            predicate: .literal(.string("ex:count")),
            object: .literal(.int(42))
        )
        let binding = VariableBinding()
        guard let tripleValue = ExpressionEvaluator.evaluate(tripleExpr, binding: binding) else {
            Issue.record("TRIPLE() should not return nil")
            return
        }

        let binding2 = VariableBinding(["?t": tripleValue])
        let object = ExpressionEvaluator.evaluate(.object(.variable(Variable("?t"))), binding: binding2)
        #expect(object == .int64(42))
    }
}

// NOTE: GraphPatternConverter tests are not included here because
// GraphPatternConverter is in the Database module, not GraphIndex.
// The conversion is tested indirectly through integration tests.
