// LiteralSPARQLTests.swift
// Tests for Literal.toSPARQLFieldValue() - SPARQL-aware Literal → FieldValue conversion

import Testing
import Foundation
import Core
import QueryIR
@testable import GraphIndex

@Suite("Literal toSPARQLFieldValue")
struct LiteralSPARQLTests {

    // MARK: - Common types (delegated to toFieldValue)

    @Test("null → .null")
    func testNull() {
        let result = QueryIR.Literal.null.toSPARQLFieldValue()
        #expect(result == .null)
    }

    @Test("bool(true) → .bool(true)")
    func testBoolTrue() {
        #expect(QueryIR.Literal.bool(true).toSPARQLFieldValue() == .bool(true))
    }

    @Test("bool(false) → .bool(false)")
    func testBoolFalse() {
        #expect(QueryIR.Literal.bool(false).toSPARQLFieldValue() == .bool(false))
    }

    @Test("int(42) → .int64(42)")
    func testInt() {
        #expect(QueryIR.Literal.int(42).toSPARQLFieldValue() == .int64(42))
    }

    @Test("double(3.14) → .double(3.14)")
    func testDouble() {
        #expect(QueryIR.Literal.double(3.14).toSPARQLFieldValue() == .double(3.14))
    }

    @Test("string → .string")
    func testString() {
        #expect(QueryIR.Literal.string("hello").toSPARQLFieldValue() == .string("hello"))
    }

    @Test("binary → .data")
    func testBinary() {
        let data = Data([0x01, 0x02, 0x03])
        #expect(QueryIR.Literal.binary(data).toSPARQLFieldValue() == .data(data))
    }

    // MARK: - RDF-specific types

    @Test("iri → .string(value)")
    func testIRI() {
        let result = QueryIR.Literal.iri("http://example.org/foo").toSPARQLFieldValue()
        #expect(result == .string("http://example.org/foo"))
    }

    @Test("blankNode → .string(_:id)")
    func testBlankNode() {
        let result = QueryIR.Literal.blankNode("b1").toSPARQLFieldValue()
        #expect(result == .string("_:b1"))
    }

    @Test("langLiteral → .string(value)")
    func testLangLiteral() {
        let result = QueryIR.Literal.langLiteral(value: "chat", language: "fr").toSPARQLFieldValue()
        #expect(result == .string("chat"))
    }

    @Test("date → .string(ISO8601 date)")
    func testDate() {
        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = TimeZone(identifier: "UTC")!
        let components = DateComponents(year: 2024, month: 1, day: 15)
        let date = cal.date(from: components)!
        let result = QueryIR.Literal.date(date).toSPARQLFieldValue()
        if case .string(let s) = result {
            #expect(s.contains("2024-01-15"))
        } else {
            Issue.record("Expected .string for date, got: \(result)")
        }
    }

    @Test("timestamp → .string(ISO8601 datetime)")
    func testTimestamp() {
        let date = Date(timeIntervalSince1970: 0) // 1970-01-01T00:00:00Z
        let result = QueryIR.Literal.timestamp(date).toSPARQLFieldValue()
        if case .string(let s) = result {
            #expect(s.contains("1970"))
        } else {
            Issue.record("Expected .string for timestamp, got: \(result)")
        }
    }

    // MARK: - typedLiteral with XSD datatypes

    @Test("typedLiteral xsd:integer → .int64")
    func testTypedLiteralInteger() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "42",
            datatype: "http://www.w3.org/2001/XMLSchema#integer"
        )
        #expect(lit.toSPARQLFieldValue() == .int64(42))
    }

    @Test("typedLiteral xsd:integer parse failure → .string")
    func testTypedLiteralIntegerFallback() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "not_a_number",
            datatype: "http://www.w3.org/2001/XMLSchema#integer"
        )
        #expect(lit.toSPARQLFieldValue() == .string("not_a_number"))
    }

    @Test("typedLiteral xsd:double → .double")
    func testTypedLiteralDouble() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "3.14",
            datatype: "http://www.w3.org/2001/XMLSchema#double"
        )
        #expect(lit.toSPARQLFieldValue() == .double(3.14))
    }

    @Test("typedLiteral xsd:float → .double")
    func testTypedLiteralFloat() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "2.5",
            datatype: "http://www.w3.org/2001/XMLSchema#float"
        )
        #expect(lit.toSPARQLFieldValue() == .double(2.5))
    }

    @Test("typedLiteral xsd:decimal → .double")
    func testTypedLiteralDecimal() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "1.23",
            datatype: "http://www.w3.org/2001/XMLSchema#decimal"
        )
        #expect(lit.toSPARQLFieldValue() == .double(1.23))
    }

    @Test("typedLiteral xsd:boolean true → .bool(true)")
    func testTypedLiteralBoolTrue() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "true",
            datatype: "http://www.w3.org/2001/XMLSchema#boolean"
        )
        #expect(lit.toSPARQLFieldValue() == .bool(true))
    }

    @Test("typedLiteral xsd:boolean 1 → .bool(true)")
    func testTypedLiteralBoolOne() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "1",
            datatype: "http://www.w3.org/2001/XMLSchema#boolean"
        )
        #expect(lit.toSPARQLFieldValue() == .bool(true))
    }

    @Test("typedLiteral xsd:boolean false → .bool(false)")
    func testTypedLiteralBoolFalse() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "false",
            datatype: "http://www.w3.org/2001/XMLSchema#boolean"
        )
        #expect(lit.toSPARQLFieldValue() == .bool(false))
    }

    @Test("typedLiteral xsd:string → .string")
    func testTypedLiteralString() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "hello",
            datatype: "http://www.w3.org/2001/XMLSchema#string"
        )
        #expect(lit.toSPARQLFieldValue() == .string("hello"))
    }

    @Test("typedLiteral xsd:anyURI → .string")
    func testTypedLiteralAnyURI() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "http://example.org",
            datatype: "http://www.w3.org/2001/XMLSchema#anyURI"
        )
        #expect(lit.toSPARQLFieldValue() == .string("http://example.org"))
    }

    @Test("typedLiteral xsd:date → .string")
    func testTypedLiteralDate() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "2024-01-15",
            datatype: "http://www.w3.org/2001/XMLSchema#date"
        )
        #expect(lit.toSPARQLFieldValue() == .string("2024-01-15"))
    }

    @Test("typedLiteral xsd:dateTime → .string")
    func testTypedLiteralDateTime() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "2024-01-15T10:30:00Z",
            datatype: "http://www.w3.org/2001/XMLSchema#dateTime"
        )
        #expect(lit.toSPARQLFieldValue() == .string("2024-01-15T10:30:00Z"))
    }

    @Test("typedLiteral xsd:base64Binary → .data")
    func testTypedLiteralBase64Binary() {
        let base64 = Data([0x01, 0x02, 0x03]).base64EncodedString()
        let lit = QueryIR.Literal.typedLiteral(
            value: base64,
            datatype: "http://www.w3.org/2001/XMLSchema#base64Binary"
        )
        #expect(lit.toSPARQLFieldValue() == .data(Data([0x01, 0x02, 0x03])))
    }

    @Test("typedLiteral xsd:base64Binary invalid → .string")
    func testTypedLiteralBase64BinaryInvalid() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "!!!not-base64!!!",
            datatype: "http://www.w3.org/2001/XMLSchema#base64Binary"
        )
        #expect(lit.toSPARQLFieldValue() == .string("!!!not-base64!!!"))
    }

    @Test("typedLiteral unknown datatype → .string")
    func testTypedLiteralUnknownDatatype() {
        let lit = QueryIR.Literal.typedLiteral(
            value: "some value",
            datatype: "http://example.org/customType"
        )
        #expect(lit.toSPARQLFieldValue() == .string("some value"))
    }

    // MARK: - Consistency with toFieldValue for common types

    @Test("Common types produce same result as toFieldValue")
    func testConsistencyWithToFieldValue() {
        let commonLiterals: [QueryIR.Literal] = [
            .null,
            .bool(true),
            .bool(false),
            .int(0),
            .int(-100),
            .int(Int64.max),
            .double(0.0),
            .double(-1.5),
            .string(""),
            .string("test"),
            .binary(Data()),
            .binary(Data([0xFF])),
        ]
        for lit in commonLiterals {
            let sparqlResult = lit.toSPARQLFieldValue()
            let genericResult = lit.toFieldValue()
            #expect(genericResult != nil, "toFieldValue() should not be nil for common type: \(lit)")
            #expect(sparqlResult == genericResult, "Mismatch for \(lit): sparql=\(sparqlResult) generic=\(String(describing: genericResult))")
        }
    }
}
