import Core
import DatabaseValue
import Testing
@testable import GraphIndex

@Suite("SPARQL numeric literals")
struct SPARQLNumericLiteralTests {
    private let xsd = "http://www.w3.org/2001/XMLSchema#"

    @Test("Canonical numeric datatypes are parsed")
    func canonicalNumericDatatypesAreParsed() throws {
        let integer = try #require(
            SPARQLNumericValue(try rdfLiteral("42", datatype: xsd + "integer"))
        )
        let decimal = try #require(
            SPARQLNumericValue(try rdfLiteral("42.5", datatype: xsd + "decimal"))
        )
        let double = try #require(
            SPARQLNumericValue(try rdfLiteral("4.25E1", datatype: xsd + "double"))
        )

        #expect(integer.exactInteger == 42)
        #expect(decimal.doubleValue == 42.5)
        #expect(double.doubleValue == 42.5)
    }

    @Test("Untyped and invalid lexical forms are rejected")
    func untypedAndInvalidLexicalFormsAreRejected() throws {
        #expect(SPARQLNumericValue(.string("42")) == nil)
        #expect(
            SPARQLNumericValue(try rdfLiteral("42", datatype: xsd + "string"))
                == nil
        )
        #expect(
            SPARQLNumericValue(try rdfLiteral("42x", datatype: xsd + "integer"))
                == nil
        )
        #expect(
            SPARQLNumericValue(try rdfLiteral("1E2", datatype: xsd + "decimal"))
                == nil
        )
        #expect(
            SPARQLNumericValue(try rdfLiteral("256", datatype: xsd + "unsignedByte"))
                == nil
        )
    }

    @Test("Numeric comparison uses value space instead of lexical order")
    func numericComparisonUsesValueSpace() throws {
        let ten = try #require(
            SPARQLNumericValue(try rdfLiteral("10", datatype: xsd + "integer"))
        )
        let two = try #require(
            SPARQLNumericValue(try rdfLiteral("2.0", datatype: xsd + "decimal"))
        )
        let equivalent = try #require(
            SPARQLNumericValue(try rdfLiteral("10.0", datatype: xsd + "decimal"))
        )

        #expect(ten.compare(to: two) == .orderedDescending)
        #expect(ten.compare(to: equivalent) == .orderedSame)
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
}
