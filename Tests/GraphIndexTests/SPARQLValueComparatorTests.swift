import DatabaseValue
import OntologyIndex
import Testing
@testable import GraphIndex

@Suite("SPARQL value comparison")
struct SPARQLValueComparatorTests {
    private static let xsd = "http://www.w3.org/2001/XMLSchema#"

    @Test("Arbitrary-size integer and decimal operands compare exactly")
    func arbitrarySizeExactComparison() throws {
        let comparator = SPARQLValueComparator()
        let magnitude = String(repeating: "9", count: 200)

        #expect(try comparator.compare(
            literal(magnitude, "integer"),
            literal(magnitude + ".0", "decimal")
        ) == .equal)
        #expect(try comparator.compare(
            literal("1" + String(repeating: "0", count: 200), "integer"),
            literal(magnitude + ".9", "decimal")
        ) == .greater)
    }

    @Test("XPath numeric promotion rounds operands to float or double")
    func numericPromotion() throws {
        let comparator = SPARQLValueComparator()

        #expect(try comparator.compare(
            literal("16777217", "integer"),
            literal("16777216", "float")
        ) == .equal)
        #expect(try comparator.compare(
            literal("9007199254740993", "integer"),
            literal("9007199254740992", "double")
        ) == .equal)
    }

    @Test("NaN and indeterminate dateTimes are unordered")
    func partialOrders() throws {
        let comparator = SPARQLValueComparator()

        #expect(try comparator.compare(
            literal("NaN", "double"),
            literal("1", "integer")
        ) == .unordered)
        #expect(try comparator.compare(
            literal("2000-01-01T00:00:00", "dateTime"),
            literal("2000-01-01T12:00:00Z", "dateTime")
        ) == .unordered)
        #expect(try comparator.compare(
            literal("2000-01-01T00:00:00Z", "dateTime"),
            literal("2000-01-01T01:00:00+01:00", "dateTimeStamp")
        ) == .equal)
    }

    @Test("Strings use Unicode codepoint collation and booleans have value order")
    func scalarAndBooleanOrders() throws {
        let comparator = SPARQLValueComparator()

        #expect(try comparator.compare(
            literal("é", "string"),
            literal("e\u{301}", "string")
        ) == .greater)
        #expect(try comparator.compare(
            literal("false", "boolean"),
            literal("true", "boolean")
        ) == .less)
    }

    @Test("Invalid, mixed, and language-tagged operands are type errors")
    func typeErrors() throws {
        let comparator = SPARQLValueComparator()

        #expect(try comparator.compare(
            literal("invalid", "integer"),
            literal("1", "integer")
        ) == .typeError)
        #expect(try comparator.compare(
            literal("1", "string"),
            literal("1", "integer")
        ) == .typeError)
        #expect(try comparator.compare(
            DatabaseRDFLiteral(lexicalForm: "1", language: "en"),
            literal("1", "integer")
        ) == .typeError)
    }

    @Test("SHACL lexical validation accepts custom datatypes and rejects ill-typed SPARQL datatypes")
    func lexicalValidationScope() throws {
        let comparator = SPARQLValueComparator()

        #expect(try comparator.validateLexicalForm(
            DatabaseRDFLiteral(
                lexicalForm: "application-defined",
                datatype: "https://example.com/CustomDatatype"
            )
        ))
        #expect(try !comparator.validateLexicalForm(
            literal("twelve", "integer")
        ))
    }

    @Test("Resource exhaustion remains a typed runtime failure")
    func resourceLimitFailure() {
        let limits = XSDValidationLimits(maxLexicalUTF8Bytes: 8)
        let comparator = SPARQLValueComparator(limits: limits)

        #expect(throws: XSDValidationFailure.self) {
            _ = try comparator.compare(
                literal("123456789", "integer"),
                literal("1", "integer")
            )
        }
    }

    private func literal(
        _ lexicalForm: String,
        _ localDatatype: String
    ) throws -> DatabaseRDFLiteral {
        try DatabaseRDFLiteral(
            lexicalForm: lexicalForm,
            datatype: Self.xsd + localDatatype
        )
    }
}
