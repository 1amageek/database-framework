import DatabaseKit
import DatabaseTypes
import OntologyIndex
import Testing

@Suite("OWL datatype validation")
struct OWLDatatypeValidatorTests {
    private static let rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    private static let rdfs = "http://www.w3.org/2000/01/rdf-schema#"
    private static let owl = "http://www.w3.org/2002/07/owl#"
    private static let xsd = "http://www.w3.org/2001/XMLSchema#"

    @Test("Boolean lexical forms are exact and aliases share one value")
    func booleanLexicalForms() throws {
        let validator = OWLDatatypeValidator()

        try validator.validateLexicalForm(literal("true", "boolean"))
        try validator.validateLexicalForm(literal("false", "boolean"))
        #expect(try validator.isIdenticalValue(
            literal("true", "boolean"),
            literal("1", "boolean")
        ))
        #expect(try validator.isIdenticalValue(
            literal("false", "boolean"),
            literal("0", "boolean")
        ))
        assertInvalidLexicalForm {
            try validator.validateLexicalForm(literal("True", "boolean"))
        }
    }

    @Test("Integer validation and comparison remain exact beyond machine widths")
    func arbitraryPrecisionIntegers() throws {
        let validator = OWLDatatypeValidator()
        let lower = "1" + String(repeating: "0", count: 500)
        let upper = "9" + String(repeating: "9", count: 500)

        try validator.validateLexicalForm(literal(lower, "integer"))
        #expect(try validator.compare(
            literal(lower, "integer"),
            literal(upper, "integer")
        ) == .less)
        #expect(try validator.membership(
            of: literal("-128", "integer"),
            in: .datatype(Self.xsd + "byte")
        ).isMember)
        #expect(try !validator.membership(
            of: literal("128", "integer"),
            in: .datatype(Self.xsd + "byte")
        ).isMember)
        #expect(try validator.membership(
            of: literal("18446744073709551615", "integer"),
            in: .datatype(Self.xsd + "unsignedLong")
        ).isMember)
        #expect(try !validator.membership(
            of: literal("18446744073709551616", "integer"),
            in: .datatype(Self.xsd + "unsignedLong")
        ).isMember)
    }

    @Test("OWL real and rational values use exact bounded arithmetic")
    func exactRationalValues() throws {
        let validator = OWLDatatypeValidator()

        #expect(try validator.isIdenticalValue(
            owlLiteral("2/4", "rational"),
            owlLiteral("1/2", "rational")
        ))
        #expect(try validator.isIdenticalValue(
            owlLiteral("1/2", "rational"),
            literal("0.5", "decimal")
        ))
        #expect(try validator.membership(
            of: literal("5", "integer"),
            in: .datatype(Self.owl + "rational")
        ).isMember)
        #expect(try validator.membership(
            of: literal("5", "integer"),
            in: .datatype(Self.owl + "real")
        ).isMember)
        #expect(try !validator.membership(
            of: literal("5", "double"),
            in: .datatype(Self.owl + "real")
        ).isMember)
        #expect(try validator.membership(
            of: owlLiteral("2/5", "rational"),
            in: .datatypeRestriction(
                datatype: Self.owl + "rational",
                facets: [FacetRestriction(
                    facet: .minExclusive,
                    value: owlLiteral("1/3", "rational")
                )]
            )
        ).isMember)
        #expect(try validator.membership(
            of: literal("true", "boolean"),
            in: .datatype(Self.rdfs + "Literal")
        ).isMember)

        assertInvalidLexicalForm {
            try validator.validateLexicalForm(owlLiteral("1", "real"))
        }
        for invalid in ["1/0", "1/+2", "1/-2", "1/2/3"] {
            assertInvalidLexicalForm {
                try validator.validateLexicalForm(
                    owlLiteral(invalid, "rational")
                )
            }
        }

        let bounded = OWLDatatypeValidator(limits: XSDValidationLimits(
            maxRationalComparisonWork: 1
        ))
        assertResourceFailure(resource: "rationalComparisonWork") {
            _ = try bounded.compare(
                owlLiteral("12345678901234567890/98765432109876543211", "rational"),
                owlLiteral("22345678901234567890/88765432109876543211", "rational")
            )
        }
    }

    @Test("XML literals validate namespaces and compare normalized DOM values")
    func xmlLiteralValues() throws {
        let validator = OWLDatatypeValidator()
        let first = try rdfLiteral(
            "<p xmlns='urn:example' b='2' a='&#49;'>x&amp;y</p>",
            "XMLLiteral"
        )
        let second = try rdfLiteral(
            "<p a=\"1\" xmlns=\"urn:example\" b=\"2\">x&#38;y</p>",
            "XMLLiteral"
        )

        try validator.validateLexicalForm(first)
        #expect(try validator.isIdenticalValue(first, second))
        #expect(try validator.isIdenticalValue(
            rdfLiteral("<empty/>", "XMLLiteral"),
            rdfLiteral("<empty></empty>", "XMLLiteral")
        ))
        #expect(try !validator.isIdenticalValue(
            rdfLiteral("<x><![CDATA[value]]></x>", "XMLLiteral"),
            rdfLiteral("<x>value</x>", "XMLLiteral")
        ))

        let invalid = [
            "<a></b>",
            "<p:x/>",
            "<x>&undeclared;</x>",
            "<?xml version='1.0'?>",
            "<!DOCTYPE x><x/>",
            "<!-- invalid -- comment -->",
            "<x xmlns:p='urn:same' xmlns:q='urn:same' p:a='1' q:a='2'/>",
        ]
        for lexicalForm in invalid {
            assertInvalidLexicalForm {
                try validator.validateLexicalForm(
                    rdfLiteral(lexicalForm, "XMLLiteral")
                )
            }
        }

        assertInvalidRestriction {
            _ = try validator.compile(.datatypeRestriction(
                datatype: Self.rdf + "XMLLiteral",
                facets: [facet(.length, "1", "integer")]
            ))
        }

        let bounded = OWLDatatypeValidator(limits: XSDValidationLimits(
            maxXMLComparisonWork: 1
        ))
        assertResourceFailure(resource: "xmlComparisonWork") {
            _ = try bounded.isIdenticalValue(first, second)
        }

        let depthBounded = OWLDatatypeValidator(limits: XSDValidationLimits(
            maxXMLDepth: 1
        ))
        assertResourceFailure(resource: "xmlDepth") {
            try depthBounded.validateLexicalForm(
                rdfLiteral("<a><b/></a>", "XMLLiteral")
            )
        }

        let parsingWorkBounded = OWLDatatypeValidator(
            limits: XSDValidationLimits(maxXMLParsingWork: 0)
        )
        assertResourceFailure(resource: "xmlParsingWork") {
            try parsingWorkBounded.validateLexicalForm(
                rdfLiteral("<x a='1' b='2'/>", "XMLLiteral")
            )
        }
    }

    @Test("Decimal identity and digit facets use exact normalized digits")
    func exactDecimalFacets() throws {
        let validator = OWLDatatypeValidator(profile: .extendedXSD11)

        #expect(try validator.isIdenticalValue(
            literal("0001.2300", "decimal"),
            literal("1.23", "decimal")
        ))
        let range = OWLDataRange.datatypeRestriction(
            datatype: Self.xsd + "decimal",
            facets: [
                try facet(.totalDigits, "3", "integer"),
                try facet(.fractionDigits, "2", "integer"),
            ]
        )
        #expect(try validator.membership(
            of: literal("1.2300", "decimal"),
            in: range
        ).isMember)
        #expect(try !validator.membership(
            of: literal("12.345", "decimal"),
            in: range
        ).isMember)

        let oneCharacter = OWLDataRange.datatypeRestriction(
            datatype: Self.xsd + "string",
            facets: [try facet(.length, "1.0", "decimal")]
        )
        #expect(try validator.membership(
            of: literal("x", "string"),
            in: oneCharacter
        ).isMember)

        let hugeLength = String(repeating: "9", count: 100)
        #expect(try validator.membership(
            of: literal("x", "string"),
            in: .datatypeRestriction(
                datatype: Self.xsd + "string",
                facets: [facet(.maxLength, hugeLength, "integer")]
            )
        ).isMember)
        #expect(try !validator.membership(
            of: literal("x", "string"),
            in: .datatypeRestriction(
                datatype: Self.xsd + "string",
                facets: [facet(.minLength, hugeLength, "integer")]
            )
        ).isMember)
    }

    @Test("Floating point special values preserve order and identity")
    func floatingPointSpecialValues() throws {
        let validator = OWLDatatypeValidator()

        try validator.validateLexicalForm(literal("+INF", "double"))
        #expect(try validator.compare(
            literal("-INF", "double"),
            literal("-1E300", "double")
        ) == .less)
        #expect(try validator.compare(
            literal("NaN", "double"),
            literal("NaN", "double")
        ) == .unordered)
        #expect(try validator.isIdenticalValue(
            literal("NaN", "double"),
            literal("NaN", "double")
        ))
        #expect(try !validator.isIdenticalValue(
            literal("0", "float"),
            literal("-0", "float")
        ))
    }

    @Test("Date-time lexical validation handles end-of-day and timezone limits")
    func dateTimeLexicalForms() throws {
        let validator = OWLDatatypeValidator()

        try validator.validateLexicalForm(literal(
            "0000-02-29T24:00:00Z",
            "dateTimeStamp"
        ))
        #expect(try validator.isIdenticalValue(
            literal("2000-01-01T24:00:00Z", "dateTime"),
            literal("2000-01-02T00:00:00+00:00", "dateTimeStamp")
        ))
        assertInvalidLexicalForm {
            try validator.validateLexicalForm(literal(
                "2000-01-01T00:00:00",
                "dateTimeStamp"
            ))
        }
        assertInvalidLexicalForm {
            try validator.validateLexicalForm(literal(
                "2000-01-01T00:00:00+14:01",
                "dateTime"
            ))
        }
    }

    @Test("Duration comparison reports the XSD partial order")
    func durationPartialOrder() throws {
        let validator = OWLDatatypeValidator(profile: .extendedXSD11)

        #expect(try validator.compare(
            literal("P1Y", "duration"),
            literal("P12M", "duration")
        ) == .equal)
        #expect(try validator.compare(
            literal("P1M", "duration"),
            literal("P30D", "duration")
        ) == .unordered)
        assertInvalidLexicalForm {
            try validator.validateLexicalForm(literal("P", "duration"))
        }
    }

    @Test("Binary lexical validation checks pad bits and compares decoded octets")
    func binaryValues() throws {
        let validator = OWLDatatypeValidator()

        #expect(try validator.isIdenticalValue(
            literal("Zg==", "base64Binary"),
            literal(" Z g = = ", "base64Binary")
        ))
        #expect(try validator.membership(
            of: literal("666F", "hexBinary"),
            in: .datatypeRestriction(
                datatype: Self.xsd + "hexBinary",
                facets: [facet(.length, "2", "integer")]
            )
        ).isMember)
        assertInvalidLexicalForm {
            try validator.validateLexicalForm(literal("Zh==", "base64Binary"))
        }
        assertInvalidLexicalForm {
            try validator.validateLexicalForm(literal("F", "hexBinary"))
        }
    }

    @Test("XSD patterns are whole lexical matches and invalid restrictions fail")
    func patternAndRestrictionFailures() throws {
        let validator = OWLDatatypeValidator()
        let matching = OWLDataRange.datatypeRestriction(
            datatype: Self.xsd + "string",
            facets: [try facet(.pattern, "[A-Z]{2}[0-9]+", "string")]
        )

        #expect(try validator.membership(
            of: literal("AB12", "string"),
            in: matching
        ).isMember)
        #expect(try !validator.membership(
            of: literal("xAB12", "string"),
            in: matching
        ).isMember)
        assertInvalidRestriction {
            _ = try validator.compile(.datatypeRestriction(
                datatype: Self.xsd + "string",
                facets: [facet(.length, "-1", "integer")]
            ))
        }
        assertInvalidRestriction {
            _ = try validator.compile(.datatypeRestriction(
                datatype: Self.xsd + "string",
                facets: [facet(.pattern, "[", "string")]
            ))
        }
    }

    @Test("Data range operators preserve value identity and typed failures")
    func dataRangeOperators() throws {
        let validator = OWLDatatypeValidator()
        let one = OWLDataRange.dataOneOf([try literal("1.0", "decimal")])

        #expect(try validator.membership(
            of: literal("1", "integer"),
            in: one
        ).isMember)
        #expect(try validator.membership(
            of: literal("hello", "string"),
            in: .dataComplementOf(.datatype(Self.xsd + "integer"))
        ).isMember)
        #expect(try !validator.membership(
            of: literal("relative/path", "anyURI"),
            in: .datatype(Self.xsd + "string")
        ).isMember)
        assertUnsupportedDatatype {
            _ = try validator.compile(.dataComplementOf(
                .datatype("https://example.com/Unknown")
            ))
        }
    }

    @Test("Invalid data-range cardinalities fail during compilation")
    func invalidDataRangeCardinalities() {
        let validator = OWLDatatypeValidator()

        assertInvalidRestriction {
            _ = try validator.compile(.dataIntersectionOf([]))
        }
        assertInvalidRestriction {
            _ = try validator.compile(.dataIntersectionOf([
                .datatype(Self.xsd + "string"),
            ]))
        }
        assertInvalidRestriction {
            _ = try validator.compile(.dataUnionOf([]))
        }
        assertInvalidRestriction {
            _ = try validator.compile(.dataUnionOf([
                .datatype(Self.xsd + "string"),
            ]))
        }
        assertInvalidRestriction {
            _ = try validator.compile(.dataOneOf([]))
        }
        assertInvalidRestriction {
            _ = try validator.compile(.datatypeRestriction(
                datatype: Self.xsd + "string",
                facets: []
            ))
        }
    }

    @Test("Lexical, data-range, and matcher bounds report resource exhaustion")
    func resourceLimits() throws {
        let lexicalValidator = OWLDatatypeValidator(limits: XSDValidationLimits(
            maxLexicalUTF8Bytes: 4
        ))
        assertResourceFailure(resource: "lexicalUTF8Bytes") {
            try lexicalValidator.validateLexicalForm(literal("12345", "integer"))
        }

        let rangeValidator = OWLDatatypeValidator(limits: XSDValidationLimits(
            maxDataRangeNodes: 1
        ))
        assertResourceFailure(resource: "dataRangeNodes") {
            _ = try rangeValidator.compile(.dataComplementOf(
                .datatype(Self.xsd + "string")
            ))
        }

        let oneOfCountValidator = OWLDatatypeValidator(
            limits: XSDValidationLimits(maxDataOneOfLiterals: 1)
        )
        assertResourceFailure(resource: "dataOneOfLiterals") {
            _ = try oneOfCountValidator.compile(.dataOneOf([
                literal("a", "string"),
                literal("b", "string"),
            ]))
        }

        let oneOfPayloadValidator = OWLDatatypeValidator(
            limits: XSDValidationLimits(maxDataOneOfPayloadUTF8Bytes: 8)
        )
        assertResourceFailure(resource: "dataOneOfPayloadUTF8Bytes") {
            _ = try oneOfPayloadValidator.compile(.dataOneOf([
                literal("payload", "string"),
            ]))
        }

        let matchValidator = OWLDatatypeValidator(limits: XSDValidationLimits(
            maxRegexTransitionWork: 1
        ))
        let compiled = try matchValidator.compile(.datatypeRestriction(
            datatype: Self.xsd + "string",
            facets: [facet(.pattern, "a*", "string")]
        ))
        assertResourceFailure(resource: "regex.activeTransitionWork") {
            _ = try matchValidator.contains(
                literal("aaaa", "string"),
                in: compiled
            )
        }
    }

    @Test("Language metadata is admitted only by its RDF datatype")
    func languageMetadata() throws {
        let validator = OWLDatatypeValidator(profile: .owl2RDF11)
        let tagged = RDFLiteral(
            lexicalForm: "hello",
            language: try RDFLanguageTag("en-US")
        )

        try validator.validateLexicalForm(tagged)
        try validator.validateLexicalForm(RDFLiteral(
            lexicalForm: "hello",
            language: try RDFLanguageTag(
                "en-Latn-US-u-ca-gregory-x-private"
            )
        ))
        assertUnsupportedDatatype {
            try OWLDatatypeValidator().validateLexicalForm(tagged)
        }
        for invalidTag in [
            "de-1901-1901",
            "en-a-foo-a-bar",
        ] {
            #expect(throws: RDFLanguageTagError.self) {
                _ = try RDFLanguageTag(invalidTag)
            }
        }
        #expect(throws: RDFLanguageTagError.self) {
            _ = try RDFLanguageTag("en-abcdefghi")
        }

        let bounded = OWLDatatypeValidator(
            profile: .owl2RDF11,
            limits: XSDValidationLimits(maxLanguageSubtags: 2)
        )
        assertResourceFailure(resource: "languageSubtags") {
            try bounded.validateLexicalForm(RDFLiteral(
                lexicalForm: "hello",
                language: try RDFLanguageTag("en-Latn-US")
            ))
        }
    }

    @Test("Plain literals split without copies and apply string and language facets")
    func plainLiteralSemantics() throws {
        let validator = OWLDatatypeValidator()
        let uppercase = try rdfLiteral("Family Guy@FOX@EN", "PlainLiteral")
        let lowercase = try rdfLiteral("Family Guy@FOX@en", "PlainLiteral")

        try validator.validateLexicalForm(uppercase)
        #expect(try validator.isIdenticalValue(uppercase, lowercase))
        #expect(try validator.membership(
            of: literal("Family Guy@FOX", "string"),
            in: .datatype(Self.rdf + "PlainLiteral")
        ).isMember)
        #expect(try !validator.membership(
            of: uppercase,
            in: .datatype(Self.xsd + "string")
        ).isMember)

        let facets = OWLDataRange.datatypeRestriction(
            datatype: Self.rdf + "PlainLiteral",
            facets: [
                try facet(.length, "14", "integer"),
                try facet(.pattern, "Family Guy@FOX", "string"),
                FacetRestriction(
                    facet: .langRange,
                    value: try literal("en", "string")
                ),
            ]
        )
        #expect(try validator.membership(of: uppercase, in: facets).isMember)
        #expect(try !validator.membership(
            of: rdfLiteral("Family Guy@FOX@de", "PlainLiteral"),
            in: facets
        ).isMember)

        assertInvalidLexicalForm {
            try validator.validateLexicalForm(
                rdfLiteral("missing-language-separator", "PlainLiteral")
            )
        }
        assertInvalidLexicalForm {
            try validator.validateLexicalForm(
                rdfLiteral("invalid@12", "PlainLiteral")
            )
        }
    }

    @Test("Pattern facet values use the same typed literal validation path")
    func patternFacetMetadata() {
        let validator = OWLDatatypeValidator(profile: .owl2RDF11)
        assertInvalidRestriction {
            _ = try validator.compile(.datatypeRestriction(
                datatype: Self.xsd + "string",
                facets: [FacetRestriction(
                    facet: .pattern,
                    value: RDFLiteral(
                        lexicalForm: "a",
                        language: .english
                    )
                )]
            ))
        }
    }

    private func literal(
        _ lexicalForm: String,
        _ datatype: String
    ) throws -> RDFLiteral {
        try RDFLiteral(
            lexicalForm: lexicalForm,
            datatype: Self.xsd + datatype
        )
    }

    private func rdfLiteral(
        _ lexicalForm: String,
        _ datatype: String
    ) throws -> RDFLiteral {
        try RDFLiteral(
            lexicalForm: lexicalForm,
            datatype: Self.rdf + datatype
        )
    }

    private func owlLiteral(
        _ lexicalForm: String,
        _ datatype: String
    ) throws -> RDFLiteral {
        try RDFLiteral(
            lexicalForm: lexicalForm,
            datatype: Self.owl + datatype
        )
    }

    private func facet(
        _ facet: XSDFacet,
        _ lexicalForm: String,
        _ datatype: String
    ) throws -> FacetRestriction {
        FacetRestriction(
            facet: facet,
            value: try literal(lexicalForm, datatype)
        )
    }

    private func assertInvalidLexicalForm(
        _ operation: () throws -> Void
    ) {
        do {
            try operation()
            Issue.record("Expected an invalid lexical form failure")
        } catch let failure as XSDValidationFailure {
            guard case .invalidLexicalForm = failure else {
                Issue.record("Expected invalidLexicalForm, received \(failure)")
                return
            }
        } catch {
            Issue.record("Expected XSDValidationFailure, received \(error)")
        }
    }

    private func assertInvalidRestriction(
        _ operation: () throws -> Void
    ) {
        do {
            try operation()
            Issue.record("Expected an invalid restriction failure")
        } catch let failure as XSDValidationFailure {
            guard case .invalidRestriction = failure else {
                Issue.record("Expected invalidRestriction, received \(failure)")
                return
            }
        } catch {
            Issue.record("Expected XSDValidationFailure, received \(error)")
        }
    }

    private func assertUnsupportedDatatype(
        _ operation: () throws -> Void
    ) {
        do {
            try operation()
            Issue.record("Expected an unsupported datatype failure")
        } catch let failure as XSDValidationFailure {
            guard case .unsupportedDatatype = failure else {
                Issue.record("Expected unsupportedDatatype, received \(failure)")
                return
            }
        } catch {
            Issue.record("Expected XSDValidationFailure, received \(error)")
        }
    }

    private func assertResourceFailure(
        resource: String,
        _ operation: () throws -> Void
    ) {
        do {
            try operation()
            Issue.record("Expected a resource limit failure")
        } catch let failure as XSDValidationFailure {
            guard case .resourceLimitExceeded(let actual, _, _) = failure else {
                Issue.record("Expected resourceLimitExceeded, received \(failure)")
                return
            }
            #expect(actual == resource)
        } catch {
            Issue.record("Expected XSDValidationFailure, received \(error)")
        }
    }
}
