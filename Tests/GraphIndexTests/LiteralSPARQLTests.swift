#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("Literal to SPARQL FieldValue", .heartbeat)
struct LiteralSPARQLTests {
    @Test("SQL unknown strings and unresolved declarations cannot become RDF terms")
    func sqlBindingFormsAreUnsupported() throws {
        let literals: [Literal] = [
            .sqlUnknownString("value"),
            .queryValue(.primitive(.string("value")), type: .sql(.custom("text", schema: "private"))),
            .queryValue(.primitive(.string("value")), type: .sql(.custom("text"))),
        ]
        for literal in literals {
            #expect(
                throws: SPARQLLiteralConversionError.queryLiteralConversion(.unsupportedSPARQLLiteral)
            ) {
                try literal.toSPARQLFieldValue()
            }
            #expect(throws: SPARQLExpressionCompilationError.self) {
                try SPARQLExpressionValidator.validate(.literal(literal))
            }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                try ExpressionEvaluator.evaluate(.literal(literal), binding: VariableBinding())
            }
            #expect(
                throws: SPARQLLiteralConversionError.queryLiteralConversion(.unsupportedSPARQLLiteral)
            ) {
                try SPARQLConstructFootprintPlanner.maximumQuadFootprint(
                    template(object: literal), binding: VariableBinding()
                )
            }
        }
        let typed = Literal.queryValue(.primitive(.string("value")), type: .sql(.text))
        #expect(try typed.toSPARQLFieldValue() == Literal.string("value").toSPARQLFieldValue())
        try SPARQLExpressionValidator.validate(.literal(typed))
    }

    @Test("scalar values preserve SPARQL datatype identity")
    func commonScalarsConvert() throws {
        #expect(
            try Literal.bool(true).toSPARQLFieldValue()
                == rdfLiteral("true", datatype: Literal.XSDDatatype.boolean.rawValue)
        )
        #expect(
            try Literal.int(42).toSPARQLFieldValue()
                == rdfLiteral("42", datatype: Literal.XSDDatatype.integer.rawValue)
        )
        #expect(
            try Literal.double(3.14).toSPARQLFieldValue()
                == rdfLiteral("3.14", datatype: Literal.XSDDatatype.double.rawValue)
        )
        #expect(
            try Literal.string("hello").toSPARQLFieldValue()
                == rdfLiteral("hello", datatype: Literal.XSDDatatype.string.rawValue)
        )
    }

    @Test("RDF scalar forms preserve their lexical values")
    func rdfScalarsConvert() throws {
        #expect(
            try Literal.iri("https://example.invalid/resource").toSPARQLFieldValue()
                == .rdfTerm(
                    try .iri(validating: "https://example.invalid/resource")
                )
        )
        #expect(
            try Literal.blankNode("b1").toSPARQLFieldValue()
                == .rdfTerm(try .blankNode(identifier: "b1"))
        )
        let language = try RDFLanguageTag("fr")
        let languageLiteral = RDFLiteral(
            lexicalForm: "chat",
            language: language
        )
        #expect(
            try Literal.langLiteral(value: "chat", language: "fr").toSPARQLFieldValue()
                == .rdfTerm(.literal(languageLiteral))
        )
    }

    @Test("date and timestamp use canonical lexical forms")
    func temporalValuesConvert() throws {
        let date = try CivilDate(year: 2024, month: 1, day: 15)
        let timestamp = try Timestamp(
            secondsSinceUnixEpoch: 0,
            nanoseconds: 0
        )

        #expect(
            try Literal.date(date).toSPARQLFieldValue()
                == rdfLiteral("2024-01-15", datatype: Literal.XSDDatatype.date.rawValue)
        )
        #expect(
            try Literal.timestamp(timestamp).toSPARQLFieldValue()
                == rdfLiteral(
                    "1970-01-01T00:00:00Z",
                    datatype: Literal.XSDDatatype.dateTime.rawValue
                )
        )
    }

    @Test("valid typed scalar literals convert by datatype")
    func typedScalarsConvert() throws {
        #expect(
            try Literal.typedLiteral(
                value: "42",
                datatype: Literal.XSDDatatype.integer.rawValue
            ).toSPARQLFieldValue()
                == rdfLiteral("42", datatype: Literal.XSDDatatype.integer.rawValue)
        )
        #expect(
            try Literal.typedLiteral(
                value: "3.14",
                datatype: Literal.XSDDatatype.double.rawValue
            ).toSPARQLFieldValue()
                == rdfLiteral("3.14", datatype: Literal.XSDDatatype.double.rawValue)
        )
        #expect(
            try Literal.typedLiteral(
                value: "0",
                datatype: Literal.XSDDatatype.boolean.rawValue
            ).toSPARQLFieldValue()
                == rdfLiteral("0", datatype: Literal.XSDDatatype.boolean.rawValue)
        )
    }

    @Test("exact numeric values preserve lexical forms and datatypes")
    func exactNumericValuesRemainExact() throws {
        #expect(
            try Literal.uint(UInt64.max).toSPARQLFieldValue()
                == rdfLiteral(
                    String(UInt64.max),
                    datatype: Literal.XSDDatatype.unsignedLong.rawValue
                )
        )
        #expect(
            try Literal.decimal(coefficient: 123, scale: 2).toSPARQLFieldValue()
                == rdfLiteral("1.23", datatype: Literal.XSDDatatype.decimal.rawValue)
        )
    }

    @Test("invalid typed lexical forms fail deterministically")
    func invalidTypedLexicalFormsFail() {
        #expect(
            throws: SPARQLLiteralConversionError.invalidLexicalForm(
                value: "not-an-integer",
                datatype: Literal.XSDDatatype.integer.rawValue
            )
        ) {
            _ = try Literal.typedLiteral(
                value: "not-an-integer",
                datatype: Literal.XSDDatatype.integer.rawValue
            ).toSPARQLFieldValue()
        }
    }

    @Test("non-RDF container values fail with typed errors")
    func unsupportedContainerValuesFail() {
        #expect(throws: SPARQLLiteralConversionError.nullTermUnsupported) {
            _ = try Literal.null.toSPARQLFieldValue()
        }
        #expect(throws: SPARQLLiteralConversionError.arrayTermUnsupported) {
            _ = try Literal.array([]).toSPARQLFieldValue()
        }
    }

    @Test("query primitive wrappers retain RDF evaluation and footprint semantics")
    func primitiveQueryValuesDelegate() throws {
        let values: [FieldValue] = [
            .bool(true), .int64(42), .uint64(.max),
            .decimal(ExactDecimal(coefficient: 123, scale: 2)),
            .string("hello"),
            .rdfTerm(try .iri(validating: "https://example.invalid/resource")),
        ]
        for value in values {
            let wrapped = Literal.queryValue(.primitive(value))
            let original = try value.queryLiteral
            let expected = try original.toSPARQLFieldValue()
            #expect(try wrapped.toSPARQLFieldValue() == expected)
            try SPARQLExpressionValidator.validate(.literal(wrapped))
            #expect(
                try ExpressionEvaluator.evaluate(
                    .literal(wrapped), binding: VariableBinding()
                ) == expected
            )
            #expect(
                try SPARQLConstructFootprintPlanner.maximumQuadFootprint(
                    template(object: wrapped), binding: VariableBinding()
                ) == SPARQLConstructFootprintPlanner.maximumQuadFootprint(
                    template(object: original), binding: VariableBinding()
                )
            )
        }
        #expect(
            throws: SPARQLLiteralConversionError.queryLiteralConversion(
                .unsupportedFieldValue
            )
        ) {
            _ = try Literal.queryValue(.primitive(.object(FieldObject())))
                .toSPARQLFieldValue()
        }
        #expect(throws: SPARQLLiteralConversionError.nullTermUnsupported) {
            _ = try Literal.queryValue(.primitive(.null)).toSPARQLFieldValue()
        }
        #expect(throws: SPARQLLiteralConversionError.arrayTermUnsupported) {
            _ = try Literal.queryValue(.primitive(.array([]))).toSPARQLFieldValue()
        }
    }

    @Test("SQL query values fail before SPARQL execution or construction")
    func richQueryValuesAreUnsupported() throws {
        for value in try richQueryValues() {
            let literal = Literal.queryValue(value)
            #expect(
                throws: SPARQLLiteralConversionError.queryLiteralConversion(
                    .unsupportedSPARQLLiteral
                )
            ) {
                _ = try literal.toSPARQLFieldValue()
            }
            #expect(
                throws: SPARQLExpressionCompilationError
                    .unsupportedExpression("literal")
            ) {
                try SPARQLExpressionValidator.validate(.literal(literal))
            }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                _ = try ExpressionEvaluator.evaluate(
                    .literal(literal), binding: VariableBinding()
                )
            }
            #expect(
                throws: SPARQLLiteralConversionError.queryLiteralConversion(
                    .unsupportedSPARQLLiteral
                )
            ) {
                _ = try SPARQLConstructFootprintPlanner.maximumQuadFootprint(
                    template(object: literal), binding: VariableBinding()
                )
            }
        }
    }

    private func template(object: Literal) -> TriplePattern {
        TriplePattern(
            subject: .iri("https://example.invalid/subject"),
            predicate: .iri("https://example.invalid/predicate"),
            object: .literal(object)
        )
    }

    private func richQueryValues() throws -> [QueryValue] {
        let limits = QueryValueLimits(
            maximumRetainedBytes: 4096, maximumTotalNodes: 32,
            maximumNestingDepth: 8, maximumCollectionElements: 32
        )
        return [
            .numeric(try QueryNumericValue(
                value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 3
            )),
            .json(try QueryJSONValue(bytes: ByteString(utf8: "null"), limits: limits)),
            .jsonb(try QueryJSONBValue(root: .null, limits: limits)),
            .array(try QueryArrayValue(
                elementType: .integer, dimensions: [], elements: [], limits: limits
            )),
            .interval(try QueryIntervalValue(
                period: CalendarPeriod(months: 2, days: 3), time: TimeSpan(seconds: 4)
            )),
        ]
    }

    private func rdfLiteral(
        _ lexicalForm: String,
        datatype: String
    ) throws -> FieldValue {
        .rdfTerm(
            .literal(
                try RDFLiteral(
                    lexicalForm: lexicalForm,
                    datatype: datatype
                )
            )
        )
    }
}
#endif
