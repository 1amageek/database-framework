#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import DatabaseKit
import Testing
@testable import DatabaseEngine

@Suite("Literal bridge contracts")
struct LiteralConversionTests {
    @Persistable
    struct Entity {
        #Directory<Entity>("tests", "literal-bridge")

        var id: String = ""
        var value: Int64 = 0
        var note: String? = nil
    }

    @Test("Canonical projection preserves every exact scalar kind")
    func canonicalProjectionPreservesExactScalars() throws {
        let date = try CivilDate(year: 2026, month: 7, day: 21)
        let timestamp = try Timestamp(
            secondsSinceUnixEpoch: 1_774_310_400,
            nanoseconds: 123_456_789
        )
        let uuid = DatabaseTypes.UUID(
            high: 0x0011_2233_4455_6677,
            low: 0x8899_AABB_CCDD_EEFF
        )

        #expect(
            try Literal.decimal(coefficient: 12_345, scale: 2)
                .toFieldValue()
                == .decimal(
                    ExactDecimal(coefficient: 12_345, scale: 2)
                )
        )
        #expect(try Literal.date(date).toFieldValue() == .date(date))
        #expect(
            try Literal.timestamp(timestamp).toFieldValue()
                == .timestamp(timestamp)
        )
        #expect(try Literal.uuid(uuid).toFieldValue() == .uuid(uuid))
    }

    @Test("Literal projection preserves values without numeric loss")
    func literalProjectionPreservesValues() throws {
        let bytes = ByteString([0x00, 0x7F, 0xFF])
        let maximumUnsigned = UInt64.max

        #expect(
            try Literal.uint(maximumUnsigned).toFieldValue()
                == .uint64(maximumUnsigned)
        )
        #expect(try Literal.binary(bytes).toFieldValue() == .bytes(bytes))
        #expect(
            try Literal.array([.int(-1), .uint(maximumUnsigned)])
                .toFieldValue()
                == .array([.int64(-1), .uint64(maximumUnsigned)])
        )
    }

    @Test("Every QueryIR-representable FieldValue kind round-trips")
    func queryRepresentableFieldValuesRoundTrip() throws {
        let rdfTerm = RDFTerm.tripleTerm(
            subject: .iri(try RDFIRI("urn:subject")),
            predicate: try RDFPredicateIRI("urn:predicate"),
            object: .blankNode(try RDFBlankNodeIdentifier("object"))
        )
        let values: [FieldValue] = [
            .null,
            .bool(true),
            .int64(.min),
            .uint64(.max),
            .float64(1.25),
            .decimal(ExactDecimal(coefficient: 12_345, scale: 2)),
            .string("value"),
            .bytes(ByteString([0x00, 0x7F, 0xFF])),
            .date(try CivilDate(year: 2026, month: 7, day: 21)),
            .timestamp(
                try Timestamp(
                    secondsSinceUnixEpoch: 1_774_310_400,
                    nanoseconds: 123_456_789
                )
            ),
            .uuid(
                DatabaseTypes.UUID(
                    high: 0x0011_2233_4455_6677,
                    low: 0x8899_AABB_CCDD_EEFF
                )
            ),
            .rdfTerm(rdfTerm),
            .array([
                .int64(-1),
                .uint64(.max),
                .decimal(ExactDecimal(coefficient: 25, scale: 1)),
                .rdfTerm(rdfTerm),
            ]),
        ]

        for value in values {
            #expect(try value.toLiteral().toFieldValue() == value)
        }
    }

    @Test("Canonical RDF projection preserves term annotations")
    func canonicalRDFProjectionPreservesAnnotations() throws {
        let datatype = "http://www.w3.org/2001/XMLSchema#integer"
        let language = try RDFLanguageTag("en-US")

        #expect(
            try Literal.typedLiteral(
                value: "42",
                datatype: datatype
            ).toFieldValue()
                == .rdfTerm(
                    .literal(
                        try RDFLiteral(
                            lexicalForm: "42",
                            datatype: datatype
                        )
                    )
                )
        )
        #expect(
            try Literal.langLiteral(
                value: "colour",
                language: "en-US"
            ).toFieldValue()
                == .rdfTerm(
                    .literal(
                        RDFLiteral(
                            lexicalForm: "colour",
                            language: language
                        )
                    )
                )
        )
        #expect(
            try Literal.dirLangLiteral(
                value: "text",
                language: "en-US",
                direction: "rtl"
            ).toFieldValue()
                == .rdfTerm(
                    .literal(
                        RDFLiteral(
                            lexicalForm: "text",
                            language: language,
                            direction: .rightToLeft
                        )
                    )
                )
        )
    }

    @Test("Binary projections retain the original byte storage")
    func binaryProjectionRetainsOriginalStorage() throws {
        let source = ByteString.copying(count: 4_096) { buffer in
            buffer.initializeMemory(as: UInt8.self, repeating: 0xA5)
        }
        guard case .bytes(let canonical) = try Literal.binary(source)
            .toFieldValue() else {
            Issue.record("Expected canonical bytes")
            return
        }
        source.withUnsafeBytes { sourceBuffer in
            canonical.withUnsafeBytes { canonicalBuffer in
                #expect(sourceBuffer.baseAddress == canonicalBuffer.baseAddress)
                #expect(sourceBuffer.count == canonicalBuffer.count)
            }
        }
    }

    @Test("Query literals reject values without a literal representation")
    func queryLiteralProjectionRejectsObjectAndReferenceValues() throws {
        #expect(
            throws: LiteralConversionError.fieldValueUnsupported(kind: .object)
        ) {
            _ = try FieldValue.object(FieldObject()).toLiteral()
        }

        let identity = try EntityReference(
            entity: "Event",
            id: .string("event-1")
        )
        #expect(
            throws: LiteralConversionError.fieldValueUnsupported(
                kind: .reference
            )
        ) {
            _ = try FieldValue.reference(identity).toLiteral()
        }
    }

    @Test("Malformed RDF literals report stable typed failures")
    func malformedRDFReportsTypedFailures() {
        #expect(throws: LiteralConversionError.invalidRDFLiteral(datatype: "")) {
            _ = try Literal.typedLiteral(value: "value", datatype: "")
                .toFieldValue()
        }
        #expect(throws: LiteralConversionError.invalidLanguageTag("")) {
            _ = try Literal.langLiteral(value: "value", language: "")
                .toFieldValue()
        }
        #expect(throws: LiteralConversionError.invalidBaseDirection("sideways")) {
            _ = try Literal.dirLangLiteral(
                value: "value",
                language: "en",
                direction: "sideways"
            ).toFieldValue()
        }
    }

    @Test("Reverse predicate bridges preserve exact decimal values")
    func predicateConversionsPreserveDecimalValues() throws {
        let expression = Expression.equal(
            .column(ColumnRef(column: "value")),
            .literal(.decimal(coefficient: 1, scale: 1))
        )
        let predicate: Predicate<Entity>? = try expression.toPredicate(
            for: Entity.self
        )
        guard case .comparison(let comparison) = predicate else {
            Issue.record("Expected a field comparison")
            return
        }
        #expect(
            comparison.value == .decimal(
                ExactDecimal(coefficient: 1, scale: 1)
            )
        )
        #expect(try comparison.toExpression() == expression)
    }

    @Test("Reverse predicates retain canonical field identity and behavior")
    func reversePredicatesRetainFieldIdentityAndBehavior() throws {
        let expression = Expression.equal(
            .column(ColumnRef(column: "value")),
            .literal(.int(42))
        )
        let predicate: Predicate<Entity>? = try expression.toPredicate(
            for: Entity.self
        )
        guard case .comparison(let comparison) = predicate else {
            Issue.record("Expected a field comparison")
            return
        }

        #expect(comparison.fieldName == "value")
        #expect(try comparison.evaluate(on: Entity(value: 42)))
        #expect(try !comparison.evaluate(on: Entity(value: 7)))
        #expect(try comparison.toExpression() == expression)
    }

    @Test("NOT IN remains distinct through predicate conversion")
    func notInRetainsSemantics() throws {
        let expression = Expression.notInList(
            .column(ColumnRef(column: "value")),
            values: [.literal(.int(1)), .literal(.int(2))]
        )
        let predicate: Predicate<Entity>? = try expression.toPredicate(
            for: Entity.self
        )
        guard case .comparison(let comparison) = predicate else {
            Issue.record("Expected a field comparison")
            return
        }

        #expect(comparison.op == .notIn)
        #expect(try !comparison.evaluate(on: Entity(value: 1)))
        #expect(try comparison.evaluate(on: Entity(value: 3)))
        #expect(try comparison.toExpression() == expression)
    }

    @Test("Empty logical groups retain their boolean identities")
    func emptyLogicalGroupsRetainBooleanIdentities() throws {
        #expect(
            try Predicate<Entity>.and([]).toExpression()
                == .literal(.bool(true))
        )
        #expect(
            try Predicate<Entity>.or([]).toExpression()
                == .literal(.bool(false))
        )
    }
}
#endif
