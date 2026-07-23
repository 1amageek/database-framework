#if !os(WASI)
import Core
import DatabaseValue
import QueryIR
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
        let date = DatabaseDate(year: 2026, month: 7, day: 21)
        let timestamp = DatabaseTimestamp(
            secondsSinceUnixEpoch: 1_774_310_400,
            nanoseconds: 123_456_789
        )
        let uuid = DatabaseUUID(
            high: 0x0011_2233_4455_6677,
            low: 0x8899_AABB_CCDD_EEFF
        )

        #expect(
            try QueryIR.Literal.decimal(coefficient: 12_345, scale: 2)
                .toDatabaseValue()
                == .decimal(coefficient: 12_345, scale: 2)
        )
        #expect(try QueryIR.Literal.date(date).toDatabaseValue() == .date(date))
        #expect(
            try QueryIR.Literal.timestamp(timestamp).toDatabaseValue()
                == .timestamp(timestamp)
        )
        #expect(try QueryIR.Literal.uuid(uuid).toDatabaseValue() == .uuid(uuid))
    }

    @Test("Narrow projection preserves supported values without numeric loss")
    func narrowProjectionPreservesSupportedValues() throws {
        let bytes = DatabaseBytes([0x00, 0x7F, 0xFF])
        let maximumUnsigned = UInt64.max

        #expect(
            try QueryIR.Literal.uint(maximumUnsigned).toFieldValue()
                == .uint64(maximumUnsigned)
        )
        #expect(try QueryIR.Literal.binary(bytes).toFieldValue() == .data(bytes))
        #expect(
            try QueryIR.Literal.array([.int(-1), .uint(maximumUnsigned)])
                .toFieldValue()
                == .array([.int64(-1), .uint64(maximumUnsigned)])
        )
    }

    @Test("Every FieldValue kind round-trips through QueryIR")
    func everyFieldValueKindRoundTrips() throws {
        let rdfTerm = DatabaseRDFTerm.tripleTerm(
            subject: .iri("urn:subject"),
            predicate: .iri("urn:predicate"),
            object: .blankNode("object")
        )
        let values: [FieldValue] = [
            .null,
            .bool(true),
            .int64(.min),
            .uint64(.max),
            .double(1.25),
            .string("value"),
            .data(DatabaseBytes([0x00, 0x7F, 0xFF])),
            .rdfTerm(rdfTerm),
            .array([.int64(-1), .uint64(.max), .rdfTerm(rdfTerm)]),
        ]

        for value in values {
            #expect(try value.toLiteral().toFieldValue() == value)
        }
    }

    @Test("Canonical RDF projection preserves term annotations")
    func canonicalRDFProjectionPreservesAnnotations() throws {
        let datatype = "http://www.w3.org/2001/XMLSchema#integer"
        let language = try DatabaseRDFLanguageTag("en-US")

        #expect(
            try QueryIR.Literal.typedLiteral(
                value: "42",
                datatype: datatype
            ).toDatabaseValue()
                == .rdfTerm(
                    .literal(
                        try DatabaseRDFLiteral(
                            lexicalForm: "42",
                            datatype: datatype
                        )
                    )
                )
        )
        #expect(
            try QueryIR.Literal.langLiteral(
                value: "colour",
                language: "en-US"
            ).toDatabaseValue()
                == .rdfTerm(
                    .literal(
                        DatabaseRDFLiteral(
                            lexicalForm: "colour",
                            language: language
                        )
                    )
                )
        )
        #expect(
            try QueryIR.Literal.dirLangLiteral(
                value: "text",
                language: "en-US",
                direction: "rtl"
            ).toDatabaseValue()
                == .rdfTerm(
                    .literal(
                        DatabaseRDFLiteral(
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
        let source = DatabaseBytes.copying(count: 4_096) { buffer in
            buffer.initializeMemory(as: UInt8.self, repeating: 0xA5)
        }
        guard case .bytes(let canonical) = try QueryIR.Literal.binary(source)
            .toDatabaseValue() else {
            Issue.record("Expected canonical bytes")
            return
        }
        guard case .data(let narrow) = try QueryIR.Literal.binary(source)
            .toFieldValue() else {
            Issue.record("Expected narrow binary data")
            return
        }

        source.withUnsafeBytes { sourceBuffer in
            canonical.withUnsafeBytes { canonicalBuffer in
                narrow.withUnsafeBytes { narrowBuffer in
                    #expect(sourceBuffer.baseAddress == canonicalBuffer.baseAddress)
                    #expect(sourceBuffer.baseAddress == narrowBuffer.baseAddress)
                    #expect(sourceBuffer.count == canonicalBuffer.count)
                    #expect(sourceBuffer.count == narrowBuffer.count)
                }
            }
        }
    }

    @Test("Narrow projection rejects canonical-only scalar kinds")
    func narrowProjectionRejectsCanonicalOnlyScalars() {
        let date = DatabaseDate(year: 2026, month: 7, day: 21)
        let timestamp = DatabaseTimestamp(secondsSinceUnixEpoch: 1)
        let uuid = DatabaseUUID(high: 1, low: 2)

        #expect(
            throws: LiteralConversionError.fieldValueUnsupported(kind: .decimal)
        ) {
            _ = try QueryIR.Literal.decimal(coefficient: 1, scale: 1)
                .toFieldValue()
        }
        #expect(throws: LiteralConversionError.fieldValueUnsupported(kind: .date)) {
            _ = try QueryIR.Literal.date(date).toFieldValue()
        }
        #expect(
            throws: LiteralConversionError.fieldValueUnsupported(kind: .timestamp)
        ) {
            _ = try QueryIR.Literal.timestamp(timestamp).toFieldValue()
        }
        #expect(throws: LiteralConversionError.fieldValueUnsupported(kind: .uuid)) {
            _ = try QueryIR.Literal.uuid(uuid).toFieldValue()
        }
        #expect(
            throws: LiteralConversionError.fieldValueUnsupported(kind: .decimal)
        ) {
            _ = try QueryIR.Literal.array([
                .int(1),
                .decimal(coefficient: 2, scale: 1),
            ]).toFieldValue()
        }
    }

    @Test("Malformed RDF literals report stable typed failures")
    func malformedRDFReportsTypedFailures() {
        #expect(throws: LiteralConversionError.invalidRDFLiteral(datatype: "")) {
            _ = try QueryIR.Literal.typedLiteral(value: "value", datatype: "")
                .toFieldValue()
        }
        #expect(throws: LiteralConversionError.invalidLanguageTag("")) {
            _ = try QueryIR.Literal.langLiteral(value: "value", language: "")
                .toFieldValue()
        }
        #expect(throws: LiteralConversionError.invalidBaseDirection("sideways")) {
            _ = try QueryIR.Literal.dirLangLiteral(
                value: "value",
                language: "en",
                direction: "sideways"
            ).toFieldValue()
        }
    }

    @Test("Reverse predicate bridges propagate literal representation failures")
    func predicateConversionsPropagateRepresentationFailures() {
        let expression = QueryIR.Expression.equal(
            .column(QueryIR.ColumnRef(column: "value")),
            .literal(.decimal(coefficient: 1, scale: 1))
        )
        let expected = LiteralConversionError.fieldValueUnsupported(kind: .decimal)

        #expect(throws: expected) {
            let _: Predicate<Entity>? = try expression.toPredicate(for: Entity.self)
        }
        #expect(throws: expected) {
            _ = try PredicateExpr(expression)
        }
    }

    @Test("Reverse predicates retain canonical field identity and behavior")
    func reversePredicatesRetainFieldIdentityAndBehavior() throws {
        let expression = QueryIR.Expression.equal(
            .column(QueryIR.ColumnRef(column: "value")),
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
        #expect(comparison.evaluate(on: Entity(value: 42)))
        #expect(!comparison.evaluate(on: Entity(value: 7)))
        #expect(comparison.toExpression() == expression)
    }

    @Test("NOT IN remains distinct through predicate conversion")
    func notInRetainsSemantics() throws {
        let expression = QueryIR.Expression.notInList(
            .column(QueryIR.ColumnRef(column: "value")),
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
        #expect(!comparison.evaluate(on: Entity(value: 1)))
        #expect(comparison.evaluate(on: Entity(value: 3)))
        #expect(comparison.toExpression() == expression)
    }

    @Test("Cascades LIKE bridge rejects a non-string pattern")
    func cascadesLIKEConversionRejectsNonStringPattern() {
        let like = PredicateExpr.comparison(
            field: "value",
            op: .like,
            value: .int64(1)
        )
        let ilike = PredicateExpr.comparison(
            field: "value",
            op: .ilike,
            value: .int64(1)
        )

        #expect(
            throws: CascadesConversionError.likePatternRequiresString(
                field: "value"
            )
        ) {
            _ = try like.toExpression()
        }
        #expect(
            throws: CascadesConversionError.ilikePatternRequiresString(
                field: "value"
            )
        ) {
            _ = try ilike.toExpression()
        }
    }

    @Test("Empty logical groups retain their boolean identities")
    func emptyLogicalGroupsRetainBooleanIdentities() throws {
        #expect(
            Predicate<Entity>.and([]).toExpression()
                == .literal(.bool(true))
        )
        #expect(
            Predicate<Entity>.or([]).toExpression()
                == .literal(.bool(false))
        )
        #expect(
            try PredicateExpr.and([]).toExpression()
                == .literal(.bool(true))
        )
        #expect(
            try PredicateExpr.or([]).toExpression()
                == .literal(.bool(false))
        )
    }
}
#endif
