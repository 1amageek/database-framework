import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine
import Testing

@Suite("FieldValue stable hashing")
struct FieldValueStableHashTests {
    @Test("Stable hashes preserve value identity and determinism")
    func identityAndDeterminism() {
        let values: [FieldValue] = [
            .int8(1),
            .int16(1),
            .int32(1),
            .int64(1),
            .uint8(1),
            .uint16(1),
            .uint32(1),
            .uint64(1),
            .float32(1),
            .float64(1),
            .string("1"),
        ]

        #expect(Set(values.map { $0.stableHash() }).count == values.count)
        #expect(
            FieldValue.string("deterministic").stableHash()
                == FieldValue.string("deterministic").stableHash()
        )
    }

    @Test("Deep values use iterative traversal")
    func deepValuesDoNotUseTheProcessStack() {
        var value = FieldValue.string("leaf")
        for _ in 0..<1_024 {
            value = .array([value])
        }

        #expect(value.stableHash() == value.stableHash())
    }

    @Test("RDF identity is hashed semantically")
    func rdfIdentityIsSemantic() throws {
        let first = FieldValue.rdfTerm(
            try .iri(validating: "urn:database:first")
        )
        let same = FieldValue.rdfTerm(
            try .iri(validating: "urn:database:first")
        )
        let different = FieldValue.rdfTerm(
            try .blankNode(identifier: "urn:database:first")
        )
        let uppercaseLanguage = FieldValue.rdfTerm(.literal(RDFLiteral(
            lexicalForm: "hello",
            language: try RDFLanguageTag("EN-Latn-US")
        )))
        let lowercaseLanguage = FieldValue.rdfTerm(.literal(RDFLiteral(
            lexicalForm: "hello",
            language: try RDFLanguageTag("en-latn-us")
        )))

        #expect(first.stableHash() == same.stableHash())
        #expect(first.stableHash() != different.stableHash())
        #expect(uppercaseLanguage == lowercaseLanguage)
        #expect(
            uppercaseLanguage.stableHash()
                == lowercaseLanguage.stableHash()
        )
    }
}
