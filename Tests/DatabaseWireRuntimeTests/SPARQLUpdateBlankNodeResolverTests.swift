#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import Testing
@testable import DatabaseWireRuntime

@Suite("SPARQL update blank node resolver")
struct SPARQLUpdateBlankNodeResolverTests {
    @Test("variable digest fields are length framed")
    func variableDigestFieldsAreLengthFramed() {
        let first = SPARQLUpdateBlankNodeResolver(
            idempotencyKey: "a",
            operationOrdinal: 0x3132_3334_3536_3738,
            solutionOrdinal: 0x4142_4344_4546_4748
        )
        let second = SPARQLUpdateBlankNodeResolver(
            idempotencyKey: "a1",
            operationOrdinal: 0x3233_3435_3637_3841,
            solutionOrdinal: 0x4243_4445_4647_487a
        )

        #expect(first.identifier(for: "zy") != second.identifier(for: "y"))
        #expect(first.identifier(for: "zy") == first.identifier(for: "zy"))
    }
}
#endif
