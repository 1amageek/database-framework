import DatabaseTypes
import GraphIndex
import DatabaseKit
import Testing
@testable import DatabaseServer

@Suite("SPARQL update quad resolver")
struct SPARQLUpdateQuadResolverTests {
    @Test("An unbound graph variable omits the quad instead of targeting default")
    func unboundGraphOmitsQuad() throws {
        let resolved = try SPARQLUpdateQuadResolver().resolve(
            Quad(
                graph: .variable("graph"),
                triple: TriplePattern(
                    subject: .iri("https://example.test/subject"),
                    predicate: .iri("https://example.test/predicate"),
                    object: .literal(.string("value"))
                )
            ),
            row: VariableBinding(),
            blankNodeResolver: nil,
            variablesAllowed: true,
            blankNodesAllowed: false
        )

        #expect(resolved.isEmpty)
    }

    @Test("An illegal variable substitution omits only that quad")
    func illegalVariableSubstitutionOmitsQuad() throws {
        let resolved = try SPARQLUpdateQuadResolver().resolve(
            Quad(
                triple: TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("https://example.test/predicate"),
                    object: .literal(.string("value"))
                )
            ),
            row: VariableBinding(
                [
                    "?subject": .rdfTerm(
                        .literal(
                            RDFLiteral(
                                lexicalForm: "not-a-subject",
                                datatype: .xsdString
                            )
                        )
                    ),
                ]
            ),
            blankNodeResolver: nil,
            variablesAllowed: true,
            blankNodesAllowed: false
        )

        #expect(resolved.isEmpty)
    }

    @Test("An illegal static RDF term remains a request error")
    func illegalStaticTermIsRejected() throws {
        do {
            _ = try SPARQLUpdateQuadResolver().resolve(
                Quad(
                    triple: TriplePattern(
                        subject: .literal(.string("not-a-subject")),
                        predicate: .iri("https://example.test/predicate"),
                        object: .literal(.string("value"))
                    )
                ),
                row: nil,
                blankNodeResolver: nil,
                variablesAllowed: false,
                blankNodesAllowed: false
            )
            Issue.record("Expected an invalid static RDF term error")
        } catch SPARQLUpdateError.invalidRDFTermRole {
            // Expected request validation failure.
        }
    }
}
