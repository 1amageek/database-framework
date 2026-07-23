import Core
import DatabaseValue
import Graph
import QueryIR
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("Canonical RDF-star Terms", .heartbeat)
struct CanonicalRDFStarTermTests {
    private let subject = DatabaseRDFTerm.iri("https://example.com/subject")
    private let predicate = DatabaseRDFTerm.iri("https://example.com/predicate")
    private let object = DatabaseRDFTerm.iri("https://example.com/object")

    @Test("Canonical storage codec round-trips triple terms")
    func storageCodecRoundTripsTripleTerms() throws {
        let term = DatabaseRDFTerm.tripleTerm(
            subject: subject,
            predicate: predicate,
            object: object
        )

        let encoded = try DatabaseRDFTermCodec.encode(term)
        let decoded = try DatabaseRDFTermCodec.decode(encoded)

        #expect(decoded == term)
    }

    @Test("Canonical storage codec round-trips nested triple terms")
    func storageCodecRoundTripsNestedTripleTerms() throws {
        let inner = DatabaseRDFTerm.tripleTerm(
            subject: subject,
            predicate: predicate,
            object: object
        )
        let outer = DatabaseRDFTerm.tripleTerm(
            subject: inner,
            predicate: .iri("https://example.com/source"),
            object: .iri("https://example.com/document")
        )

        let encoded = try DatabaseRDFTermCodec.encode(outer)
        let decoded = try DatabaseRDFTermCodec.decode(encoded)

        #expect(decoded == outer)
    }

    @Test("Execution term materializes a canonical triple term")
    func executionTermMaterializesCanonicalTripleTerm() {
        let term = ExecutionTerm.tripleTerm(
            subject: .value(.rdfTerm(subject)),
            predicate: .value(.rdfTerm(predicate)),
            object: .value(.rdfTerm(object))
        )

        #expect(term.isBound)
        #expect(
            term.literalValue
                == .rdfTerm(
                    .tripleTerm(
                        subject: subject,
                        predicate: predicate,
                        object: object
                    )
                )
        )
    }

    @Test("Execution term rejects an invalid predicate role")
    func executionTermRejectsInvalidPredicateRole() {
        let term = ExecutionTerm.tripleTerm(
            subject: .value(.rdfTerm(subject)),
            predicate: .value(
                .rdfTerm(
                    .literal(
                        DatabaseRDFLiteral(
                            lexicalForm: "predicate",
                            datatype: .xsdString
                        )
                    )
                )
            ),
            object: .value(.rdfTerm(object))
        )

        #expect(term.literalValue == nil)
    }

    @Test("Substitution preserves canonical RDF terms")
    func substitutionPreservesCanonicalRDFTerms() {
        let term = ExecutionTerm.tripleTerm(
            subject: .variable("?subject"),
            predicate: .value(.rdfTerm(predicate)),
            object: .variable("?object")
        )
        let binding = VariableBinding()
            .binding("?subject", to: .rdfTerm(subject))
            .binding("?object", to: .rdfTerm(object))

        let substituted = term.substitute(binding)

        #expect(
            substituted.literalValue
                == .rdfTerm(
                    .tripleTerm(
                        subject: subject,
                        predicate: predicate,
                        object: object
                    )
                )
        )
    }
}

@Suite("RDF-star Triple Variables", .heartbeat)
struct RDFStarTripleVariableTests {
    @Test("Variables include nested triple positions")
    func variablesIncludeNestedTriplePositions() {
        let triple = ExecutionTriple(
            subject: .tripleTerm(
                subject: .variable("?innerSubject"),
                predicate: .value(
                    .rdfTerm(.iri("https://example.com/predicate"))
                ),
                object: .variable("?innerObject")
            ),
            predicate: .value(.rdfTerm(.iri("https://example.com/source"))),
            object: .variable("?source")
        )

        #expect(
            triple.variables
                == Set(["?innerSubject", "?innerObject", "?source"])
        )
    }

    @Test("Repeated nested variables are deduplicated")
    func repeatedNestedVariablesAreDeduplicated() {
        let triple = ExecutionTriple(
            subject: .variable("?subject"),
            predicate: .value(.rdfTerm(.iri("https://example.com/about"))),
            object: .tripleTerm(
                subject: .variable("?subject"),
                predicate: .value(
                    .rdfTerm(.iri("https://example.com/predicate"))
                ),
                object: .variable("?object")
            )
        )

        #expect(triple.variables == Set(["?subject", "?object"]))
    }
}

@Suite("RDF-star Expression Evaluation", .heartbeat)
struct RDFStarExpressionEvaluationTests {
    private let subject = DatabaseRDFTerm.iri("https://example.com/subject")
    private let predicate = DatabaseRDFTerm.iri("https://example.com/predicate")
    private let object = DatabaseRDFTerm.iri("https://example.com/object")

    @Test("TRIPLE constructs a canonical RDF term")
    func tripleConstructsCanonicalRDFTerm() throws {
        let expression = QueryIR.Expression.triple(
            subject: .literal(.rdfTerm(subject)),
            predicate: .literal(.rdfTerm(predicate)),
            object: .literal(.rdfTerm(object))
        )

        let result = try ExpressionEvaluator.evaluate(
            expression,
            binding: VariableBinding()
        )

        #expect(
            result
                == .rdfTerm(
                    .tripleTerm(
                        subject: subject,
                        predicate: predicate,
                        object: object
                    )
                )
        )
    }

    @Test("TRIPLE rejects a literal predicate")
    func tripleRejectsLiteralPredicate() {
        let literalPredicate = DatabaseRDFTerm.literal(
            DatabaseRDFLiteral(
                lexicalForm: "predicate",
                datatype: .xsdString
            )
        )
        let expression = QueryIR.Expression.triple(
            subject: .literal(.rdfTerm(subject)),
            predicate: .literal(.rdfTerm(literalPredicate)),
            object: .literal(.rdfTerm(object))
        )

        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try ExpressionEvaluator.evaluate(
                expression,
                binding: VariableBinding()
            )
        }
    }

    @Test("RDF-star accessors preserve term roles")
    func accessorsPreserveTermRoles() throws {
        let triple = DatabaseRDFTerm.tripleTerm(
            subject: subject,
            predicate: predicate,
            object: object
        )
        let binding = VariableBinding(["?triple": .rdfTerm(triple)])
        let variable = QueryIR.Expression.variable(Variable("triple"))

        #expect(
            try ExpressionEvaluator.evaluate(.isTriple(variable), binding: binding)
                == .rdfTerm(
                    .literal(
                        try DatabaseRDFLiteral(
                            lexicalForm: "true",
                            datatype: "http://www.w3.org/2001/XMLSchema#boolean"
                        )
                    )
                )
        )
        #expect(
            try ExpressionEvaluator.evaluate(.subject(variable), binding: binding)
                == .rdfTerm(subject)
        )
        #expect(
            try ExpressionEvaluator.evaluate(.predicate(variable), binding: binding)
                == .rdfTerm(predicate)
        )
        #expect(
            try ExpressionEvaluator.evaluate(.object(variable), binding: binding)
                == .rdfTerm(object)
        )
    }

    @Test("IS TRIPLE rejects ordinary RDF terms")
    func isTripleRejectsOrdinaryRDFTerms() throws {
        let binding = VariableBinding(["?value": .rdfTerm(subject)])
        let expression = QueryIR.Expression.isTriple(
            .variable(Variable("value"))
        )

        #expect(
            try ExpressionEvaluator.evaluate(expression, binding: binding)
                == .rdfTerm(
                    .literal(
                        try DatabaseRDFLiteral(
                            lexicalForm: "false",
                            datatype: "http://www.w3.org/2001/XMLSchema#boolean"
                        )
                    )
                )
        )
    }
}
