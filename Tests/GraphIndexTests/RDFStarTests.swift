import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import DatabaseEngine
@testable import GraphIndex

@Suite("Canonical RDF-star Terms", .heartbeat)
struct CanonicalRDFStarTermTests {
    private let subject = RDFSubject.iri(.xsdString)
    private let predicate = RDFPredicateIRI(.rdfLanguageString)
    private let object = RDFTerm.iri(.rdfDirectionalLanguageString)

    @Test("Canonical storage codec round-trips triple terms")
    func storageCodecRoundTripsTripleTerms() throws {
        let term = RDFTerm.tripleTerm(
            subject: subject,
            predicate: predicate,
            object: object
        )

        let encoded = try RDFTermStorageFormat.encode(term)
        let decoded = try RDFTermStorageFormat.decode(encoded)

        #expect(decoded == term)
    }

    @Test("Canonical storage codec round-trips nested triple terms")
    func storageCodecRoundTripsNestedTripleTerms() throws {
        let inner = RDFTerm.tripleTerm(
            subject: subject,
            predicate: predicate,
            object: object
        )
        let outer = RDFTerm.tripleTerm(
            subject: subject,
            predicate: predicate,
            object: inner
        )

        let encoded = try RDFTermStorageFormat.encode(outer)
        let decoded = try RDFTermStorageFormat.decode(encoded)

        #expect(decoded == outer)
    }

    @Test("Execution term materializes a canonical triple term")
    func executionTermMaterializesCanonicalTripleTerm() {
        let term = ExecutionTerm.tripleTerm(
            subject: .value(.rdfTerm(subject.term)),
            predicate: .value(.rdfTerm(predicate.term)),
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
            subject: .value(.rdfTerm(subject.term)),
            predicate: .value(
                .rdfTerm(
                    .literal(
                        RDFLiteral(
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
            predicate: .value(.rdfTerm(predicate.term)),
            object: .variable("?object")
        )
        let binding = VariableBinding()
            .binding("?subject", to: .rdfTerm(subject.term))
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
    func variablesIncludeNestedTriplePositions() throws {
        let triple = ExecutionTriple(
            subject: .tripleTerm(
                subject: .variable("?innerSubject"),
                predicate: .value(
                    .rdfTerm(
                        try .iri(
                            validating:
                                "https://example.com/predicate"
                        )
                    )
                ),
                object: .variable("?innerObject")
            ),
            predicate: .value(
                .rdfTerm(
                    try .iri(
                        validating: "https://example.com/source"
                    )
                )
            ),
            object: .variable("?source")
        )

        #expect(
            triple.variables
                == Set(["?innerSubject", "?innerObject", "?source"])
        )
    }

    @Test("Repeated nested variables are deduplicated")
    func repeatedNestedVariablesAreDeduplicated() throws {
        let triple = ExecutionTriple(
            subject: .variable("?subject"),
            predicate: .value(
                .rdfTerm(
                    try .iri(
                        validating: "https://example.com/about"
                    )
                )
            ),
            object: .tripleTerm(
                subject: .variable("?subject"),
                predicate: .value(
                    .rdfTerm(
                        try .iri(
                            validating:
                                "https://example.com/predicate"
                        )
                    )
                ),
                object: .variable("?object")
            )
        )

        #expect(triple.variables == Set(["?subject", "?object"]))
    }
}

@Suite("RDF-star Expression Evaluation", .heartbeat)
struct RDFStarExpressionEvaluationTests {
    private let subject = RDFSubject.iri(.xsdString)
    private let predicate = RDFPredicateIRI(.rdfLanguageString)
    private let object = RDFTerm.iri(.rdfDirectionalLanguageString)

    @Test("TRIPLE constructs a canonical RDF term")
    func tripleConstructsCanonicalRDFTerm() throws {
        let expression = Expression.triple(
            subject: .literal(.rdfTerm(subject.term)),
            predicate: .literal(.rdfTerm(predicate.term)),
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
        let literalPredicate = RDFTerm.literal(
            RDFLiteral(
                lexicalForm: "predicate",
                datatype: .xsdString
            )
        )
        let expression = Expression.triple(
            subject: .literal(.rdfTerm(subject.term)),
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
        let triple = RDFTerm.tripleTerm(
            subject: subject,
            predicate: predicate,
            object: object
        )
        let binding = VariableBinding(["?triple": .rdfTerm(triple)])
        let variable = Expression.variable(Variable("triple"))

        #expect(
            try ExpressionEvaluator.evaluate(.isTriple(variable), binding: binding)
                == .rdfTerm(
                    .literal(
                        try RDFLiteral(
                            lexicalForm: "true",
                            datatype: "http://www.w3.org/2001/XMLSchema#boolean"
                        )
                    )
                )
        )
        #expect(
            try ExpressionEvaluator.evaluate(.subject(variable), binding: binding)
                == .rdfTerm(subject.term)
        )
        #expect(
            try ExpressionEvaluator.evaluate(.predicate(variable), binding: binding)
                == .rdfTerm(predicate.term)
        )
        #expect(
            try ExpressionEvaluator.evaluate(.object(variable), binding: binding)
                == .rdfTerm(object)
        )
    }

    @Test("IS TRIPLE rejects ordinary RDF terms")
    func isTripleRejectsOrdinaryRDFTerms() throws {
        let binding = VariableBinding([
            "?value": .rdfTerm(subject.term)
        ])
        let expression = Expression.isTriple(
            .variable(Variable("value"))
        )

        #expect(
            try ExpressionEvaluator.evaluate(expression, binding: binding)
                == .rdfTerm(
                    .literal(
                        try RDFLiteral(
                            lexicalForm: "false",
                            datatype: "http://www.w3.org/2001/XMLSchema#boolean"
                        )
                    )
                )
        )
    }
}
