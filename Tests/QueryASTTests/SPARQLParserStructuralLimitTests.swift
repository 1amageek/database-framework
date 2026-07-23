import QueryAST
import QueryIR
import Testing

@Suite("SPARQL parser structural limits")
struct SPARQLParserStructuralLimitTests {
    @Test("Text SPARQL accepts exactly the maximum input token count")
    func maximumInputTokenCountIsAccepted() throws {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(maximumInputTokens: 9)
        )

        _ = try parser.parseSelect(
            "SELECT * WHERE { ?subject <urn:predicate> ?object . }"
        )
    }

    @Test("Text SPARQL rejects the first token above the maximum")
    func excessiveInputTokenCountIsRejected() {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(maximumInputTokens: 8)
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .inputTokens,
                actual: 9,
                maximum: 8
            )
        ) {
            _ = try parser.parseSelect(
                "SELECT * WHERE { ?subject <urn:predicate> ?object . }"
            )
        }
    }

    @Test("Text SPARQL accepts the maximum nesting depth")
    func maximumTextNestingDepthIsAccepted() throws {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumNestingDepth: 9
            )
        )

        _ = try parser.parseSelect(queryWithNestedTerm(levels: 4))
    }

    @Test("Text SPARQL rejects nesting above the maximum before stack exhaustion")
    func excessiveTextNestingDepthIsRejected() {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumNestingDepth: 9
            )
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .nestingDepth,
                actual: 10,
                maximum: 9
            )
        ) {
            _ = try parser.parseSelect(queryWithNestedTerm(levels: 5))
        }
    }

    @Test("Text SPARQL rejects excessive nesting before parsing its malformed tail")
    func excessiveTextNestingPrecedesTrailingSyntaxFailure() {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumNestingDepth: 9
            )
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .nestingDepth,
                actual: 10,
                maximum: 9
            )
        ) {
            _ = try parser.parseSelect(
                queryWithUnclosedNestedTerm(levels: 5)
            )
        }
    }

    @Test("Text SPARQL accepts the maximum VALUES cell count")
    func maximumTextValuesCellsAreAccepted() throws {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumValuesCells: 4
            )
        )

        _ = try parser.parseSelect(
            "SELECT * WHERE { VALUES ?value { 1 2 3 4 } }"
        )
    }

    @Test("Text SPARQL rejects VALUES cells above the maximum")
    func excessiveTextValuesCellsAreRejected() {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumValuesCells: 4
            )
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .valuesCells,
                actual: 5,
                maximum: 4
            )
        ) {
            _ = try parser.parseSelect(
                "SELECT * WHERE { VALUES ?value { 1 2 3 4 5 } }"
            )
        }
    }

    @Test("Text SPARQL rejects zero-width VALUES rows before appending them")
    func excessiveTextValuesRowsAreRejected() {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(maximumValuesRows: 2)
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .valuesRows,
                actual: 3,
                maximum: 2
            )
        ) {
            _ = try parser.parseSelect(
                "SELECT * WHERE { VALUES () { () () () } }"
            )
        }
    }

    @Test("Text SPARQL rejects VALUES variables before appending cap plus one")
    func excessiveTextValuesVariablesAreRejected() {
        let parser = SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumValuesVariables: 2
            )
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .valuesVariables,
                actual: 3,
                maximum: 2
            )
        ) {
            _ = try parser.parseSelect(
                "SELECT * WHERE { VALUES (?a ?b ?c) { } }"
            )
        }
    }

    @Test("Text SPARQL admits every triple before retaining it")
    func triplePatternAdmissionIsBounded() throws {
        let query = """
            SELECT * WHERE {
                ?first <urn:predicate> ?second .
                ?second <urn:predicate> ?third .
            }
            """

        _ = try SPARQLParser(
            structuralLimits: QueryStructuralLimits(maximumTriplePatterns: 2)
        ).parseSelect(query)

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .triplePatterns,
                actual: 2,
                maximum: 1
            )
        ) {
            _ = try SPARQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumTriplePatterns: 1
                )
            ).parseSelect(query)
        }
    }

    @Test("Text SPARQL admits every basic graph pattern")
    func basicGraphPatternAdmissionIsBounded() throws {
        let query = """
            SELECT * WHERE {
                ?subject <urn:required> ?value .
                OPTIONAL { ?subject <urn:optional> ?optionalValue . }
            }
            """

        _ = try SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumBasicGraphPatterns: 2
            )
        ).parseSelect(query)

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .basicGraphPatterns,
                actual: 2,
                maximum: 1
            )
        ) {
            _ = try SPARQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumBasicGraphPatterns: 1
                )
            ).parseSelect(query)
        }
    }

    @Test("Text SPARQL bounds reified triple expansion")
    func reifiedTripleExpansionIsBounded() throws {
        let query = """
            SELECT * WHERE {
                << <urn:subject> <urn:predicate> <urn:object> ~<urn:reifier> >>
                    <urn:assertedBy> <urn:source> .
            }
            """

        _ = try SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumReifiedTripleExpansions: 1
            )
        ).parseSelect(query)

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .reifiedTripleExpansions,
                actual: 1,
                maximum: 0
            )
        ) {
            _ = try SPARQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumReifiedTripleExpansions: 0
                )
            ).parseSelect(query)
        }
    }

    @Test("Text SPARQL admits every semantic AST node before construction")
    func totalNodeAdmissionIsBounded() throws {
        let query = """
            SELECT ?value WHERE {
                ?subject <urn:predicate> ?value .
            }
            """

        _ = try SPARQLParser(
            structuralLimits: QueryStructuralLimits(maximumTotalNodes: 10)
        ).parseSelect(query)

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .totalNodes,
                actual: 10,
                maximum: 9
            )
        ) {
            _ = try SPARQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumTotalNodes: 9
                )
            ).parseSelect(query)
        }
    }

    @Test("Text SPARQL admits collection members before retaining them")
    func collectionAdmissionIsBounded() throws {
        let query = """
            SELECT ?value WHERE {
                ?subject <urn:predicate> ?value .
            }
            """

        _ = try SPARQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumCollectionElements: 2
            )
        ).parseSelect(query)

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .collectionElements,
                actual: 2,
                maximum: 1
            )
        ) {
            _ = try SPARQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumCollectionElements: 1
                )
            ).parseSelect(query)
        }
    }

    private func queryWithNestedTerm(levels: Int) -> String {
        var term = "<urn:leaf>"
        for level in 0..<levels {
            term = "<< \(term) <urn:predicate:\(level)> <urn:object:\(level)> >>"
        }
        return "SELECT * WHERE { \(term) <urn:root-predicate> <urn:root-object> . }"
    }

    private func queryWithUnclosedNestedTerm(levels: Int) -> String {
        var query = "SELECT * WHERE { "
        for _ in 0..<levels {
            query += "<< "
        }
        query += "<urn:leaf>"
        return query
    }
}
