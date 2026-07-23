import QueryAST
import QueryIR
import Testing

@Suite("SQL parser structural limits")
struct SQLParserStructuralLimitTests {
    @Test("Text SQL accepts exactly the maximum input token count")
    func maximumInputTokenCountIsAccepted() throws {
        let parser = SQLParser(
            structuralLimits: QueryStructuralLimits(maximumInputTokens: 4)
        )

        _ = try parser.parseSelect("SELECT id FROM Event")
    }

    @Test("Text SQL rejects the first token above the maximum")
    func excessiveInputTokenCountIsRejected() {
        let parser = SQLParser(
            structuralLimits: QueryStructuralLimits(maximumInputTokens: 3)
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .inputTokens,
                actual: 4,
                maximum: 3
            )
        ) {
            _ = try parser.parseSelect("SELECT id FROM Event")
        }
    }

    @Test("Text SQL accepts exactly the maximum parenthesized depth")
    func maximumParenthesizedDepthIsAccepted() throws {
        let parser = SQLParser(
            structuralLimits: QueryStructuralLimits(maximumNestingDepth: 5)
        )

        _ = try parser.parseSelect("SELECT (((id))) FROM Event")
    }

    @Test("Text SQL rejects parenthesized depth above the maximum")
    func excessiveParenthesizedDepthIsRejected() {
        let parser = SQLParser(
            structuralLimits: QueryStructuralLimits(maximumNestingDepth: 4)
        )

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .nestingDepth,
                actual: 5,
                maximum: 4
            )
        ) {
            _ = try parser.parseSelect("SELECT (((id))) FROM Event")
        }
    }

    @Test("Text SQL accounts for iterative NOT nesting")
    func iterativeNotNestingIsBounded() throws {
        _ = try SQLParser(
            structuralLimits: QueryStructuralLimits(maximumNestingDepth: 4)
        ).parseSelect("SELECT NOT NOT id FROM Event")

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .nestingDepth,
                actual: 4,
                maximum: 3
            )
        ) {
            _ = try SQLParser(
                structuralLimits: QueryStructuralLimits(maximumNestingDepth: 3)
            ).parseSelect("SELECT NOT NOT id FROM Event")
        }
    }

    @Test("Text SQL accounts for iterative unary nesting")
    func iterativeUnaryNestingIsBounded() throws {
        _ = try SQLParser(
            structuralLimits: QueryStructuralLimits(maximumNestingDepth: 4)
        ).parseSelect("SELECT - - id FROM Event")

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .nestingDepth,
                actual: 4,
                maximum: 3
            )
        ) {
            _ = try SQLParser(
                structuralLimits: QueryStructuralLimits(maximumNestingDepth: 3)
            ).parseSelect("SELECT - - id FROM Event")
        }
    }

    @Test("Text SQL admits every retained AST node")
    func totalNodeAdmissionIsBounded() throws {
        _ = try SQLParser(
            structuralLimits: QueryStructuralLimits(maximumTotalNodes: 6)
        ).parseSelect("SELECT firstName, lastName FROM Person")

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .totalNodes,
                actual: 6,
                maximum: 5
            )
        ) {
            _ = try SQLParser(
                structuralLimits: QueryStructuralLimits(maximumTotalNodes: 5)
            ).parseSelect("SELECT firstName, lastName FROM Person")
        }
    }

    @Test("Text SQL admits collection members before retaining them")
    func collectionAdmissionIsBounded() throws {
        _ = try SQLParser(
            structuralLimits: QueryStructuralLimits(
                maximumCollectionElements: 2
            )
        ).parseSelect("SELECT firstName, lastName FROM Person")

        #expect(
            throws: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .collectionElements,
                actual: 2,
                maximum: 1
            )
        ) {
            _ = try SQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumCollectionElements: 1
                )
            ).parseSelect("SELECT firstName, lastName FROM Person")
        }
    }
}
