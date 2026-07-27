import Testing
import GraphIndex
import DatabaseKit

@Suite("Graph pattern conversion failures")
struct GraphPatternConverterFailureTests {
    @Test("Unsupported patterns fail instead of changing query meaning")
    func unsupportedPatternsFail() {
        let empty = GraphPattern.basic([])

        #expect(throws: GraphPatternConversionError.unsupportedGraphPattern("SERVICE")) {
            try GraphPatternConverter.convert(
                .service(endpoint: "https://example.invalid/sparql", pattern: empty, silent: false)
            )
        }
    }

    @Test("Malformed VALUES tables fail before execution")
    func malformedValuesFail() {
        #expect(
            throws: SPARQLSemanticValidationError.duplicateValuesVariable("value")
        ) {
            try GraphPatternConverter.convert(
                .values(
                    variables: ["value", "value"],
                    bindings: [[.int(1), .int(2)]]
                )
            )
        }
        #expect(
            throws: SPARQLSemanticValidationError.valuesRowWidth(
                row: 0,
                expected: 2,
                actual: 1
            )
        ) {
            try GraphPatternConverter.convert(
                .values(
                    variables: ["left", "right"],
                    bindings: [[.int(1)]]
                )
            )
        }
    }

    @Test("A blank node label cannot cross basic graph pattern boundaries")
    func blankNodeLabelScopeIsValidated() {
        let first = TriplePattern(
            subject: .blankNode("shared"),
            predicate: .iri("https://example.invalid/p"),
            object: .variable("left")
        )
        let second = TriplePattern(
            subject: .blankNode("shared"),
            predicate: .iri("https://example.invalid/q"),
            object: .variable("right")
        )

        #expect(
            throws: SPARQLSemanticValidationError
                .labelCrossesBasicGraphPatterns("shared")
        ) {
            try GraphPatternConverter.convert(
                .join(.basic([first]), .basic([second]))
            )
        }
    }

    @Test("Deep RDF-star lowering emits one triple per reification")
    func deepRDFStarLoweringEmitsExpectedTriples() throws {
        let depth = 62
        var object = SPARQLTerm.iri("urn:leaf")
        for index in 0..<depth {
            object = .reifiedTriple(
                subject: object,
                predicate: .iri("urn:nested-predicate"),
                object: .iri("urn:nested-object"),
                reifier: .blankNode("reifier-\(index)")
            )
        }
        let pattern = try GraphPatternConverter.convert(
            .basic([
                TriplePattern(
                    subject: .iri("urn:root"),
                    predicate: .iri("urn:predicate"),
                    object: object
                )
            ])
        )

        guard case .basic(let triples) = pattern else {
            Issue.record("Expected one lowered basic graph pattern")
            return
        }
        #expect(triples.count == depth + 1)
    }

    @Test("RDF-star lowering rejects nesting above the structural limit")
    func excessiveRDFStarNestingFailsBeforeLowering() {
        var object = SPARQLTerm.iri("urn:leaf")
        for index in 0..<63 {
            object = .reifiedTriple(
                subject: object,
                predicate: .iri("urn:nested-predicate"),
                object: .iri("urn:nested-object"),
                reifier: .blankNode("reifier-\(index)")
            )
        }

        #expect(
            throws: SPARQLSemanticValidationError.structural(
                .resourceLimitExceeded(
                    resource: .nestingDepth,
                    actual: 65,
                    maximum: 64
                )
            )
        ) {
            try GraphPatternConverter.convert(
                .basic([
                    TriplePattern(
                        subject: .iri("urn:root"),
                        predicate: .iri("urn:predicate"),
                        object: object
                    )
                ])
            )
        }
    }

    @Test("BIND compiles to an Extend node without changing its expression")
    func bindCompilesToExtend() throws {
        let expression = Expression.literal(.int(1))
        let pattern = try GraphPatternConverter.convert(
            .bind(.basic([]), variable: "value", expression: expression)
        )

        guard case .extend(
            .basic(let triples),
            let variable,
            let plan
        ) = pattern else {
            Issue.record("Expected BIND to compile to Extend")
            return
        }
        #expect(triples.isEmpty)
        #expect(variable == "?value")
        #expect(plan.expression == expression)
    }

    @Test("Group expressions compile to explicit key plans")
    func groupExpressionIsPreserved() throws {
        let expression = Expression.literal(.int(1))
        let pattern = try GraphPatternConverter.convert(
            .groupBy(
                .basic([]),
                expressions: [expression],
                aggregates: []
            )
        )

        guard case .groupBy(_, let grouping, let aggregates, let having) = pattern else {
            Issue.record("Expected an explicit Group algebra node")
            return
        }
        guard case .explicit(let keys) = grouping else {
            Issue.record("Expected explicit grouping")
            return
        }
        #expect(keys.count == 1)
        #expect(keys[0].expression.expression == expression)
        #expect(aggregates.isEmpty)
        #expect(having == nil)
    }

    @Test("Aggregate operands preserve arbitrary expressions")
    func aggregateExpressionIsPreserved() throws {
        let expression = Expression.add(
            .variable(Variable("left")),
            .variable(Variable("right"))
        )
        let binding = AggregateBinding(
            variable: "sum",
            aggregate: .sum(expression, distinct: false)
        )

        let aggregate = try GraphPatternConverter.convertAggregate(binding)

        #expect(aggregate.alias == "?sum")
        #expect(aggregate.inputExpression?.expression == expression)
        #expect(!aggregate.isDistinct)
    }

    @Test("ARRAY_AGG is not rewritten as GROUP_CONCAT")
    func arrayAggregationFailsExplicitly() {
        let binding = AggregateBinding(
            variable: "values",
            aggregate: .arrayAgg(
                .variable(Variable("value")),
                orderBy: nil,
                distinct: false
            )
        )

        #expect(
            throws: GraphPatternConversionError.unsupportedAggregateExpression("ARRAY_AGG")
        ) {
            try GraphPatternConverter.convertAggregate(binding)
        }
    }
}
