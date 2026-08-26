import DatabaseKit
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("SPARQL expression compilation", .heartbeat)
struct SPARQLExpressionCompilationTests {
    @Test("Every valid absolute IRI scheme can identify an extension function")
    func extensionFunctionIRIClassification() throws {
        for identifier in [
            "mailto:function@example.com",
            "tag:example.com,2026:function",
            "did:example:function",
            "urn:example:function",
            "https://example.com/function",
        ] {
            let resolved = try SPARQLFunctionIdentifier.resolve(identifier)
            guard case .extensionFunction(let iri) = resolved else {
                Issue.record("Expected an extension function for \(identifier)")
                continue
            }
            #expect(iri.rawValue == identifier)
        }
    }

    @Test("XSD constructor IRIs are not extension functions")
    func datatypeConstructorClassification() throws {
        let identifier =
            "http://www.w3.org/2001/XMLSchema#integer"
        let resolved = try SPARQLFunctionIdentifier.resolve(identifier)

        guard case .datatypeConstructor(let iri) = resolved else {
            Issue.record("Expected an XSD datatype constructor")
            return
        }
        #expect(iri.rawValue == identifier)
    }

    @Test("A non-IRI unknown function fails before execution")
    func invalidFunctionIdentifierFails() {
        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                .function(
                    FunctionCall(
                        name: "not a function",
                        arguments: []
                    )
                )
            )
        }
    }

    @Test("BOUND requires a variable operand")
    func boundRequiresVariable() {
        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                .function(
                    FunctionCall(
                        name: "BOUND",
                        arguments: [.literal(.bool(true))]
                    )
                )
            )
        }
    }

    @Test("Directly constructed expressions obey the compilation depth limit")
    func compilationDepthIsBounded() {
        let expression = Expression.not(
            .not(.not(.not(.literal(.bool(true)))))
        )

        #expect(
            throws: SPARQLExpressionCompilationError.structural(
                .resourceLimitExceeded(
                    resource: .nestingDepth,
                    actual: 4,
                    maximum: 3
                )
            )
        ) {
            try SPARQLExpressionValidator.validate(
                expression,
                limits: SPARQLExpressionCompilationLimits(
                    structuralLimits: QueryStructuralLimits(
                        maximumNestingDepth: 3
                    )
                )
            )
        }
    }

    @Test("Collection limits are cumulative across an expression")
    func expressionCollectionsShareTheCanonicalLedger() {
        let expression = Expression.and(
            .function(
                FunctionCall(
                    name: "urn:example:left",
                    arguments: [
                        .literal(.int(1)),
                        .literal(.int(2)),
                    ]
                )
            ),
            .function(
                FunctionCall(
                    name: "urn:example:right",
                    arguments: [
                        .literal(.int(3)),
                        .literal(.int(4)),
                    ]
                )
            )
        )

        #expect(
            throws: SPARQLExpressionCompilationError.structural(
                .resourceLimitExceeded(
                    resource: .collectionElements,
                    actual: 4,
                    maximum: 3
                )
            )
        ) {
            _ = try SPARQLExpressionPlan(
                expression,
                limits: SPARQLExpressionCompilationLimits(
                    structuralLimits: QueryStructuralLimits(
                        maximumCollectionElements: 3
                    )
                )
            )
        }
    }

    @Test("Function argument collections are bounded after decode")
    func functionArgumentsAreBounded() {
        let expression = Expression.function(
            FunctionCall(
                name: "CONCAT",
                arguments: [
                    .literal(.string("a")),
                    .literal(.string("b")),
                    .literal(.string("c")),
                ]
            )
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                expression,
                limits: SPARQLExpressionCompilationLimits(
                    structuralLimits: QueryStructuralLimits(
                        maximumCollectionElements: 2
                    )
                )
            )
        }
    }

    @Test("Directly constructed literal strings obey the UTF-8 limit")
    func literalStringsAreBounded() {
        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                .literal(.string("four")),
                limits: SPARQLExpressionCompilationLimits(
                    maximumStringUTF8Count: 3
                )
            )
        }
    }

    @Test("Expression plans cannot bypass bounded compilation")
    func expressionPlanUsesCompilerLimits() {
        #expect(
            throws: SPARQLExpressionCompilationError.resourceLimitExceeded(
                resource: .stringUTF8,
                actual: 4,
                maximum: 3
            )
        ) {
            _ = try SPARQLExpressionPlan(
                .literal(.string("four")),
                limits: SPARQLExpressionCompilationLimits(
                    maximumStringUTF8Count: 3
                )
            )
        }
    }

    @Test("SELECT compilation preserves the explicit string limit")
    func selectCompilationUsesExplicitStringLimit() {
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .literal(.string("four")),
                    alias: "value"
                ),
            ]),
            source: .graphPattern(.basic([]))
        )

        #expect(
            throws: SPARQLExpressionCompilationError.resourceLimitExceeded(
                resource: .stringUTF8,
                actual: 4,
                maximum: 3
            )
        ) {
            _ = try SPARQLSelectPlanCompiler.compile(
                query,
                expressionLimits: SPARQLExpressionCompilationLimits(
                    maximumStringUTF8Count: 3
                )
            )
        }
    }

    @Test("Canonical SELECT compilation enforces its structural limit authority")
    func canonicalSelectCompilationUsesStructuralLimits() throws {
        var expression = Expression.literal(.bool(true))
        for _ in 0..<62 {
            expression = .not(expression)
        }
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(expression, alias: "value")
            ]),
            source: .graphPattern(.basic([]))
        )

        #expect(
            throws: SPARQLSemanticValidationError.structural(
                .resourceLimitExceeded(
                    resource: .nestingDepth,
                    actual: 65,
                    maximum: 64
                )
            )
        ) {
            _ = try SPARQLSelectPlanCompiler.compileForCanonicalPagination(
                query,
                structuralLimits: QueryStructuralLimits(
                    maximumNestingDepth: 64
                )
            )
        }

        _ = try SPARQLSelectPlanCompiler.compileForCanonicalPagination(
            query,
            structuralLimits: QueryStructuralLimits(
                maximumNestingDepth: 65
            )
        )
    }

    @Test("GROUP_CONCAT separators obey the expression string limit")
    func aggregateSeparatorsAreBounded() {
        let binding = AggregateBinding(
            variable: "joined",
            aggregate: .groupConcat(
                .variable(Variable("value")),
                separator: "four",
                distinct: false
            )
        )

        #expect(
            throws: SPARQLExpressionCompilationError.resourceLimitExceeded(
                resource: .stringUTF8,
                actual: 4,
                maximum: 3
            )
        ) {
            _ = try GraphPatternConverter.convertAggregate(
                binding,
                limits: SPARQLExpressionCompilationLimits(
                    maximumStringUTF8Count: 3
                )
            )
        }
    }

    @Test("EXISTS recursively validates its graph pattern")
    func existsGraphPatternIsValidated() {
        let expression = Expression.exists(
            SelectQuery(
                projection: .all,
                source: .graphPattern(
                    .service(
                        endpoint: "https://example.com/sparql",
                        pattern: .basic([]),
                        silent: false
                    )
                )
            )
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            _ = try SPARQLExpressionPlan(expression)
        }
    }

    @Test("EXISTS cannot hide a VALUES row beyond canonical limits")
    func existsUsesCanonicalSpecializedLimits() {
        let expression = Expression.exists(
            SelectQuery(
                projection: .all,
                source: .graphPattern(
                    .values(
                        variables: ["value"],
                        bindings: [[.int(1)]]
                    )
                )
            )
        )

        #expect(
            throws: SPARQLExpressionCompilationError.structural(
                .resourceLimitExceeded(
                    resource: .valuesRows,
                    actual: 1,
                    maximum: 0
                )
            )
        ) {
            _ = try SPARQLExpressionPlan(
                expression,
                limits: SPARQLExpressionCompilationLimits(
                    structuralLimits: QueryStructuralLimits(
                        maximumValuesRows: 0
                    )
                )
            )
        }
    }

    @Test("EXISTS rejects query modifiers that its algebra cannot preserve")
    func existsRejectsIgnoredQuerySemantics() {
        let sources = [
            SelectQuery(
                projection: .items([
                    ProjectionItem(.variable(Variable("value"))),
                ]),
                source: .graphPattern(.basic([]))
            ),
            SelectQuery(
                projection: .all,
                source: .graphPattern(.basic([])),
                distinct: true
            ),
            SelectQuery(
                projection: .all,
                source: .graphPattern(.basic([])),
                dataset: .explicit(
                    defaultGraphs: ["urn:example:graph"],
                    namedGraphs: []
                )
            ),
        ]

        for query in sources {
            #expect(
                throws: SPARQLExpressionCompilationError.invalidExistsSource
            ) {
                _ = try SPARQLExpressionPlan(.exists(query))
            }
        }
    }
}
