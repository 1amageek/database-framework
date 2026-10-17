import DatabaseKit
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("SPARQL expression compilation", .heartbeat)
struct SPARQLExpressionCompilationTests {
    @Test("SQL aggregates, grouping and windows fail every SPARQL expression admission")
    func sqlAggregateAndWindowBoundary() throws {
        let call = SQLAggregateCall(function: .count, arguments: [], filter: .literal(.bool(false)))
        let expressions: [(Expression, String)] = [
            (.aggregate(.sql(call)), "SQL aggregate"),
            (.grouping([.variable(Variable("value"))]), "SQL grouping or window expression"),
            (.window(WindowCall(function: .aggregate(call), reference: .specification(WindowSpecification()))),
                "SQL grouping or window expression"),
            (.window(WindowCall(function: .analytic(.rowNumber, arguments: []), reference: .named("window"))),
                "SQL grouping or window expression"),
        ]
        for (expression, reason) in expressions {
            let expected = SPARQLExpressionCompilationError.unsupportedExpression(reason)
            #expect(throws: expected) { try SPARQLExpressionValidator.validate(expression) }
            var aggregate = SPARQLAggregateRewriter()
            #expect(throws: expected) { try aggregate.rewrite(expression) }
            var grouped = SPARQLGroupedExpressionBindings()
            grouped.insert(variable: "alias", for: expression)
            var scope = SPARQLGroupScopeExpressionRewriter(groupedExpressions: grouped,
                directlyGroupedVariables: [], eligibleVariables: [], protectedVariables: [])
            #expect(throws: expected) { try scope.rewrite(expression, aggregateRewriter: &aggregate) }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                try ExpressionEvaluator.evaluate(expression, binding: VariableBinding())
            }
        }
        #expect(throws: SPARQLExpressionCompilationError.unsupportedExpression("SQL aggregate")) {
            try SPARQLExpressionValidator.validateAggregate(.sql(call))
        }
        // Generic graph COUNT retains its existing aggregate translation.
        let generic = AggregateFunction.count(nil, distinct: false)
        try SPARQLExpressionValidator.validateAggregate(generic)
        var rewriter = SPARQLAggregateRewriter()
        _ = try rewriter.rewrite(.aggregate(generic))
        #expect(rewriter.aggregateBindings.count == 1)
        #expect(rewriter.aggregateBindings[0].aggregate == generic)
    }

    @Test("SPARQL accepts expression grouping and rejects SQL grouping trees and named windows")
    func sqlGroupingTreeAndNamedWindowBoundary() throws {
        let key = Expression.variable(Variable("value"))
        let query = SelectQuery(projection: [.expression(.aggregate(.count(nil, distinct: false)), alias: "count")],
            source: .graphPattern(.basic([])))
        for elements: [GroupingElement] in [
            [.rollup([[key]])], [.cube([[key]])], [.groupingSets([[], [.expression(key)]])],
        ] {
            #expect(throws: SPARQLSemanticValidationError.unsupportedSQLExpression) {
                try SPARQLSelectPlanCompiler.compile(query.replacing(groupBy: elements))
            }
        }
        #expect(throws: SPARQLSemanticValidationError.unsupportedSQLExpression) {
            try SPARQLSelectPlanCompiler.compile(query.replacing(windows: [NamedWindow(name: "named",
                specification: WindowSpecification())]))
        }
        // A constant key is valid even with the empty graph input.
        let grouped = query.replacing(groupBy: [.expression(.literal(.int(1)))])
        _ = try SPARQLSelectPlanCompiler.compile(grouped)
    }

    @Test("SQL-qualified function identity survives rewriting and fails SPARQL compilation")
    func qualifiedFunctionsRemainUnsupported() throws {
        let expression = Expression.function(
            FunctionCall(name: "STRLEN", arguments: [.literal(.string("value"))], schema: "pg_catalog")
        )
        var aggregateRewriter = SPARQLAggregateRewriter()
        let aggregateRewritten = try aggregateRewriter.rewrite(expression)
        #expect(aggregateRewritten == expression)
        var scopeRewriter = SPARQLGroupScopeExpressionRewriter(
            groupedExpressions: SPARQLGroupedExpressionBindings(),
            directlyGroupedVariables: [],
            eligibleVariables: [],
            protectedVariables: []
        )
        let scopeRewritten = try scopeRewriter.rewrite(
            expression, aggregateRewriter: &aggregateRewriter
        )
        #expect(scopeRewritten == expression)
        for candidate in [expression, aggregateRewritten, scopeRewritten] {
            #expect(
                throws: SPARQLExpressionCompilationError.unsupportedExpression("SQL-qualified function")
            ) {
                try SPARQLExpressionValidator.validate(candidate)
            }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                try ExpressionEvaluator.evaluate(candidate, binding: VariableBinding())
            }
        }
        let unqualified = Expression.function(
            FunctionCall(name: "STRLEN", arguments: [.literal(.string("value"))])
        )
        try SPARQLExpressionValidator.validate(unqualified)
        #expect(try ExpressionEvaluator.evaluate(unqualified, binding: VariableBinding())
            == Literal.int(5).toSPARQLFieldValue())
    }

    @Test("SQL-only quantifiers and pagination are rejected by SPARQL compilation")
    func sqlRelationalModifiersFailCompilation() throws {
        let query = SelectQuery(projection: [.wildcard()], source: .graphPattern(.basic([])))
        for candidate in [
            query.replacing(quantifier: .distinctOn([.int(1)])),
            query.replacing(pagination: QueryPagination(limit: .count(.expression(.int(1)), withTies: false))),
            query.replacing(pagination: QueryPagination(limit: .count(.fixed(1), withTies: true))),
            query.replacing(pagination: QueryPagination(offset: .expression(.int(1)))),
        ] {
            #expect(throws: SPARQLSemanticValidationError.unsupportedSQLExpression) { try SPARQLSelectPlanCompiler.compile(candidate) }
        }
    }

    @Test("SQL constructors fail SPARQL compilation and cannot become false or unbound")
    func sqlConstructorsFailCompilation() {
        let collectionExpressions: [Expression] = [
            .collate(.col("unread"), collation: .c),
            .wholeRow("unread"),
            .field(.wholeRow("unread"), name: "unreadField"),
            .fieldExpansion(.wholeRow("unread")),
            .arraySubquery(SelectQuery(projection: [.expression(.col("unread"))], source: .values([[]]))),
            .arrayAccess(.col("unread"), subscripts: [.index(.col("unreadIndex"))]),
            .collectionOperation(.contains, left: .col("unread"), right: .col("unreadRight")),
        ]
        let variadic = Expression.function(FunctionCall(name: "CONCAT", arguments: [.col("unread")], finalArrayIsVariadic: true))
        for (expression, reason) in collectionExpressions.map({ ($0, "SQL collection expression") }) + [(variadic, "SQL VARIADIC")] {
            #expect(throws: SPARQLExpressionCompilationError.unsupportedExpression(reason)) {
                try SPARQLExpressionValidator.validate(expression)
            }
            var aggregate = SPARQLAggregateRewriter()
            #expect(throws: SPARQLExpressionCompilationError.unsupportedExpression(reason)) {
                try aggregate.rewrite(expression)
            }
            var grouped = SPARQLGroupedExpressionBindings()
            grouped.insert(variable: "grouped", for: expression)
            var scope = SPARQLGroupScopeExpressionRewriter(groupedExpressions: grouped,
                directlyGroupedVariables: [], eligibleVariables: [], protectedVariables: [])
            #expect(throws: SPARQLExpressionCompilationError.unsupportedExpression(reason)) {
                try scope.rewrite(expression, aggregateRewriter: &aggregate)
            }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                try ExpressionEvaluator.evaluateAsBoolean(expression, binding: VariableBinding())
            }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                try ExpressionEvaluator.evaluateForOrdering(expression, binding: VariableBinding())
            }
        }
        for expression: Expression in [.array([.int(1)]), .row([.null])] {
            #expect(throws: SPARQLExpressionCompilationError.unsupportedExpression("SQL ARRAY/ROW constructor")) {
                try SPARQLExpressionValidator.validate(expression)
            }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                try ExpressionEvaluator.evaluateAsBoolean(expression, binding: VariableBinding())
            }
            #expect(throws: SPARQLExpressionEvaluationError.self) {
                try ExpressionEvaluator.evaluateForOrdering(expression, binding: VariableBinding())
            }
        }
    }

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
        let query = SelectQuery(projection: [
                ProjectionItem(
                    .literal(.string("four")),
                    alias: "value"
                ),
            ],
            source: .graphPattern(.basic([])))

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
        let query = SelectQuery(projection: [
                ProjectionItem(expression, alias: "value")
            ],
            source: .graphPattern(.basic([])))

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

    @Test("Canonical explicit projection excludes covering fields")
    func canonicalExplicitProjectionIncludesCoveringFieldsInIdentityCheck()
        throws
    {
        let query = SelectQuery(projection: [
                ProjectionItem(.variable(Variable("target"))),
            ],
            source: .graphPattern(
                .values(
                    variables: ["target"],
                    bindings: [[.int(1)]]
                )
            ))

        let plan = try SPARQLSelectPlanCompiler
            .compileForCanonicalPagination(
                query,
                additionalProjectionVariables: ["hidden"]
            )

        #expect(plan.projectionVariables == ["?target"])
        #expect(!plan.projectionIsIdentity)
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
            SelectQuery(projection: [.wildcard()],
            source: .graphPattern(
                    .service(
                        endpoint: "https://example.com/sparql",
                        pattern: .basic([]),
                        silent: false
                    )
                ))
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            _ = try SPARQLExpressionPlan(expression)
        }
    }

    @Test("EXISTS cannot hide a VALUES row beyond canonical limits")
    func existsUsesCanonicalSpecializedLimits() {
        let expression = Expression.exists(
            SelectQuery(projection: [.wildcard()],
            source: .graphPattern(
                    .values(
                        variables: ["value"],
                        bindings: [[.int(1)]]
                    )
                ))
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
            SelectQuery(projection: [
                    ProjectionItem(.variable(Variable("value"))),
                ],
            source: .graphPattern(.basic([]))),
            SelectQuery(projection: [.wildcard()],
            source: .graphPattern(.basic([])),
            quantifier: .distinct),
            SelectQuery(projection: [.wildcard()],
            source: .graphPattern(.basic([])),
            dataset: .explicit(
                    defaultGraphs: ["urn:example:graph"],
                    namedGraphs: []
                )),
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
