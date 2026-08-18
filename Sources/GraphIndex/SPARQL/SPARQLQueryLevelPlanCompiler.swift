import DatabaseKit

struct SPARQLQueryLevelCompilation {
    var algebra: ExecutionPattern
    let rewrittenOrderBy: [SortKey]
    let sourceVisibleVariables: Set<String>
    let groupedExpressions: SPARQLGroupedExpressionBindings
    let directlyGroupedVariables: Set<String>
    let hasGrouping: Bool
}

/// Compiles the solution pipeline shared by every SPARQL query form.
///
/// SELECT adds projection and duplicate handling after this compilation.
/// ASK, CONSTRUCT, and DESCRIBE consume the retained solution sequence
/// directly, so no synthetic wildcard SELECT is required.
public enum SPARQLQueryLevelPlanCompiler {
    public static func compile(
        _ query: AskQuery,
        structuralLimits: QueryStructuralLimits = .default
    ) throws -> SPARQLSolutionFormExecutionPlan {
        try SPARQLSemanticValidator.validate(
            .ask(query),
            limits: structuralLimits
        )
        return try compile(
            pattern: query.pattern,
            dataset: query.dataset,
            modifiers: query.modifiers,
            structuralLimits: structuralLimits
        )
    }

    public static func compile(
        _ query: ConstructQuery,
        structuralLimits: QueryStructuralLimits = .default
    ) throws -> SPARQLSolutionFormExecutionPlan {
        try SPARQLSemanticValidator.validate(
            .construct(query),
            limits: structuralLimits
        )
        return try compile(
            pattern: query.pattern,
            dataset: query.dataset,
            modifiers: query.modifiers,
            structuralLimits: structuralLimits
        )
    }

    public static func compile(
        _ query: DescribeQuery,
        structuralLimits: QueryStructuralLimits = .default
    ) throws -> SPARQLSolutionFormExecutionPlan {
        try SPARQLSemanticValidator.validate(
            .describe(query),
            limits: structuralLimits
        )
        return try compile(
            pattern: query.pattern ?? .basic([]),
            dataset: query.dataset,
            modifiers: query.modifiers,
            structuralLimits: structuralLimits
        )
    }

    static func prepare(
        source: DataSource,
        filter: Expression?,
        groupBy: [Expression],
        having: [Expression],
        orderBy: [SortKey],
        orderVisibleVariables: Set<String>,
        inputVariables: Set<String>,
        context: inout SPARQLAlgebraCompilationContext,
        aggregateRewriter: inout SPARQLAggregateRewriter
    ) throws -> SPARQLQueryLevelCompilation {
        var algebra = try compileSource(
            source,
            inputVariables: inputVariables,
            context: &context
        )
        if let filter {
            algebra = .filter(
                algebra,
                .query(
                    try SPARQLExpressionPlan(
                        filter,
                        limits: context.expressionLimits
                    )
                )
            )
        }
        let sourceVisibleVariables = algebra.outputVariables.union(
            inputVariables
        )

        let groupCompilation = try compileGroupKeys(
            groupBy,
            limits: context.expressionLimits
        )
        var rewrittenHaving: [Expression] = []
        rewrittenHaving.reserveCapacity(having.count)
        for condition in having {
            rewrittenHaving.append(
                try aggregateRewriter.rewrite(condition)
            )
        }
        var rewrittenOrderBy: [SortKey] = []
        rewrittenOrderBy.reserveCapacity(orderBy.count)
        for key in orderBy {
            rewrittenOrderBy.append(
                SortKey(
                    try aggregateRewriter.rewrite(key.expression),
                    direction: key.direction,
                    nulls: key.nulls
                )
            )
        }

        let hasGrouping = !groupCompilation.keys.isEmpty
            || !aggregateRewriter.aggregateBindings.isEmpty
        if hasGrouping {
            for index in rewrittenHaving.indices {
                var scopeRewriter = SPARQLGroupScopeExpressionRewriter(
                    groupedExpressions: groupCompilation.expressionVariables,
                    directlyGroupedVariables: groupCompilation.directVariables,
                    eligibleVariables: sourceVisibleVariables,
                    protectedVariables: []
                )
                rewrittenHaving[index] = try scopeRewriter.rewrite(
                    rewrittenHaving[index],
                    aggregateRewriter: &aggregateRewriter
                )
            }
            for index in rewrittenOrderBy.indices {
                var scopeRewriter = SPARQLGroupScopeExpressionRewriter(
                    groupedExpressions: groupCompilation.expressionVariables,
                    directlyGroupedVariables: groupCompilation.directVariables,
                    eligibleVariables: sourceVisibleVariables,
                    protectedVariables: orderVisibleVariables
                )
                let key = rewrittenOrderBy[index]
                rewrittenOrderBy[index] = SortKey(
                    try scopeRewriter.rewrite(
                        key.expression,
                        aggregateRewriter: &aggregateRewriter
                    ),
                    direction: key.direction,
                    nulls: key.nulls
                )
            }

            let aggregateBindings = aggregateRewriter.aggregateBindings
            var aggregates: [AggregateExpression] = []
            aggregates.reserveCapacity(aggregateBindings.count)
            for binding in aggregateBindings {
                aggregates.append(
                    try GraphPatternConverter.convertAggregate(
                        binding,
                        limits: context.expressionLimits
                    )
                )
            }
            let grouping: SPARQLGroupingPlan = groupCompilation.keys.isEmpty
                ? .implicitSingleGroup
                : .explicit(groupCompilation.keys)
            algebra = .groupBy(
                algebra,
                grouping: grouping,
                aggregates: consume aggregates,
                having: nil
            )
        }
        for condition in rewrittenHaving {
            algebra = .filter(
                algebra,
                try GraphPatternConverter.convertFilter(
                    condition,
                    limits: context.expressionLimits
                )
            )
        }

        return SPARQLQueryLevelCompilation(
            algebra: consume algebra,
            rewrittenOrderBy: consume rewrittenOrderBy,
            sourceVisibleVariables: sourceVisibleVariables,
            groupedExpressions: groupCompilation.expressionVariables,
            directlyGroupedVariables: groupCompilation.directVariables,
            hasGrouping: hasGrouping
        )
    }

    package static func makeOrderedPlan(
        dataset: SPARQLDataset,
        algebra: consuming ExecutionPattern,
        rewrittenOrderBy: [SortKey],
        limits: SPARQLExpressionCompilationLimits
    ) throws -> SPARQLOrderedSolutionPlan {
        let visibleVariables = algebra.outputVariables
            .filter { !SPARQLInternalVariable.isInternal($0) }
            .sorted()
        return SPARQLOrderedSolutionPlan(
            dataset: try SPARQLExecutionDataset(dataset),
            algebra: consume algebra,
            orderKeys: try compileOrderKeys(
                rewrittenOrderBy,
                limits: limits
            ),
            visibleVariables: visibleVariables
        )
    }

    package static func compileSource(
        _ source: DataSource,
        inputVariables: Set<String>,
        context: inout SPARQLAlgebraCompilationContext
    ) throws -> ExecutionPattern {
        switch source {
        case .graphPattern(let pattern):
            return try GraphPatternConverter.convert(
                pattern,
                prefixes: [:],
                context: &context,
                subqueryInputPolicy: .isolated,
                inputVariables: inputVariables
            )
        case .namedGraph(let name, let pattern):
            return try GraphPatternConverter.convert(
                .graph(name: .iri(name), pattern: pattern),
                prefixes: [:],
                context: &context,
                subqueryInputPolicy: .isolated,
                inputVariables: inputVariables
            )
        case .table, .logical, .subquery, .join, .values, .graphTable,
             .service, .union, .unionAll, .intersect, .except:
            throw SPARQLSelectPlanCompilationError.unsupportedSource
        #if DATABASE_MULTIPLE_BASES
        case .base:
            throw SPARQLSelectPlanCompilationError.unsupportedSource
        #endif
        }
    }

    private static func compile(
        pattern: GraphPattern,
        dataset: SPARQLDataset,
        modifiers: SPARQLSolutionModifiers,
        structuralLimits: QueryStructuralLimits
    ) throws -> SPARQLSolutionFormExecutionPlan {
        let slice = try SPARQLSlice(
            offset: modifiers.offset ?? 0,
            limit: modifiers.limit
        )
        var context = SPARQLAlgebraCompilationContext(
            structuralLimits: structuralLimits
        )
        var aggregateRewriter = SPARQLAggregateRewriter()
        let compilation = try prepare(
            source: .graphPattern(pattern),
            filter: nil,
            groupBy: modifiers.groupBy,
            having: modifiers.having,
            orderBy: modifiers.orderBy,
            orderVisibleVariables: [],
            inputVariables: [],
            context: &context,
            aggregateRewriter: &aggregateRewriter
        )
        let ordered = try makeOrderedPlan(
            dataset: dataset,
            algebra: compilation.algebra,
            rewrittenOrderBy: compilation.rewrittenOrderBy,
            limits: context.expressionLimits
        )
        return SPARQLSolutionFormExecutionPlan(
            ordered: consume ordered,
            slice: slice
        )
    }

    private static func compileOrderKeys(
        _ sortKeys: [SortKey],
        limits: SPARQLExpressionCompilationLimits
    ) throws -> [SPARQLOrderKeyPlan] {
        var plans: [SPARQLOrderKeyPlan] = []
        plans.reserveCapacity(sortKeys.count)
        for sortKey in sortKeys {
            plans.append(
                SPARQLOrderKeyPlan(
                    expression: try SPARQLExpressionPlan(
                        sortKey.expression,
                        limits: limits
                    ),
                    ascending: sortKey.direction == .ascending,
                    nullsLast: sortKey.nulls == .last
                )
            )
        }
        return plans
    }

    private static func compileGroupKeys(
        _ expressions: [Expression],
        limits: SPARQLExpressionCompilationLimits
    ) throws -> (
        keys: [SPARQLGroupKeyPlan],
        expressionVariables: SPARQLGroupedExpressionBindings,
        directVariables: Set<String>
    ) {
        var keys: [SPARQLGroupKeyPlan] = []
        var expressionVariables = SPARQLGroupedExpressionBindings()
        var directVariables: Set<String> = []
        keys.reserveCapacity(expressions.count)
        for (index, expression) in expressions.enumerated() {
            let rawVariable: String
            switch expression {
            case .variable(let variable):
                rawVariable = variable.name
                directVariables.insert(prefixedVariable(rawVariable))
            case .column(let column):
                rawVariable = column.column
                directVariables.insert(prefixedVariable(rawVariable))
            default:
                rawVariable = SPARQLInternalVariable.groupKeyRaw(
                    UInt64(index)
                )
            }
            expressionVariables.insert(
                variable: rawVariable,
                for: expression
            )
            keys.append(
                SPARQLGroupKeyPlan(
                    outputVariable: prefixedVariable(rawVariable),
                    expression: try SPARQLExpressionPlan(
                        expression,
                        limits: limits
                    )
                )
            )
        }
        return (
            keys: keys,
            expressionVariables: expressionVariables,
            directVariables: directVariables
        )
    }

    private static func prefixedVariable(_ name: String) -> String {
        "?\(name)"
    }
}
