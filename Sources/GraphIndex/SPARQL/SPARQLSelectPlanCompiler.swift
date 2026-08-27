import DatabaseKit

public enum SPARQLSelectPlanCompiler {
    public static func compile(
        _ query: SelectQuery,
        additionalProjectionVariables: [String] = [],
        structuralLimits: QueryStructuralLimits = .default
    ) throws -> SPARQLSelectExecutionPlan {
        try compile(
            query,
            additionalProjectionVariables: additionalProjectionVariables,
            expressionLimits: SPARQLExpressionCompilationLimits(
                structuralLimits: structuralLimits
            )
        )
    }

    /// Compiles a SELECT plan with one explicit expression limit authority
    /// shared by the top-level query, nested subqueries, and EXISTS algebra.
    public static func compile(
        _ query: SelectQuery,
        additionalProjectionVariables: [String] = [],
        expressionLimits: SPARQLExpressionCompilationLimits
    ) throws -> SPARQLSelectExecutionPlan {
        try SPARQLSemanticValidator.validate(
            query,
            limits: expressionLimits.structuralLimits
        )
        var context = SPARQLAlgebraCompilationContext(
            expressionLimits: expressionLimits
        )
        return try compile(
            query,
            additionalProjectionVariables: additionalProjectionVariables,
            isSubquery: false,
            includesSolutionSlice: true,
            inputVariables: [],
            context: &context
        )
    }

    package static func compileForCanonicalPagination(
        _ query: SelectQuery,
        additionalProjectionVariables: [String] = [],
        structuralLimits: QueryStructuralLimits = .default
    ) throws -> SPARQLSelectExecutionPlan {
        try SPARQLSemanticValidator.validate(
            query,
            limits: structuralLimits
        )
        var context = SPARQLAlgebraCompilationContext(
            structuralLimits: structuralLimits
        )
        return try compile(
            query,
            additionalProjectionVariables: additionalProjectionVariables,
            isSubquery: false,
            includesSolutionSlice: true,
            inputVariables: [],
            context: &context
        )
    }

    package static func compileSubquery(
        _ query: SelectQuery,
        inputPolicy: SPARQLSubqueryInputPolicy,
        inputVariables: Set<String>,
        context: inout SPARQLAlgebraCompilationContext
    ) throws -> SPARQLSubqueryExecutionPlan {
        let occurrenceIdentifier = context.takeSubqueryOccurrence()
        let select = try compile(
            query,
            additionalProjectionVariables: [],
            isSubquery: true,
            includesSolutionSlice: true,
            inputVariables: inputPolicy == .lateral ? inputVariables : [],
            context: &context
        )
        return SPARQLSubqueryExecutionPlan(
            occurrenceIdentifier: occurrenceIdentifier,
            select: consume select,
            inputPolicy: inputPolicy
        )
    }

    private static func compile(
        _ query: SelectQuery,
        additionalProjectionVariables: [String],
        isSubquery: Bool,
        includesSolutionSlice: Bool,
        inputVariables: Set<String>,
        context: inout SPARQLAlgebraCompilationContext
    ) throws -> SPARQLSelectExecutionPlan {
        guard query.subqueries == nil || query.subqueries?.isEmpty == true else {
            throw SPARQLSelectPlanCompilationError.namedSubqueriesUnsupported
        }
        guard query.accessPath == nil else {
            throw SPARQLSelectPlanCompilationError.accessPathUnsupported
        }
        if isSubquery, query.dataset != .implicit {
            throw SPARQLSelectPlanCompilationError.explicitDatasetInSubquery
        }
        let slice = try SPARQLSlice(
            offset: query.offset ?? 0,
            limit: query.limit
        )
        var aggregateRewriter = SPARQLAggregateRewriter()
        var rewrittenProjection = try rewriteProjection(
            query.projection,
            using: &aggregateRewriter
        )
        let having: [Expression]
        if let condition = query.having {
            having = [condition]
        } else {
            having = []
        }
        var queryLevel = try SPARQLQueryLevelPlanCompiler.prepare(
            source: query.source,
            filter: query.filter,
            groupBy: query.groupBy ?? [],
            having: having,
            orderBy: query.orderBy ?? [],
            orderVisibleVariables: projectionAliasVariables(
                in: rewrittenProjection
            ),
            inputVariables: inputVariables,
            context: &context,
            aggregateRewriter: &aggregateRewriter
        )
        if queryLevel.hasGrouping {
            rewrittenProjection = try rewriteGroupedProjection(
                rewrittenProjection,
                groupedExpressions: queryLevel.groupedExpressions,
                directlyGroupedVariables: queryLevel.directlyGroupedVariables,
                aggregateRewriter: &aggregateRewriter
            )
        }

        queryLevel.algebra = try GraphPatternConverter
            .applyingProjectionExpressions(
            rewrittenProjection,
            to: queryLevel.algebra,
            limits: context.expressionLimits,
            inputVariables: queryLevel.hasGrouping ? [] : inputVariables,
            reservedTargetVariables: queryLevel.sourceVisibleVariables,
            restrictExpressionReferencesToScope: queryLevel.hasGrouping
        )

        let projection = try projectionVariables(
            for: rewrittenProjection,
            algebra: queryLevel.algebra,
            additionalVariables: additionalProjectionVariables,
            inputVariables: queryLevel.hasGrouping ? [] : inputVariables,
            requiresInScopeVariables: queryLevel.hasGrouping
        )
        var possibleProjectionVariables = queryLevel.algebra.outputVariables
            .union(
                queryLevel.hasGrouping ? [] : inputVariables
            )
        if !queryLevel.hasGrouping {
            possibleProjectionVariables.formUnion(
                additionalProjectionVariables.map(prefixedVariable)
            )
        }
        let projectionIsIdentity = possibleProjectionVariables.isSubset(
            of: Set(projection)
        )
        let ordered = try SPARQLQueryLevelPlanCompiler.makeOrderedPlan(
            dataset: query.dataset,
            algebra: queryLevel.algebra,
            rewrittenOrderBy: queryLevel.rewrittenOrderBy,
            limits: context.expressionLimits
        )
        let distinctFromProjection: Bool
        if case .distinctItems = query.projection {
            distinctFromProjection = true
        } else {
            distinctFromProjection = false
        }

        return SPARQLSelectExecutionPlan(
            ordered: consume ordered,
            projectionVariables: projection,
            projectionIsIdentity: projectionIsIdentity,
            duplicatePolicy: query.distinct
                    || query.reduced
                    || distinctFromProjection
                ? .distinct
                : .preserve,
            slice: includesSolutionSlice ? slice : try SPARQLSlice()
        )
    }

    private static func projectionVariables(
        for projection: Projection,
        algebra: ExecutionPattern,
        additionalVariables: [String],
        inputVariables: Set<String>,
        requiresInScopeVariables: Bool
    ) throws -> [String] {
        let availableVariables = algebra.outputVariables.union(inputVariables)
        switch projection {
        case .all:
            var variables = availableVariables
            variables = Set(
                variables.lazy.filter {
                    !SPARQLInternalVariable.isInternal($0)
                }
            )
            for variable in additionalVariables {
                variables.insert(prefixedVariable(variable))
            }
            return variables.sorted()
        case .allFrom:
            throw SPARQLSelectPlanCompilationError
                .allFromProjectionUnsupported
        case .items(let items), .distinctItems(let items):
            var variables: [String] = []
            variables.reserveCapacity(items.count)
            var seen: Set<String> = []
            for item in items {
                let variable = try projectionVariable(from: item)
                if requiresInScopeVariables {
                    try validateReferences(
                        [variable],
                        availableVariables: availableVariables
                    )
                }
                guard seen.insert(variable).inserted else {
                    throw SPARQLSelectPlanCompilationError
                        .duplicateProjectionVariable(variable)
                }
                variables.append(variable)
            }
            return variables
        }
    }

    private static func projectionVariable(
        from item: ProjectionItem
    ) throws -> String {
        if let alias = item.alias {
            return prefixedVariable(alias)
        }
        switch item.expression {
        case .variable(let variable):
            return prefixedVariable(variable.name)
        case .column(let column):
            return prefixedVariable(column.column)
        case .aggregate:
            throw GraphPatternConversionError
                .unsupportedAggregateExpression(
                    "aggregate projection without an alias"
                )
        default:
            throw GraphPatternConversionError
                .projectionExpressionRequiresAlias
        }
    }

    private static func rewriteProjection(
        _ projection: Projection,
        using rewriter: inout SPARQLAggregateRewriter
    ) throws -> Projection {
        let items: [ProjectionItem]
        let isDistinct: Bool
        switch projection {
        case .items(let projectionItems),
             .distinctItems(let projectionItems):
            items = projectionItems
            if case .distinctItems = projection {
                isDistinct = true
            } else {
                isDistinct = false
            }
        case .all:
            return .all
        case .allFrom(let source):
            return .allFrom(source)
        }

        var rewrittenItems: [ProjectionItem] = []
        rewrittenItems.reserveCapacity(items.count)
        for item in items {
            if item.alias == nil {
                switch item.expression {
                case .variable, .column:
                    break
                default:
                    throw GraphPatternConversionError
                        .projectionExpressionRequiresAlias
                }
            }
            let occurrenceCount = rewriter.aggregateOccurrenceCount
            let expression = try rewriter.rewrite(item.expression)
            if rewriter.aggregateOccurrenceCount != occurrenceCount,
               item.alias == nil {
                throw GraphPatternConversionError
                    .projectionExpressionRequiresAlias
            }
            rewrittenItems.append(
                ProjectionItem(expression, alias: item.alias)
            )
        }
        return isDistinct
            ? .distinctItems(consume rewrittenItems)
            : .items(consume rewrittenItems)
    }

    private static func rewriteGroupedProjection(
        _ projection: Projection,
        groupedExpressions: SPARQLGroupedExpressionBindings,
        directlyGroupedVariables: Set<String>,
        aggregateRewriter: inout SPARQLAggregateRewriter
    ) throws -> Projection {
        let items: [ProjectionItem]
        let isDistinct: Bool
        switch projection {
        case .items(let projectionItems):
            items = projectionItems
            isDistinct = false
        case .distinctItems(let projectionItems):
            items = projectionItems
            isDistinct = true
        case .all, .allFrom:
            return projection
        }

        var rewrittenItems: [ProjectionItem] = []
        rewrittenItems.reserveCapacity(items.count)
        for item in items {
            var scopeRewriter = SPARQLGroupScopeExpressionRewriter(
                groupedExpressions: groupedExpressions,
                directlyGroupedVariables: directlyGroupedVariables,
                eligibleVariables: [],
                protectedVariables: []
            )
            rewrittenItems.append(
                ProjectionItem(
                    try scopeRewriter.rewrite(
                        item.expression,
                        aggregateRewriter: &aggregateRewriter
                    ),
                    alias: item.alias
                )
            )
        }
        return isDistinct
            ? .distinctItems(consume rewrittenItems)
            : .items(consume rewrittenItems)
    }

    private static func projectionAliasVariables(
        in projection: Projection
    ) -> Set<String> {
        let items: [ProjectionItem]
        switch projection {
        case .items(let projectionItems),
             .distinctItems(let projectionItems):
            items = projectionItems
        case .all, .allFrom:
            return []
        }
        var variables: Set<String> = []
        for item in items {
            if let alias = item.alias {
                variables.insert(prefixedVariable(alias))
            }
        }
        return variables
    }

    private static func validateReferences(
        _ references: Set<String>,
        availableVariables: Set<String>
    ) throws {
        if let missing = references
            .subtracting(availableVariables)
            .sorted()
            .first {
            throw GraphPatternConversionError
                .projectionVariableNotInScope(missing)
        }
    }

    private static func prefixedVariable(_ name: String) -> String {
        return "?\(name)"
    }
}
