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
        guard query.windows.isEmpty else {
            throw SPARQLExpressionCompilationError.unsupportedExpression("SQL named WINDOW")
        }
        let groupBy = try (query.groupBy ?? []).map { element in
            guard case .expression(let expression) = element else {
                throw SPARQLExpressionCompilationError.unsupportedExpression("SQL grouping sets")
            }
            return expression
        }
        if isSubquery, query.dataset != .implicit {
            throw SPARQLSelectPlanCompilationError.explicitDatasetInSubquery
        }
        let offset: UInt64
        switch query.pagination.offset {
        case nil: offset = 0
        case .fixed(let value): offset = value
        case .expression:
            throw SPARQLExpressionCompilationError.unsupportedExpression("SQL pagination expression")
        }
        let limit: UInt64?
        switch query.pagination.limit {
        case nil, .all: limit = nil
        case .count(.fixed(let value), withTies: false): limit = value
        case .count:
            throw SPARQLExpressionCompilationError.unsupportedExpression("SQL pagination expression or WITH TIES")
        }
        let distinct: Bool
        switch query.quantifier {
        case .all: distinct = false
        case .distinct: distinct = true
        case .distinctOn:
            throw SPARQLExpressionCompilationError.unsupportedExpression("SQL DISTINCT ON")
        }
        let slice = try SPARQLSlice(offset: offset, limit: limit)
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
            groupBy: groupBy,
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
        return SPARQLSelectExecutionPlan(
            ordered: consume ordered,
            projectionVariables: projection,
            projectionIsIdentity: projectionIsIdentity,
            duplicatePolicy: distinct || query.reduced
                ? .distinct
                : .preserve,
            slice: includesSolutionSlice ? slice : try SPARQLSlice()
        )
    }

    private static func projectionVariables(
        for projection: [ProjectionItem],
        algebra: ExecutionPattern,
        additionalVariables: [String],
        inputVariables: Set<String>,
        requiresInScopeVariables: Bool
    ) throws -> [String] {
        let availableVariables = algebra.outputVariables.union(inputVariables)
        if projection == [.wildcard()] {
            var variables = Set(availableVariables.filter { !SPARQLInternalVariable.isInternal($0) })
            variables.formUnion(additionalVariables.map(prefixedVariable))
            return variables.sorted()
        }
        var variables: [String] = []
        var seen: Set<String> = []
        for item in projection {
            let variable = try projectionVariable(from: item)
            if requiresInScopeVariables {
                try validateReferences([variable], availableVariables: availableVariables)
            }
            guard seen.insert(variable).inserted else {
                throw SPARQLSelectPlanCompilationError.duplicateProjectionVariable(variable)
            }
            variables.append(variable)
        }
        return variables
    }

    private static func projectionVariable(from item: ProjectionItem) throws -> String {
        guard case .expression(let expression, let alias) = item else {
            throw SPARQLSelectPlanCompilationError.allFromProjectionUnsupported
        }
        if let alias { return prefixedVariable(alias) }
        switch expression {
        case .variable(let variable): return prefixedVariable(variable.name)
        case .column(let column): return prefixedVariable(column.column)
        case .aggregate:
            throw GraphPatternConversionError.unsupportedAggregateExpression("aggregate projection without an alias")
        default: throw GraphPatternConversionError.projectionExpressionRequiresAlias
        }
    }

    private static func rewriteProjection(
        _ projection: [ProjectionItem], using rewriter: inout SPARQLAggregateRewriter
    ) throws -> [ProjectionItem] {
        if projection == [.wildcard()] { return projection }
        return try projection.map { item in
            guard case .expression(let expression, let alias) = item else {
                throw SPARQLSelectPlanCompilationError.allFromProjectionUnsupported
            }
            if alias == nil {
                switch expression {
                case .variable, .column: break
                default: throw GraphPatternConversionError.projectionExpressionRequiresAlias
                }
            }
            let occurrenceCount = rewriter.aggregateOccurrenceCount
            let rewritten = try rewriter.rewrite(expression)
            if rewriter.aggregateOccurrenceCount != occurrenceCount, alias == nil {
                throw GraphPatternConversionError.projectionExpressionRequiresAlias
            }
            return .expression(rewritten, alias: alias)
        }
    }

    private static func rewriteGroupedProjection(
        _ projection: [ProjectionItem], groupedExpressions: SPARQLGroupedExpressionBindings,
        directlyGroupedVariables: Set<String>, aggregateRewriter: inout SPARQLAggregateRewriter
    ) throws -> [ProjectionItem] {
        if projection == [.wildcard()] { return projection }
        return try projection.map { item in
            guard case .expression(let expression, let alias) = item else {
                throw SPARQLSelectPlanCompilationError.allFromProjectionUnsupported
            }
            var rewriter = SPARQLGroupScopeExpressionRewriter(
                groupedExpressions: groupedExpressions,
                directlyGroupedVariables: directlyGroupedVariables,
                eligibleVariables: [], protectedVariables: [])
            return .expression(try rewriter.rewrite(expression, aggregateRewriter: &aggregateRewriter), alias: alias)
        }
    }

    private static func projectionAliasVariables(in projection: [ProjectionItem]) -> Set<String> {
        Set(projection.compactMap { item in
            guard case .expression(_, let alias) = item, let alias else { return nil }
            return prefixedVariable(alias)
        })
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
