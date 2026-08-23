import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

extension SPARQLQueryExecutor {
    // MARK: - Pattern Execution

    /// Execute one triple atom against the active RDF graph.
    func executePattern(
        _ pattern: ExecutionTriple,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        filter: FilterExpression? = nil,
        resultLimit: Int? = nil
    ) async throws -> EvaluationResult {
        let scanResult = try await datasetScanner.scan(
            subject: try boundRDFTerm(pattern.subject),
            predicate: try boundRDFTerm(pattern.predicate),
            object: try boundRDFTerm(pattern.object),
            graphTarget: activeGraph.graphTarget,
            limit: resultLimit,
            readMode: readMode,
            transaction: transaction,
            workMeter: try requiredWorkMeter()
        )

        var bindings = try SPARQLRetainedBindingBuilder.make(
            workMeter: try requiredWorkMeter(),
            stage: .bindingCandidate,
            expectedCount: 0
        )
        rowLoop: for row in scanResult {
            try requiredWorkMeter().consume(at: .bindingCandidate)
            var binding = VariableBinding()
            guard matchTerm(
                pattern.subject,
                against: .rdfTerm(row.subject),
                binding: &binding
            ), matchTerm(
                pattern.predicate,
                against: .rdfTerm(row.predicate),
                binding: &binding
            ), matchTerm(
                pattern.object,
                against: .rdfTerm(row.object),
                binding: &binding
            ) else {
                continue
            }
            let properties = try row.decodeProperties()
            for (fieldName, value) in properties {
                guard binding.merge(
                    variable: "?\(fieldName)",
                    value: value
                ) else {
                    continue rowLoop
                }
            }
            if let filter {
                try requiredWorkMeter().consume(at: .filterEvaluation)
                if try await !evaluateFilterExpression(
                    filter,
                    binding: binding,
                    transaction: transaction,
                    activeGraph: activeGraph
                ) { continue }
            }
            try bindings.append(binding, at: .bindingCandidate)
            if let resultLimit, bindings.count >= resultLimit { break }
        }

        var statistics = ExecutionStatistics()
        statistics.indexScans = scanResult.physicalScanCount
        return EvaluationResult(
            bindings: bindings.finish(),
            stats: statistics
        )
    }

    func boundRDFTerm(
        _ term: ExecutionTerm
    ) throws -> RDFTerm? {
        guard term.isBound else { return nil }
        guard let value = term.literalValue,
              case .rdfTerm(let rdfTerm) = value else {
            throw SPARQLQueryError.invalidRDFTerm(term.description)
        }
        return rdfTerm
    }

    func makeScanSignature(
        for pattern: ExecutionTriple,
        graphTarget: RDFGraphScanTarget
    ) -> ScanSignature {
        ScanSignature(
            subject: pattern.subject,
            predicate: pattern.predicate,
            object: pattern.object,
            graphTarget: graphTarget
        )
    }

    func matchTerm(
        _ term: ExecutionTerm,
        against value: FieldValue,
        binding: inout VariableBinding
    ) -> Bool {
        binding.match(term, against: value)
    }


    // MARK: - Join Order Optimization

    /// Optimize join order using greedy algorithm
    ///
    /// Selects patterns in order of:
    /// 1. Most selective (most bound terms) first
    /// 2. Patterns sharing variables with already-bound variables
    ///
    /// **Reference**: "Optimal Ordering of BGP Evaluation"
    /// W3C SPARQL 1.1 Query Language, Section 18.4
    func optimizeJoinOrder(_ patterns: [ExecutionTriple]) -> [ExecutionTriple] {
        guard patterns.count > 1 else { return patterns }

        var remaining = patterns
        var ordered: [ExecutionTriple] = []
        var boundVariables = Set<String>()

        while !remaining.isEmpty {
            // Score each remaining pattern
            let scores = remaining.map { pattern -> (pattern: ExecutionTriple, score: Int) in
                var score = pattern.selectivityScore

                // Bonus for sharing variables with already-bound variables
                let sharedVars = pattern.variables.intersection(boundVariables)
                score += sharedVars.count * 10

                return (pattern, score)
            }

            // Select highest scoring pattern
            let best = scores.max { $0.score < $1.score }!
            ordered.append(best.pattern)
            boundVariables.formUnion(best.pattern.variables)

            // Remove from remaining
            if let idx = remaining.firstIndex(of: best.pattern) {
                remaining.remove(at: idx)
            }
        }

        return ordered
    }

    // MARK: - Pattern Substitution

    /// Substitute variables in a pattern with bound values
    func substitutePattern(
        _ pattern: ExecutionPattern,
        with binding: VariableBinding
    ) throws -> ExecutionPattern {
        switch pattern {
        case .basic(let patterns):
            return .basic(patterns.map { $0.substitute(binding) })

        case .join(let left, let right):
            return .join(
                try substitutePattern(left, with: binding),
                try substitutePattern(right, with: binding)
            )

        case .optional(let left, let right):
            return .optional(
                try substitutePattern(left, with: binding),
                try substitutePattern(right, with: binding)
            )

        case .union(let left, let right):
            return .union(
                try substitutePattern(left, with: binding),
                try substitutePattern(right, with: binding)
            )

        case .filter(let innerPattern, let expression):
            return .filter(
                try substitutePattern(innerPattern, with: binding),
                expression
            )

        case .extend(let innerPattern, let variable, let expression):
            return .extend(
                try substitutePattern(innerPattern, with: binding),
                variable: variable,
                expression: expression
            )

        case .values:
            // Compatibility with the outer seed is resolved row-by-row by the
            // VALUES evaluator without copying or rewriting the table.
            return pattern

        case .graph(let selector, let innerPattern):
            let substitutedSelector: ExecutionGraphSelector
            switch selector {
            case .named:
                substitutedSelector = selector
            case .variable(let variable):
                if let value = binding[variable] {
                    guard case .rdfTerm(let term) = value else {
                        throw SPARQLQueryError.invalidGraphBinding(variable)
                    }
                    do {
                        substitutedSelector = .named(try RDFGraphName(term))
                    } catch {
                        throw SPARQLQueryError.invalidGraphBinding(variable)
                    }
                } else {
                    substitutedSelector = selector
                }
            }
            return .graph(
                substitutedSelector,
                try substitutePattern(innerPattern, with: binding)
            )

        case .minus(let left, let right):
            return .minus(
                try substitutePattern(left, with: binding),
                try substitutePattern(right, with: binding)
            )

        case .groupBy(let sourcePattern, let grouping, let aggs, let havingExpr):
            return .groupBy(
                try substitutePattern(sourcePattern, with: binding),
                grouping: grouping,
                aggregates: aggs,
                having: havingExpr
            )

        case .propertyPath(let subject, let path, let object):
            return .propertyPath(
                subject: subject.substitute(binding),
                path: path,
                object: object.substitute(binding)
            )

        case .lateral(let left, let right):
            return .lateral(
                try substitutePattern(left, with: binding),
                try substitutePattern(right, with: binding)
            )

        case .subquery:
            // Select is an algebra barrier. Isolated plans ignore the outer
            // seed; lateral plans receive it explicitly at evaluation time.
            return pattern
        }
    }

    // MARK: - Property Path Evaluation

    func evaluateExecutionPropertyPath(
        subject: ExecutionTerm,
        path: ExecutionPropertyPath,
        object: ExecutionTerm,
        seed: consuming VariableBinding,
        resultLimit: Int?,
        transaction: any TransactionReadAccess,
        activeGraph: ActiveGraph,
        config: ExecutionPropertyPathConfiguration = .default
    ) async throws -> EvaluationResult {
        let evaluator = SPARQLPropertyPathEvaluator(
            datasetScanner: datasetScanner,
            readMode: readMode,
            ontologyContext: ontologyContext,
            workMeter: try requiredWorkMeter(),
            configuration: config
        )
        let result = try await evaluator.execute(
            subject: subject,
            path: path,
            object: object,
            seed: consume seed,
            resultLimit: resultLimit,
            graphTarget: activeGraph.graphTarget,
            transaction: transaction
        )
        return EvaluationResult(
            bindings: consume result.bindings,
            stats: result.statistics
        )
    }
}
