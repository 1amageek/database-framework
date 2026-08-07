import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import StorageKit

extension SPARQLQueryExecutor {
    package func executeAskInTransaction(
        _ query: AskQuery,
        structuralLimits: QueryStructuralLimits,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let plan = try SPARQLQueryLevelPlanCompiler.compile(
            query,
            structuralLimits: structuralLimits
        )
        guard plan.slice.limit != 0 else { return false }
        let result = try await evaluateSlicedSolutionFormInTransaction(
            plan: plan,
            transaction: transaction,
            workMeter: workMeter
        )
        return !result.bindings.isEmpty
    }

    package func executeConstructInTransaction(
        _ query: ConstructQuery,
        nodeNamespace: GraphResultNodeNamespace,
        structuralLimits: QueryStructuralLimits,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedRDFGraph {
        let plan = try SPARQLQueryLevelPlanCompiler.compile(
            query,
            structuralLimits: structuralLimits
        )
        let result = try await evaluateSlicedSolutionFormInTransaction(
            plan: plan,
            transaction: transaction,
            workMeter: workMeter
        )
        let bindings = consume result.bindings
        var output = try DatabaseRetainedRDFGraphBuilder(
            workMeter: workMeter
        )
        guard !query.template.isEmpty else {
            return output.finish()
        }
        let orderedSolutions = try SPARQLSolutionFingerprintOrder.build(
            bindings: bindings,
            workMeter: workMeter
        )
        try orderedSolutions.forEach {
            sourceIndex,
            fingerprint,
            occurrence in
            var blankNodeResolver = try SPARQLConstructBlankNodeResolver(
                nodeNamespace: nodeNamespace,
                bindingFingerprint: copy fingerprint,
                occurrence: occurrence,
                workMeter: workMeter
            )
            try bindings.withElement(at: sourceIndex) { binding in
                for template in query.template {
                    try workMeter.consume(at: .projection)
                    try SPARQLConstructTemplateInstantiator.append(
                        template,
                        binding: binding,
                        blankNodeResolver: &blankNodeResolver,
                        to: &output
                    )
                }
            }
        }
        return output.finish()
    }

    package func executeDescribeInTransaction(
        _ query: DescribeQuery,
        structuralLimits: QueryStructuralLimits,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseRetainedRDFGraph {
        let plan = try SPARQLQueryLevelPlanCompiler.compile(
            query,
            structuralLimits: structuralLimits
        )
        var resources = try SPARQLDescribeResourceCollector.make(
            workMeter: workMeter
        )

        switch query.selection {
        case .all:
            let result = try await evaluateSlicedSolutionFormInTransaction(
                plan: plan,
                transaction: transaction,
                workMeter: workMeter
            )
            let bindings = consume result.bindings
            for index in 0..<bindings.count {
                try bindings.withElement(at: index) { binding in
                    for variable in plan.ordered.visibleVariables {
                        try workMeter.consume(at: .projection)
                        guard let value = binding[variable],
                              let resource = describeResource(from: value) else {
                            continue
                        }
                        try resources.insert(
                            resource,
                            workMeter: workMeter
                        )
                    }
                }
            }

        case .resources(let first, let additional):
            try collectStaticDescribeResource(
                first,
                into: &resources,
                workMeter: workMeter
            )
            for term in additional {
                try collectStaticDescribeResource(
                    term,
                    into: &resources,
                    workMeter: workMeter
                )
            }

            if hasDescribeVariable(first, additional: additional) {
                let result = try await evaluateSlicedSolutionFormInTransaction(
                    plan: plan,
                    transaction: transaction,
                    workMeter: workMeter
                )
                let bindings = consume result.bindings
                try collectBoundDescribeResource(
                    first,
                    bindings: bindings,
                    into: &resources,
                    workMeter: workMeter
                )
                for term in additional {
                    try collectBoundDescribeResource(
                        term,
                        bindings: bindings,
                        into: &resources,
                        workMeter: workMeter
                    )
                }
            }
        }

        let retainedResources = resources.finish()
        var output = try DatabaseRetainedRDFGraphBuilder(
            workMeter: workMeter
        )
        for index in 0..<retainedResources.count {
            let scan = try await retainedResources.withElement(
                at: index
            ) { resource in
                try await datasetScanner.scan(
                    subject: copy resource,
                    predicate: nil,
                    object: nil,
                    graphTarget: plan.ordered.dataset.defaultGraphTarget,
                    limit: nil,
                    readMode: readMode,
                    transaction: transaction,
                    workMeter: workMeter
                )
            }
            for row in scan {
                try output.append(row.quad)
            }
        }
        return output.finish()
    }

    private func collectStaticDescribeResource(
        _ term: SPARQLTerm,
        into resources: inout SPARQLDescribeResourceCollector,
        workMeter: DatabaseWorkMeter
    ) throws {
        guard case .iri(let value) = term else { return }
        try workMeter.consume(at: .projection)
        try resources.insert(
            .iri(try RDFIRI(value)),
            workMeter: workMeter
        )
    }

    private func collectBoundDescribeResource(
        _ term: SPARQLTerm,
        bindings: borrowing SPARQLRetainedBindings,
        into resources: inout SPARQLDescribeResourceCollector,
        workMeter: DatabaseWorkMeter
    ) throws {
        guard case .variable(let name) = term else { return }
        let variable = "?\(name)"
        for index in 0..<bindings.count {
            try bindings.withElement(at: index) { binding in
                try workMeter.consume(at: .bindingCandidate)
                guard let value = binding[variable],
                      let resource = describeResource(from: value) else {
                    return
                }
                try resources.insert(resource, workMeter: workMeter)
            }
        }
    }

    private func hasDescribeVariable(
        _ first: SPARQLTerm,
        additional: [SPARQLTerm]
    ) -> Bool {
        if case .variable = first { return true }
        for term in additional {
            if case .variable = term { return true }
        }
        return false
    }

    private func describeResource(
        from value: FieldValue
    ) -> RDFTerm? {
        guard case .rdfTerm(let term) = value else { return nil }
        switch term {
        case .iri, .blankNode:
            return term
        case .literal, .tripleTerm:
            return nil
        }
    }
}
