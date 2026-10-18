import DatabaseKit

/// Describes bound operators using the selectors consumed by ordinary execution.
struct SQLExplainPlanBuilder {
    let context: DatabaseContext
    let options: ReadExecutionContext
    private var meter: DatabaseWorkMeter { options.workMeter }

    func select(_ original: SelectQuery, plan: CanonicalBoundSelect) throws -> SQLExplainPlanNode {
        guard let source = plan.sourcePlan else {
            throw CanonicalReadError.unsupportedSelectQuery("EXPLAIN requires a bound source")
        }
        let query = source.query ?? original
        let claim = try admit(source, additionalNodes: plan.ctes.count + 8)
        var selected = try self.source(source, query: query)
        if plan.filter != nil {
            selected = node("Filter", children: [selected], claim: claim)
        }
        if canonicalQueryRequiresAggregation(query) {
            selected = node("Aggregate", children: [selected], claim: claim)
        }
        if plan.having != nil { selected = node("Having", children: [selected], claim: claim) }
        if canonicalQueryRequiresWindows(query) { selected = node("Window", children: [selected], claim: claim) }
        selected = node("Projection", outputs: plan.columns, children: [selected], claim: claim)
        if query.quantifier != .all { selected = node("Distinct", children: [selected], claim: claim) }
        if !plan.orderBy.isEmpty { selected = node("Sort", children: [selected], claim: claim) }
        if query.pagination != QueryPagination() { selected = node("Limit", children: [selected], claim: claim) }
        if !plan.ctes.isEmpty {
            var definitions: [SQLExplainPlanNode] = []
            definitions.reserveCapacity(plan.ctes.count + 1)
            for definition in plan.ctes {
                try meter.consume(at: .bindingCandidate)
                let child: SQLExplainPlanNode
                if let recursive = definition.recursive {
                    child = node("Recursive Union", children: [
                        try select(recursive.seedQuery, plan: recursive.seedPlan),
                        try select(recursive.termQuery, plan: recursive.termPlan)], claim: claim)
                } else {
                    child = try select(definition.definition.query, plan: definition.plan)
                }
                let inline = definition.recursive == nil && definition.definition.materialized == .notMaterialized && !definition.isVolatile
                definitions.append(node(inline ? "CTE Inline" : "CTE Materialize", details: [
                    .init(name: "Name", value: definition.definition.name)], children: [child], claim: claim))
            }
            definitions.append(selected)
            selected = node("With", children: definitions, claim: claim)
        }
        return SQLExplainPlanNode(operation: selected.operation, details: selected.details,
            outputs: plan.columns, children: selected.children, retainedPlan: plan, storageReservation: claim)
    }

    private func source(_ plan: CanonicalSourceBindingPlan, query: SelectQuery) throws -> SQLExplainPlanNode {
        let claim = try admit(plan)
        if plan.statementRelation != nil { return node("Statement Relation", claim: claim) }
        if plan.statementSnapshot != nil { return node("Statement Snapshot", claim: claim) }
        if let descriptor = plan.informationSchema {
            return node("Information Schema", details: [.init(name: "Relation", value: descriptor.rawValue)], claim: claim)
        }
        switch plan.source {
        case .table(let table):
            let details: [SQLExplainPlanDetail] = [
                .init(name: "Relation", value: table.table),
                .init(name: "Alias", value: table.effectiveName, verboseOnly: true)]
            if plan.recursiveSelf != nil { return node("Working Table", details: details, claim: claim) }
            if let nested = plan.viewQuery, let selected = plan.queryPlan {
                return node("View", details: details, children: [try select(nested, plan: selected)], claim: claim)
            }
            if plan.queryPlan != nil { return node("CTE Read", details: details, claim: claim) }
            if let resolution = plan.binding.sqlResolution,
               let index = try context.selectedSQLIndex(tableRef: table,
                    options: options.withSQLResolution(resolution), authorizationQuery: query) {
                return node("Index Scan", details: details + [.init(name: "Index", value: index.name)], claim: claim)
            }
            if case .index(let index) = query.accessPath {
                return node("Index Read", details: details + [.init(name: "Index", value: index.indexName)], claim: claim)
            }
            // The typed reader owns lifecycle-backed physical index selection.
            return node("Model Read", details: details, claim: claim)
        case .aliased(_, let name, _):
            guard let child = plan.children.first else { throw missingSource() }
            return node("Alias", details: [.init(name: "Name", value: name, verboseOnly: true)],
                children: [try source(child, query: query)], claim: claim)
        case .subquery(let query):
            guard let selected = plan.queryPlan else { throw missingSource() }
            return node("Subquery", children: [try select(query, plan: selected)], claim: claim)
        case .values:
            return node("Values", claim: claim)
        case .tableFunction:
            guard let function = plan.tableFunction else { throw missingSource() }
            let kind: String
            switch function.kind {
            case .array: kind = "Array"
            case .json: kind = "JSON"
            case .patternMatches: kind = "Pattern matches"
            case .patternSplit: kind = "Pattern split"
            case .series: kind = "Series"
            case .scalar: kind = "Scalar"
            }
            return node("Function", details: [.init(name: "Producer", value: kind)], outputs: function.columns, claim: claim)
        case .join(let clause):
            guard plan.children.count == 2 else { throw missingSource() }
            let hash = try context.selectedJoinHashKey(clause: clause, plan: plan, meter: meter) != nil
            let implicit = context.canonicalSourceIsImplicitlyLateral(clause.right)
                && ![.right, .full, .naturalRight, .naturalFull].contains(clause.type)
            let lateral = clause.type == .lateral || clause.type == .leftLateral || implicit
            let operation = hash && !lateral ? "Hash Join" : "Nested Loop Join"
            return node(operation, details: [.init(name: "Lateral", value: lateral ? "true" : "false")], children: [
                try source(plan.children[0], query: query), try source(plan.children[1], query: query)], claim: claim)
        case .setOperation(let operation, let quantifier, _, _):
            let name: String
            switch operation { case .union: name = "Union"; case .intersect: name = "Intersect"; case .except: name = "Except" }
            return node(name, details: [.init(name: "All", value: quantifier == .all ? "true" : "false")],
                children: try plan.children.map { try source($0, query: query) }, claim: claim)
        case .logical: return node("Logical Read", claim: claim)
        case .graphTable: return node("Graph Table", claim: claim)
        case .graphPattern, .namedGraph, .service:
            throw CanonicalReadError.unsupportedSource("EXPLAIN requires an ordinary SQL source")
        #if DATABASE_MULTI_BASE
        case .base: return node("Base Read", claim: claim)
        #endif
        }
    }

    private func admit(_ source: CanonicalSourceBindingPlan, additionalNodes: Int = 0) throws -> DatabaseIntermediateReservation {
        try meter.consume(at: .bindingCandidate)
        let nodes = try DatabaseIntermediateFootprint(bytes: 1_024).multiplied(by: UInt64(additionalNodes + 1))
        let schema = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: source.schema.layout.slots.count,
            element: QueryColumn.self)
        return try meter.reserveIntermediate(bytes: nodes.adding(schema).bytes, at: .bindingCandidate)
    }

    private func node(_ operation: String, details: [SQLExplainPlanDetail] = [], outputs: [QueryColumn] = [],
                      children: [SQLExplainPlanNode] = [], claim: DatabaseIntermediateReservation) -> SQLExplainPlanNode {
        SQLExplainPlanNode(operation: operation, details: details, outputs: outputs, children: children, storageReservation: claim)
    }

    private func missingSource() -> CanonicalReadError {
        .unsupportedSelectQuery("EXPLAIN source lost its bound execution metadata")
    }
}
