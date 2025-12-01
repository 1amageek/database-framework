// PlanExplanation.swift
// QueryPlanner - Human-readable plan explanation

import Foundation
import Core

/// Type-erased plan information for display purposes
///
/// This struct captures the essential information from a `QueryPlan<T>` in a
/// type-erased form suitable for logging, debugging, and display.
private struct ErasedPlanInfo: Sendable {
    let estimatedCost: PlanCost
    let usedIndexNames: [(name: String, kind: String)]
    let operatorTree: String
    let usedFields: Set<String>
    let orderingSatisfied: Bool
}

/// Human-readable explanation of a query plan
///
/// Uses type erasure internally to avoid exposing `QueryPlan<T>` generic parameter.
/// Create via `QueryPlanner.explain(query:)` or `QueryExecutor.explain()`.
public struct PlanExplanation: CustomStringConvertible, Sendable {

    private let info: ErasedPlanInfo

    /// Create explanation from a typed query plan
    public init<T: Persistable>(plan: QueryPlan<T>) {
        self.info = ErasedPlanInfo(
            estimatedCost: plan.estimatedCost,
            usedIndexNames: plan.usedIndexes.map { ($0.name, $0.kindIdentifier) },
            operatorTree: Self.buildOperatorTree(plan.rootOperator, indent: 2),
            usedFields: plan.usedFields,
            orderingSatisfied: plan.orderingSatisfied
        )
    }

    /// Estimated cost of the plan
    public var estimatedCost: PlanCost {
        info.estimatedCost
    }

    /// Indexes used by the plan
    public var usedIndexes: [(name: String, kind: String)] {
        info.usedIndexNames
    }

    /// Whether ordering is satisfied by the plan
    public var orderingSatisfied: Bool {
        info.orderingSatisfied
    }

    /// Fields referenced by the plan
    public var usedFields: Set<String> {
        info.usedFields
    }

    public var description: String {
        var lines: [String] = []
        lines.append("Query Plan:")
        lines.append("  Estimated Cost: \(String(format: "%.2f", info.estimatedCost.totalCost))")
        lines.append("  Index Reads: \(String(format: "%.1f", info.estimatedCost.indexReads))")
        lines.append("  Record Fetches: \(String(format: "%.1f", info.estimatedCost.recordFetches))")
        lines.append("  Post Filter: \(String(format: "%.1f", info.estimatedCost.postFilterCount))")
        lines.append("  Requires Sort: \(info.estimatedCost.requiresSort)")
        lines.append("  Ordering Satisfied: \(info.orderingSatisfied)")
        lines.append("")
        lines.append("Execution Tree:")
        lines.append(info.operatorTree)

        if !info.usedIndexNames.isEmpty {
            lines.append("")
            lines.append("Used Indexes:")
            for (name, kind) in info.usedIndexNames {
                lines.append("  - \(name) (\(kind))")
            }
        }

        if !info.usedFields.isEmpty {
            lines.append("")
            lines.append("Referenced Fields:")
            lines.append("  \(info.usedFields.sorted().joined(separator: ", "))")
        }

        return lines.joined(separator: "\n")
    }

    /// Build operator tree string recursively
    private static func buildOperatorTree<T: Persistable>(
        _ op: PlanOperator<T>,
        indent: Int
    ) -> String {
        var lines: [String] = []
        let prefix = String(repeating: " ", count: indent)

        switch op {
        case .tableScan(let scanOp):
            lines.append("\(prefix)-> TableScan")
            lines.append("\(prefix)   est. rows: \(scanOp.estimatedRows)")
            if scanOp.filterPredicate != nil {
                lines.append("\(prefix)   filter: applied")
            }

        case .indexScan(let scanOp):
            lines.append("\(prefix)-> IndexScan[\(scanOp.index.name)]")
            lines.append("\(prefix)   bounds: \(describeBounds(scanOp.bounds))")
            lines.append("\(prefix)   reverse: \(scanOp.reverse)")
            lines.append("\(prefix)   est. entries: \(scanOp.estimatedEntries)")
            if !scanOp.satisfiedConditions.isEmpty {
                let fields = scanOp.satisfiedConditions.map { $0.field.fieldName }
                lines.append("\(prefix)   satisfies: \(fields.joined(separator: ", "))")
            }

        case .indexSeek(let seekOp):
            lines.append("\(prefix)-> IndexSeek[\(seekOp.index.name)]")
            lines.append("\(prefix)   lookups: \(seekOp.seekValues.count)")
            if !seekOp.satisfiedConditions.isEmpty {
                let fields = seekOp.satisfiedConditions.map { $0.field.fieldName }
                lines.append("\(prefix)   satisfies: \(fields.joined(separator: ", "))")
            }

        case .union(let unionOp):
            lines.append("\(prefix)-> Union")
            lines.append("\(prefix)   deduplicate: \(unionOp.deduplicate)")
            lines.append("\(prefix)   children: \(unionOp.children.count)")
            lines.append("\(prefix)   NOTE: Output is UNORDERED")
            for (i, child) in unionOp.children.enumerated() {
                lines.append("\(prefix)   [\(i)]:")
                lines.append(buildOperatorTree(child, indent: indent + 6))
            }

        case .intersection(let intersectionOp):
            lines.append("\(prefix)-> Intersection")
            lines.append("\(prefix)   children: \(intersectionOp.children.count)")
            for (i, child) in intersectionOp.children.enumerated() {
                lines.append("\(prefix)   [\(i)]:")
                lines.append(buildOperatorTree(child, indent: indent + 6))
            }

        case .filter(let filterOp):
            lines.append("\(prefix)-> Filter")
            lines.append("\(prefix)   selectivity: \(String(format: "%.2f", filterOp.selectivity))")
            lines.append(buildOperatorTree(filterOp.input, indent: indent + 3))

        case .sort(let sortOp):
            let fields = sortOp.sortDescriptors.map { "\($0.fieldName) \($0.order)" }
            lines.append("\(prefix)-> Sort[\(fields.joined(separator: ", "))]")
            lines.append("\(prefix)   est. input: \(sortOp.estimatedInputSize)")
            lines.append(buildOperatorTree(sortOp.input, indent: indent + 3))

        case .limit(let limitOp):
            var limitDesc = "Limit["
            if let limit = limitOp.limit {
                limitDesc += "\(limit)"
            } else {
                limitDesc += "∞"
            }
            if let offset = limitOp.offset, offset > 0 {
                limitDesc += ", offset: \(offset)"
            }
            limitDesc += "]"
            lines.append("\(prefix)-> \(limitDesc)")
            lines.append(buildOperatorTree(limitOp.input, indent: indent + 3))

        case .project(let projectOp):
            lines.append("\(prefix)-> Project[\(projectOp.fields.count) fields]")
            lines.append(buildOperatorTree(projectOp.input, indent: indent + 3))

        case .fullTextScan(let ftOp):
            lines.append("\(prefix)-> FullTextScan[\(ftOp.index.name)]")
            lines.append("\(prefix)   terms: \(ftOp.searchTerms.joined(separator: ", "))")
            lines.append("\(prefix)   mode: \(ftOp.matchMode)")
            lines.append("\(prefix)   est. results: \(ftOp.estimatedResults)")

        case .vectorSearch(let vectorOp):
            lines.append("\(prefix)-> VectorSearch[\(vectorOp.index.name)]")
            lines.append("\(prefix)   k: \(vectorOp.k)")
            lines.append("\(prefix)   metric: \(vectorOp.distanceMetric)")
            if let efSearch = vectorOp.efSearch {
                lines.append("\(prefix)   ef_search: \(efSearch)")
            }

        case .spatialScan(let spatialOp):
            lines.append("\(prefix)-> SpatialScan[\(spatialOp.index.name)]")
            lines.append("\(prefix)   constraint: \(describeConstraint(spatialOp.constraint))")
            lines.append("\(prefix)   est. results: \(spatialOp.estimatedResults)")

        case .aggregation(let aggOp):
            lines.append("\(prefix)-> Aggregation[\(aggOp.index.name)]")
            lines.append("\(prefix)   type: \(aggOp.aggregationType)")
            if !aggOp.groupByFields.isEmpty {
                lines.append("\(prefix)   group by: \(aggOp.groupByFields.joined(separator: ", "))")
            }
        }

        return lines.joined(separator: "\n")
    }

    private static func describeBounds(_ bounds: IndexScanBounds) -> String {
        if bounds.isUnbounded {
            return "[unbounded]"
        }

        var parts: [String] = []

        if !bounds.start.isEmpty {
            let startDesc = bounds.start.map { component in
                if let value = component.value {
                    return component.inclusive ? "[\(value.value)]" : "(\(value.value))"
                }
                return "-∞"
            }.joined(separator: ", ")
            parts.append("start: \(startDesc)")
        }

        if !bounds.end.isEmpty {
            let endDesc = bounds.end.map { component in
                if let value = component.value {
                    return component.inclusive ? "[\(value.value)]" : "(\(value.value))"
                }
                return "+∞"
            }.joined(separator: ", ")
            parts.append("end: \(endDesc)")
        }

        return parts.isEmpty ? "[unbounded]" : parts.joined(separator: " .. ")
    }

    private static func describeConstraint(_ constraint: SpatialConstraint) -> String {
        switch constraint.type {
        case .withinDistance(let center, let radius):
            return "within \(radius)m of (\(center.latitude), \(center.longitude))"
        case .withinBounds(let minLat, let minLon, let maxLat, let maxLon):
            return "within bounds [\(minLat),\(minLon) to \(maxLat),\(maxLon)]"
        case .withinPolygon(let points):
            return "within polygon (\(points.count) points)"
        }
    }
}

// MARK: - JSON Export

extension PlanExplanation {
    /// Export plan explanation as JSON for tooling
    public func toJSON() throws -> Data {
        let dict: [String: Any] = [
            "estimatedCost": [
                "total": info.estimatedCost.totalCost,
                "indexReads": info.estimatedCost.indexReads,
                "recordFetches": info.estimatedCost.recordFetches,
                "postFilterCount": info.estimatedCost.postFilterCount,
                "requiresSort": info.estimatedCost.requiresSort
            ],
            "usedIndexes": info.usedIndexNames.map { ["name": $0.name, "kind": $0.kind] },
            "usedFields": Array(info.usedFields),
            "orderingSatisfied": info.orderingSatisfied,
            "operatorTree": info.operatorTree
        ]

        return try JSONSerialization.data(withJSONObject: dict, options: [.prettyPrinted])
    }
}
