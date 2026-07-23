import DatabaseEngine
import Foundation

extension PlanExplanation {
    /// Serializes this explanation for Foundation-based diagnostic tooling.
    public func serializedJSON() throws -> Data {
        let object: [String: Any] = [
            "estimatedCost": [
                "total": estimatedCost.totalCost,
                "indexReads": estimatedCost.indexReads,
                "recordFetches": estimatedCost.recordFetches,
                "postFilterCount": estimatedCost.postFilterCount,
                "requiresSort": estimatedCost.requiresSort,
            ],
            "usedIndexes": usedIndexes.map {
                ["name": $0.name, "kind": $0.kind]
            },
            "usedFields": Array(usedFields),
            "orderingSatisfied": orderingSatisfied,
            "operatorTree": operatorTree,
        ]
        return try JSONSerialization.data(
            withJSONObject: object,
            options: [.prettyPrinted]
        )
    }
}
