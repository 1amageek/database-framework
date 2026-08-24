import DatabaseKit
import DatabaseTypes

/// Immutable time-window leaderboard input for a canonical Fusion plan.
public struct Leaderboard<Item: Persistable>: FusionQueryInput, Sendable {
    private let scoreField: FieldIdentity
    private var groupingValues: [FieldValue]?
    private var indexName: String?
    private var windowID: Int64?
    private var resultLimit: UInt64 = 100

    public init(_ scoreField: Field<Item, Int64>) {
        self.scoreField = scoreField.identity
    }

    public func index(named name: String) -> Self {
        var copy = self
        copy.indexName = name
        return copy
    }

    public func group<Value: FieldValueRepresentable>(_ value: Value) -> Self {
        group(by: [value.fieldValue])
    }

    public func group(by values: [FieldValue]) -> Self {
        var copy = self
        copy.groupingValues = values
        return copy
    }

    public func window(_ windowID: Int64) -> Self {
        var copy = self
        copy.windowID = windowID
        return copy
    }

    public func top(_ count: UInt64) -> Self {
        var copy = self
        copy.resultLimit = count
        return copy
    }

    public var fusionInput: FusionInput {
        var parameters: [String: FieldValue] = [
            LeaderboardFusionReadParameter.scoreField: .string(scoreField.name),
        ]
        if let groupingValues {
            parameters[LeaderboardFusionReadParameter.grouping] =
                .array(groupingValues)
        }
        if let windowID {
            parameters[LeaderboardFusionReadParameter.windowID] = .int64(windowID)
        }
        let selection: FusionIndexSelection = if let indexName {
            .named(name: indexName, type: .leaderboard)
        } else {
            .matching(
                type: .leaderboard,
                fields: [scoreField],
                fieldMatch: .contains
            )
        }
        return FusionInput(
            operation: .index(
                FusionIndexSource(
                    selection: selection,
                    referencedFields: [scoreField],
                    parameters: parameters
                )
            ),
            scoring: .annotation(name: "score", order: .higherIsBetter),
            limit: resultLimit
        )
    }
}
