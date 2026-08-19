import DatabaseKit

/// Validated execution configuration for one time-window leaderboard index.
struct TimeWindowLeaderboardConfiguration: Sendable, Equatable {
    let fieldNames: [String]
    let scoreFieldName: String
    let groupingFieldNames: [String]
    let window: LeaderboardWindowType
    let windowCount: Int

    init(definition: IndexDefinition<FieldIdentity>) throws {
        guard
            case .leaderboard(
                let groupBy,
                let score,
                let window, let windowCount) = definition
        else {
            throw TimeWindowLeaderboardConfigurationError.invalidDefinition(
                definition.type
            )
        }
        self.groupingFieldNames = groupBy.map { $0.field.name }
        self.scoreFieldName = score.name
        self.fieldNames = groupingFieldNames + [score.name]
        self.window = window
        self.windowCount = windowCount
    }
}

enum TimeWindowLeaderboardConfigurationError: Error, Sendable, Equatable {
    case invalidDefinition(IndexType)
}
