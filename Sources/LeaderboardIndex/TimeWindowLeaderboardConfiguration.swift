import DatabaseKit

/// Validated execution configuration for one time-window leaderboard index.
struct TimeWindowLeaderboardConfiguration: Sendable, Equatable {
    let fieldNames: [String]
    let scoreFieldName: String
    let groupingFieldNames: [String]
    let window: LeaderboardWindowType
    let windowCount: Int

    init(metadata: IndexKindMetadata) throws {
        let definition = try IndexDefinition(metadata: metadata)
        guard case .timeWindowLeaderboard(let window, let windowCount) = definition,
              let scoreFieldName = metadata.fieldNames.last else {
            throw TimeWindowLeaderboardConfigurationError.invalidKind(
                metadata.identifier
            )
        }
        self.fieldNames = metadata.fieldNames
        self.scoreFieldName = scoreFieldName
        self.groupingFieldNames = Array(metadata.fieldNames.dropLast())
        self.window = window
        self.windowCount = windowCount
    }
}

enum TimeWindowLeaderboardConfigurationError: Error, Sendable, Equatable {
    case invalidKind(String)
}
