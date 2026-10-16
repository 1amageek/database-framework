import DatabaseTypes

/// Immutable clock and zone capabilities for one admitted SQL statement.
public struct SQLTemporalEnvironment: Sendable {
    public let transactionInstant: Timestamp
    public let statementInstant: Timestamp
    public let wallClock: any WallClock
    public let configuration: SQLTemporalConfiguration

    public init(
        transactionInstant: Timestamp,
        statementInstant: Timestamp,
        wallClock: any WallClock,
        configuration: SQLTemporalConfiguration = .utc
    ) {
        self.transactionInstant = transactionInstant
        self.statementInstant = statementInstant
        self.wallClock = wallClock
        self.configuration = configuration
    }
}
