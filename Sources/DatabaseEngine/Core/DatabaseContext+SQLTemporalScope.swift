import DatabaseTypes

package struct SQLTemporalStatementBinding: Sendable {
    let container: DBContainer
    let environment: SQLTemporalEnvironment
}

package enum ActiveSQLTemporalStatement {
    @TaskLocal package static var binding: SQLTemporalStatementBinding?
}

extension DatabaseContext {
    package var inheritedSQLTemporalEnvironment: SQLTemporalEnvironment? {
        guard let binding = ActiveSQLTemporalStatement.binding,
              binding.container === container else { return nil }
        return binding.environment
    }

    /// Captures a public statement boundary without acquiring data authority.
    @_spi(DatabaseExecution)
    public func makeSQLTemporalEnvironment() -> SQLTemporalEnvironment {
        let statementInstant = container.wallClock.now
        let transactionInstant: Timestamp
        if let binding = ActiveDatabaseTransactionContext.binding,
           binding.container === container {
            transactionInstant = binding.transactionInstant
        } else {
            transactionInstant = statementInstant
        }
        return SQLTemporalEnvironment(
            transactionInstant: transactionInstant,
            statementInstant: statementInstant,
            wallClock: container.wallClock,
            configuration: container.configuration.sqlTemporal
        )
    }

    /// Internal reentry retains the explicitly admitted statement environment.
    @_spi(DatabaseExecution)
    public func withSQLTemporalStatement<Result: ~Copyable & Sendable>(
        environment: SQLTemporalEnvironment,
        _ operation: @Sendable () async throws -> sending Result
    ) async rethrows -> sending Result {
        let binding = SQLTemporalStatementBinding(container: container, environment: environment)
        let result = try await ActiveSQLTemporalStatement.$binding.withValue(binding) {
            let value = try await operation()
            return DatabaseReadResultBox(consume value)
        }
        return result.take()
    }
}
