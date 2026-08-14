package func ensureDatabaseTaskIsActive() throws {
    guard !Task<Never, Never>.isCancelled else {
        throw CancellationError()
    }
}

@_spi(DatabaseExecution)
public func ensureDatabaseExecutionTaskIsActive() throws {
    try ensureDatabaseTaskIsActive()
}
