@_spi(DatabaseExecution)
public struct SQLRelationCleanupError: Error, Sendable {
    public let operationError: any Error
    public let cleanupError: any Error
}

