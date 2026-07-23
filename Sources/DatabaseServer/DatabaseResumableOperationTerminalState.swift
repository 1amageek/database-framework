import DatabaseWire

public enum DatabaseResumableOperationTerminalState: Sendable, Hashable {
    case failed(DatabaseRemoteError)
    case cancelled
}
