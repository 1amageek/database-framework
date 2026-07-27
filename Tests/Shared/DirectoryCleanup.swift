import StorageKit

/// Removes a test directory when it exists and propagates every storage failure.
public func ensureDirectoryRemoved(
    from engine: any StorageEngine,
    path: [String]
) async throws {
    guard try await engine.namespaceExists(path: path) else {
        return
    }

    try await engine.removeNamespace(path: path)
}
