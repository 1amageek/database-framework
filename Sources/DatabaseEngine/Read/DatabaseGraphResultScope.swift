import DatabaseTypes

/// Domain-separated caller identity for deterministic graph-result nodes.
public struct DatabaseGraphResultScope: Sendable, Hashable {
    public static let byteCount = 32

    public let bytes: ByteString

    public init(_ bytes: ByteString) throws {
        guard bytes.count == Self.byteCount else {
            throw DatabaseGraphResultScopeError.invalidByteCount(
                actual: bytes.count,
                expected: Self.byteCount
            )
        }
        self.bytes = bytes
    }
}

public enum DatabaseGraphResultScopeError: Error, Sendable, Equatable {
    case invalidByteCount(actual: Int, expected: Int)
}
