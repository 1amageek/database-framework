import DatabaseTypes

/// Domain-separated caller identity for deterministic graph-result nodes.
public struct GraphResultNodeNamespace: Sendable, Hashable {
    public static let byteCount = 32

    public let bytes: ByteString

    public init(_ bytes: ByteString) throws {
        guard bytes.count == Self.byteCount else {
            throw GraphResultNodeNamespaceError.invalidByteCount(
                actual: bytes.count,
                expected: Self.byteCount
            )
        }
        self.bytes = bytes
    }
}

public enum GraphResultNodeNamespaceError: Error, Sendable, Equatable {
    case invalidByteCount(actual: Int, expected: Int)
}
