#if DATABASE_MULTIPLE_BASES
import DatabaseTypes

/// One durable checkpoint produced by a bounded placement-transfer scan.
@_spi(DatabaseExecution)
public struct DatabaseBasePlacementTransferProgress: Sendable, Hashable {
    public let continuation: ByteString?
    public let digest: ByteString
    public let keyCount: UInt64
    public let byteCount: UInt64

    public var isComplete: Bool { continuation == nil }
}

#endif
