import DatabaseTypes

/// One durable checkpoint produced by a bounded placement-transfer scan.
package struct DatabaseBasePlacementTransferProgress: Sendable, Hashable {
    package let continuation: ByteString?
    package let digest: ByteString
    package let keyCount: UInt64
    package let byteCount: UInt64

    package var isComplete: Bool { continuation == nil }
}
