import DatabaseTypes

final class FusionTestByteOwner: ByteStringOwner, @unchecked Sendable {
    let count: Int
    let retainedByteCount: Int?
    let isStorageSelfContained: Bool
    private let bytes: [UInt8]
    private let onBorrow: (@Sendable () -> Void)?

    init(
        bytes: [UInt8],
        retainedByteCount: Int?,
        isStorageSelfContained: Bool = true,
        onBorrow: (@Sendable () -> Void)? = nil
    ) {
        self.bytes = bytes
        self.count = bytes.count
        self.retainedByteCount = retainedByteCount
        self.isStorageSelfContained = isStorageSelfContained
        self.onBorrow = onBorrow
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        onBorrow?()
        try bytes.withUnsafeBytes(body)
    }
}
