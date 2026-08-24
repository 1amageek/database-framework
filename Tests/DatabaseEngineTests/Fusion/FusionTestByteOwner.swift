import DatabaseTypes

final class FusionTestByteOwner: ByteStringOwner, @unchecked Sendable {
    let count: Int
    let retainedByteCount: Int?
    let isStorageSelfContained: Bool
    private let bytes: [UInt8]

    init(
        bytes: [UInt8],
        retainedByteCount: Int?,
        isStorageSelfContained: Bool = true
    ) {
        self.bytes = bytes
        self.count = bytes.count
        self.retainedByteCount = retainedByteCount
        self.isStorageSelfContained = isStorageSelfContained
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}
