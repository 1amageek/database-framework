import DatabaseTypes
import StorageKit

/// Keeps a StorageKit byte value alive while DatabaseTypes borrows it.
struct RetainedStorageBytesOwner: ByteStringOwner {
    let bytes: Bytes

    var count: Int {
        bytes.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}
