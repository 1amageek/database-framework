import DatabaseValue
import StorageKit

/// Keeps a StorageKit byte value alive while DatabaseWire borrows it.
struct RetainedStorageBytesOwner: DatabaseByteOwner {
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
