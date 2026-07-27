import DatabaseTypes
import StorageKit

/// Keeps a canonical byte string alive while StorageKit borrows it.
struct RetainedByteStringOwner: BytesOwner {
    let bytes: ByteString

    var count: Int {
        bytes.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}
