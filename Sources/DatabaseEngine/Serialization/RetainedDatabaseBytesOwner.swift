import DatabaseValue
import StorageKitEmbeddedCore

/// Keeps a DatabaseWire byte value alive while StorageKit borrows it.
struct RetainedDatabaseBytesOwner: EmbeddedByteOwner {
    let bytes: DatabaseBytes

    var count: Int {
        bytes.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}
