import DatabaseTypes
import StorageKit

public extension ByteString {
    /// Retains StorageKit ownership while exposing the same allocation.
    init(retaining bytes: Bytes) {
        self.init(retaining: RetainedStorageBytesOwner(bytes: bytes))
    }
}

public extension Bytes {
    /// Retains canonical field-byte ownership while exposing the same allocation.
    init(retaining bytes: ByteString) {
        self.init(retaining: RetainedByteStringOwner(bytes: bytes))
    }
}
