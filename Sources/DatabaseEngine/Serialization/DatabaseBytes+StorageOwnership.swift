import DatabaseValue
import StorageKit
import StorageKitEmbeddedCore

public extension DatabaseBytes {
    /// Retains StorageKit ownership while exposing the same allocation.
    init(retaining bytes: Bytes) {
        switch bytes.embeddedBytes.sharedStorage {
        case .array(let storage, let range):
            self.init(sharing: storage, storageRange: range)
        case .allocation(let allocation, let range):
            guard !range.isEmpty else {
                self.init([])
                return
            }
            let (address, overflow) = allocation.unsafeAddress
                .addingReportingOverflow(UInt(range.lowerBound))
            precondition(!overflow)
            self.init(
                allocation: DatabaseByteAllocation(
                    unsafeAddress: address,
                    count: range.count,
                    deallocator: { _, _ in
                        withExtendedLifetime(allocation) {}
                    }
                )
            )
        case .owner:
            self.init(retaining: RetainedStorageBytesOwner(bytes: bytes))
        }
    }
}

public extension Bytes {
    /// Retains database wire ownership while exposing the same allocation.
    init(retaining bytes: DatabaseBytes) {
        switch bytes.sharedStorage {
        case .array(let storage, let range):
            self.init(
                EmbeddedBytes(sharing: storage, storageRange: range)
            )
        case .allocation(let allocation, let range):
            guard !range.isEmpty else {
                self.init()
                return
            }
            let (address, overflow) = allocation.unsafeAddress
                .addingReportingOverflow(UInt(range.lowerBound))
            precondition(!overflow)
            self.init(
                EmbeddedBytes(
                    allocation: EmbeddedByteAllocation(
                        unsafeAddress: address,
                        count: range.count,
                        deallocator: { _, _ in
                            withExtendedLifetime(allocation) {}
                        }
                    )
                )
            )
        case .owner:
            self.init(
                EmbeddedBytes(
                    retaining: RetainedDatabaseBytesOwner(bytes: bytes)
                )
            )
        }
    }
}
