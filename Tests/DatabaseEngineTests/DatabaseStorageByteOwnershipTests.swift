import DatabaseEngine
import DatabaseValue
import StorageKit
import StorageKitEmbeddedCore
import Synchronization
import Testing

@Suite("Database storage byte ownership")
struct DatabaseStorageByteOwnershipTests {
    @Test("Slices share storage across database and storage views")
    func slicesShareStorageAcrossDatabaseAndStorageViews() throws {
        let databaseSource = DatabaseBytes([0x10, 0x20, 0x30, 0x40])
            .slice(1..<3)
        let storageView = Bytes(retaining: databaseSource)
        let databaseRoundTrip = DatabaseBytes(retaining: storageView)

        let sourceAddress = try address(of: databaseSource)
        let storageAddress = try address(of: storageView)
        let roundTripAddress = try address(of: databaseRoundTrip)

        #expect(sourceAddress == storageAddress)
        #expect(storageAddress == roundTripAddress)
        #expect(databaseRoundTrip == [0x20, 0x30])
    }

    @Test("StorageKit retains an adopted database allocation")
    func storageRetainsDatabaseAllocation() throws {
        let probe = ReleaseProbe()
        var source: DatabaseBytes? = makeDatabaseOwnedPayload(probe: probe)
        var destination: Bytes? = Bytes(
            retaining: try #require(source).slice(1..<3)
        )

        source = nil
        #expect(probe.releaseCount == 0)
        #expect(destination == [0x20, 0x30])

        destination = nil
        #expect(probe.releaseCount == 1)
    }

    @Test("External database owners share storage with storage bytes")
    func externalDatabaseOwnerSharesStorage() throws {
        let owner = DatabaseBorrowingByteOwner(
            bytes: [0x10, 0x20, 0x30, 0x40]
        )
        let databaseBytes = DatabaseBytes(retaining: owner).slice(1..<3)
        let storageBytes = Bytes(retaining: databaseBytes)

        #expect(try address(of: storageBytes) == address(of: databaseBytes))
        #expect(storageBytes == [0x20, 0x30])
    }

    @Test("External storage owners share storage with database bytes")
    func externalStorageOwnerSharesStorage() throws {
        let owner = StorageBorrowingByteOwner(
            bytes: [0x10, 0x20, 0x30, 0x40]
        )
        let storageBytes = Bytes(retaining: owner)[1..<3]
        let databaseBytes = DatabaseBytes(retaining: storageBytes)

        #expect(try address(of: databaseBytes) == address(of: storageBytes))
        #expect(databaseBytes == [0x20, 0x30])
    }

    @Test("Database bytes retain an adopted StorageKit allocation")
    func databaseRetainsStorageAllocation() throws {
        let probe = ReleaseProbe()
        var source: Bytes? = Bytes(makeStorageOwnedPayload(probe: probe))
        var destination: DatabaseBytes? = DatabaseBytes(
            retaining: try #require(source)[1..<3]
        )

        source = nil
        #expect(probe.releaseCount == 0)
        #expect(destination == [0x20, 0x30])

        destination = nil
        #expect(probe.releaseCount == 1)
    }

    private func address(of bytes: DatabaseBytes) throws -> UInt {
        try #require(
            bytes.withUnsafeBytes { buffer in
                buffer.baseAddress.map(UInt.init(bitPattern:))
            }
        )
    }

    private func address(of bytes: Bytes) throws -> UInt {
        try #require(
            bytes.withUnsafeBytes { buffer in
                buffer.baseAddress.map(UInt.init(bitPattern:))
            }
        )
    }

    private func makeDatabaseOwnedPayload(probe: ReleaseProbe) -> DatabaseBytes {
        let pointer = allocatePayload()
        return DatabaseBytes(
            allocation: DatabaseByteAllocation(
                unsafeAddress: UInt(bitPattern: pointer),
                count: 4,
                deallocator: { address, _ in
                    UnsafeMutableRawPointer(bitPattern: address)?.deallocate()
                    probe.recordRelease()
                }
            )
        )
    }

    private func makeStorageOwnedPayload(probe: ReleaseProbe) -> EmbeddedBytes {
        let pointer = allocatePayload()
        return EmbeddedBytes(
            allocation: EmbeddedByteAllocation(
                unsafeAddress: UInt(bitPattern: pointer),
                count: 4,
                deallocator: { address, _ in
                    UnsafeMutableRawPointer(bitPattern: address)?.deallocate()
                    probe.recordRelease()
                }
            )
        )
    }

    private func allocatePayload() -> UnsafeMutableRawPointer {
        let pointer = UnsafeMutableRawPointer.allocate(
            byteCount: 4,
            alignment: MemoryLayout<UInt8>.alignment
        )
        pointer.storeBytes(of: UInt8(0x10), toByteOffset: 0, as: UInt8.self)
        pointer.storeBytes(of: UInt8(0x20), toByteOffset: 1, as: UInt8.self)
        pointer.storeBytes(of: UInt8(0x30), toByteOffset: 2, as: UInt8.self)
        pointer.storeBytes(of: UInt8(0x40), toByteOffset: 3, as: UInt8.self)
        return pointer
    }

    private final class ReleaseProbe: Sendable {
        private let count = Mutex(0)

        var releaseCount: Int {
            count.withLock { $0 }
        }

        func recordRelease() {
            count.withLock { $0 += 1 }
        }
    }
}

private struct DatabaseBorrowingByteOwner: DatabaseByteOwner {
    let bytes: [UInt8]

    var count: Int {
        bytes.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private struct StorageBorrowingByteOwner: BytesOwner {
    let bytes: [UInt8]

    var count: Int {
        bytes.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}
