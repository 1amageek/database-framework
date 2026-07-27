import DatabaseEngine
import DatabaseTypes
import StorageKit
import StorageKitEmbeddedCore
import Synchronization
import Testing

@Suite("Database storage byte ownership")
struct DatabaseStorageByteOwnershipTests {
    @Test("Slices share storage across database and storage views")
    func slicesShareStorageAcrossDatabaseAndStorageViews() throws {
        let databaseSource = ByteString([0x10, 0x20, 0x30, 0x40])[1..<3]
        let storageView = Bytes(retaining: databaseSource)
        let databaseRoundTrip = ByteString(retaining: storageView)

        let sourceAddress = try address(of: databaseSource)
        let storageAddress = try address(of: storageView)
        let roundTripAddress = try address(of: databaseRoundTrip)
        let sourceBytes = databaseSource.copyBytes()
        let storageBytes = storageView.copyBytes()
        let roundTripBytes = databaseRoundTrip.copyBytes()

        #expect(
            sourceBytes == [0x20, 0x30],
            Comment(rawValue: "Source bytes: \(sourceBytes)")
        )
        #expect(
            storageBytes == [0x20, 0x30],
            Comment(rawValue: "Storage bytes: \(storageBytes)")
        )
        #expect(sourceAddress == storageAddress)
        #expect(storageAddress == roundTripAddress)
        #expect(
            roundTripBytes == [0x20, 0x30],
            Comment(rawValue: "Round-trip bytes: \(roundTripBytes)")
        )
        #expect(databaseRoundTrip == ByteString([0x20, 0x30]))
    }

    @Test("StorageKit retains an adopted database allocation")
    func storageRetainsDatabaseAllocation() throws {
        let probe = ReleaseProbe()
        var source: ByteString? = makeDatabaseOwnedPayload(probe: probe)
        var destination: Bytes? = Bytes(
            retaining: try #require(source)[1..<3]
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
        let databaseBytes = ByteString(retaining: owner)[1..<3]
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
        let databaseBytes = ByteString(retaining: storageBytes)

        #expect(try address(of: databaseBytes) == address(of: storageBytes))
        #expect(databaseBytes == [0x20, 0x30])
    }

    @Test("Database bytes retain an adopted StorageKit allocation")
    func databaseRetainsStorageAllocation() throws {
        let probe = ReleaseProbe()
        var source: Bytes? = Bytes(makeStorageOwnedPayload(probe: probe))
        var destination: ByteString? = ByteString(
            retaining: try #require(source)[1..<3]
        )

        source = nil
        #expect(probe.releaseCount == 0)
        #expect(destination == [0x20, 0x30])

        destination = nil
        #expect(probe.releaseCount == 1)
    }

    private func address(of bytes: ByteString) throws -> UInt {
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

    private func makeDatabaseOwnedPayload(probe: ReleaseProbe) -> ByteString {
        let pointer = allocatePayload()
        return ByteString(
            retaining: AllocatedByteStringOwner(
                unsafeAddress: UInt(bitPattern: pointer),
                count: 4,
                releaseProbe: probe
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

private struct DatabaseBorrowingByteOwner: ByteStringOwner {
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

private final class AllocatedByteStringOwner: ByteStringOwner {
    let unsafeAddress: UInt
    let count: Int
    let releaseProbe: ReleaseProbe

    init(
        unsafeAddress: UInt,
        count: Int,
        releaseProbe: ReleaseProbe
    ) {
        self.unsafeAddress = unsafeAddress
        self.count = count
        self.releaseProbe = releaseProbe
    }

    deinit {
        UnsafeMutableRawPointer(bitPattern: unsafeAddress)?.deallocate()
        releaseProbe.recordRelease()
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        let pointer = UnsafeRawPointer(bitPattern: unsafeAddress)
        try body(UnsafeRawBufferPointer(start: pointer, count: count))
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
