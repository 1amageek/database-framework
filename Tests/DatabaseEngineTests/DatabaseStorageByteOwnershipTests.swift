import DatabaseEngine
import DatabaseTypes
import StorageKit
import Synchronization
import Testing

@Suite("Database storage byte ownership")
struct DatabaseStorageByteOwnershipTests {
    @Test("Database validation and storage preserve one canonical byte owner")
    func canonicalByteOwnerSurvivesStorageRoundTrip() async throws {
        let probe = ReleaseProbe()

        try await exerciseStorageRoundTrip(releaseProbe: probe)

        #expect(probe.releaseCount == 1)
    }

    private func exerciseStorageRoundTrip(
        releaseProbe: ReleaseProbe
    ) async throws {
        let source = ByteString(
            retaining: AllocatedByteStringOwner(
                unsafeAddress: UInt(bitPattern: allocatePayload()),
                count: 4,
                releaseProbe: releaseProbe
            )
        )
        let slice = source[1..<3]
        let sourceAddress = try address(of: slice)
        let validated = try validatedValue(slice)

        #expect(try address(of: validated) == sourceAddress)

        let engine = InMemoryEngine()
        let key = ByteString([0x01])
        try await engine.withTransaction { transaction in
            try transaction.setValue(validated, for: key)
        }

        let stored = try await engine.withTransaction { transaction in
            try await transaction.getValue(for: key)
        }
        let requiredStored = try #require(stored)

        #expect(requiredStored == ByteString([0x20, 0x30]))
        #expect(try address(of: requiredStored) == sourceAddress)
        #expect(releaseProbe.releaseCount == 0)
    }

    private func address(of bytes: ByteString) throws -> UInt {
        try #require(
            bytes.withUnsafeBytes { buffer in
                buffer.baseAddress.map(UInt.init(bitPattern:))
            }
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
