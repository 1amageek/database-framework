import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Database Format Descriptor Tests")
struct DatabaseFormatDescriptorTests {
    private let golden: ByteString = [
        0x44, 0x42, 0x46, 0x4D, 0x02, 0x01, 0x00, 0x01,
        0x00, 0x01, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x5F, 0x90,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x5F, 0x90,
        0x5C, 0xE4, 0x6A, 0xDB,
    ]

    @Test("The current single-database descriptor matches its golden vector")
    func canonicalGoldenVector() throws {
        let descriptor = DatabaseFormatDescriptor.current(
            layoutKind: .singleDatabase,
            itemStorage: .v1
        )

        #expect(descriptor.serialize() == golden)
        #expect(try DatabaseFormatDescriptor.deserialize(golden) == descriptor)
        #expect(descriptor.layoutVersion == 1)
    }

    @Test("A rebased retained view decodes without copying")
    func retainedSlice() throws {
        let descriptor = DatabaseFormatDescriptor.current(
            layoutKind: .multiBase,
            itemStorage: .v1
        )
        let framed = ByteString([0xFF])
            .appending(contentsOf: descriptor.serialize())
            .appending(0xEE)
        let slice = framed[1..<(1 + DatabaseFormatDescriptor.serializedSize)]

        #expect(slice.startIndex == 0)
        let framedAddress = framed.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        let sliceAddress = slice.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        #expect(sliceAddress == framedAddress.map { $0 + 1 })
        #expect(try DatabaseFormatDescriptor.deserialize(slice) == descriptor)
    }

    @Test("Every truncated and trailing descriptor is rejected")
    func rejectsNonExactLengths() {
        for end in 0..<golden.count {
            #expect(throws: DatabaseFormatDescriptorError.self) {
                _ = try DatabaseFormatDescriptor.deserialize(golden[0..<end])
            }
        }
        #expect(throws: DatabaseFormatDescriptorError.self) {
            _ = try DatabaseFormatDescriptor.deserialize(
                golden.appending(0x00)
            )
        }
    }

    @Test("Magic, descriptor version, and checksum corruption are distinct")
    func rejectsCorruptedFields() {
        let invalidMagic = replacingByte(
            in: golden,
            at: 0,
            with: golden[0] ^ 0xFF
        )
        #expect(throws: DatabaseFormatDescriptorError.invalidMagic) {
            _ = try DatabaseFormatDescriptor.deserialize(invalidMagic)
        }

        let invalidVersion = replacingByte(
            in: golden,
            at: 4,
            with: 0x03
        )
        #expect(
            throws: DatabaseFormatDescriptorError
                .unsupportedDescriptorVersion(0x03)
        ) {
            _ = try DatabaseFormatDescriptor.deserialize(invalidVersion)
        }

        let invalidPayload = replacingByte(
            in: golden,
            at: 20,
            with: golden[20] ^ 0x01
        )
        #expect(throws: DatabaseFormatDescriptorError.self) {
            _ = try DatabaseFormatDescriptor.deserialize(invalidPayload)
        }
    }

    @Test("A descriptor from the removed layout is explicitly unsupported")
    func rejectsRemovedDescriptorVersion() {
        let removedDescriptor: ByteString = [
            0x44, 0x42, 0x46, 0x4D, 0x01,
        ]
        #expect(
            throws: DatabaseFormatDescriptorError
                .unsupportedDescriptorVersion(0x01)
        ) {
            _ = try DatabaseFormatDescriptor.deserialize(removedDescriptor)
        }
    }

    private func replacingByte(
        in source: ByteString,
        at index: Int,
        with replacement: UInt8
    ) -> ByteString {
        ByteString.copying(count: source.count) { destination in
            source.withUnsafeBytes { bytes in
                destination.copyMemory(from: bytes)
            }
            destination[index] = replacement
        }
    }
}
