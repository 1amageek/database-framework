import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Database Format Descriptor Tests")
struct DatabaseFormatDescriptorTests {
    private let goldenV1: ByteString = [
        0x44, 0x42, 0x46, 0x4D, 0x01, 0x00, 0x01, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x5F, 0x90,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x5F, 0x90,
        0x05, 0x55, 0x6B, 0x76
    ]

    @Test("Canonical v1 descriptor matches its golden vector")
    func canonicalGoldenVector() throws {
        let descriptor = DatabaseFormatDescriptor.v1(itemStorage: .v1)

        #expect(descriptor.serialize() == goldenV1)
        #expect(try DatabaseFormatDescriptor.deserialize(goldenV1) == descriptor)
    }

    @Test("Every truncated and trailing descriptor is rejected")
    func rejectsNonExactLengths() {
        for end in 0..<goldenV1.count {
            #expect(throws: DatabaseFormatDescriptorError.self) {
                _ = try DatabaseFormatDescriptor.deserialize(goldenV1[0..<end])
            }
        }
        #expect(throws: DatabaseFormatDescriptorError.self) {
            _ = try DatabaseFormatDescriptor.deserialize(
                goldenV1.appending(0x00)
            )
        }
    }

    @Test("Magic, descriptor version, and checksum corruption are distinct")
    func rejectsCorruptedFields() {
        let invalidMagic = replacingByte(
            in: goldenV1,
            at: 0,
            with: goldenV1[0] ^ 0xFF
        )
        #expect(throws: DatabaseFormatDescriptorError.invalidMagic) {
            _ = try DatabaseFormatDescriptor.deserialize(invalidMagic)
        }

        let invalidVersion = replacingByte(
            in: goldenV1,
            at: 4,
            with: 0x02
        )
        #expect(
            throws: DatabaseFormatDescriptorError
                .unsupportedDescriptorVersion(0x02)
        ) {
            _ = try DatabaseFormatDescriptor.deserialize(invalidVersion)
        }

        let invalidPayload = replacingByte(
            in: goldenV1,
            at: 20,
            with: goldenV1[20] ^ 0x01
        )
        #expect(throws: DatabaseFormatDescriptorError.self) {
            _ = try DatabaseFormatDescriptor.deserialize(invalidPayload)
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
