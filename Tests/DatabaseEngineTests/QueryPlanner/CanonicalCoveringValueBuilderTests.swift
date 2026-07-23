#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import StorageKit
import Testing

@Suite("Canonical covering value builder", .heartbeat)
struct CanonicalCoveringValueBuilderTests {
    @Test("Stored fields retain null, arrays, and typed references")
    func storedFieldsRoundTrip() throws {
        let record = try IndexProjectionRecordFactory.record()
        let descriptor = IndexProjectionRecordFactory.descriptor()
        let bytes = try CoveringValueBuilder.build(
            for: record,
            index: IndexProjectionRecordFactory.runtimeIndex(from: descriptor)
        )
        let properties = try CoveringValueBuilder.decode(
            bytes,
            storedFieldNames: descriptor.storedFieldNames
        )

        #expect(Array(bytes.prefix(4)) == [0x44, 0x42, 0x49, 0x58])
        #expect(properties["name"] == .string(record.name))
        #expect(properties["age"] == .int64(Int64(record.age)))
        #expect(properties["nickname"] == .null)
        #expect(properties["tags"] == .array([
            .string("calendar"),
            .string("graph"),
        ]))
        #expect(properties["target"] == .reference(
            try #require(record.target).identity
        ))
    }

    @Test("A non-covering index without stored fields has an empty value")
    func nonCoveringIndexHasNoProjection() throws {
        let bytes = try CoveringValueBuilder.build(
            for: IndexProjectionRecordFactory.record(),
            index: IndexProjectionRecordFactory.runtimeIndex(
                from: IndexProjectionRecordFactory.descriptor(storedFields: [])
            )
        )

        #expect(bytes.isEmpty)
    }

    @Test("A fully covering key-only index still writes canonical bytes")
    func keyOnlyIndexWritesProjection() throws {
        var record = IndexProjectionKeyOnlyRecord(email: "key@example.com")
        record.id = "key-1"
        let descriptor = IndexDescriptor(
            name: "IndexProjectionKeyOnlyRecord_email",
            keyPaths: [\IndexProjectionKeyOnlyRecord.email],
            kind: ScalarIndexKind<IndexProjectionKeyOnlyRecord>(fields: [\.email])
        )
        let bytes = try CoveringValueBuilder.build(
            for: record,
            index: IndexProjectionRecordFactory.runtimeIndex(from: descriptor)
        )

        #expect(!bytes.isEmpty)
        #expect(Array(bytes.prefix(4)) == [0x44, 0x42, 0x49, 0x58])
    }

    @Test("Unknown stored fields fail before an index write")
    func unknownFieldFails() throws {
        let descriptor = IndexProjectionRecordFactory.descriptor(
            storedFields: ["unknown"]
        )
        #expect(throws: CanonicalIndexProjectionError.self) {
            _ = try CoveringValueBuilder.build(
                for: IndexProjectionRecordFactory.record(),
                index: IndexProjectionRecordFactory.runtimeIndex(from: descriptor)
            )
        }
    }

    @Test("Truncated projections are rejected deterministically")
    func truncatedProjectionFails() throws {
        let record = try IndexProjectionRecordFactory.record()
        let descriptor = IndexProjectionRecordFactory.descriptor()
        let bytes = try CoveringValueBuilder.build(
            for: record,
            index: IndexProjectionRecordFactory.runtimeIndex(from: descriptor)
        )
        #expect(throws: DatabaseWireError.self) {
            _ = try CoveringValueBuilder.decode(
                bytes.dropLast(),
                storedFieldNames: descriptor.storedFieldNames
            )
        }
    }

    @Test("Selected byte fields retain the frame allocation")
    func selectedBytesRetainFrameAllocation() throws {
        let limits = try testLimits(maximumFrameBytes: 131_072)
        let payload = DatabaseBytes.copying(count: 4_096) { buffer in
            buffer.initializeMemory(as: UInt8.self, repeating: 0xA5)
        }
        let frame = try DatabaseRecordFieldFrameCodec.encode(
            magic: [0x54, 0x45, 0x53, 0x54],
            version: 1,
            entity: "Projection",
            fields: [
                DatabaseRecordField(
                    number: 1,
                    name: "selected",
                    value: .bytes(payload)
                ),
            ],
            limits: limits
        )
        let decoded = try DatabaseRecordFieldFrameCodec.decodeSelected(
            frame,
            magic: [0x54, 0x45, 0x53, 0x54],
            version: 1,
            selectedFieldNames: ["selected"],
            limits: limits
        )
        guard case .bytes(let selected)? = decoded.fieldsByName["selected"] else {
            Issue.record("Expected a selected byte value")
            return
        }

        frame.withUnsafeBytes { frameBuffer in
            selected.withUnsafeBytes { selectedBuffer in
                let frameStart = frameBuffer.baseAddress.map(UInt.init(bitPattern:))
                let selectedStart = selectedBuffer.baseAddress.map(UInt.init(bitPattern:))
                #expect(frameStart != nil)
                #expect(selectedStart != nil)
                if let frameStart, let selectedStart {
                    #expect(selectedStart >= frameStart)
                    #expect(
                        selectedStart + UInt(selectedBuffer.count)
                            <= frameStart + UInt(frameBuffer.count)
                    )
                }
            }
        }
        #expect(selected == payload)
    }

    @Test("Unselected value payloads are skipped without decoding")
    func unselectedPayloadIsNotDecoded() throws {
        let limits = try testLimits(maximumFrameBytes: 131_072)
        let ignoredPayload = DatabaseBytes.copying(count: 65_536) { buffer in
            buffer.initializeMemory(as: UInt8.self, repeating: 0xA5)
        }
        var frame = try DatabaseRecordFieldFrameCodec.encode(
            magic: [0x54, 0x45, 0x53, 0x54],
            version: 1,
            entity: "Projection",
            fields: [
                DatabaseRecordField(
                    number: 1,
                    name: "selected",
                    value: .string("calendar")
                ),
                DatabaseRecordField(
                    number: 2,
                    name: "ignored",
                    value: .bytes(ignoredPayload)
                ),
            ],
            limits: limits
        )
        let ignoredValueSignature: [UInt8] = [
            7,
            0x00, 0x00, 0x01, 0x00,
            0xA5, 0xA5, 0xA5, 0xA5,
        ]
        let ignoredTagOffset = try #require(
            firstOffset(of: ignoredValueSignature, in: frame)
        )
        frame[ignoredTagOffset] = 0xFE

        let selected = try DatabaseRecordFieldFrameCodec.decodeSelected(
            frame,
            magic: [0x54, 0x45, 0x53, 0x54],
            version: 1,
            selectedFieldNames: ["selected"],
            limits: limits
        )
        #expect(selected.fieldsByName == ["selected": .string("calendar")])
        #expect(throws: DatabaseWireError.self) {
            _ = try DatabaseRecordFieldFrameCodec.decode(
                frame,
                magic: [0x54, 0x45, 0x53, 0x54],
                version: 1,
                limits: limits
            )
        }
    }

    private func testLimits(
        maximumFrameBytes: Int
    ) throws -> DatabaseWireLimits {
        try DatabaseWireLimits(
            maximumFrameBytes: maximumFrameBytes,
            maximumStringBytes: maximumFrameBytes,
            maximumByteStringBytes: maximumFrameBytes,
            maximumCollectionCount: 1_000,
            maximumNestingDepth: 16,
            maximumObjectCount: 10_000
        )
    }

    private func firstOffset(
        of signature: [UInt8],
        in bytes: Bytes
    ) -> Int? {
        guard !signature.isEmpty, signature.count <= bytes.count else {
            return nil
        }
        let finalStart = bytes.count - signature.count
        for start in 0...finalStart {
            var matches = true
            for offset in signature.indices where bytes[start + offset] != signature[offset] {
                matches = false
                break
            }
            if matches { return start }
        }
        return nil
    }
}
#endif
#endif
