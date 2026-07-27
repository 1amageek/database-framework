#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit
import Testing

@Suite("Canonical covering value builder", .heartbeat)
struct CanonicalCoveringValueBuilderTests {
    @Test("Stored fields retain null, arrays, and typed references")
    func storedFieldsRoundTrip() throws {
        let entity = try IndexProjectionEntityFactory.entity()
        let descriptor = try IndexProjectionEntityFactory.descriptor()
        let bytes = try CoveringValueBuilder.build(
            for: entity,
            index: IndexProjectionEntityFactory.runtimeIndex(from: descriptor)
        )
        let properties = try CoveringValueBuilder.decode(
            bytes,
            storedFieldNames: descriptor.storedFieldNames
        )

        #expect(Array(bytes.prefix(4)) == [0x44, 0x42, 0x49, 0x58])
        #expect(properties["name"] == .string(entity.name))
        #expect(properties["age"] == .int64(Int64(entity.age)))
        #expect(properties["nickname"] == .null)
        #expect(properties["tags"] == .array([
            .string("calendar"),
            .string("graph"),
        ]))
        #expect(properties["target"] == .reference(
            try #require(entity.target).identity
        ))
    }

    @Test("A non-covering index without stored fields has an empty value")
    func nonCoveringIndexHasNoProjection() throws {
        let bytes = try CoveringValueBuilder.build(
            for: IndexProjectionEntityFactory.entity(),
            index: IndexProjectionEntityFactory.runtimeIndex(
                from: try IndexProjectionEntityFactory.descriptor(
                    includesStoredFields: false
                )
            )
        )

        #expect(bytes.isEmpty)
    }

    @Test("A fully covering key-only index still writes canonical bytes")
    func keyOnlyIndexWritesProjection() throws {
        var entity = IndexProjectionKeyOnlyEntity(email: "key@example.com")
        entity.id = "key-1"
        let descriptor = try IndexDescriptor(
            name: "IndexProjectionKeyOnlyEntity_email",
            definition: .scalar,
            fields: [IndexProjectionKeyOnlyEntity.fields.email.ascending]
        )
        let bytes = try CoveringValueBuilder.build(
            for: entity,
            index: IndexProjectionEntityFactory.runtimeIndex(from: descriptor)
        )

        #expect(!bytes.isEmpty)
        #expect(Array(bytes.prefix(4)) == [0x44, 0x42, 0x49, 0x58])
    }

    @Test("Unknown stored fields fail before an index write")
    func unknownFieldFails() throws {
        let descriptor = try IndexProjectionEntityFactory.descriptor(
            includesStoredFields: false
        )
        #expect(throws: CanonicalIndexProjectionError.self) {
            _ = try CoveringValueBuilder.build(
                for: IndexProjectionEntityFactory.entity(),
                index: IndexProjectionEntityFactory.runtimeIndex(
                    from: descriptor,
                    storedFieldNames: ["unknown"]
                )
            )
        }
    }

    @Test("Truncated projections are rejected deterministically")
    func truncatedProjectionFails() throws {
        let entity = try IndexProjectionEntityFactory.entity()
        let descriptor = try IndexProjectionEntityFactory.descriptor()
        let bytes = try CoveringValueBuilder.build(
            for: entity,
            index: IndexProjectionEntityFactory.runtimeIndex(from: descriptor)
        )
        #expect(throws: StorageFrameError.self) {
            _ = try CoveringValueBuilder.decode(
                bytes.dropLast(),
                storedFieldNames: descriptor.storedFieldNames
            )
        }
    }

    @Test("Selected byte fields retain the frame allocation")
    func selectedBytesRetainFrameAllocation() throws {
        let limits = try testLimits(maximumFrameBytes: 131_072)
        let payload = ByteString.copying(count: 4_096) { buffer in
            buffer.initializeMemory(as: UInt8.self, repeating: 0xA5)
        }
        let frame = try PersistableFieldFrameCodec.encode(
            magic: [0x54, 0x45, 0x53, 0x54],
            version: 1,
            entity: "Projection",
            fields: [
                PersistableField(
                    number: 1,
                    name: "selected",
                    value: .bytes(payload)
                ),
            ],
            limits: limits
        )
        let decoded = try PersistableFieldFrameCodec.decodeSelected(
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

        let selectedRetainsFrameAllocation = frame.withUnsafeBytes { frameBuffer in
            selected.withUnsafeBytes { selectedBuffer in
                guard let frameBaseAddress = frameBuffer.baseAddress,
                      let selectedBaseAddress = selectedBuffer.baseAddress else {
                    return false
                }
                let frameStart = UInt(bitPattern: frameBaseAddress)
                let selectedStart = UInt(bitPattern: selectedBaseAddress)
                return selectedStart >= frameStart
                    && selectedStart + UInt(selectedBuffer.count)
                        <= frameStart + UInt(frameBuffer.count)
            }
        }
        #expect(selectedRetainsFrameAllocation)
        #expect(selected == payload)
    }

    @Test("Unselected value payloads are skipped without decoding")
    func unselectedPayloadIsNotDecoded() throws {
        let limits = try testLimits(maximumFrameBytes: 131_072)
        let ignoredPayload = ByteString.copying(count: 65_536) { buffer in
            buffer.initializeMemory(as: UInt8.self, repeating: 0xA5)
        }
        var frame = try PersistableFieldFrameCodec.encode(
            magic: [0x54, 0x45, 0x53, 0x54],
            version: 1,
            entity: "Projection",
            fields: [
                PersistableField(
                    number: 1,
                    name: "selected",
                    value: .string("calendar")
                ),
                PersistableField(
                    number: 2,
                    name: "ignored",
                    value: .bytes(ignoredPayload)
                ),
            ],
            limits: limits
        )
        let ignoredValueSignature: [UInt8] = [
            14,
            0x00, 0x00, 0x01, 0x00,
            0xA5, 0xA5, 0xA5, 0xA5,
        ]
        let ignoredTagOffset = try #require(
            firstOffset(of: ignoredValueSignature, in: frame)
        )
        frame[ignoredTagOffset] = 0xFE

        let selected = try PersistableFieldFrameCodec.decodeSelected(
            frame,
            magic: [0x54, 0x45, 0x53, 0x54],
            version: 1,
            selectedFieldNames: ["selected"],
            limits: limits
        )
        let expectedFields: [String: FieldValue] = [
            "selected": .string("calendar"),
        ]
        #expect(selected.fieldsByName == expectedFields)
        #expect(throws: StorageFrameError.self) {
            _ = try PersistableFieldFrameCodec.decode(
                frame,
                magic: [0x54, 0x45, 0x53, 0x54],
                version: 1,
                limits: limits
            )
        }
    }

    private func testLimits(
        maximumFrameBytes: Int
    ) throws -> StorageFrameLimits {
        try StorageFrameLimits(
            maximumFrameBytes: maximumFrameBytes,
            maximumStringBytes: maximumFrameBytes,
            maximumByteStringBytes: maximumFrameBytes,
            maximumCollectionCount: 1_000,
            maximumNestingDepth: 16
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
