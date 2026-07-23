#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import StorageKit
import Testing

@Suite("Canonical index entry decoder", .heartbeat)
struct IndexEntryDecoderTests {
    @Test("Generated entity decoder restores the complete model")
    func completeEntityRoundTrip() throws {
        let entity = try IndexProjectionEntityFactory.entity()
        let descriptor = IndexProjectionEntityFactory.descriptor()
        let metadata = CoveringIndexMetadata.build(
            for: descriptor,
            type: IndexProjectionEntity.self
        )
        let bytes = try CoveringValueBuilder.build(
            for: entity,
            index: IndexProjectionEntityFactory.runtimeIndex(from: descriptor)
        )
        let entry = IndexEntry(
            itemID: Tuple([entity.id]),
            keyValues: Tuple([entity.email]),
            coveringValue: bytes
        )

        let decoded = try IndexEntryDecoder<IndexProjectionEntity>(
            metadata: metadata
        ).decode(from: entry)

        #expect(decoded.id == entity.id)
        #expect(decoded.email == entity.email)
        #expect(decoded.name == entity.name)
        #expect(decoded.age == entity.age)
        #expect(decoded.nickname == entity.nickname)
        #expect(decoded.tags == entity.tags)
        #expect(decoded.target == entity.target)
    }

    @Test("Partial metadata cannot construct a decoder")
    func partialMetadataFails() {
        let metadata = CoveringIndexMetadata.build(
            for: IndexProjectionEntityFactory.descriptor(storedFields: []),
            type: IndexProjectionEntity.self
        )
        #expect(throws: CanonicalIndexProjectionError.self) {
            _ = try IndexEntryDecoder<IndexProjectionEntity>(metadata: metadata)
        }
    }

    @Test("Corrupt DBIX never falls back to tuple reconstruction")
    func corruptProjectionFails() throws {
        let descriptor = IndexProjectionEntityFactory.descriptor()
        let metadata = CoveringIndexMetadata.build(
            for: descriptor,
            type: IndexProjectionEntity.self
        )
        let entry = IndexEntry(
            itemID: Tuple(["owner-1"]),
            keyValues: Tuple(["owner@example.com"]),
            coveringValue: [0, 0, 0, 0]
        )
        let decoder = try IndexEntryDecoder<IndexProjectionEntity>(
            metadata: metadata
        )

        #expect(throws: PersistableFieldFrameError.invalidMagic) {
            _ = try decoder.decode(from: entry)
        }
    }
}
#endif
#endif
