#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import StorageKit
import Testing

@Suite("Canonical index entry decoder", .heartbeat)
struct IndexEntryDecoderTests {
    @Test("Generated record decoder restores the complete model")
    func completeRecordRoundTrip() throws {
        let record = try IndexProjectionRecordFactory.record()
        let descriptor = IndexProjectionRecordFactory.descriptor()
        let metadata = CoveringIndexMetadata.build(
            for: descriptor,
            type: IndexProjectionRecord.self
        )
        let bytes = try CoveringValueBuilder.build(
            for: record,
            index: IndexProjectionRecordFactory.runtimeIndex(from: descriptor)
        )
        let entry = IndexEntry(
            itemID: Tuple([record.id]),
            keyValues: Tuple([record.email]),
            coveringValue: bytes
        )

        let decoded = try IndexEntryDecoder<IndexProjectionRecord>(
            metadata: metadata
        ).decode(from: entry)

        #expect(decoded.id == record.id)
        #expect(decoded.email == record.email)
        #expect(decoded.name == record.name)
        #expect(decoded.age == record.age)
        #expect(decoded.nickname == record.nickname)
        #expect(decoded.tags == record.tags)
        #expect(decoded.target == record.target)
    }

    @Test("Partial metadata cannot construct a decoder")
    func partialMetadataFails() {
        let metadata = CoveringIndexMetadata.build(
            for: IndexProjectionRecordFactory.descriptor(storedFields: []),
            type: IndexProjectionRecord.self
        )
        #expect(throws: CanonicalIndexProjectionError.self) {
            _ = try IndexEntryDecoder<IndexProjectionRecord>(metadata: metadata)
        }
    }

    @Test("Corrupt DBIX never falls back to tuple reconstruction")
    func corruptProjectionFails() throws {
        let descriptor = IndexProjectionRecordFactory.descriptor()
        let metadata = CoveringIndexMetadata.build(
            for: descriptor,
            type: IndexProjectionRecord.self
        )
        let entry = IndexEntry(
            itemID: Tuple(["owner-1"]),
            keyValues: Tuple(["owner@example.com"]),
            coveringValue: [0, 0, 0, 0]
        )
        let decoder = try IndexEntryDecoder<IndexProjectionRecord>(
            metadata: metadata
        )

        #expect(throws: DatabaseRecordFrameError.invalidMagic) {
            _ = try decoder.decode(from: entry)
        }
    }
}
#endif
#endif
