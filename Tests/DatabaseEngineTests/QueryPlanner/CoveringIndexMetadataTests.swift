#if !os(WASI)
#if FOUNDATION_DB
import DatabaseEngine
import Testing

@Suite("Covering index metadata", .heartbeat)
struct CoveringIndexMetadataTests {
    @Test("Complete compiled fields are required")
    func completeCoverage() {
        let full = CoveringIndexMetadata.build(
            for: IndexProjectionEntityFactory.descriptor(),
            type: IndexProjectionEntity.self
        )
        let partial = CoveringIndexMetadata.build(
            for: IndexProjectionEntityFactory.descriptor(storedFields: []),
            type: IndexProjectionEntity.self
        )

        #expect(full.isFullyCovering)
        #expect(partial.isFullyCovering == false)
        #expect(partial.allFields == ["id", "email"])
    }
}
#endif
#endif
