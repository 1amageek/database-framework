import Testing

@testable import DatabaseEngine

@Suite("Database store cache")
struct DatabaseStoreCacheTests {
    @Test("lookup remains correct across sorted insertion and replacement")
    func insertionAndReplacement() throws {
        let alpha = key(entity: "Document", components: ["alpha"])
        let nested = key(
            entity: "Document",
            components: ["alpha", "nested"]
        )
        let omega = key(entity: "Document", components: ["omega"])
        var cache = DatabaseStoreCache<String>()

        cache.insert("omega", for: omega)
        cache.insert("alpha", for: alpha)
        cache.insert("nested", for: nested)
        cache.insert("replacement", for: alpha)

        #expect(cache.count == 3)
        #expect(cache.value(for: alpha) == "replacement")
        #expect(cache.value(for: nested) == "nested")
        #expect(cache.value(for: omega) == "omega")
        #expect(
            cache.value(
                for: key(
                    entity: "Missing",
                    components: []
                )
            ) == nil
        )
    }

    private func key(
        entity: String,
        components: [String]
    ) -> DatabaseStoreCacheKey {
        #if MultipleBases
        DatabaseStoreCacheKey(
            basePlacementGeneration: 1,
            entity: entity,
            components: components
        )
        #else
        DatabaseStoreCacheKey(entity: entity, components: components)
        #endif
    }
}
