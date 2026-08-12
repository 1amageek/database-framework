import DatabaseTypes
import Testing

@testable import DatabaseOperations
@testable import DatabaseMutationOperations

@Suite("Resolved entity map")
struct ResolvedEntityMapTests {
    @Test("complete entity identity selects and replaces values")
    func insertionAndReplacement() {
        let alpha = key(entity: "Document", id: [0x01])
        let nested = key(
            entity: "Document",
            id: [0x01],
            partitionPath: ["tenant", "nested"]
        )
        let omega = key(entity: "Document", id: [0xFF])
        var values = ResolvedEntityMap<String>()

        let insertedOmega = values.insert("omega", for: omega)
        let insertedAlpha = values.insert("alpha", for: alpha)
        let insertedNested = values.insert("nested", for: nested)
        let insertedReplacement = values.insert("replacement", for: alpha)

        #expect(insertedOmega)
        #expect(insertedAlpha)
        #expect(insertedNested)
        #expect(!insertedReplacement)

        #expect(values.count == 3)
        #expect(values.value(for: alpha) == "replacement")
        #expect(values.value(for: nested) == "nested")
        #expect(values.value(for: omega) == "omega")
        #expect(
            values.value(
                for: key(entity: "Missing", id: [0x01])
            ) == nil
        )
    }

    private func key(
        entity: String,
        id: ByteString,
        partitionPath: [String] = []
    ) -> ResolvedEntityReference.Key {
        ResolvedEntityReference.Key(
            entity: entity,
            id: id,
            partitionPath: partitionPath
        )
    }
}
