import DatabaseTypes
import Testing

@testable import DatabaseEngine

@Suite("Entity reference set")
struct EntityReferenceSetTests {
    @Test("membership remains correct across insertion and removal")
    func insertionAndRemoval() throws {
        let alpha = try reference(entity: "Document", id: "alpha")
        let nested = try reference(
            entity: "Document",
            id: "alpha",
            partitions: FieldObject([
                (key: "tenant", value: FieldValue.string("nested"))
            ])
        )
        let omega = try reference(entity: "Document", id: "omega")
        var references = EntityReferenceSet(minimumCapacity: 3)

        let insertedOmega = references.insert(omega)
        let insertedAlpha = references.insert(alpha)
        let insertedNested = references.insert(nested)
        let insertedDuplicate = references.insert(alpha)
        #expect(insertedOmega)
        #expect(insertedAlpha)
        #expect(insertedNested)
        #expect(!insertedDuplicate)
        #expect(references.count == 3)
        #expect(references.contains(alpha))
        #expect(references.contains(nested))
        #expect(references.contains(omega))

        let removedNested = references.remove(nested)
        let removedMissingNested = references.remove(nested)
        #expect(removedNested)
        #expect(!removedMissingNested)
        #expect(!references.contains(nested))
        #expect(references.count == 2)

        references.removeAll(keepingCapacity: true)
        #expect(references.count == 0)
    }

    private func reference(
        entity: String,
        id: String,
        partitions: FieldObject = FieldObject()
    ) throws -> EntityReference {
        try EntityReference(
            entity: entity,
            id: .string(id),
            partitions: partitions
        )
    }
}
