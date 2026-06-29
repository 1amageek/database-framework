import Testing
import StorageKit
@testable import RelationshipIndex

@Suite("Relationship index entry subspace")
struct RelationshipIndexEntrySubspaceTests {
    @Test("entry metadata key-space cannot collide with tuple-encoded index data")
    func entryMetadataDoesNotCollideWithIndexData() throws {
        let indexSubspace = Subspace("relationship", "index")
        let formerMetadataValue = "__relationship_index_entries"
        let indexDataKey = indexSubspace.pack(Tuple(formerMetadataValue, "owner-1"))
        let entrySubspace = RelationshipIndexEntrySubspace.make(from: indexSubspace)
        let entryKey = entrySubspace.pack(Tuple("owner-1"))

        #expect(indexDataKey != entryKey)
        #expect(indexSubspace.contains(entryKey))
        #expect(!indexSubspace.subspace(formerMetadataValue).contains(entryKey))
    }
}
