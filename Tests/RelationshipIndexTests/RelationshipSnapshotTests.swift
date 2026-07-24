import Core
import DatabaseValue
import Relationship
@testable import RelationshipIndex
import Testing

@Persistable
private struct SnapshotTarget {
    var name: String
}

@Persistable
private struct OtherSnapshotTarget {
    var name: String
}

@Persistable
private struct SnapshotOwner {
    @Relationship
    var optionalTarget: DatabaseReference<SnapshotTarget>?

    @Relationship
    var targets: [DatabaseReference<SnapshotTarget>]
}

@Suite("Relationship snapshot")
struct RelationshipSnapshotTests {
    @Test("An unloaded to-one relationship fails explicitly")
    func unloadedToOneFailsExplicitly() {
        let snapshot = RelationshipSnapshot(
            item: SnapshotOwner(optionalTarget: nil, targets: [])
        )

        #expect(
            throws: RelationshipSnapshotError.relationNotLoaded(
                owner: SnapshotOwner.persistableType,
                field: "optionalTarget"
            )
        ) {
            try snapshot.ref(\.optionalTarget)
        }
    }

    @Test("A loaded absent optional relationship remains distinguishable")
    func loadedAbsentToOneReturnsNil() throws {
        let snapshot = RelationshipSnapshot(
            item: SnapshotOwner(optionalTarget: nil, targets: [])
        ).with(\.optionalTarget, loadedAs: nil)

        #expect(try snapshot.ref(\.optionalTarget) == nil)
    }

    @Test("A loaded to-many relationship preserves its array storage")
    func loadedToManyPreservesArrayStorage() throws {
        let related = [
            SnapshotTarget(name: "first"),
            SnapshotTarget(name: "second"),
        ]
        let snapshot = RelationshipSnapshot(
            item: SnapshotOwner(optionalTarget: nil, targets: [])
        ).with(\.targets, loadedAs: related)

        let loaded = try snapshot.refs(\.targets)
        #expect(loaded.map(\.name) == ["first", "second"])

        let sharesStorage = related.withUnsafeBufferPointer { originalBuffer in
            loaded.withUnsafeBufferPointer { loadedBuffer in
                originalBuffer.baseAddress == loadedBuffer.baseAddress
            }
        }
        #expect(sharesStorage)
    }

    @Test("A mismatched loaded entity type fails explicitly")
    func mismatchedLoadedTypeFailsExplicitly() {
        let snapshot = RelationshipSnapshot(
            item: SnapshotOwner(optionalTarget: nil, targets: []),
            loadedRelationships: [
                "optionalTarget": .toOne(OtherSnapshotTarget(name: "wrong"))
            ]
        )

        #expect(
            throws: RelationshipSnapshotError.relatedTypeMismatch(
                owner: SnapshotOwner.persistableType,
                field: "optionalTarget",
                expected: SnapshotTarget.persistableType,
                actual: OtherSnapshotTarget.persistableType
            )
        ) {
            try snapshot.ref(\.optionalTarget)
        }
    }
}
