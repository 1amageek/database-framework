#if !os(WASI)
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipArrayOwner {
    #Directory<RelationshipArrayOwner>("test", "relationship_v1", "array_owners")

    var id: String = ""

    @Relationship(deleteRule: .nullify)
    var targets: [PersistableReference<RelationshipTarget>] = []
}
#endif
