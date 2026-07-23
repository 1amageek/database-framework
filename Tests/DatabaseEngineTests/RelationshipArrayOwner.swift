#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipArrayOwner {
    #Directory<RelationshipArrayOwner>("test", "relationship_v1", "array_owners")

    var id: String = ""

    @Relationship(deleteRule: .nullify)
    var targets: [DatabaseReference<RelationshipTarget>] = []
}
#endif
#endif
