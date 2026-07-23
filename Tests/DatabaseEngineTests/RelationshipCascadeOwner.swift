#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipCascadeOwner {
    #Directory<RelationshipCascadeOwner>("test", "relationship_v1", "cascade_owners")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var target: DatabaseReference<RelationshipTarget>
}
#endif
#endif
