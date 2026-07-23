#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipDenyOwner {
    #Directory<RelationshipDenyOwner>("test", "relationship_v1", "deny_owners")

    var id: String = ""

    @Relationship(deleteRule: .deny)
    var target: DatabaseReference<RelationshipTarget>
}
#endif
#endif
