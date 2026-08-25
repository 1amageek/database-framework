#if !os(WASI)
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipDenyOwner {
    #Directory<RelationshipDenyOwner>("test", "relationship_v1", "deny_owners")

    var id: String = ""

    @Relationship(deleteRule: .deny)
    var target: PersistableReference<RelationshipTarget>
}
#endif
