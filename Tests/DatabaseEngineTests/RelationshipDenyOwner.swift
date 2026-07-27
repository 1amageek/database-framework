#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import DatabaseKit

@Persistable
struct RelationshipDenyOwner {
    #Directory<RelationshipDenyOwner>("test", "relationship_v1", "deny_owners")

    var id: String = ""

    @Relationship(deleteRule: .deny)
    var target: PersistableReference<RelationshipTarget>
}
#endif
#endif
