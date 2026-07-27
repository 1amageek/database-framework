#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipCascadeOwner {
    #Directory<RelationshipCascadeOwner>("test", "relationship_v1", "cascade_owners")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var target: PersistableReference<RelationshipTarget>
}
#endif
#endif
