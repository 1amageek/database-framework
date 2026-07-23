#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipCycleNode {
    #Directory<RelationshipCycleNode>("test", "relationship_v1", "cycle_nodes")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var peer: DatabaseReference<RelationshipCycleNode>
}
#endif
#endif
