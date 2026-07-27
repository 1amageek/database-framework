#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipCycleNode {
    #Directory<RelationshipCycleNode>("test", "relationship_v1", "cycle_nodes")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var peer: PersistableReference<RelationshipCycleNode>
}
#endif
#endif
