#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPartitionedOwner {
    #Directory<RelationshipPartitionedOwner>("test", "relationship_v1", "partitioned_owners")

    var id: String = ""

    @Relationship(deleteRule: .deny)
    var target: DatabaseReference<RelationshipPartitionedTarget>
}
#endif
#endif
