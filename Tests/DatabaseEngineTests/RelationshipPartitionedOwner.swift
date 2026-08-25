#if !os(WASI)
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipPartitionedOwner {
    #Directory<RelationshipPartitionedOwner>("test", "relationship_v1", "partitioned_owners")

    var id: String = ""

    @Relationship(deleteRule: .deny)
    var target: PersistableReference<RelationshipPartitionedTarget>
}
#endif
