#if !os(WASI)
#if FOUNDATION_DB
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
#endif
