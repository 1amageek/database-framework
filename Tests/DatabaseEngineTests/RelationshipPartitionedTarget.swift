#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue
import DatabaseEngine

@Persistable
struct RelationshipPartitionedTarget {
    #Directory<RelationshipPartitionedTarget>(
        "test",
        "relationship_v1",
        Field<RelationshipPartitionedTarget>(\.tenantID),
        "partitioned_targets",
        layer: .partition
    )

    var id: String = ""
    var tenantID: String
    var name: String
}
#endif
#endif
