#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import DatabaseEngine

@Persistable
struct RelationshipPartitionedTarget {
    #Directory<RelationshipPartitionedTarget>(
        "test",
        "relationship_v1",
        \RelationshipPartitionedTarget.tenantID,
        "partitioned_targets",
        layer: .partition
    )

    var id: String = ""
    var tenantID: String
    var name: String
}
#endif
#endif
