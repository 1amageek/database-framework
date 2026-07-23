import Core
import DatabaseValue

@Persistable
struct RelationshipPlannerPartitionedTarget {
    #Directory<RelationshipPlannerPartitionedTarget>(
        "tests",
        "relationship-planner",
        Field<RelationshipPlannerPartitionedTarget>(\.runID),
        "targets"
    )

    var id: String = ""
    var runID: String
}
