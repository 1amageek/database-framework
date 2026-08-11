import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipPlannerPartitionedTarget {
    #Directory<RelationshipPlannerPartitionedTarget>(
        "tests",
        "relationship-planner",
        \RelationshipPlannerPartitionedTarget.runID,
        "targets"
    )

    var id: String = ""
    var runID: String
}
