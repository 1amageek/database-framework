import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipPlannerPartitionedOwner {
    #Directory<RelationshipPlannerPartitionedOwner>(
        "tests",
        "relationship-planner",
        \RelationshipPlannerPartitionedOwner.runID,
        "owners"
    )

    var id: String = ""
    var runID: String

    @Relationship(deleteRule: .cascade)
    var target: PersistableReference<RelationshipPlannerPartitionedTarget>? = nil
}
