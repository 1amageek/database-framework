import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPlannerPartitionedOwner {
    #Directory<RelationshipPlannerPartitionedOwner>(
        "tests",
        "relationship-planner",
        Field<RelationshipPlannerPartitionedOwner>(\.runID),
        "owners"
    )

    var id: String = ""
    var runID: String

    @Relationship(deleteRule: .cascade)
    var target: DatabaseReference<RelationshipPlannerPartitionedTarget>? = nil
}
