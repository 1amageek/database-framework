import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPlannerCycleNode {
    #Directory<RelationshipPlannerCycleNode>("tests", "relationship-planner", "cycles")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var parent: DatabaseReference<RelationshipPlannerCycleNode>? = nil
}
