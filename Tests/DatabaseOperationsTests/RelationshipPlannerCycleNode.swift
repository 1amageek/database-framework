import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipPlannerCycleNode {
    #Directory<RelationshipPlannerCycleNode>("tests", "relationship-planner", "cycles")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var parent: PersistableReference<RelationshipPlannerCycleNode>? = nil
}
