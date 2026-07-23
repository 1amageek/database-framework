import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPlannerDenyOwner {
    #Directory<RelationshipPlannerDenyOwner>("tests", "relationship-planner", "deny")

    var id: String = ""

    @Relationship(deleteRule: .deny)
    var target: DatabaseReference<RelationshipPlannerTarget>? = nil
}
