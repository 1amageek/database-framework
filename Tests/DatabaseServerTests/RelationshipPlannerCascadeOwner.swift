import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPlannerCascadeOwner {
    #Directory<RelationshipPlannerCascadeOwner>("tests", "relationship-planner", "cascade")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var target: DatabaseReference<RelationshipPlannerTarget>? = nil
}
