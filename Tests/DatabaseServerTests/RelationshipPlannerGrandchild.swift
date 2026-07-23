import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPlannerGrandchild {
    #Directory<RelationshipPlannerGrandchild>("tests", "relationship-planner", "grandchildren")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var owner: DatabaseReference<RelationshipPlannerCascadeOwner>? = nil
}
