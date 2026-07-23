import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPlannerNullifyOwner {
    #Directory<RelationshipPlannerNullifyOwner>("tests", "relationship-planner", "nullify")

    var id: String = ""

    @Relationship(deleteRule: .nullify)
    var target: DatabaseReference<RelationshipPlannerTarget>? = nil
}
