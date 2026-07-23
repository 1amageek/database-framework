import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipPlannerArrayOwner {
    #Directory<RelationshipPlannerArrayOwner>("tests", "relationship-planner", "array")

    var id: String = ""

    @Relationship(deleteRule: .nullify)
    var targets: [DatabaseReference<RelationshipPlannerTarget>] = []
}
