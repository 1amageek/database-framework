import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipPlannerArrayOwner {
    #Directory<RelationshipPlannerArrayOwner>("tests", "relationship-planner", "array")

    var id: String = ""

    @Relationship(deleteRule: .nullify)
    var targets: [PersistableReference<RelationshipPlannerTarget>] = []
}
