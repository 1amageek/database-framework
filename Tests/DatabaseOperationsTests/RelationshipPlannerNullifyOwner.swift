import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipPlannerNullifyOwner {
    #Directory<RelationshipPlannerNullifyOwner>("tests", "relationship-planner", "nullify")

    var id: String = ""

    @Relationship(deleteRule: .nullify)
    var target: PersistableReference<RelationshipPlannerTarget>? = nil
}
