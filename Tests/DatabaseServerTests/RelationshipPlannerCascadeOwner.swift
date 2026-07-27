import DatabaseKit
import DatabaseTypes
import DatabaseKit

@Persistable
struct RelationshipPlannerCascadeOwner {
    #Directory<RelationshipPlannerCascadeOwner>("tests", "relationship-planner", "cascade")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var target: PersistableReference<RelationshipPlannerTarget>? = nil
}
