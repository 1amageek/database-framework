import DatabaseKit
import DatabaseTypes
import DatabaseKit

@Persistable
struct RelationshipPlannerDenyOwner {
    #Directory<RelationshipPlannerDenyOwner>("tests", "relationship-planner", "deny")

    var id: String = ""

    @Relationship(deleteRule: .deny)
    var target: PersistableReference<RelationshipPlannerTarget>? = nil
}
