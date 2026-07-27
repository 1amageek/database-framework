import DatabaseKit
import DatabaseTypes
import DatabaseKit

@Persistable
struct RelationshipPlannerGrandchild {
    #Directory<RelationshipPlannerGrandchild>("tests", "relationship-planner", "grandchildren")

    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var owner: PersistableReference<RelationshipPlannerCascadeOwner>? = nil
}
