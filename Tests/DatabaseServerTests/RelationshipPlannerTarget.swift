import Core
import DatabaseValue

@Persistable
struct RelationshipPlannerTarget {
    #Directory<RelationshipPlannerTarget>("tests", "relationship-planner", "targets")

    var id: String = ""
    var name: String
}
