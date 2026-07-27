import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipPlannerTarget {
    #Directory<RelationshipPlannerTarget>("tests", "relationship-planner", "targets")

    var id: String = ""
    var name: String
}
