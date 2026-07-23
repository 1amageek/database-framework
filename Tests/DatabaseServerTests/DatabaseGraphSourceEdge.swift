import Core
import DatabaseValue
import Graph

@Persistable
struct DatabaseGraphSourceEdge {
    #Directory<DatabaseGraphSourceEdge>("test", "database-graph-source")

    var id: String = ""
    var source: String = ""
    var label: String = ""
    var target: String = ""
    var weight: Double = 0

    #Index(GraphIndexKind<DatabaseGraphSourceEdge>(
        from: \.source,
        edge: \.label,
        to: \.target,
        strategy: .tripleStore
    ), storedFields: [\DatabaseGraphSourceEdge.weight], name: "source_graph")
}
