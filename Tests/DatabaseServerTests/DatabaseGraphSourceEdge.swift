import DatabaseKit
import DatabaseTypes

@Persistable
struct DatabaseGraphSourceEdge {
    #Directory<DatabaseGraphSourceEdge>("test", "database-graph-source")

    var id: String = ""
    var source: String = ""
    var label: String = ""
    var target: String = ""
    var graph: String? = nil
    var weight: Double = 0

    #Index(
        .propertyGraph(strategy: .adjacency),
        from: \DatabaseGraphSourceEdge.source,
        edge: \DatabaseGraphSourceEdge.label,
        to: \DatabaseGraphSourceEdge.target,
        graph: \DatabaseGraphSourceEdge.graph,
        storedFields: [\DatabaseGraphSourceEdge.weight],
        name: "source_graph"
    )

    #Index(
        .propertyGraph(strategy: .adjacency),
        from: \DatabaseGraphSourceEdge.source,
        edge: \DatabaseGraphSourceEdge.label,
        to: \DatabaseGraphSourceEdge.target,
        storedFields: [\DatabaseGraphSourceEdge.weight],
        name: "source_graph_default"
    )
}
