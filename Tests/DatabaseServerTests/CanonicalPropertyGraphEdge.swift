import DatabaseKit

@Persistable
struct CanonicalPropertyGraphEdge {
    #Directory<CanonicalPropertyGraphEdge>(
        "test",
        "canonical-property-graph-edge"
    )

    var id: String
    var source: String
    var label: String
    var target: String
    var weight: Double
}
