import DatabaseKit
import DatabaseTypes

@Persistable
struct CanonicalRDFGraphStatement {
    #Directory<CanonicalRDFGraphStatement>(
        "test",
        "canonical-rdf-graph-statement"
    )

    var id: String
    var subject: RDFTerm
    var predicate: RDFTerm
    var object: RDFTerm
    var graph: RDFTerm?
    var weight: Double

    #Index(
        .rdfDataset,
        from: \CanonicalRDFGraphStatement.subject,
        edge: \CanonicalRDFGraphStatement.predicate,
        to: \CanonicalRDFGraphStatement.object,
        graph: \CanonicalRDFGraphStatement.graph,
        storedFields: [\CanonicalRDFGraphStatement.weight],
        name: "rdf-graph"
    )
}
