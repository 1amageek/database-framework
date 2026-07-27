import DatabaseKit
import DatabaseTypes
import DatabaseKitFoundation

@Persistable
struct DatabaseGraphQueryStatement {
    #Directory<DatabaseGraphQueryStatement>(
        "test",
        "database-server-graph-query"
    )

    var id: String
    var subject: RDFTerm
    var predicate: RDFTerm
    var object: RDFTerm
    var graph: RDFTerm?

    #Index(
        .rdfDataset,
        from: \DatabaseGraphQueryStatement.subject,
        edge: \DatabaseGraphQueryStatement.predicate,
        to: \DatabaseGraphQueryStatement.object,
        graph: \DatabaseGraphQueryStatement.graph,
        name: "rdf_quad"
    )
}
