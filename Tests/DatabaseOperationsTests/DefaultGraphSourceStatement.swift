import DatabaseKit
import DatabaseTypes
import DatabaseKitFoundation

@Persistable
struct DefaultGraphSourceStatement {
    #Directory<DefaultGraphSourceStatement>(
        "test",
        "database-server-default-graph-source"
    )

    var id: String
    var subject: RDFTerm
    var predicate: RDFTerm
    var object: RDFTerm

    #Index(
        .rdfDataset,
        from: \DefaultGraphSourceStatement.subject,
        edge: \DefaultGraphSourceStatement.predicate,
        to: \DefaultGraphSourceStatement.object,
        name: "default_rdf"
    )
}
