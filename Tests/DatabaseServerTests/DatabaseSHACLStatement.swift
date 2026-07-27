import DatabaseKit
import DatabaseTypes
import DatabaseKitFoundation

@Persistable
struct DatabaseSHACLStatement {
    #Directory<DatabaseSHACLStatement>("test", "database-server-shacl")

    var id: String
    var subject: RDFTerm
    var predicate: RDFTerm
    var object: RDFTerm
    var graph: RDFTerm

    #Index(
        .rdfDataset,
        from: \DatabaseSHACLStatement.subject,
        edge: \DatabaseSHACLStatement.predicate,
        to: \DatabaseSHACLStatement.object,
        graph: \DatabaseSHACLStatement.graph
    )
}
