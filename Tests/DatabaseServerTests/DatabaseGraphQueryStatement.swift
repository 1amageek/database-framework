import Core
import DatabaseValue
import DatabaseValueCodable
import Graph

@Persistable
struct DatabaseGraphQueryStatement {
    #Directory<DatabaseGraphQueryStatement>(
        "test",
        "database-server-graph-query"
    )

    var id: String = ""
    var subject: DatabaseRDFTerm = .iri("urn:subject")
    var predicate: DatabaseRDFTerm = .iri("urn:predicate")
    var object: DatabaseRDFTerm = .iri("urn:object")
    var graph: DatabaseRDFTerm? = nil

    #Index(
        RDFQuadIndexKind<DatabaseGraphQueryStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            graph: \.graph
        ),
        name: "rdf_quad"
    )
}
