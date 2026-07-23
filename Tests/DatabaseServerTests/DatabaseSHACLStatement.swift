import Core
import DatabaseValue
import DatabaseValueCodable
import Graph

@Persistable
struct DatabaseSHACLStatement {
    #Directory<DatabaseSHACLStatement>("test", "database-server-shacl")

    var id: String = ""
    var subject: DatabaseRDFTerm = .iri("urn:subject")
    var predicate: DatabaseRDFTerm = .iri("urn:predicate")
    var object: DatabaseRDFTerm = .iri("urn:object")
    var graph: DatabaseRDFTerm = .iri("urn:graph")

    #Index(RDFQuadIndexKind<DatabaseSHACLStatement>(
        subject: \.subject,
        predicate: \.predicate,
        object: \.object,
        graph: \.graph
    ))
}
