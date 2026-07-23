import Core
import DatabaseValue
import Graph

@Persistable
struct RDFQuadStatement {
    #Directory<RDFQuadStatement>("integration", "rdf_quad", "statements")
    #Index(
        RDFQuadIndexKind<RDFQuadStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            graph: \.graph
        ),
        name: "rdf_quad"
    )

    var id: String = ""
    var subject: DatabaseRDFTerm = .iri("https://example.com/resource")
    var predicate: DatabaseRDFTerm = .iri("https://example.com/predicate")
    var object: DatabaseRDFTerm = .iri("https://example.com/object")
    var graph: DatabaseRDFTerm?
}
