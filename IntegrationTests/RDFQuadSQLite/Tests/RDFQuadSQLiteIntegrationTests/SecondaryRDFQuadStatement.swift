import Core
import DatabaseValue
import Graph

@Persistable
struct SecondaryRDFQuadStatement {
    #Directory<SecondaryRDFQuadStatement>("integration", "rdf_quad", "secondary_statements")
    #Index(
        RDFQuadIndexKind<SecondaryRDFQuadStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object,
            graph: \.graph
        ),
        name: "rdf_quad_secondary"
    )

    var id: String = ""
    var subject: DatabaseRDFTerm = .iri("https://example.com/resource")
    var predicate: DatabaseRDFTerm = .iri("https://example.com/predicate")
    var object: DatabaseRDFTerm = .iri("https://example.com/object")
    var graph: DatabaseRDFTerm?
}
