import Core
import DatabaseValue
import DatabaseValueCodable
import Graph

@Persistable
struct DefaultGraphSourceStatement {
    #Directory<DefaultGraphSourceStatement>(
        "test",
        "database-server-default-graph-source"
    )

    var id: String = ""
    var subject: DatabaseRDFTerm = .iri("urn:subject")
    var predicate: DatabaseRDFTerm = .iri("urn:predicate")
    var object: DatabaseRDFTerm = .iri("urn:object")

    #Index(
        RDFQuadIndexKind<DefaultGraphSourceStatement>(
            subject: \.subject,
            predicate: \.predicate,
            object: \.object
        ),
        name: "default_rdf"
    )
}
