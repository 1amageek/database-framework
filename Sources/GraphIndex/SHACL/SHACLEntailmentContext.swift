import DatabaseTypes

/// Graph entailment operations required by SHACL validation.
public protocol SHACLEntailmentContext: Sendable {
    func subClasses(of classIRI: String) -> Set<String>
    func equivalentClasses(of classIRI: String) -> Set<String>
    func instances(of classIRI: String) throws -> Set<RDFTerm>
    func contains(_ node: RDFTerm, in classIRI: String) -> Bool
    func subsumes(superClass: String, subClass: String) -> Bool
    func subProperties(of propertyIRI: String) -> Set<String>
}
