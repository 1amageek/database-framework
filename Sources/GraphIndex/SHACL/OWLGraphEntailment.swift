import DatabaseKit
import DatabaseTypes
import OntologyIndex

/// Adapts OWL graph classification to SHACL entailment operations.
public struct OWLGraphEntailment: SHACLEntailmentContext, Sendable {
    private let reasoner: OWLReasoner

    public init(reasoner: OWLReasoner) {
        self.reasoner = reasoner
    }

    public func subClasses(of classIRI: String) -> Set<String> {
        reasoner.subClasses(of: classIRI)
    }

    public func equivalentClasses(of classIRI: String) -> Set<String> {
        reasoner.equivalentClasses(of: classIRI)
    }

    public func instances(of classIRI: String) throws -> Set<RDFTerm> {
        var result = Set<RDFTerm>()
        for identifier in reasoner.instances(of: .named(classIRI)) {
            result.insert(.iri(try RDFIRI(identifier)))
        }
        return result
    }

    public func contains(_ node: RDFTerm, in classIRI: String) -> Bool {
        guard case .iri(let iri) = node else { return false }
        return reasoner.isInstanceOf(
            individual: iri.rawValue,
            classExpr: .named(classIRI)
        ).value
    }

    public func subsumes(
        superClass: String,
        subClass: String
    ) -> Bool {
        reasoner.subsumes(
            superClass: .named(superClass),
            subClass: .named(subClass)
        ).value
    }

    public func subProperties(of propertyIRI: String) -> Set<String> {
        reasoner.subProperties(of: propertyIRI)
    }
}
