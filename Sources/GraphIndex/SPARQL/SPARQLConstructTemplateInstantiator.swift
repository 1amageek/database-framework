import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Instantiates a CONSTRUCT template directly from one borrowed solution.
struct SPARQLConstructTemplateInstantiator {
    private enum Role {
        case subject
        case predicate
        case object

        var semanticRole: RDFTermRole {
            switch self {
            case .subject: .subject
            case .predicate: .predicate
            case .object: .object
            }
        }
    }

    static func append(
        _ pattern: TriplePattern,
        binding: borrowing VariableBinding,
        blankNodeScope: inout SPARQLConstructBlankNodeScope,
        to output: inout DatabaseRetainedRDFGraphBuilder
    ) throws {
        let subject = try resolve(
            pattern.subject,
            binding: binding,
            role: .subject,
            blankNodeScope: &blankNodeScope,
            output: &output
        )
        let predicate = try resolve(
            pattern.predicate,
            binding: binding,
            role: .predicate,
            blankNodeScope: &blankNodeScope,
            output: &output
        )
        let object = try resolve(
            pattern.object,
            binding: binding,
            role: .object,
            blankNodeScope: &blankNodeScope,
            output: &output
        )
        guard let subject, let predicate, let object else { return }
        try output.append(
            RDFQuad(
                subject: try rdfSubject(from: subject),
                predicate: try rdfPredicate(from: predicate),
                object: object
            )
        )
    }

    private static func resolve(
        _ term: SPARQLTerm,
        binding: borrowing VariableBinding,
        role: Role,
        blankNodeScope: inout SPARQLConstructBlankNodeScope,
        output: inout DatabaseRetainedRDFGraphBuilder
    ) throws -> RDFTerm? {
        let resolved: RDFTerm
        let isVariableSubstitution: Bool
        switch term {
        case .variable(let name):
            isVariableSubstitution = true
            guard let value = binding["?\(name)"],
                  case .rdfTerm(let rdfTerm) = value else {
                return nil
            }
            resolved = rdfTerm

        case .iri(let value):
            isVariableSubstitution = false
            resolved = .iri(try RDFIRI(value))

        case .literal(let literal):
            isVariableSubstitution = false
            guard case .rdfTerm(let rdfTerm) = try literal
                .toSPARQLFieldValue() else {
                throw SPARQLQueryError.invalidRDFTerm(
                    literal.description
                )
            }
            resolved = rdfTerm

        case .blankNode(let label):
            isVariableSubstitution = false
            let identifier = try blankNodeScope.identifier(for: label)
            resolved = .blankNode(
                try RDFBlankNodeIdentifier(identifier)
            )

        case .tripleTerm(let subject, let predicate, let object):
            isVariableSubstitution = false
            let resolvedSubject = try resolve(
                subject,
                binding: binding,
                role: .subject,
                blankNodeScope: &blankNodeScope,
                output: &output
            )
            let resolvedPredicate = try resolve(
                predicate,
                binding: binding,
                role: .predicate,
                blankNodeScope: &blankNodeScope,
                output: &output
            )
            let resolvedObject = try resolve(
                object,
                binding: binding,
                role: .object,
                blankNodeScope: &blankNodeScope,
                output: &output
            )
            guard let resolvedSubject,
                  let resolvedPredicate,
                  let resolvedObject else {
                return nil
            }
            resolved = .tripleTerm(
                subject: try rdfSubject(from: resolvedSubject),
                predicate: try rdfPredicate(from: resolvedPredicate),
                object: resolvedObject
            )

        case .reifiedTriple(
            let subject,
            let predicate,
            let object,
            let reifier
        ):
            isVariableSubstitution = false
            let resolvedSubject = try resolve(
                subject,
                binding: binding,
                role: .subject,
                blankNodeScope: &blankNodeScope,
                output: &output
            )
            let resolvedPredicate = try resolve(
                predicate,
                binding: binding,
                role: .predicate,
                blankNodeScope: &blankNodeScope,
                output: &output
            )
            let resolvedObject = try resolve(
                object,
                binding: binding,
                role: .object,
                blankNodeScope: &blankNodeScope,
                output: &output
            )
            let resolvedReifier = try resolve(
                reifier,
                binding: binding,
                role: .subject,
                blankNodeScope: &blankNodeScope,
                output: &output
            )
            guard let resolvedSubject,
                  let resolvedPredicate,
                  let resolvedObject,
                  let resolvedReifier else {
                return nil
            }
            try output.append(
                RDFQuad(
                    subject: try rdfSubject(from: resolvedReifier),
                    predicate: RDFPredicateIRI(
                        try RDFIRI(
                            "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
                        )
                    ),
                    object: .tripleTerm(
                        subject: try rdfSubject(from: resolvedSubject),
                        predicate: try rdfPredicate(from: resolvedPredicate),
                        object: resolvedObject
                    )
                )
            )
            resolved = resolvedReifier
        }

        do {
            try RDFTermValidation.validate(
                resolved,
                role: role.semanticRole
            )
        } catch {
            if isVariableSubstitution {
                return nil
            }
            throw SPARQLQueryError.invalidRDFTerm(
                resolved.description
            )
        }
        return resolved
    }

    private static func rdfSubject(
        from term: RDFTerm
    ) throws -> RDFSubject {
        switch term {
        case .iri(let iri):
            return .iri(iri)
        case .blankNode(let identifier):
            return .blankNode(identifier)
        case .literal, .tripleTerm:
            throw SPARQLQueryError.invalidRDFTerm(
                term.description
            )
        }
    }

    private static func rdfPredicate(
        from term: RDFTerm
    ) throws -> RDFPredicateIRI {
        guard case .iri(let iri) = term else {
            throw SPARQLQueryError.invalidRDFTerm(
                term.description
            )
        }
        return RDFPredicateIRI(iri)
    }
}
