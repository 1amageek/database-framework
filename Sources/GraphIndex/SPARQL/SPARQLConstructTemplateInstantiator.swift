import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import QueryIR

/// Instantiates a CONSTRUCT template directly from one borrowed solution.
struct SPARQLConstructTemplateInstantiator {
    private enum Role {
        case subject
        case predicate
        case object

        var binaryRole: DatabaseRDFTermRole {
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
            try DatabaseRDFQuad(
                subject: subject,
                predicate: predicate,
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
    ) throws -> DatabaseRDFTerm? {
        let resolved: DatabaseRDFTerm
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
            resolved = .iri(value)

        case .literal(let literal):
            isVariableSubstitution = false
            guard case .rdfTerm(let rdfTerm) = try literal
                .toSPARQLFieldValue() else {
                throw SPARQLQueryError.invalidRDFTerm(
                    String(describing: literal)
                )
            }
            resolved = rdfTerm

        case .blankNode(let label):
            isVariableSubstitution = false
            resolved = .blankNode(try blankNodeScope.identifier(for: label))

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
                subject: resolvedSubject,
                predicate: resolvedPredicate,
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
                try DatabaseRDFQuad(
                    subject: resolvedReifier,
                    predicate: .iri(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
                    ),
                    object: .tripleTerm(
                        subject: resolvedSubject,
                        predicate: resolvedPredicate,
                        object: resolvedObject
                    )
                )
            )
            resolved = resolvedReifier
        }

        do {
            try DatabaseRDFTermCodec.validate(
                resolved,
                role: role.binaryRole
            )
        } catch {
            if isVariableSubstitution {
                return nil
            }
            throw SPARQLQueryError.invalidRDFTerm(
                String(describing: resolved)
            )
        }
        return resolved
    }
}
