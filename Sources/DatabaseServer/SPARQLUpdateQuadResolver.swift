#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import DatabaseKit
import GraphIndex

struct SPARQLUpdateQuadResolver: Sendable {
    private enum Role {
        case subject
        case predicate
        case object
        case graphName

        var binaryRole: RDFTermRole {
            switch self {
            case .subject: .subject
            case .predicate: .predicate
            case .object: .object
            case .graphName: .graphName
            }
        }
    }

    func resolve(
        _ quad: Quad,
        row: VariableBinding?,
        blankNodeResolver: SPARQLUpdateBlankNodeResolver?,
        variablesAllowed: Bool,
        blankNodesAllowed: Bool
    ) throws -> [RDFQuad] {
        var reifications: [RDFQuad] = []
        let graph: RDFTerm?
        if let graphTerm = quad.graph {
            guard let resolvedGraph = try resolve(
                graphTerm,
                row: row,
                role: .graphName,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ) else {
                // A present but unbound graph variable omits the whole quad;
                // it is not the same as an absent graph term (default graph).
                return []
            }
            graph = resolvedGraph
        } else {
            graph = nil
        }
        guard let subject = try resolve(
            quad.triple.subject,
            row: row,
            role: .subject,
            blankNodeResolver: blankNodeResolver,
            variablesAllowed: variablesAllowed,
            blankNodesAllowed: blankNodesAllowed,
            reifications: &reifications
        ), let predicate = try resolve(
            quad.triple.predicate,
            row: row,
            role: .predicate,
            blankNodeResolver: blankNodeResolver,
            variablesAllowed: variablesAllowed,
            blankNodesAllowed: blankNodesAllowed,
            reifications: &reifications
        ), let object = try resolve(
            quad.triple.object,
            row: row,
            role: .object,
            blankNodeResolver: blankNodeResolver,
            variablesAllowed: variablesAllowed,
            blankNodesAllowed: blankNodesAllowed,
            reifications: &reifications
        ) else {
            return []
        }

        var result: [RDFQuad] = []
        result.reserveCapacity(1 + reifications.count)
        result.append(
            try RDFQuad(
                validatingSubject: subject,
                predicate: predicate,
                object: object,
                graph: graph
            )
        )
        for reification in reifications {
            result.append(
                RDFQuad(
                    subject: reification.subject,
                    predicate: reification.predicate,
                    object: reification.object,
                    graph: try graph.map(RDFGraphName.init)
                )
            )
        }
        return result
    }

    private func resolve(
        _ term: SPARQLTerm,
        row: VariableBinding?,
        role: Role,
        blankNodeResolver: SPARQLUpdateBlankNodeResolver?,
        variablesAllowed: Bool,
        blankNodesAllowed: Bool,
        reifications: inout [RDFQuad]
    ) throws -> RDFTerm? {
        let resolved: RDFTerm
        let isVariableSubstitution: Bool
        switch term {
        case .variable(let name):
            isVariableSubstitution = true
            guard variablesAllowed else {
                throw SPARQLUpdateError.variableInGroundData(name)
            }
            guard let value = row?[normalizedVariable(name)] else {
                return nil
            }
            guard case .rdfTerm(let rdfTerm) = value else {
                throw SPARQLUpdateError.nonRDFBinding(
                    variable: name,
                    value: value
                )
            }
            resolved = rdfTerm

        case .iri(let value):
            isVariableSubstitution = false
            resolved = .iri(try RDFIRI(value))

        case .literal(let literal):
            isVariableSubstitution = false
            if case .blankNode(let label) = literal {
                resolved = try blankNode(
                    label,
                    blankNodeResolver: blankNodeResolver,
                    allowed: blankNodesAllowed
                )
            } else {
                let value = try literal.toSPARQLFieldValue()
                guard case .rdfTerm(let rdfTerm) = value else {
                    throw SPARQLUpdateError.invalidRDFTermRole(
                        "RDF term value is required"
                    )
                }
                resolved = rdfTerm
            }

        case .blankNode(let label):
            isVariableSubstitution = false
            resolved = try blankNode(
                label,
                blankNodeResolver: blankNodeResolver,
                allowed: blankNodesAllowed
            )

        case .tripleTerm(let subject, let predicate, let object):
            isVariableSubstitution = false
            guard let subject = try resolve(
                subject,
                row: row,
                role: .subject,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let predicate = try resolve(
                predicate,
                row: row,
                role: .predicate,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let object = try resolve(
                object,
                row: row,
                role: .object,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ) else {
                return nil
            }
            resolved = .tripleTerm(
                subject: try rdfSubject(subject),
                predicate: try rdfPredicate(predicate),
                object: object
            )

        case .reifiedTriple(
            let subject,
            let predicate,
            let object,
            let reifier
        ):
            isVariableSubstitution = false
            guard let subject = try resolve(
                subject,
                row: row,
                role: .subject,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let predicate = try resolve(
                predicate,
                row: row,
                role: .predicate,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let object = try resolve(
                object,
                row: row,
                role: .object,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let reifier = try resolve(
                reifier,
                row: row,
                role: .subject,
                blankNodeResolver: blankNodeResolver,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ) else {
                return nil
            }
            reifications.append(
                RDFQuad(
                    subject: try rdfSubject(reifier),
                    predicate: try RDFPredicateIRI(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
                    ),
                    object: .tripleTerm(
                        subject: try rdfSubject(subject),
                        predicate: try rdfPredicate(predicate),
                        object: object
                    )
                )
            )
            resolved = reifier
        }

        do {
            try RDFTermStorageFormat.validate(
                resolved,
                role: role.binaryRole
            )
        } catch {
            if isVariableSubstitution {
                // SPARQL template instantiation omits a triple whose bound
                // variable is not legal in its RDF term position.
                return nil
            }
            throw SPARQLUpdateError.invalidRDFTermRole(
                resolved.description
            )
        }
        return resolved
    }

    private func blankNode(
        _ label: String,
        blankNodeResolver: SPARQLUpdateBlankNodeResolver?,
        allowed: Bool
    ) throws -> RDFTerm {
        guard allowed else {
            throw SPARQLUpdateError.blankNodeNotAllowed(label)
        }
        guard let blankNodeResolver else {
            throw SPARQLUpdateError.blankNodeNotAllowed(label)
        }
        return .blankNode(
            try RDFBlankNodeIdentifier(blankNodeResolver.identifier(for: label))
        )
    }

    private func rdfSubject(_ term: RDFTerm) throws -> RDFSubject {
        switch term {
        case .iri(let iri):
            return .iri(iri)
        case .blankNode(let identifier):
            return .blankNode(identifier)
        case .literal, .tripleTerm:
            throw SPARQLUpdateError.invalidRDFTermRole(
                term.description
            )
        }
    }

    private func rdfPredicate(_ term: RDFTerm) throws -> RDFPredicateIRI {
        guard case .iri(let iri) = term else {
            throw SPARQLUpdateError.invalidRDFTermRole(
                term.description
            )
        }
        return RDFPredicateIRI(iri)
    }

    private func normalizedVariable(_ name: String) -> String {
        return "?" + name
    }
}
#endif
