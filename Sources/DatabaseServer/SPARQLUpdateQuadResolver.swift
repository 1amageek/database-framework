import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import QueryIR

struct SPARQLUpdateQuadResolver: Sendable {
    private enum Role {
        case subject
        case predicate
        case object
        case graphName

        var binaryRole: DatabaseRDFTermRole {
            switch self {
            case .subject: .subject
            case .predicate: .predicate
            case .object: .object
            case .graphName: .graphName
            }
        }
    }

    func resolve(
        _ quad: QueryIR.Quad,
        row: VariableBinding?,
        blankNodeScope: SPARQLBlankNodeScope?,
        variablesAllowed: Bool,
        blankNodesAllowed: Bool
    ) throws -> [Graph.RDFQuad] {
        var reifications: [Graph.RDFQuad] = []
        let graph: DatabaseRDFTerm?
        if let graphTerm = quad.graph {
            guard let resolvedGraph = try resolve(
                graphTerm,
                row: row,
                role: .graphName,
                blankNodeScope: blankNodeScope,
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
            blankNodeScope: blankNodeScope,
            variablesAllowed: variablesAllowed,
            blankNodesAllowed: blankNodesAllowed,
            reifications: &reifications
        ), let predicate = try resolve(
            quad.triple.predicate,
            row: row,
            role: .predicate,
            blankNodeScope: blankNodeScope,
            variablesAllowed: variablesAllowed,
            blankNodesAllowed: blankNodesAllowed,
            reifications: &reifications
        ), let object = try resolve(
            quad.triple.object,
            row: row,
            role: .object,
            blankNodeScope: blankNodeScope,
            variablesAllowed: variablesAllowed,
            blankNodesAllowed: blankNodesAllowed,
            reifications: &reifications
        ) else {
            return []
        }

        var result: [Graph.RDFQuad] = []
        result.reserveCapacity(1 + reifications.count)
        result.append(
            Graph.RDFQuad(
                subject: subject,
                predicate: predicate,
                object: object,
                graph: graph
            )
        )
        for reification in reifications {
            result.append(
                Graph.RDFQuad(
                    subject: reification.subject,
                    predicate: reification.predicate,
                    object: reification.object,
                    graph: graph
                )
            )
        }
        return result
    }

    private func resolve(
        _ term: SPARQLTerm,
        row: VariableBinding?,
        role: Role,
        blankNodeScope: SPARQLBlankNodeScope?,
        variablesAllowed: Bool,
        blankNodesAllowed: Bool,
        reifications: inout [Graph.RDFQuad]
    ) throws -> DatabaseRDFTerm? {
        let resolved: DatabaseRDFTerm
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
            resolved = .iri(value)

        case .literal(let literal):
            isVariableSubstitution = false
            if case .blankNode(let label) = literal {
                resolved = try blankNode(
                    label,
                    scope: blankNodeScope,
                    allowed: blankNodesAllowed
                )
            } else {
                let value = try literal.toSPARQLFieldValue()
                guard case .rdfTerm(let rdfTerm) = value else {
                    throw SPARQLUpdateError.invalidRDFTermRole(
                        String(describing: value)
                    )
                }
                resolved = rdfTerm
            }

        case .blankNode(let label):
            isVariableSubstitution = false
            resolved = try blankNode(
                label,
                scope: blankNodeScope,
                allowed: blankNodesAllowed
            )

        case .tripleTerm(let subject, let predicate, let object):
            isVariableSubstitution = false
            guard let subject = try resolve(
                subject,
                row: row,
                role: .subject,
                blankNodeScope: blankNodeScope,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let predicate = try resolve(
                predicate,
                row: row,
                role: .predicate,
                blankNodeScope: blankNodeScope,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let object = try resolve(
                object,
                row: row,
                role: .object,
                blankNodeScope: blankNodeScope,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ) else {
                return nil
            }
            resolved = .tripleTerm(
                subject: subject,
                predicate: predicate,
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
                blankNodeScope: blankNodeScope,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let predicate = try resolve(
                predicate,
                row: row,
                role: .predicate,
                blankNodeScope: blankNodeScope,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let object = try resolve(
                object,
                row: row,
                role: .object,
                blankNodeScope: blankNodeScope,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ), let reifier = try resolve(
                reifier,
                row: row,
                role: .subject,
                blankNodeScope: blankNodeScope,
                variablesAllowed: variablesAllowed,
                blankNodesAllowed: blankNodesAllowed,
                reifications: &reifications
            ) else {
                return nil
            }
            reifications.append(
                Graph.RDFQuad(
                    subject: reifier,
                    predicate: .iri(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
                    ),
                    object: .tripleTerm(
                        subject: subject,
                        predicate: predicate,
                        object: object
                    )
                )
            )
            resolved = reifier
        }

        do {
            try DatabaseRDFTermCodec.validate(
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
                String(describing: resolved)
            )
        }
        return resolved
    }

    private func blankNode(
        _ label: String,
        scope: SPARQLBlankNodeScope?,
        allowed: Bool
    ) throws -> DatabaseRDFTerm {
        guard allowed else {
            throw SPARQLUpdateError.blankNodeNotAllowed(label)
        }
        guard let scope else {
            throw SPARQLUpdateError.blankNodeNotAllowed(label)
        }
        return .blankNode(scope.identifier(for: label))
    }

    private func normalizedVariable(_ name: String) -> String {
        return "?" + name
    }
}
