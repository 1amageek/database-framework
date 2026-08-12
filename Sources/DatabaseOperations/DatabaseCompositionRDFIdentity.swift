import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
#if DATABASE_OPERATIONS_MULTIPLE_BASES
import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Qualifies every RDF blank-node identity by its source Base before values
/// cross the Composition boundary.
package enum DatabaseCompositionRDFIdentity {
    package static func qualifyBlankNodes(
        in row: DatabaseEngine.QueryRow,
        baseID: Base.ID
    ) throws -> DatabaseEngine.QueryRow {
        DatabaseEngine.QueryRow(
            fields: try row.fields.mapValues {
                try qualifyBlankNodes(in: $0, baseID: baseID)
            },
            annotations: try row.annotations.mapValues {
                try qualifyBlankNodes(in: $0, baseID: baseID)
            },
            version: row.version
        )
    }

    package static func qualifyBlankNodes(
        in quad: RDFQuad,
        baseID: Base.ID
    ) throws -> RDFQuad {
        RDFQuad(
            subject: try qualifyBlankNodes(in: quad.subject, baseID: baseID),
            predicate: quad.predicate,
            object: try qualifyBlankNodes(in: quad.object, baseID: baseID),
            graph: try quad.graph.map {
                RDFGraphName(
                    try qualifyBlankNodes(in: $0.subject, baseID: baseID)
                )
            }
        )
    }

    package static func qualifyBlankNodes(
        in value: FieldValue,
        baseID: Base.ID
    ) throws -> FieldValue {
        switch value {
        case .rdfTerm(let term):
            return .rdfTerm(try qualifyBlankNodes(in: term, baseID: baseID))
        case .array(let values):
            return .array(
                try values.map {
                    try qualifyBlankNodes(in: $0, baseID: baseID)
                }
            )
        case .object(let object):
            return .object(
                try FieldObject(
                    try object.fields.map { field in
                        (
                            key: field.key,
                            value: try qualifyBlankNodes(
                                in: field.value,
                                baseID: baseID
                            )
                        )
                    }
                )
            )
        default:
            return value
        }
    }

    package static func qualifyBlankNodes(
        in term: RDFTerm,
        baseID: Base.ID
    ) throws -> RDFTerm {
        switch term {
        case .iri, .literal:
            return term
        case .blankNode(let identifier):
            return .blankNode(
                try qualified(identifier, baseID: baseID)
            )
        case .tripleTerm(let subject, let predicate, let object):
            return .tripleTerm(
                subject: try qualifyBlankNodes(
                    in: subject,
                    baseID: baseID
                ),
                predicate: predicate,
                object: try qualifyBlankNodes(in: object, baseID: baseID)
            )
        }
    }

    package static func qualifyBlankNodes(
        in subject: RDFSubject,
        baseID: Base.ID
    ) throws -> RDFSubject {
        switch subject {
        case .iri:
            return subject
        case .blankNode(let identifier):
            return .blankNode(
                try qualified(identifier, baseID: baseID)
            )
        }
    }

    private static func qualified(
        _ identifier: RDFBlankNodeIdentifier,
        baseID: Base.ID
    ) throws -> RDFBlankNodeIdentifier {
        let base = baseID.value
        return try RDFBlankNodeIdentifier(
            "base:\(base.utf8.count):\(base):\(identifier.rawValue)"
        )
    }
}

#endif
