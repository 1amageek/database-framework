// OntologyIndexError.swift
// OntologyIndex - Error types for ontology index operations

/// Errors specific to ontology index operations.
public enum OntologyIndexError: Error, CustomStringConvertible {
    /// The type does not conform to OWLClassEntity.
    ///
    /// OWLTripleIndexKind requires the target type to be annotated with `@OWLClass`.
    case notOWLClassEntity(typeName: String)

    public var description: String {
        switch self {
        case .notOWLClassEntity(let typeName):
            return "OntologyIndexError: '\(typeName)' does not conform to OWLClassEntity. OWLTripleIndexKind requires @OWLClass annotation."
        }
    }
}
