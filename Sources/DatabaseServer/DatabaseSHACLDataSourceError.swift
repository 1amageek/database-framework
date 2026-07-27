import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

public enum DatabaseSHACLDataSourceError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case schemaEntityNotFound(String)
    case indexNotFound(entity: String, index: String)
    case indexIsNotRDFDataset(entity: String, index: String)
    case graphNotCovered(entity: String, index: String)
    case invalidGraphName(RDFTerm)
    case invalidPartition(entity: String, reason: String)
    case focusEntityMismatch(expected: String, actual: String)
    case focusPartitionMismatch(EntityReference)
    case focusEntityNotFound(EntityReference)
    case focusSubjectMissing(entity: EntityReference, field: String)
    case unsupportedEntailment(SHACLExecuteOperation.Entailment)

    public var description: String {
        switch self {
        case .schemaEntityNotFound(let entity):
            return "SHACL data entity was not found: \(entity)"
        case .indexNotFound(let entity, let index):
            return "SHACL RDF index '\(index)' was not found on entity '\(entity)'"
        case .indexIsNotRDFDataset(let entity, let index):
            return "SHACL index '\(index)' on entity '\(entity)' is not an RDF dataset index"
        case .graphNotCovered(let entity, let index):
            return "SHACL data graph is not covered by RDF index '\(entity).\(index)'"
        case .invalidGraphName(let graph):
            return "SHACL named graph is not a valid RDF graph name: \(graph)"
        case .invalidPartition(let entity, let reason):
            return "SHACL partition for entity '\(entity)' is invalid: \(reason)"
        case .focusEntityMismatch(let expected, let actual):
            return "SHACL focus entity belongs to '\(actual)', expected '\(expected)'"
        case .focusPartitionMismatch(let identity):
            return "SHACL focus entity is outside the selected data partition: \(identity)"
        case .focusEntityNotFound(let identity):
            return "SHACL focus entity was not found: \(identity)"
        case .focusSubjectMissing(let identity, let field):
            return "SHACL focus entity \(identity) has no RDF subject in field '\(field)'"
        case .unsupportedEntailment(let entailment):
            return "SHACL entailment is not implemented completely: \(entailment)"
        }
    }
}
