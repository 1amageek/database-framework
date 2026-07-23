import DatabaseValue
import DatabaseWire

public enum DatabaseSHACLDataSourceError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case entityNotFound(String)
    case indexNotFound(entity: String, index: String)
    case indexIsNotRDFDataset(entity: String, index: String)
    case graphNotCovered(entity: String, index: String)
    case invalidGraphName(DatabaseRDFTerm)
    case invalidPartition(entity: String, reason: String)
    case recordEntityMismatch(expected: String, actual: String)
    case recordPartitionMismatch(RecordIdentity)
    case recordNotFound(RecordIdentity)
    case recordSubjectMissing(record: RecordIdentity, field: String)
    case unsupportedEntailment(SHACLExecuteOperation.Entailment)

    public var description: String {
        switch self {
        case .entityNotFound(let entity):
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
        case .recordEntityMismatch(let expected, let actual):
            return "SHACL focus record belongs to '\(actual)', expected '\(expected)'"
        case .recordPartitionMismatch(let identity):
            return "SHACL focus record is outside the selected data partition: \(identity)"
        case .recordNotFound(let identity):
            return "SHACL focus record was not found: \(identity)"
        case .recordSubjectMissing(let identity, let field):
            return "SHACL focus record \(identity) has no RDF subject in field '\(field)'"
        case .unsupportedEntailment(let entailment):
            return "SHACL entailment is not implemented completely: \(entailment)"
        }
    }
}
