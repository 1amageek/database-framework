import DatabaseKit
import DatabaseTypes

public enum CanonicalFusionExecutionError: Error, Sendable, Equatable {
    case noInputs
    case emptyIdentityField
    case weightCountMismatch(expected: Int, actual: Int)
    case nonFiniteWeight(index: Int)
    case negativeWeight(index: Int)
    case missingIdentity(indexName: String, field: String)
    case duplicateIdentity(indexName: String, identity: FieldValue)
    case inconsistentRows(identity: FieldValue)
    case invalidScoreAnnotation(indexName: String, annotation: String)
    case inconsistentScoreAnnotation(indexName: String)
    case nonFiniteScore(indexName: String)
    case unorderedSourceRequiresScore(indexName: String)
    case scoreOverflow(identity: FieldValue)
    case inputCountOverflow
    case inputWorkMeterMismatch(indexName: String)
}
