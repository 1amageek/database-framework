public enum DatabaseQueryExecutionError: Error, Sendable, CustomStringConvertible {
    case pageLimitMustBePositive
    case solutionModifierMustBeNonNegative(name: String, value: Int)
    case continuationNotSupported(String)
    case invalidContinuation
    case compositionPlanUnsupported(String)
    case compositionAggregateFailure(String)
    case compositionSnapshotUnavailable(String)
    case compositionSnapshotStale
    case compositionSnapshotExpired
    case compositionSnapshotCorrupted
    case compositionSnapshotLimitExceeded(maximum: UInt8)
    case mutationRequiresMutationOperation
    case featureUnavailable(String)
    #if DATABASE_SERVER_GRAPH_INDEXES
    case unresolvedConstructTerm(String)
    case nonRDFBinding(String)
    case invalidRDFTermRole(String)
    case invalidRDFLiteralDatatype(String)
    case unsupportedRDFLiteral(String)
    case rdfLiteralTooLarge(requiredUTF8Count: UInt64, maximumUTF8Count: UInt64)
    case reifiedTripleRequiresTemplateContext
    case describeVariableRequiresPattern(String)
    case invalidDescribeResource(String)
    #endif

    public var description: String {
        switch self {
        case .pageLimitMustBePositive:
            return "Query page limit must be greater than zero"
        case .solutionModifierMustBeNonNegative(let name, let value):
            return "Query solution modifier \(name) must be non-negative, got \(value)"
        case .continuationNotSupported(let statement):
            return "\(statement) does not accept a continuation"
        case .invalidContinuation:
            return "Query continuation is invalid or no longer readable"
        case .compositionPlanUnsupported(let reason):
            return "Composition query plan is unsupported: \(reason)"
        case .compositionAggregateFailure(let reason):
            return "Composition aggregate failed: \(reason)"
        case .compositionSnapshotUnavailable(let reason):
            return "Composition snapshot is unavailable: \(reason)"
        case .compositionSnapshotStale:
            return "Composition snapshot no longer matches the active schema or Composition generation"
        case .compositionSnapshotExpired:
            return "Composition snapshot has expired"
        case .compositionSnapshotCorrupted:
            return "Composition snapshot storage is corrupted"
        case .compositionSnapshotLimitExceeded(let maximum):
            return "Principal already owns the maximum of \(maximum) active Composition snapshots"
        case .mutationRequiresMutationOperation:
            return "Mutation statements must be sent through mutation.execute"
        case .featureUnavailable(let reason):
            return "Query feature is unavailable: \(reason)"
        #if DATABASE_SERVER_GRAPH_INDEXES
        case .unresolvedConstructTerm(let name):
            return "CONSTRUCT template variable '\(name)' is not bound"
        case .nonRDFBinding(let value):
            return "Graph query binding is not a canonical RDF term: \(value)"
        case .invalidRDFTermRole(let value):
            return "RDF term cannot be used in this statement position: \(value)"
        case .invalidRDFLiteralDatatype(let datatype):
            return "RDF literal has an invalid datatype or annotation: \(datatype)"
        case .unsupportedRDFLiteral(let kind):
            return "Value cannot be represented as an RDF literal: \(kind)"
        case .rdfLiteralTooLarge(let required, let maximum):
            return "RDF literal requires \(required) UTF-8 bytes; maximum is \(maximum)"
        case .reifiedTripleRequiresTemplateContext:
            return "Reified triple syntax requires a graph pattern or CONSTRUCT template context"
        case .describeVariableRequiresPattern(let name):
            return "DESCRIBE variable '\(name)' requires a WHERE pattern"
        case .invalidDescribeResource(let resource):
            return "DESCRIBE resource '\(resource)' cannot be used as an RDF subject"
        #endif
        }
    }
}
